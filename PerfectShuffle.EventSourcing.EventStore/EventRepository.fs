namespace PerfectShuffle.EventSourcing.EventStore

open System
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open EventStore.ClientAPI
open FSharp.Control

module EventStore =
  open System.Net
  
  /// Creates and opens an EventStore connection.
  let Connect (endPoint:IPEndPoint) =   
      let conn = EventStoreConnection.Create(endPoint)      
      conn.ConnectAsync().Wait()
      conn

/// Implementation of an event repository that connects to Greg Young's EventStore (www.geteventstore.com)
type EventRepository<'event>(conn:IEventStoreConnection, streamId:string, serializer : Serialization.IEventSerializer<'event>) =
    
  let unpack (e:ResolvedEvent) =      
    serializer.Deserialize({TypeName = e.Event.EventType; Payload = e.Event.Data})
    
  let commit (concurrencyCheck:WriteConcurrencyCheck) (evts:EventWithMetadata<'event>[]) = async {
      let eventsData =
        evts |> Array.map (fun e ->
          let serializedEvent = serializer.Serialize e
          let metaData = [||] : byte array
          new EventData(e.Id, serializedEvent.TypeName, true, serializedEvent.Payload, metaData)          
        )

      let expectedVersion =
        match concurrencyCheck with
        | Any -> ExpectedVersion.Any
        | NoStream -> ExpectedVersion.NoStream
        | EmptyStream -> ExpectedVersion.EmptyStream
        | NewEventNumber n -> n - 1

      let! result =
        conn.AppendToStreamAsync(streamId, expectedVersion, eventsData)
        |> Async.AwaitTask
        |> Async.Catch
        
      let rec getInnerException (exn:Exception) =
        match exn with
        | :? System.AggregateException as e ->
          if e.InnerExceptions.Count = 1
            then getInnerException e.InnerException
            else exn
        | e -> e
        
      let r =
        match result with
        | Choice.Choice1Of2(writeResult) -> Choice1Of2 (StreamVersion (writeResult.NextExpectedVersion))
        | Choice.Choice2Of2(exn) ->
          match getInnerException exn with
          | :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException -> WriteFailure.ConcurrencyCheckFailed |> Choice2Of2
          | e -> WriteFailure.WriteException e |> Choice2Of2          
      return r
  }

  let eventsFrom n =
    let batchSize = 1000
    let rec eventsFromAux n =
      asyncSeq {
      let! events = conn.ReadStreamEventsForwardAsync(streamId, n, batchSize, false) |> Async.AwaitTask
      for evt in events.Events do
        let unpacked = unpack evt
        let eventNumber = evt.OriginalEventNumber
        let evt = { StartVersion = evt.OriginalEventNumber; Events = [|unpacked|]}
        yield evt
        if not events.IsEndOfStream then
          yield! eventsFromAux (n + batchSize)
      }
    eventsFromAux n

  interface IStream<'event> with
    member __.FirstVersion = 0
    member __.EventsFrom version = eventsFrom version
    member __.Save evts concurrencyCheck = commit concurrencyCheck evts