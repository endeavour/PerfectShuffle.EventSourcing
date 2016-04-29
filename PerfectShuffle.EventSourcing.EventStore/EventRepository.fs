namespace PerfectShuffle.EventSourcing.EventStore

open System
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open EventStore.ClientAPI

module EventStore =
  open System.Net
  
  /// Creates and opens an EventStore connection.
  let Connect (endPoint:IPEndPoint) =   
      let conn = EventStoreConnection.Create(endPoint)      
      conn.ConnectAsync().Wait()
      conn

/// Implementation of an event repository that connects to Greg Young's EventStore (www.geteventstore.com)
type EventRepository<'TEvent>(conn:IEventStoreConnection, streamId:string, serializer : Serialization.IEventSerializer<'TEvent>) =
    
  let unpack (e:ResolvedEvent) =      
    serializer.Deserialize({TypeName = e.Event.EventType; Payload = e.Event.Data})
    
//  let load () = async {
//    let! eventsSlice = conn.ReadStreamEventsForwardAsync(streamId, 0, Int32.MaxValue, false) |> Async.AwaitTask
//    return
//      eventsSlice.Events |> Seq.map unpack
//  }

  let commit (concurrencyCheck:WriteConcurrencyCheck) (evts:EventWithMetadata<'TEvent>[]) = async {
      let eventsData =
        evts |> Array.map (fun e ->
          let serializedEvent = serializer.Serialize e
          let metaData = [||] : byte array
          new EventData(Guid.NewGuid(), serializedEvent.TypeName, true, serializedEvent.Payload, metaData)          
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
        | Choice.Choice1Of2(writeResult) -> Success
        | Choice.Choice2Of2(exn) ->
          match getInnerException exn with
          | :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException -> ConcurrencyCheckFailed
          | e -> WriteException(e)            
      return r
  }

  let eventsObservable =

    {new System.IObservable<_> with
      member __.Subscribe(observer) =

        let f = System.Action<_, ResolvedEvent>(fun _ evt ->
            let unpacked = unpack evt
            let eventNumber = evt.OriginalEventNumber
            let evt = {EventNumber = eventNumber; Event = unpacked}
            observer.OnNext evt              
          )
      
        let subscription = conn.SubscribeToStreamFrom(streamId, EventStore.ClientAPI.StreamCheckpoint.StreamStart, false, f)

        {new IDisposable with member __.Dispose() = subscription.Stop()}                
      }      

  interface IEventRepository<'TEvent> with
    member __.Events = eventsObservable
    member __.Save evts concurrencyCheck = commit concurrencyCheck evts