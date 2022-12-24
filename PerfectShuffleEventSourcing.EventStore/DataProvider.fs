namespace PerfectShuffle.EventSourcing.EventStore

open EventStore.ClientAPI.Exceptions
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control

module EventStoreStorage =
  open System
  open System.Data
  open PerfectShuffle.EventSourcing.Store
  open EventStore
  open EventStore.ClientAPI
  open System.Text
  
  type EventStoreDataProvider(connection:IEventStoreConnection) =
    
    let eventsPerCall = 1000
    
    interface IAllEventReader with

      member __.GetAllEvents(fromCommitVersion: int64): AsyncSeq<RawEvent> = 
        
        
        let rec events startIndex =
          asyncSeq {
          let! slice = connection.ReadStreamEventsForwardAsync("$all", startIndex, eventsPerCall, false) |> Async.AwaitTask
          
          for event in slice.Events do
            yield event
          
          if not slice.IsEndOfStream then
            yield! events (startIndex + int64 eventsPerCall)
          }
        
        events fromCommitVersion
        |> AsyncSeq.map (fun resolvedEvent ->
           
          let metadata : RecordedMetadata =
            {
            TypeName = resolvedEvent.Event.EventType     
            CommitVersion = resolvedEvent.
            StreamName = string
            StreamVersion =
            DeduplicationId = 
            EventStamp =
            CommitStamp =              
            }
          let r : RawEvent =
            {
              Payload = resolvedEvent.Event.Data
              Metadata = metadata
            }
          )
          

    interface IStreamDataProvider with
      
      member __.GetStreamEvents(streamName: string) (fromStreamVersion: int64): AsyncSeq<RawEvent> = 
        failwith "notimpl"
      
      member __.SaveEvents(streamName: string) (concurrencyCheck: WriteConcurrencyCheck) (evts: EventToRecord []): Async<PerfectShuffle.EventSourcing.Store.WriteResult> =
        
        let events =
          evts
          |> Seq.map (fun evt ->
            
            let isJson = true
            let data = evt.SerializedEventToRecord.Payload
            let metadata = [||]
            EventData(evt.Metadata.DeduplicationId, evt.SerializedEventToRecord.TypeName, isJson, data, metadata)
            )
        
        let expectedVersion =
          match concurrencyCheck with
          /// This disables the optimistic concurrency check.
          | Any -> int64 ExpectedVersion.Any
          /// this specifies the expectation that target stream does not yet exist.
          | NoStream -> int64 ExpectedVersion.NoStream
          /// this specifies the expectation that the target stream has been explicitly created, but does not yet have any user events written in it.
          | EmptyStream _ -> int64 ExpectedVersion.EmptyStream
          /// Any other integer value	The event number that you expect the stream to currently be at.
          | NewEventNumber n -> n

        
        async {
          try
            let! result = connection.AppendToStreamAsync(streamName, expectedVersion, events) |> Async.AwaitTask
            return WriteResult.Ok (WriteSuccess.StreamVersion result.NextExpectedVersion) //TODO: is this right? off by one error?
          with e ->
            match e with
            | :? WrongExpectedVersionException ->
              return WriteResult.Error (WriteFailure.ConcurrencyCheckFailed)
            | e ->
              return WriteResult.Error (WriteFailure.WriteException e)
        }

      member __.FirstVersion = 1L

  

