namespace PerfectShuffle.EventSourcing

open System
open FSharp.Control
open Store
open Serialization

type ProjectionBuilder(allEventsReader:IAllEventReader, serializer:IEventSerializer<obj>) =
  
  let read startCommitVersion batchSize (maxTimeToBuffer:TimeSpan) : IObservable<RecordedEvent<obj>> =

    allEventsReader.GetAllEvents startCommitVersion
    |> AsyncSeq.map (fun rawEvent ->
      let serializedEvent = {TypeName = rawEvent.Metadata.TypeName; Payload = rawEvent.Payload}
      let evt = serializer.Deserialize serializedEvent
      let recordedEvent = {RecordedEvent = evt; Metadata = rawEvent.Metadata}
      recordedEvent
      )
    |> AsyncSeq.toObservable

  member __.EventStream startCommitVersion batchSize maxTimeToBuffer = read startCommitVersion batchSize maxTimeToBuffer
