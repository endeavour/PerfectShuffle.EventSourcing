namespace PerfectShuffle.EventSourcing

open Store
open FSharp.Control

type Stream<'event>(firstVersion : int64, streamName:string, serializer : Serialization.IEventSerializer<'event>, dataProvider:IDataProvider) =
  
  let eventsFrom version =
    dataProvider.GetStreamEvents streamName version
    |> AsyncSeq.map (fun rawEvent ->
          let event = serializer.Deserialize({TypeName = rawEvent.Metadata.TypeName; Payload = rawEvent.Payload})                        
          { RecordedEvent = event; Metadata = rawEvent.Metadata}
        )

  let commit concurrencyCheck (evts:EventToRecord<'event>[]) =
    let rawEvents =
      evts
      |> Array.map(fun evt ->
        let serializedEvent = evt.EventToRecord |> serializer.Serialize         
        let rawEvent : EventToRecord = { SerializedEventToRecord = serializedEvent; Metadata = evt.Metadata}
        rawEvent
        ) 
      
    dataProvider.SaveEvents streamName concurrencyCheck rawEvents

  interface IStream<'event> with
    member __.FirstVersion = firstVersion
    member __.EventsFrom version = eventsFrom version
    member __.Save evts concurrencyCheck = commit concurrencyCheck evts