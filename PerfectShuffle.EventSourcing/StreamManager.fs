namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open System

type IStreamFactory =
  abstract member CreateStream<'event> : string -> IStream<'event>

type EventProcessorFactory<'event, 'state> = IStream<'event> -> IEventProcessor<'event, 'state>

type StreamEvent =
| StreamCreated of name:string

/// Manages a collection of streams for a particular event type
type StreamManager<'event,'state>(streamFactory:IStreamFactory, eventProcessorFactory : EventProcessorFactory<'event, 'state>, onError) =
  let sha256 = System.Security.Cryptography.SHA256.Create()

  // Create a stream which tracks creation of other streams with a specific name
  let streamOfStreamsName =
    sprintf "%s-%s" "streams" typeof<'event>.FullName
  let streamOfStreams = streamFactory.CreateStream<StreamEvent> streamOfStreamsName

  let streamOfStreamsaggregate : ConflictFreeaggregate<_,StreamEvent> =

    let createEventProcessorForStream name =
      let hashedName = hash name
      let stream = streamFactory.CreateStream<'event> name
      let eventProcessor = eventProcessorFactory stream
      eventProcessor

    // TODO: Instead of Lazy we could do something more clever with caching so infrequently accessed streams
    // drop out of memory until they are next required
    let apply state (evt:StreamEvent) =
      match evt with
      | StreamCreated name -> state |> Map.add name (lazy createEventProcessorForStream name)
    ConflictFreeaggregate<_,_>(Map.empty, apply, streamOfStreams.FirstVersion)

  let streamOfStreamsEventProcessor =
    new EventProcessor<_, _>(streamOfStreamsaggregate, streamOfStreams, onError)
 

  // TODO: Change eventprocessor so that it doesn't always enforce concurrency checking
  // in this case the events are commutative, associative and idempotent so the aggregate shouldn't care which order
  // we add StreamCreated events or need to apply concurrency checks when writing to the stream
  let createdStream (name:string) =
    let streamOfStreamsEventProcessor = streamOfStreamsEventProcessor :> IEventProcessor<_,_>
    async {
    
    let createMetadata() : EventToRecordMetadata =
      
      let deduplicationId =
        name
        |> System.Text.Encoding.UTF8.GetBytes
        |> sha256.ComputeHash
        |> Array.truncate 16
        |> Guid

      {DeduplicationId = Guid.NewGuid(); EventStamp = DateTime.UtcNow}
    let evts =
      [|
        {EventToRecord = StreamCreated name; Metadata = createMetadata()}
      |]
    let! streams = streamOfStreamsEventProcessor.ExtendedState()    
    let batch : Batch<_> = {StartVersion = streams.NextExpectedStreamVersion; Events = evts}
    return! streamOfStreamsEventProcessor.Persist batch
    }

  let rec getEventProcessor name =
    let streamOfStreamsEventProcessor = streamOfStreamsEventProcessor :> IEventProcessor<_,_>
    async {
    let! streams = streamOfStreamsEventProcessor.State()
    return!
      match streams.TryFind name with
      | None ->
        async {
        let newStream = streamFactory.CreateStream<'event> name
        let! result = createdStream name
        match result with
        | Result.Ok _ -> return! getEventProcessor name
        | Result.Error err -> return failwith <| sprintf "Unable to obtain event stream for %s" name
        }
      | Some eventProcessor -> async {return eventProcessor.Value}
    }

  member __.Streams() =
    let streamOfStreamsEventProcessor = streamOfStreamsEventProcessor :> IEventProcessor<_,_>
    async {
    let! state = streamOfStreamsEventProcessor.State()
    return state |> Map.toSeq |> Seq.map fst |> Seq.toArray
    }

  member __.GetEventProcessor (name:string) = getEventProcessor name

  interface IDisposable with
    member __.Dispose() =
      (streamOfStreamsEventProcessor :> IDisposable).Dispose()