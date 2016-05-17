namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store

type IStreamFactory =
  abstract member CreateStream<'event> : string -> IStream<'event>

type EventProcessorFactory<'event, 'state> = IStream<'event> -> IEventProcessor<'event, 'state>

type StreamEvent =
| StreamCreated of name:string

/// Manages a collection of streams for a particular event type
type StreamManager<'event,'state>(streamFactory:IStreamFactory, eventProcessorFactory : EventProcessorFactory<'event, 'state>) =
  
  // Create a stream which tracks creation of other streams with a specific name
  let streamOfStreamsName =
    sprintf "%s-%s" "streams" typeof<'event>.FullName
  let streamOfStreams = streamFactory.CreateStream<StreamEvent> streamOfStreamsName

  let streamOfStreamsReadModel =

    let createEventProcessorForStream name =
      let hashedName = hash name
      let stream = streamFactory.CreateStream<'event> name
      let eventProcessor = eventProcessorFactory stream
      eventProcessor

    // TODO: Instead of Lazy we could do something more clever with caching so infrequently accessed streams
    // drop out of memory until they are next required
    let apply state (evt:EventWithMetadata<StreamEvent>) =
      match evt.Event with
      | StreamCreated name -> state |> Map.add name (lazy createEventProcessorForStream name)
    ConflictFreeReadModel<_,_>(Map.empty, apply, streamOfStreams.FirstVersion)

  let streamOfStreamsEventProcessor =
    EventProcessor<_,_>(streamOfStreamsReadModel, streamOfStreams) :> IEventProcessor<_,_>

  // TODO: Change eventprocessor so that is doesn't always enforce concurrency checking
  // in this case the events are commutative, associative and idempotent so the readmodel shouldn't care which order
  // we add StreamCreated events or need to apply concurrency checks when writing to the stream
  let createdStream name =
    async {    
    let evts =
      [|
        StreamCreated name
      |] |> Array.map EventWithMetadata<_>.Wrap
    let! streams = streamOfStreamsEventProcessor.ExtendedState()    
    let batch : Batch<_> = {StartVersion = streams.NextExpectedStreamVersion; Events = evts}
    return! streamOfStreamsEventProcessor.Persist batch
    }

  let rec getEventProcessor name =
    async {
    let! streams = streamOfStreamsEventProcessor.State()
    return!
      match streams.TryFind name with
      | None ->
        async {
        let newStream = streamFactory.CreateStream<'event> name
        let! result = createdStream name
        match result with
        | Choice1Of2 _ -> return! getEventProcessor name
        | Choice2Of2 err -> return failwith <| sprintf "Unable to obtain event stream for %s" name
        }
      | Some eventProcessor -> async {return eventProcessor.Value}
    }

  member __.Streams() =
    async {
    let! state = streamOfStreamsEventProcessor.State()
    return state |> Map.toSeq |> Seq.map fst |> Seq.toArray
    }

  member __.GetEventProcessor (name:string) = getEventProcessor name