namespace PerfectShuffle.EventSourcing.InMemory

open System
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control
open FSharpx.Collections

module InMemory =

  type internal Stream<'event> = FSharpx.Collections.PersistentVector<'event>

  type internal Msg =
  | SaveEvents of streamName:string * concurrencyCheck: WriteConcurrencyCheck * evts: EventToRecord [] * replyChannel:AsyncReplyChannel<WriteResult>
  | GetStreamEvents of streamName: string * fromStreamVersion: int64 * replyChannel:AsyncReplyChannel<AsyncSeq<RawEvent>>
  | GetAllEvents of fromCommitVersion:int64 * replyChannel:AsyncReplyChannel<AsyncSeq<RawEvent>>

  /// Implementation of an non-persistent event repository that keeps everything in memory
  /// for the purpose of unit testing
  type InMemoryDataProvider() =

    let withDefault value option = defaultArg option value

    let (|IsEmptyAndBindZero|_|) arg =
      match arg with
      | WriteConcurrencyCheck.EmptyStream -> Some 0L
      | _ -> None

    let agent =
      MailboxProcessor<_>.Start(fun inbox ->
        
        let conj x y = PersistentVector.conj y x
        let evtStream = Event<RawEvent>()

        let rec loop (streams:Map<string, Stream<RawEvent>>) (allEvents:Stream<RawEvent>) (commitVersion:int64) =
          async {
          let! msg = inbox.Receive()
          match msg with
          | GetAllEvents (fromCommitVersion, replyChannel) ->
            let existingEvents =
              asyncSeq {
              for evt in allEvents do
                yield evt
              }
            let futureEvents =
              evtStream.Publish
              |> AsyncSeq.ofObservableBuffered
            let allEventStream =
              AsyncSeq.append existingEvents futureEvents
              |> AsyncSeq.skipWhile (fun x -> x.Metadata.CommitVersion < fromCommitVersion)
            replyChannel.Reply (allEventStream)
            return! loop streams allEvents commitVersion
          | GetStreamEvents (streamName, fromStreamVersion, replyChannel) ->
            let result =
              match streams.TryFind streamName with
              | None -> AsyncSeq.empty
              | Some evts ->
                evts |> Seq.skipNoFail (int fromStreamVersion) |> AsyncSeq.ofSeq
            replyChannel.Reply result
            return! loop streams allEvents commitVersion

          | SaveEvents (streamName, concurrencyCheck, events, replyChannel) ->    
            
            let rawEvents currentStreamPosition =
              events
              |> Array.mapi (fun i evt ->
                {
                  Payload = evt.SerializedEventToRecord.Payload
                  Metadata =
                    {
                      TypeName = evt.SerializedEventToRecord.TypeName
                      CommitVersion = commitVersion + int64 i
                      StreamName = streamName
                      StreamVersion = currentStreamPosition + int64 i
                      DeduplicationId = Guid.NewGuid()
                      EventStamp = DateTime.UtcNow
                      CommitStamp = DateTime.UtcNow            
                    }
                })
                      
            match concurrencyCheck with
            | WriteConcurrencyCheck.Any ->              
              let stream = streams |> Map.tryFind streamName |> withDefault (Stream.Empty())
              let rawEvents = rawEvents (int64 stream.Length)
              let updatedAllStream = rawEvents |> Seq.fold conj allEvents
              let updatedStream = rawEvents |> Seq.fold conj stream
              let newStreams =
                streams.Add(streamName, updatedStream)
              replyChannel.Reply (WriteResult.Choice1Of2(WriteSuccess.StreamVersion (int64 updatedStream.Length)))
              for evt in rawEvents do
                evtStream.Trigger evt
              return! loop newStreams updatedAllStream (commitVersion + int64 events.Length)
            | IsEmptyAndBindZero n | WriteConcurrencyCheck.NewEventNumber n ->
              let stream = streams |> Map.tryFind streamName
              match stream with
              | Some s when s.Length - 1 = int n ->                
                let rawEvents = rawEvents (int64 s.Length)
                let updatedStream = rawEvents |> Seq.fold conj s
                let updatedAllStream = rawEvents |> Seq.fold conj allEvents

                let newStreams =
                  streams.Add(streamName, updatedStream)
                replyChannel.Reply (WriteResult.Choice1Of2(WriteSuccess.StreamVersion (int64 updatedStream.Length)))
                for evt in rawEvents do
                  evtStream.Trigger evt
                return! loop newStreams updatedAllStream (commitVersion + int64 events.Length)      
              | None ->
                let rawEvents = rawEvents (int64 0L)
                let updatedStream = rawEvents |> Seq.fold conj (Stream.Empty())
                let updatedAllStream = rawEvents |> Seq.fold conj allEvents

                let newStreams =
                  streams.Add(streamName, updatedStream)
                replyChannel.Reply (WriteResult.Choice1Of2(WriteSuccess.StreamVersion (int64 updatedStream.Length)))
                for evt in rawEvents do
                  evtStream.Trigger evt
                return! loop newStreams updatedAllStream (commitVersion + int64 events.Length)                      
              | _ ->
                replyChannel.Reply (WriteResult.Choice2Of2(WriteFailure.ConcurrencyCheckFailed))
                return! loop streams allEvents commitVersion
            | WriteConcurrencyCheck.NoStream ->
              let stream = streams |> Map.tryFind streamName
              match stream with
              | None ->                
                let rawEvents = rawEvents 0L
                let updatedStream = rawEvents |> Seq.fold conj (Stream.Empty())
                let updatedAllStream = rawEvents |> Seq.fold conj allEvents

                let newStreams =
                  streams.Add(streamName, updatedStream)
                replyChannel.Reply (WriteResult.Choice1Of2(WriteSuccess.StreamVersion (int64 updatedStream.Length)))
                for evt in rawEvents do
                  evtStream.Trigger evt
                return! loop newStreams updatedAllStream (commitVersion + int64 events.Length)      
              | _ ->
                replyChannel.Reply (WriteResult.Choice2Of2(WriteFailure.ConcurrencyCheckFailed))
                return! loop streams allEvents commitVersion
            | WriteConcurrencyCheck.EmptyStream -> failwith "assertion failed"
              
            
          } 

        let streams = Map.empty<string, Stream<RawEvent>>

        loop streams (Stream.Empty()) 0L
        )

    interface IAllEventReader with

      member __.GetAllEvents(fromCommitVersion: int64): AsyncSeq<RawEvent> = 
        agent.PostAndReply(fun replyChannel -> GetAllEvents (fromCommitVersion, replyChannel))
  
    interface IStreamDataProvider with

      member __.GetStreamEvents(streamName: string) (fromStreamVersion: int64): AsyncSeq<RawEvent> = 
        agent.PostAndReply(fun replyChannel -> GetStreamEvents(streamName, fromStreamVersion, replyChannel))

      member __.SaveEvents(streamName: string) (concurrencyCheck: WriteConcurrencyCheck) (evts: EventToRecord []): Async<WriteResult> = 
        agent.PostAndAsyncReply(fun replyChannel -> SaveEvents(streamName, concurrencyCheck, evts, replyChannel))

      member __.FirstVersion = 0L