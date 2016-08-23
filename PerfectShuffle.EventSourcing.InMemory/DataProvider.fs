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

        let rec loop (streams:Map<string, Stream<RawEvent>>) (commitVersion:int64) =
          async {
          let! msg = inbox.Receive()
          match msg with
          | GetStreamEvents (streamName, fromStreamVersion, replyChannel) ->
            printfn "STREAM %s" streamName
            let result =
              match streams.TryFind streamName with
              | None -> AsyncSeq.empty
              | Some evts ->
                asyncSeq {
                for evt in evts do
                  yield evt
                }
            replyChannel.Reply result
            return! loop streams commitVersion

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
              let updatedStream = rawEvents (int64 stream.Length) |> Seq.fold conj stream
              let newStreams =
                streams.Add(streamName, updatedStream)
              replyChannel.Reply (WriteResult.Choice1Of2(WriteSuccess.StreamVersion (int64 updatedStream.Length)))
              return! loop newStreams (commitVersion + int64 events.Length)
            | IsEmptyAndBindZero n | WriteConcurrencyCheck.NewEventNumber n ->
              let stream = streams |> Map.tryFind streamName
              match stream with
              | Some s when s.Length - 1 = int n ->                
                let updatedStream = rawEvents (int64 s.Length) |> Seq.fold conj s
                let newStreams =
                  streams.Add(streamName, updatedStream)
                replyChannel.Reply (WriteResult.Choice1Of2(WriteSuccess.StreamVersion (int64 updatedStream.Length)))
                return! loop newStreams (commitVersion + int64 events.Length)      
              | None ->
                let updatedStream = rawEvents (int64 0L) |> Seq.fold conj (Stream.Empty())
                let newStreams =
                  streams.Add(streamName, updatedStream)
                replyChannel.Reply (WriteResult.Choice1Of2(WriteSuccess.StreamVersion (int64 updatedStream.Length)))
                return! loop newStreams (commitVersion + int64 events.Length)                      
              | _ ->
                replyChannel.Reply (WriteResult.Choice2Of2(WriteFailure.ConcurrencyCheckFailed))
                return! loop streams commitVersion
            | WriteConcurrencyCheck.NoStream ->
              let stream = streams |> Map.tryFind streamName
              match stream with
              | None ->                
                let updatedStream = rawEvents 0L |> Seq.fold conj (Stream.Empty())
                let newStreams =
                  streams.Add(streamName, updatedStream)
                replyChannel.Reply (WriteResult.Choice1Of2(WriteSuccess.StreamVersion (int64 updatedStream.Length)))
                return! loop newStreams (commitVersion + int64 events.Length)      
              | _ ->
                replyChannel.Reply (WriteResult.Choice2Of2(WriteFailure.ConcurrencyCheckFailed))
                return! loop streams commitVersion
            | WriteConcurrencyCheck.EmptyStream -> failwith "assertion failed"
              
            
          } 

        let streams = Map.empty<string, Stream<RawEvent>>

        loop streams 0L
        )

  
    interface IStreamDataProvider with

      member __.GetStreamEvents(streamName: string) (fromStreamVersion: int64): AsyncSeq<RawEvent> = 
        agent.PostAndReply(fun replyChannel -> GetStreamEvents(streamName, fromStreamVersion, replyChannel))

      member __.SaveEvents(streamName: string) (concurrencyCheck: WriteConcurrencyCheck) (evts: EventToRecord []): Async<WriteResult> = 
        agent.PostAndAsyncReply(fun replyChannel -> SaveEvents(streamName, concurrencyCheck, evts, replyChannel))

      member __.FirstVersion = 0L