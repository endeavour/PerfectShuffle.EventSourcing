namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control

type PersistenceFailure =
| WriteFailure of WriteFailure
| ReadModelException of exn

type private Msg<'event, 'state> =
| ReadLatestFromStore
| Persist of Batch<'event> * AsyncReplyChannel<Choice<ReadModelState<'state>,PersistenceFailure>>
| ReadState of AsyncReplyChannel<ReadModelState<'state>>
| Exit

type IEventProcessor<'event, 'state> =
  abstract member Persist : Batch<'event> -> Async<Choice<ReadModelState<'state>, PersistenceFailure>>
  abstract member ExtendedState : unit -> Async<ReadModelState<'state>>
  abstract member State : unit -> Async<'state>

// TODO: This current reads from the position specified by the readmodel which doesn't change for nonsequenced
// streams so it's inefficient. Either split this class into two (Sequenced/Unsequenced) or factor out the functionality
// for determining where to read from and keep a poiter in the event processor for unordered streams instead of in the readmodel
type EventProcessor<'event, 'state> (readModel:IReadModel<'state, 'event>, stream:Store.IStream<'event>) = 
  
  let readEventsFromStore() =
    asyncSeq {
      let! currentState = readModel.CurrentStateAsync()
      let nextUnreadEvent = currentState.NextExpectedStreamVersion
      yield! stream.EventsFrom nextUnreadEvent      
    }
 
  let agent =
    MailboxProcessor<_>.Start(fun inbox ->
      
      let rec persistEvents (batch:Batch<_>) : Async<Choice<ReadModelState<'state>, PersistenceFailure>>  =
        async {
            // TODO: refactor this
            let concurrency =
              match readModel.IsOrdered with
              | true -> Store.WriteConcurrencyCheck.NewEventNumber(batch.StartVersion - 1L)
              | false -> Store.Any
            let! r = stream.Save batch.Events concurrency
            
            match r with
            | Choice1Of2 (StreamVersion n) ->
              let! currentState = readModel.CurrentStateAsync()
              let newEvents = stream.EventsFrom currentState.NextExpectedStreamVersion
              let! readModelResult =
                newEvents
                |> AsyncSeq.fold (fun acc x -> readModel.Apply x.RecordedEvent x.Metadata.StreamVersion :: acc) [] 
              let readModelResults =
                readModelResult
                |> Seq.fold (fun (acc:Choice<unit, exn list>) (x:Choice<unit, exn>) ->
                  match acc with
                  | Choice1Of2 () ->
                    match x with
                    | Choice1Of2 () -> Choice1Of2 ()
                    | Choice2Of2 e -> Choice2Of2 [e]
                  | Choice2Of2 es ->
                    match x with
                    | Choice1Of2 () -> Choice2Of2 es
                    | Choice2Of2 e -> Choice2Of2 (e::es)) (Choice1Of2 ())
              
              match readModelResults with
              | Choice1Of2 (()) ->
                let! result = readModel.CurrentStateAsync()
                return Choice1Of2(result)
              | Choice2Of2 es ->
                let aggregateException = System.AggregateException es
                return Choice2Of2 (ReadModelException aggregateException)
            | Choice2Of2 reason ->
              match reason with
              | WriteFailure.ConcurrencyCheckFailed ->
                inbox.Post ReadLatestFromStore
              | WriteFailure.NoItems | WriteFailure.WriteException _ -> ()
              return Choice2Of2 (WriteFailure reason)
        }

      let rec loop() =
        async {
        
        let! msg = inbox.Receive()
        match msg with        
        | Persist (batch, replyChannel) ->
            let! r = persistEvents batch
            replyChannel.Reply r
            return! loop()
        | ReadState replyChannel ->     
            let! state = readModel.CurrentStateAsync()            
            replyChannel.Reply state
            return! loop()
        | ReadLatestFromStore ->
          printfn "Reading latest"
          do! readEventsFromStore() |> AsyncSeq.iter (fun item ->
          printfn "Applying an item"
          match readModel.Apply item.RecordedEvent item.Metadata.StreamVersion with
          | Choice1Of2 _ -> printfn "Applied event %d" item.Metadata.StreamVersion
          | Choice2Of2 e -> printfn "%A" e)  
          printfn "Up to date"
          return! loop()        
        | Exit -> ()        
        }
      loop()
      )
  
  do agent.Post ReadLatestFromStore

  interface IEventProcessor<'event, 'state> with
    /// Applies a batch of events and persists them to disk
    member this.Persist (batch:Batch<'event>)=
      agent.PostAndAsyncReply(fun replyChannel -> Persist(batch, replyChannel))

    member this.ExtendedState () =
      agent.Post ReadLatestFromStore
      agent.PostAndAsyncReply(fun replyChannel -> ReadState(replyChannel))

    member this.State () =
      async {
        let! result = (this :> IEventProcessor<'event, 'state>).ExtendedState()
        return result.State
      }