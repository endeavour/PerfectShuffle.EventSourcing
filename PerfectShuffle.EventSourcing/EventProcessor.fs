namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control

type PersistenceFailure =
| WriteFailure of WriteFailure
| AggregateException of exn

type Batch<'event> =
  {
    StartVersion : int64
    Events : EventToRecord<'event>[]
  }

type private Msg<'event, 'state> =
| ReadLatestFromStore
| Persist of Batch<'event> * AsyncReplyChannel<Choice<AggregateState<'state>,PersistenceFailure>>
| ReadState of AsyncReplyChannel<AggregateState<'state>>
| Exit

type IEventProcessor<'event, 'state> =
  abstract member Persist : Batch<'event> -> Async<Choice<AggregateState<'state>, PersistenceFailure>>
  abstract member ExtendedState : unit -> Async<AggregateState<'state>>
  abstract member State : unit -> Async<'state>

// TODO: This current reads from the position specified by the aggregate which doesn't change for nonsequenced
// streams so it's inefficient. Either split this class into two (Sequenced/Unsequenced) or factor out the functionality
// for determining where to read from and keep a poiter in the event processor for unordered streams instead of in the aggregate
type EventProcessor<'event, 'state> (aggregate:IAggregate<'state, 'event>, stream:Store.IStream<'event>) = 
  
  let readEventsFromStore() =
    asyncSeq {
      let! currentState = aggregate.CurrentStateAsync()
      let nextUnreadEvent = currentState.NextExpectedStreamVersion
      yield! stream.EventsFrom nextUnreadEvent      
    }
 
  let agent =
    MailboxProcessor<_>.Start(fun inbox ->
      
      let rec persistEvents (batch:Batch<_>) : Async<Choice<AggregateState<'state>, PersistenceFailure>>  =
        async {
            // TODO: refactor this
            let concurrency =
              match aggregate.IsOrdered with
              | true -> Store.WriteConcurrencyCheck.NewEventNumber(batch.StartVersion - 1L)
              | false -> Store.Any
            let! r = stream.Save batch.Events concurrency
            
            match r with
            | Choice1Of2 (StreamVersion n) ->
              let! currentState = aggregate.CurrentStateAsync()
              let newEvents = stream.EventsFrom currentState.NextExpectedStreamVersion
              let! aggregateResult =
                newEvents
                |> AsyncSeq.fold (fun acc x -> aggregate.Apply x.RecordedEvent x.Metadata.StreamVersion :: acc) [] 
              let aggregateResults =
                aggregateResult
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
              
              match aggregateResults with
              | Choice1Of2 (()) ->
                let! result = aggregate.CurrentStateAsync()
                return Choice1Of2(result)
              | Choice2Of2 es ->
                let aggregateException = System.AggregateException es
                return Choice2Of2 (AggregateException aggregateException)
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
            let! state = aggregate.CurrentStateAsync()            
            replyChannel.Reply state
            return! loop()
        | ReadLatestFromStore ->
          printfn "Reading latest"
          do! readEventsFromStore() |> AsyncSeq.iter (fun item ->
          printfn "Applying an item"
          match aggregate.Apply item.RecordedEvent item.Metadata.StreamVersion with
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