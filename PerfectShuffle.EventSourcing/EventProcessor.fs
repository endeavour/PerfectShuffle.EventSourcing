namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control

type PersistenceFailure =
| WriteFailure of WriteFailure
| ReadModelException of exn

type private Msg<'TEvent, 'TState> =
| ReadLatestFromStore
| Persist of Batch<'TEvent> * AsyncReplyChannel<Choice<ReadModelState<'TState>,PersistenceFailure>>
| ReadState of AsyncReplyChannel<ReadModelState<'TState>>
| Exit

type IEventProcessor<'TState, 'TEvent> =
  abstract member Persist : Batch<'TEvent> -> Async<Choice<ReadModelState<'TState>, PersistenceFailure>>
  abstract member ExtendedState : unit -> Async<ReadModelState<'TState>>
  abstract member State : unit -> Async<'TState>

type EventProcessor<'TState, 'TEvent> (readModel:IReadModel<'TState, 'TEvent>, store : Store.IEventRepository<'TEvent>) = 
  
  let asyncAgent = Agent<_>.Start(fun inbox ->
    let rec loop() =
      async {
      let! msg = inbox.Receive()
      return! loop()
      }
    loop()
  )

  let readEventsFromStore() =
    asyncSeq {
      let! currentState = readModel.CurrentStateAsync()
      let nextUnreadEvent = currentState.NextExpectedStreamVersion
      yield! store.EventsFrom nextUnreadEvent      
    }
 
  let agent =
    MailboxProcessor<_>.Start(fun inbox ->
      
      let rec persistEvents (batch:Batch<_>) : Async<Choice<ReadModelState<'TState>, PersistenceFailure>>  =
        async {
            let concurrency = Store.WriteConcurrencyCheck.NewEventNumber(batch.StartVersion - 1)
            let! r = store.Save batch.Events concurrency
            
            match r with
            | Choice1Of2 writeSucess ->
              let readModelResult = readModel.Apply batch
              match readModelResult with
              | Choice1Of2 (()) ->
                let! result = readModel.CurrentStateAsync()
                return Choice1Of2(result)
              | Choice2Of2 e ->
                return Choice2Of2 (ReadModelException e)
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
          match readModel.Apply item with
          | Choice1Of2 _ -> printfn "Applied batch %d" item.StartVersion
          | Choice2Of2 e -> printfn "%A" e)  
          printfn "Up to date"
          return! loop()        
        | Exit -> ()        
        }
      loop()
      )
  
  do agent.Post ReadLatestFromStore
    
  // TODO: Move this retry policy out of here and make it more powerful (exponential backoff, triggers etc)
  do
    let rec timer() =
      async {
        agent.Post ReadLatestFromStore
        do! Async.Sleep 1000
        return! timer()
      }
    timer() |> Async.Start

  interface IEventProcessor<'TState, 'TEvent> with
    /// Applies a batch of events and persists them to disk
    member this.Persist (batch:Batch<'TEvent>)=
      agent.PostAndAsyncReply(fun replyChannel -> Persist(batch, replyChannel))

    member this.ExtendedState () =
      agent.PostAndAsyncReply(fun replyChannel -> ReadState(replyChannel))

    member this.State () =
      async {
        let! result = (this :> IEventProcessor<'TState, 'TEvent>).ExtendedState()
        return result.State
      }