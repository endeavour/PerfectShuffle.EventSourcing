namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store

type PersistenceFailure =
| WriteFailure of WriteFailure
| ReadModelException of exn

type private Msg<'TEvent, 'TState> =
| Persist of Changeset<'TEvent> * AsyncReplyChannel<Choice<ReadModelState<'TState>,PersistenceFailure>>
| ReadState of AsyncReplyChannel<ReadModelState<'TState>>
| Exit

type IEventProcessor<'TState, 'TEvent> =
  abstract member Persist : Changeset<'TEvent> -> Async<Choice<ReadModelState<'TState>, PersistenceFailure>>
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
 
  let agent =
    MailboxProcessor<_>.Start(fun inbox ->
      
      let rec persistEvents (changeset:Changeset<_>) : Async<Choice<ReadModelState<'TState>, PersistenceFailure>>  =
        async {
            let concurrency = Store.WriteConcurrencyCheck.NewEventNumber(changeset.StreamVersion - 1)
            let! r = store.Save changeset.Events concurrency
            
            match r with
            | Choice1Of2 (()) ->
              let readModelResult = readModel.Apply changeset
              match readModelResult with
              | Choice1Of2 (()) ->
                let! result = readModel.CurrentStateAsync()
                return Choice1Of2(result)
              | Choice2Of2 e ->
                return Choice2Of2 (ReadModelException e)
            | Choice2Of2 reason ->
              return Choice2Of2 (WriteFailure reason)
        }

      let rec loop() =
        async {
        
        let! msg = inbox.Receive()
        match msg with        
        | Persist (changeset, replyChannel) ->
            let! r = persistEvents changeset
            replyChannel.Reply r
            return! loop()
        | ReadState replyChannel ->          
            let! state = readModel.CurrentStateAsync()
            replyChannel.Reply state
            return! loop()
        | Exit -> ()        
        }
      loop()
      )

  interface IEventProcessor<'TState, 'TEvent> with
    /// Applies a batch of events and persists them to disk
    member this.Persist (changeset:Changeset<'TEvent>)=
      agent.PostAndAsyncReply(fun replyChannel -> Persist(changeset, replyChannel))

    member this.ExtendedState () =
      agent.PostAndAsyncReply(fun replyChannel -> ReadState(replyChannel))

    member this.State () =
      async {
        let! result = (this :> IEventProcessor<'TState, 'TEvent>).ExtendedState()
        return result.State
      }