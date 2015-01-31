﻿namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing

type private Msg<'TEvent, 'TState> =
| Persist of EventWithMetadata<'TEvent>[] * AsyncReplyChannel<ReadModelState<'TState>>
| ReadState of AsyncReplyChannel<ReadModelState<'TState>>
| Exit

type IEventProcessor<'TState, 'TEvent> =
  abstract member Persist : EventWithMetadata<'TEvent>[] -> Async<ReadModelState<'TState>>
  abstract member ExtendedState : unit -> Async<ReadModelState<'TState>>
  abstract member State : unit -> Async<'TState>

type EventProcessor<'TState, 'TEvent> (readModel:IReadModel<'TState, 'TEvent>, store : Store.EventRepository<'TEvent>) = 
  
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
      
      let rec persistEvents events =
        async {
            let! currentState = readModel.CurrentStateAsync()
            let nextEventNumber = currentState.NextEventNumber
            let concurrency = Store.WriteConcurrencyCheck.NewEventNumber(nextEventNumber)
            let! r = store.Save(events, concurrency)
            
            match r with
            | Store.WriteResult.Success ->
              readModel.Apply(nextEventNumber, events)
              let! result = readModel.CurrentStateAsync()
              return Choice1Of2(result)
            | Store.WriteResult.ConcurrencyCheckFailed -> return! persistEvents events
            | Store.WriteResult.WriteException e -> return Choice2Of2(e)
        }

      let rec loop() =
        async {
        
        let! msg = inbox.Receive()
        match msg with        
        | Persist (events, replyChannel) ->

            let! r = persistEvents events
            match r with
            | Choice1Of2 state -> replyChannel.Reply state
            | Choice2Of2 e -> raise e
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
    member this.Persist (events:EventWithMetadata<'TEvent>[]) =
      agent.PostAndAsyncReply(fun replyChannel -> Persist(events, replyChannel))

    member this.ExtendedState () =
      agent.PostAndAsyncReply(fun replyChannel -> ReadState(replyChannel))

    member this.State () =
      async {
        let! result = (this :> IEventProcessor<'TState, 'TEvent>).ExtendedState()
        return result.State
      }