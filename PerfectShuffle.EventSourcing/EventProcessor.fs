namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing

type private Msg<'TEvent, 'TState> =
| Persist of seq<EventWithMetadata<'TEvent>>
| ReadState of AsyncReplyChannel<'TState>
| Exit

type IEventProcessor<'TState, 'TEvent> =
  abstract member Persist : seq<EventWithMetadata<'TEvent>> -> unit
  abstract member ApplicationState : unit -> Async<'TState>

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
      
      let persist events =
        async {
        // The sequence might have side effects which we don't want to repeat
        let events = Seq.cache events
        readModel.Apply(events)
        for evt in events do
          do! store.Save(evt)      
        }
      
      let rec loop() =
        async {
        let! msg = inbox.Receive()
        match msg with        
        | Persist events ->
            do! persist events
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
    member __.Persist (events:seq<EventWithMetadata<'TEvent>>) =
      agent.Post <| Persist events

    member __.ApplicationState () =
      async {
        let! state = agent.PostAndAsyncReply(fun replyChannel -> ReadState(replyChannel))
        return state
      }