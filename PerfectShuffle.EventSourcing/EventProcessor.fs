namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing

type private Msg<'TEvent, 'TState> =
| Persist of seq<EventWithMetadata<'TEvent>>
| ReadState of AsyncReplyChannel<'TState>
| Exit

type IEventProcessor<'TEvent, 'TState> =
  abstract member Persist : seq<EventWithMetadata<'TEvent>> -> unit
  abstract member ApplicationState : unit -> Async<'TState>

type EventProcessor<'TEvent, 'TState> (readModel:IReadModel<'TEvent,'TState>, store : EventStore) = 
  let agent =
    MailboxProcessor<_>.Start(fun inbox ->
      let rec loop() =
        async {
        let! msg = inbox.Receive()
        match msg with
        | Persist events ->          
            readModel.Apply(events |> Seq.map (fun x -> x.Event))
            store.SaveAll(events)
            return! loop()
        | ReadState replyChannel ->          
            let! state = readModel.CurrentStateAsync()
            replyChannel.Reply state
            return! loop()
        | Exit -> ()        
        }
      loop()
      )

  interface IEventProcessor<'TEvent, 'TState> with
    /// Applies a batch of events and persists them to disk
    member __.Persist (events:seq<EventWithMetadata<'TEvent>>) =
      agent.Post <| Persist events

    member __.ApplicationState () =
      async {
        let! state = agent.PostAndAsyncReply(fun replyChannel -> ReadState(replyChannel))
        return state
      }