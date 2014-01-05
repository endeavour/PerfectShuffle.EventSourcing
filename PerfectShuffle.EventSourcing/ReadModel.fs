namespace PerfectShuffle.EventSourcing
  open Microsoft.FSharp.Control

  type Agent<'t> = MailboxProcessor<'t>

  type Id = int64
  type EventWithMetadata<'event> = {Id : Id; Timestamp : System.DateTime; Event : 'event}
 
  type IReadModel<'TEvent, 'TState> =
    abstract member Apply : events:seq<EventWithMetadata<'TEvent>> -> unit
    abstract member CurrentState : unit -> 'TState
    abstract member CurrentStateAsync : unit -> Async<'TState>
    abstract member Events : unit -> EventWithMetadata<'TEvent>[]

  type private ReadModelMsg<'TEvent, 'TExternalState> =
    | Update of List<EventWithMetadata<'TEvent>>
    | CurrentState of AsyncReplyChannel<'TExternalState>     
    | EventsSnapshot of AsyncReplyChannel<EventWithMetadata<'TEvent>[]>

  type ReadModel<'TEvent, 'TInternalState, 'TExternalState>(initialState, evtAccumulator, expose) = 
    let agent =
      Agent<ReadModelMsg<'TEvent, 'TExternalState>>.Start(fun inbox ->
        let events : EventWithMetadata<'TEvent> list ref = ref []
        let rec loop pendingEvents (internalState:'TInternalState) =
          match pendingEvents with
          | [] ->
            async {            
            let! msg = inbox.Receive()
            
            match msg with
            | Update(evts) ->  
              return! loop evts internalState
            | CurrentState replyChannel ->
                let currentState = expose internalState
                replyChannel.Reply currentState
                return! loop pendingEvents internalState                      
            | EventsSnapshot(replyChannel) ->
              let evts = !events |> List.rev |> List.toArray              
              replyChannel.Reply(evts)
              return! loop pendingEvents internalState 
            }
          | firstEvent::remainingEvents ->
            let newState = evtAccumulator internalState firstEvent
            events := firstEvent :: !events
            loop remainingEvents newState                   

        loop [] initialState
        )

    interface IReadModel<'TEvent, 'TExternalState> with  
      member __.Apply evts =
        let msg = Update(evts |> Seq.toList)
        agent.Post msg
       
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
      member __.Events() = agent.PostAndReply(fun reply -> EventsSnapshot(reply))
   