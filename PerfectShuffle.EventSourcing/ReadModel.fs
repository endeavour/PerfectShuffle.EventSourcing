namespace PerfectShuffle.EventSourcing
  open Microsoft.FSharp.Control

  type Agent<'t> = MailboxProcessor<'t>

  type Id = System.Guid
  type EventWithMetadata<'event> =
    {Id : Id; Timestamp : System.DateTime; Event : 'event}
      with
        static member Wrap evt = {Id = System.Guid.NewGuid(); Timestamp = System.DateTime.UtcNow; Event = evt}
 
  type IReadModel<'TState, 'TEvent> =
    abstract member Apply : events:seq<EventWithMetadata<'TEvent>> -> unit
    abstract member CurrentState : unit -> 'TState
    abstract member CurrentStateAsync : unit -> Async<'TState>
    abstract member Events : unit -> EventWithMetadata<'TEvent>[]

  type private ReadModelMsg<'TExternalState, 'TEvent> =
    | Update of List<EventWithMetadata<'TEvent>>
    | CurrentState of AsyncReplyChannel<'TExternalState>     
    | EventsSnapshot of AsyncReplyChannel<EventWithMetadata<'TEvent>[]>

  type ReadModel<'TState,'TEvent>(initialState, apply) = 
    let agent =
      Agent<ReadModelMsg<'TState, 'TEvent>>.Start(fun inbox ->
        let events : EventWithMetadata<'TEvent> list ref = ref []
        let rec loop pendingEvents (internalState:'TState) =
          match pendingEvents with
          | [] ->
            async {            
            let! msg = inbox.Receive()
            
            match msg with
            | Update(evts) ->  
              return! loop evts internalState
            | CurrentState replyChannel ->
                replyChannel.Reply internalState
                return! loop pendingEvents internalState                      
            | EventsSnapshot(replyChannel) ->
              let evts = !events |> List.rev |> List.toArray              
              replyChannel.Reply(evts)
              return! loop pendingEvents internalState 
            }
          | firstEvent::remainingEvents ->
            let newState = apply internalState firstEvent
            events := firstEvent :: !events
            loop remainingEvents newState                   

        loop [] initialState
        )

    interface IReadModel<'TState, 'TEvent> with  
      member __.Apply evts =
        let msg = Update(evts |> Seq.toList)
        agent.Post msg
       
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
      member __.Events() = agent.PostAndReply(fun reply -> EventsSnapshot(reply))
   