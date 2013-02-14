namespace PerfectShuffle.EventSourcing
  open Microsoft.FSharp.Control

  type Agent<'t> = MailboxProcessor<'t>
 
  type IReadModel<'TEvent, 'TState> =
    abstract member Apply : events:seq<'TEvent> -> unit
    abstract member CurrentState : unit -> 'TState
    abstract member CurrentStateAsync : unit -> Async<'TState>

  type private Msg<'TEvent, 'TExternalState> =
    | Update of List<'TEvent>
    | CurrentState of AsyncReplyChannel<'TExternalState>     

  type ReadModel<'TEvent, 'TInternalState, 'TExternalState>(initialState, evtAccumulator, expose) = 
    let agent =
      Agent<Msg<'TEvent, 'TExternalState>>.Start(fun inbox ->

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
            }

          | firstEvent::remainingEvents ->
            let newState = evtAccumulator internalState firstEvent
            loop remainingEvents newState                   

        loop [] initialState
        )

    interface IReadModel<'TEvent, 'TExternalState> with  
      member __.Apply evts =
        let msg = Update(evts |> Seq.toList)
        agent.Post msg
       
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
   