namespace PerfectShuffle.EventSourcing
  open Microsoft.FSharp.Control

  type Agent<'t> = MailboxProcessor<'t>

  type Id = System.Guid
  type EventWithMetadata<'event> =
    {Id : Id; Timestamp : System.DateTime; Event : 'event}
      with
        static member Wrap evt = {Id = System.Guid.NewGuid(); Timestamp = System.DateTime.UtcNow; Event = evt}
 
  type ReadModelState<'TState> = {State:'TState; NextEventNumber : int}

  type IReadModel<'TState, 'TEvent> =
    abstract member Apply : nextEventNumber:int * events:EventWithMetadata<'TEvent>[] -> unit
    abstract member CurrentState : unit -> ReadModelState<'TState>
    abstract member CurrentStateAsync : unit -> Async<ReadModelState<'TState>>
    abstract member Events : unit -> EventWithMetadata<'TEvent>[]
    abstract member Error : IEvent<Handler<exn>, exn>

  type private ReadModelMsg<'TExternalState, 'TEvent> =
    | Update of firstEventNumber:int * events:List<EventWithMetadata<'TEvent>>
    | CurrentState of AsyncReplyChannel<ReadModelState<'TExternalState>>
    | EventsSnapshot of AsyncReplyChannel<EventWithMetadata<'TEvent>[]>

  type ReadModel<'TState,'TEvent>(initialState, apply) = 
    let agent =
      Agent<ReadModelMsg<'TState, 'TEvent>>.Start(fun inbox ->
        let events : EventWithMetadata<'TEvent> list ref = ref []
        let rec loop nextEventNumber pendingEvents (internalState:'TState) =
          match pendingEvents with
          | [] ->
            async {            
            let! msg = inbox.Receive()
            
            match msg with
            | Update(firstEventNumber, evts) ->
              if firstEventNumber = nextEventNumber then                
                return! loop nextEventNumber evts internalState
              elif firstEventNumber < nextEventNumber then
                // Already applied this event
                #if DEBUG
                let allEvts = !events |> List.rev |> List.toArray
                for i = 0 to evts.Length - 1 do
                  assert (allEvts.[firstEventNumber + i].Id = evts.[i].Id)
                #endif
                return! loop nextEventNumber pendingEvents internalState                
              elif firstEventNumber > nextEventNumber then
                failwith <| sprintf "Was expecting a lower event number. Given: %d, Expected %d." firstEventNumber nextEventNumber
            | CurrentState replyChannel ->
                replyChannel.Reply {State = internalState; NextEventNumber = nextEventNumber}
                return! loop nextEventNumber pendingEvents internalState                      
            | EventsSnapshot(replyChannel) ->
              let evts = !events |> List.rev |> List.toArray              
              replyChannel.Reply(evts)
              return! loop nextEventNumber pendingEvents internalState 
            }
          | firstEvent::remainingEvents ->
            printfn "Applying %-5d: %A" nextEventNumber firstEvent.Id
            #if DEBUG
            printfn "TYPE: %s" (firstEvent.Event.GetType().Name)
            #endif
            let newState = apply internalState firstEvent
            events := firstEvent :: !events
            loop (nextEventNumber+1) remainingEvents newState                   

        loop 0 [] initialState
        )

    interface IReadModel<'TState, 'TEvent> with  
      
      member __.Apply (firstEventNumber, evts) =
        let msg = Update(firstEventNumber, evts |> Seq.toList)
        agent.Post msg
       
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
      member __.Events() = agent.PostAndReply(fun reply -> EventsSnapshot(reply))
      member __.Error = agent.Error
   