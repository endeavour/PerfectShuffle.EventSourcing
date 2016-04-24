namespace PerfectShuffle.EventSourcing
  open Microsoft.FSharp.Control
  open FSharpx.Collections

  type Agent<'t> = MailboxProcessor<'t>

  type Id = System.Guid
  type EventWithMetadata<'event> =
    {Id : Id; Timestamp : System.DateTime; Event : 'event}
      with
        static member Wrap evt = {Id = System.Guid.NewGuid(); Timestamp = System.DateTime.UtcNow; Event = evt}
 
  type ReadModelState<'TState> = {State:'TState; NextEventNumber : int}

  type IReadModel<'TState, 'TEvent> =
    abstract member Apply : nextEventNumber:int * events:EventWithMetadata<'TEvent>[] -> Choice<unit,exn>
    abstract member CurrentState : unit -> ReadModelState<'TState>
    abstract member CurrentStateAsync : unit -> Async<ReadModelState<'TState>>
    abstract member Events : unit -> PersistentVector<EventWithMetadata<'TEvent>>
    abstract member Error : IEvent<Handler<exn>, exn>

  type private ReadModelMsg<'TExternalState, 'TEvent> =
    | Update of firstEventNumber:int * events:List<EventWithMetadata<'TEvent>> * AsyncReplyChannel<Choice<unit,exn>>
    | CurrentState of AsyncReplyChannel<ReadModelState<'TExternalState>>
    | EventsSnapshot of AsyncReplyChannel<PersistentVector<EventWithMetadata<'TEvent>>>

  exception ReadModelException of string

  type ReadModel<'TState,'TEvent>(initialState, apply) = 
    let agent =
      Agent<ReadModelMsg<'TState, 'TEvent>>.Start(fun inbox ->
        let rec loop nextEventNumber pendingEvents (internalState:'TState) (appliedEvents:PersistentVector<_>) =
          match pendingEvents with
          | [] ->
            async {            
            let! msg = inbox.Receive()
            
            match msg with
            | Update(firstEventNumber, evts, replyChannel) when firstEventNumber < nextEventNumber ->
                // Already applied this event
                try
                  for i = 0 to evts.Length - 1 do
                    if appliedEvents.[firstEventNumber + i].Id <> evts.[i].Id then
                      raise <| ReadModelException("Unexpected event ID")
                  replyChannel.Reply (Choice1Of2 ())                  
                with e ->
                  replyChannel.Reply (Choice2Of2 e)
                return! loop nextEventNumber pendingEvents internalState appliedEvents   
            | Update(firstEventNumber, evts, replyChannel) when firstEventNumber > nextEventNumber ->         
                replyChannel.Reply(Choice2Of2 <| ReadModelException(sprintf "Was expecting a lower event number. Given: %d, Expected %d." firstEventNumber nextEventNumber))
                return! loop nextEventNumber pendingEvents internalState appliedEvents   
            | Update(firstEventNumber, evts, replyChannel) ->              
              replyChannel.Reply(Choice1Of2 (()))
              return! loop nextEventNumber evts internalState appliedEvents
            | CurrentState replyChannel ->
                replyChannel.Reply {State = internalState; NextEventNumber = nextEventNumber}
                return! loop nextEventNumber pendingEvents internalState appliedEvents                    
            | EventsSnapshot(replyChannel) ->
              replyChannel.Reply(appliedEvents)
              return! loop nextEventNumber pendingEvents internalState appliedEvents
            }
          | firstEvent::remainingEvents ->
            printfn "Applying %-5d: %A" nextEventNumber firstEvent.Id
            #if DEBUG
            printfn "TYPE: %s" (firstEvent.Event.GetType().Name)
            #endif
            let newState = apply internalState firstEvent
            loop (nextEventNumber+1) remainingEvents newState (appliedEvents.Conj firstEvent)

        loop 0 [] initialState PersistentVector.empty
        )

    interface IReadModel<'TState, 'TEvent> with  
      
      member __.Apply (firstEventNumber, evts) =
        agent.PostAndReply (fun reply -> Update(firstEventNumber, evts |> Seq.toList, reply))
       
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
      member __.Events() = agent.PostAndReply(fun reply -> EventsSnapshot(reply))
      member __.Error = agent.Error
   