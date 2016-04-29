namespace PerfectShuffle.EventSourcing
  open Microsoft.FSharp.Control

  type Agent<'t> = MailboxProcessor<'t>

  type Id = System.Guid
  
  type EventWithMetadata<'event> =
    {Id : Id; Timestamp : System.DateTime; Event : 'event}
      with
        static member Wrap evt = {Id = System.Guid.NewGuid(); Timestamp = System.DateTime.UtcNow; Event = evt}

  type Changeset<'TEvent> =
    {
      StreamVersion : int
      Events : EventWithMetadata<'TEvent>[]
    }
 
  type ReadModelState<'TState> = {State:'TState; NextExpectedStreamVersion : Option<int>}

  type IReadModel<'TState, 'TEvent> =
    abstract member Apply : Changeset<'TEvent> -> Choice<unit,exn>
    abstract member CurrentState : unit -> ReadModelState<'TState>
    abstract member CurrentStateAsync : unit -> Async<ReadModelState<'TState>>
    abstract member Error : IEvent<Handler<exn>, exn>

  type private ReadModelMsg<'TExternalState, 'TEvent> =
    | Update of Changeset<'TEvent> * AsyncReplyChannel<Choice<unit,exn>>
    | CurrentState of AsyncReplyChannel<ReadModelState<'TExternalState>>

  exception ReadModelException of string

  type ReadModel<'TState,'TEvent>(initialState, apply) = 
    
    let agent =
      Agent<ReadModelMsg<'TState, 'TEvent>>.Start(fun inbox ->
        let rec loop nextExpectedStreamVersion (internalState:'TState) =
          async {            
          let! msg = inbox.Receive()
            
          match msg with
          | Update(changeset, replyChannel) ->
            match nextExpectedStreamVersion with
            | Some(nextStreamVersion) when changeset.StreamVersion <> nextStreamVersion ->
              replyChannel.Reply (Choice2Of2 <| ReadModelException "Wrong stream version")
            | None | Some _ ->
              replyChannel.Reply (Choice1Of2 ()) 
                
              let newState =
                changeset.Events
                |> Seq.fold apply internalState

              for evt in changeset.Events do
                printfn "Readmodel applying event %d / %A" changeset.StreamVersion evt.Id

              return! loop (Some(changeset.StreamVersion + 1)) newState
          | CurrentState replyChannel ->
              replyChannel.Reply {State = internalState; NextExpectedStreamVersion = nextExpectedStreamVersion}
              return! loop nextExpectedStreamVersion internalState                    
          }

        loop None initialState
        )

    interface IReadModel<'TState, 'TEvent> with  
      member __.Apply changeset = agent.PostAndReply (fun reply -> Update(changeset, reply))
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
      member __.Error = agent.Error
   