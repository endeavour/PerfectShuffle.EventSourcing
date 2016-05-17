namespace PerfectShuffle.EventSourcing
  open Microsoft.FSharp.Control

  type Agent<'t> = MailboxProcessor<'t>

  type Id = System.Guid
  
  type EventWithMetadata<'event> =
    {Id : Id; Timestamp : System.DateTime; Event : 'event}
      with
        static member Wrap evt = {Id = System.Guid.NewGuid(); Timestamp = System.DateTime.UtcNow; Event = evt}

  type Batch<'event> =
    {
      StartVersion : int
      Events : EventWithMetadata<'event>[]
    }


  type ReadModelState<'state> = {State:'state; NextExpectedStreamVersion : int}

  type IReadModel<'state, 'event> =
    abstract member Apply : Batch<'event> -> Choice<unit,exn>
    abstract member CurrentState : unit -> ReadModelState<'state>
    abstract member CurrentStateAsync : unit -> Async<ReadModelState<'state>>
    abstract member Error : IEvent<Handler<exn>, exn>
    abstract member IsOrdered : bool

  type private ReadModelMsg<'TExternalState, 'event> =
    | Update of Batch<'event> * AsyncReplyChannel<Choice<unit,exn>>
    | CurrentState of AsyncReplyChannel<ReadModelState<'TExternalState>>

  exception ReadModelException of string

  type SequencedReadModel<'state,'event>(initialState, apply, firstVersion) = 
    
    let agent =
      Agent<ReadModelMsg<'state, 'event>>.Start(fun inbox ->
        let rec loop (nextExpectedStreamVersion:int) (internalState:'state) =
          async {            
          let! msg = inbox.Receive()
            
          match msg with
          | Update(batch, replyChannel) ->
            if batch.StartVersion <> nextExpectedStreamVersion
              then
                replyChannel.Reply (Choice2Of2 <| ReadModelException "Wrong stream version")
                return! loop nextExpectedStreamVersion internalState
              else
                replyChannel.Reply (Choice1Of2 ()) 
                
                let newState =
                  batch.Events
                  |> Seq.fold apply internalState

                return! loop (batch.StartVersion + batch.Events.Length) newState
          | CurrentState replyChannel ->
              replyChannel.Reply {State = internalState; NextExpectedStreamVersion = nextExpectedStreamVersion}
              return! loop nextExpectedStreamVersion internalState                    
          }

        loop firstVersion initialState
        )

    interface IReadModel<'state, 'event> with  
      member __.Apply batch = agent.PostAndReply (fun reply -> Update(batch, reply))
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
      member __.Error = agent.Error
      member __.IsOrdered = true
   
  // This readmodel represents a CRDT
  // In particular, events might be applied in any order, more than once, and must be idempotent
  type ConflictFreeReadModel<'state,'event>(initialState, apply, firstVersion) = 
    
    let agent =
      Agent<ReadModelMsg<'state, 'event>>.Start(fun inbox ->
        let rec loop (internalState:'state) =
          async {            
          let! msg = inbox.Receive()
            
          match msg with
          | Update(batch, replyChannel) ->

              replyChannel.Reply (Choice1Of2 ()) 
                
              let newState =
                batch.Events
                |> Seq.fold apply internalState

              return! loop newState
          | CurrentState replyChannel ->
              replyChannel.Reply {State = internalState; NextExpectedStreamVersion = firstVersion}
              return! loop internalState                    
          }

        loop initialState
        )

    interface IReadModel<'state, 'event> with  
      member __.Apply batch = agent.PostAndReply (fun reply -> Update(batch, reply))
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
      member __.Error = agent.Error
      member __.IsOrdered = false