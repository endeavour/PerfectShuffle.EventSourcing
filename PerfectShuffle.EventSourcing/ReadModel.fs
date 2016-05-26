namespace PerfectShuffle.EventSourcing
  open Microsoft.FSharp.Control
  open System

  type Agent<'t> = MailboxProcessor<'t>

  type Id = System.Guid
  
  
  //type Metadata = {Id : Id; Timestamp : System.DateTime}

  type EventToRecord<'event> = 
    {
      DeduplicationId : Id; 
      Timestamp : DateTime      
      Event : 'event
    }

  type RecordedMetadata = 
    {
      TypeName : string      
      CommitVersion : int64
      StreamName : string
      StreamVersion : int64
      DeduplicationId : Guid
      EventStamp : DateTime
      CommitStamp : DateTime
    }

  type RecordedEvent<'event> =
    {
      Event : 'event
      Metadata : RecordedMetadata
    }

  //type EventWithMetadataAndVersion<'event> = {Event : 'event; Metadata : Metadata; Version: int}


  type ReadModelState<'state> = {State:'state; NextExpectedStreamVersion : int64}

  type IReadModel<'state, 'event> =
    abstract member Apply : event:'event -> streamVersion:int64 -> Choice<unit,exn>
    abstract member CurrentState : unit -> ReadModelState<'state>
    abstract member CurrentStateAsync : unit -> Async<ReadModelState<'state>>
    abstract member Error : IEvent<Handler<exn>, exn>
    abstract member IsOrdered : bool

  type private ReadModelMsg<'TExternalState, 'event> =
    | Update of 'event * streamVersion:int64 * AsyncReplyChannel<Choice<unit,exn>>
    | CurrentState of AsyncReplyChannel<ReadModelState<'TExternalState>>

  exception ReadModelException of string

  type SequencedReadModel<'state,'event>(initialState, apply, firstVersion) = 
    
    let agent =
      Agent<ReadModelMsg<'state, 'event>>.Start(fun inbox ->
        let rec loop (nextExpectedStreamVersion:int64) (internalState:'state) =
          async {            
          let! msg = inbox.Receive()
            
          match msg with
          | Update(event, streamVersion, replyChannel) ->
            if streamVersion <> nextExpectedStreamVersion
              then
                replyChannel.Reply (Choice2Of2 <| ReadModelException "Wrong stream version")
                return! loop nextExpectedStreamVersion internalState
              else
                replyChannel.Reply (Choice1Of2 ()) 
                                
                let newState = apply internalState event

                return! loop (streamVersion + 1L) newState
          | CurrentState replyChannel ->
              replyChannel.Reply {State = internalState; NextExpectedStreamVersion = nextExpectedStreamVersion}
              return! loop nextExpectedStreamVersion internalState                    
          }

        loop firstVersion initialState
        )

    interface IReadModel<'state, 'event> with  
      member __.Apply event streamVersion = agent.PostAndReply (fun reply -> Update(event, streamVersion, reply))
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
      member __.Error = agent.Error
      member __.IsOrdered = true
   
  // This readmodel represents a CRDT
  // In particular, events might be applied in any order, more than once, and must be idempotent
  type ConflictFreeReadModel<'state,'event>(initialState, apply, firstVersion) = 
    
    let agent =
      Agent<ReadModelMsg<'state, 'event>>.Start(fun inbox ->
        let rec loop (nextExpectedStreamVersion:int64) (pending:List<int64>) (internalState:'state) =
          match pending with
          | n::ns when n = nextExpectedStreamVersion ->
            printfn "Consuming %d" n
            loop (nextExpectedStreamVersion + 1L) ns internalState
          | _ ->
            async {            
            let! msg = inbox.Receive()
            
            match msg with
            | Update(event, streamVersion, replyChannel) ->

                replyChannel.Reply (Choice1Of2 ()) 
                
                let newState = apply internalState event

                let newPending = streamVersion :: pending

                return! loop nextExpectedStreamVersion newPending newState
            | CurrentState replyChannel ->
                replyChannel.Reply {State = internalState; NextExpectedStreamVersion = nextExpectedStreamVersion}
                return! loop nextExpectedStreamVersion pending internalState                    
            }

        loop firstVersion [] initialState
        )

    interface IReadModel<'state, 'event> with  
      member __.Apply event streamVersion = agent.PostAndReply (fun reply -> Update(event, streamVersion, reply))
      member __.CurrentState() = agent.PostAndReply(fun reply -> CurrentState(reply))
      member __.CurrentStateAsync() = agent.PostAndAsyncReply(fun reply -> CurrentState(reply))
      member __.Error = agent.Error
      member __.IsOrdered = false