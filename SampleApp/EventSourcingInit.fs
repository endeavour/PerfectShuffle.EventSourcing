namespace SampleApp

module MySampleApp =

  open PerfectShuffle.EventSourcing
  open SampleApp.Domain
  open SampleApp.Events
  
  // Exposed to outside world, optimised for read access.
  type State = {Users : Map<string,User>}
    with
      static member Zero = {Users = Map.empty}

  let apply (state:State) (eventWithMetadata:EventWithMetadata<SampleApp.Events.DomainEvent>) =
    match eventWithMetadata.Event with
    | DomainEvent.UserCreated userInfo ->
      let newUser : User =
        {
          Name = userInfo.Name
          Company = userInfo.Company
          Email = userInfo.Email
          Password = userInfo.Password
        }
      let newUsers = state.Users.Add(userInfo.Email, newUser)
      {state with Users = newUsers}

  let mutable eventStoreSubscription : Option<System.IDisposable> = None

  exception EventProcessorException of exn

  let getBootstrapEvents (readModel:IReadModel<State,DomainEvent>) =
      [||]
      |> Array.map (EventWithMetadata<_>.Wrap)

  let initialiseEventProcessor() =
    // TODO: Can we encapsulate all the GregYoung eventstore stuff in PerfectShuffle.EventSourcing?
    let eventStoreEndpoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("127.0.0.1"), 1113)
    let eventStoreConnection = PerfectShuffle.EventSourcing.Store.conn eventStoreEndpoint
    let s,d = PerfectShuffle.EventSourcing.Serialization.serializer
    let readModel = PerfectShuffle.EventSourcing.ReadModel(State.Zero, apply) :> IReadModel<_,_>            

    readModel.Error.Subscribe(fun e ->
      raise <| EventProcessorException(e)
      ) |> ignore<System.IDisposable>

    let repository = Store.EventRepository<DomainEvent>(eventStoreConnection, "SampleAppEvents", s, d)
    
    let evtProcessor = EventProcessor<State, DomainEvent>(readModel, repository) :> IEventProcessor<_,_>  

    let initialBootstrapResult =
      let bootstrapEvents = getBootstrapEvents readModel
      let bootstrapResult =
        repository.Save(bootstrapEvents, Store.WriteConcurrencyCheck.NoStream)
        |> Async.RunSynchronously
      match bootstrapResult with
      | Store.WriteResult.ConcurrencyCheckFailed ->
        printfn "Stream already exists, skipping bootstrap events."
      | Store.Success ->
        printfn "Boostrapped"
      | Store.WriteException e -> raise e

    printf "Subscribing to events feed..."    
    let subscription = repository.Subscribe(fun e ->      
      readModel.Apply(e.EventNumber, [|e.Event|]))
    eventStoreSubscription <- Some(subscription)
    printfn "[OK]"
        
    evtProcessor