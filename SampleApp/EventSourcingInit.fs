namespace SampleApp

module MySampleApp =

  open PerfectShuffle.EventSourcing
  open PerfectShuffle.EventSourcing.EventStore
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

  exception EventProcessorException of exn

  let getBootstrapEvents (readModel:IReadModel<State,DomainEvent>) =
      [||]
      |> Array.map (EventWithMetadata<_>.Wrap)

  let initialiseEventProcessor() =    
    let eventStoreEndpoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("127.0.0.1"), 1113)
    let eventStoreConnection = EventStore.Connect eventStoreEndpoint

    let readModel = PerfectShuffle.EventSourcing.ReadModel(State.Zero, apply) :> IReadModel<_,_>            
    
    readModel.Error.Subscribe(fun e ->
      raise <| EventProcessorException(e)
      ) |> ignore<System.IDisposable>

    let serializer = Serialization.CreateDefaultSerializer<DomainEvent>()

    let repository = EventRepository<DomainEvent>(eventStoreConnection, "SampleAppEvents", serializer) :> Store.IEventRepository<_>
    
    let evtProcessor = EventProcessor<State, DomainEvent>(readModel, repository) :> IEventProcessor<_,_>  

    let initialBootstrapResult =
      let bootstrapEvents = getBootstrapEvents readModel
      let bootstrapResult =
        repository.Save bootstrapEvents Store.WriteConcurrencyCheck.NoStream
        |> Async.RunSynchronously
      match bootstrapResult with
      | Store.WriteResult.ConcurrencyCheckFailed ->
        printfn "Stream already exists, skipping bootstrap events."
      | Store.Success ->
        printfn "Boostrapped"
      | Store.WriteException e -> raise e

    printf "Subscribing to events feed..."    
    let subscription = repository.Events.Subscribe(fun e ->      
      readModel.Apply(e.EventNumber, [|e.Event|]))
    
    printfn "[OK]"
        
    subscription, evtProcessor