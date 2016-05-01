namespace SampleApp

module MySampleApp =

  open PerfectShuffle.EventSourcing
  
  open SampleApp.Domain
  open SampleApp.Events
  
  // Exposed to outside world, optimised for read access.
  type State = {Users : Map<string,User>}
    with
      static member Zero = {Users = Map.empty}

  let apply (state:State) (eventWithMetadata:EventWithMetadata<SampleApp.Events.DomainEvent>) : State =
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

//  open PerfectShuffle.EventSourcing.EventStore

  let initialiseEventProcessor() =    
//    let eventStoreEndpoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse("127.0.0.1"), 1113)
//    let eventStoreConnection = EventStore.Connect eventStoreEndpoint


    let serializer = Serialization.CreateDefaultSerializer<DomainEvent>()

//    let repository = EventRepository<DomainEvent>(eventStoreConnection, "SampleAppEvents", serializer) :> Store.IEventRepository<_>    
//    let evtProcessor = EventProcessor<State, DomainEvent>(readModel, repository) :> IEventProcessor<_,_>  

    let repository =
      let credentials = Microsoft.WindowsAzure.Storage.Auth.StorageCredentials("pseventstoretest", "TPrq6CzszWwTpWcHwXTJ7Nc0xCHaSP9SvwdJkCcwcmQcmiPyK9DoIzoo45cfLc1L3HPboksozbMzNsVn3hgL3A==")
      new PerfectShuffle.EventSourcing.AzureTableStorage.EventRepository<_>(credentials, "eventstoresample", "mypartition", serializer) :> Store.IEventRepository<_>   

    let readModel = PerfectShuffle.EventSourcing.ReadModel(State.Zero, apply, repository.FirstVersion) :> IReadModel<_,_>            
    readModel.Error.Subscribe(fun e ->
      raise <| EventProcessorException(e)
      ) |> ignore<System.IDisposable>

    
    let evtProcessor = EventProcessor<State, DomainEvent>(readModel, repository)
        
    evtProcessor :> IEventProcessor<_,_>