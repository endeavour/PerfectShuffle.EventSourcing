namespace SampleApp

module MySampleApp =

  open PerfectShuffle.EventSourcing
  open PerfectShuffle.EventSourcing.Store
  
  open SampleApp.Domain
  open SampleApp.Events
  
  // Exposed to outside world, optimised for read access.
  type UserState = {User : Option<User>}
    with
      static member Zero = {User = None}

  let apply (state:UserState) (evt:SampleApp.Events.UserEvent) : UserState =
    match evt with
    | UserEvent.UserCreated userInfo ->
      let newUser : User =
        {
          Name = userInfo.Name
          Company = userInfo.Company
          Email = userInfo.Email
          Password = userInfo.Password
        }
      match state.User with
      | None -> {state with User = Some newUser}
      | Some user ->
        printfn "User already exists" 
        state

    | UserEvent.PasswordChanged newPw ->
      match state.User with
      | Some user ->
        let updatedUser = {user with Password = newPw}
        {state with User = Some updatedUser }
      | None ->
        printfn "Could not find user" 
        state

  exception EventProcessorException of exn


  let getUserStreamManager() =    

    let credentials = Microsoft.WindowsAzure.Storage.Auth.StorageCredentials("YOUR_STORAGE_ACCOUNT_HERE", "YOUR_KEY_HERE")

    let userStreamManager =
      
      let streamFactory =
        
        { new IStreamFactory with
            member __.CreateStream<'event> name =
              let serializer = Serialization.CreateDefaultSerializer<'event>()
              let dataProvider = new PerfectShuffle.EventSourcing.AzureTableStorage.AzureTableStorage.AzureTableDataProvider(credentials, "TestEventStore2")
              let stream = Stream(1L, name, serializer, dataProvider) :> IStream<_>
              stream
              }
      
      let eventProcessorFactory (stream:IStream<UserEvent>) : IEventProcessor<UserEvent, UserState> =
        let aggregate = SequencedAggregate(UserState.Zero, apply, stream.FirstVersion)
        EventProcessor<UserEvent, UserState>(aggregate, stream) :> IEventProcessor<_,_>

      StreamManager<UserEvent, UserState>(streamFactory, eventProcessorFactory)

    userStreamManager