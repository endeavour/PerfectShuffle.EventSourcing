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

  let dataProvider = new PerfectShuffle.EventSourcing.InMemory.InMemory.InMemoryDataProvider()
  // let dataProvider = new PerfectShuffle.EventSourcing.SqlStorage.SqlStorage.SqlDataProvider("""CONNECTION_STRING""", System.TimeSpan.FromSeconds(5.0))

  let onError = fun exn -> printfn "%A" exn

  let streamFactory =
        
    { new IStreamFactory with
        member __.CreateStream<'event> name =
          let serializer = Serialization.CreateDefaultSerializer<'event>()      
          let stream = Stream(name, serializer, dataProvider) :> IStream<_>
          stream
          }

  let eventProcessorFactory (stream:IStream<UserEvent>) : IEventProcessor<UserEvent, UserState> =
    let aggregate = SequencedAggregate(UserState.Zero, apply, stream.FirstVersion)
    
    new EventProcessor<UserEvent, UserState>(aggregate, stream, onError) :> IEventProcessor<_,_>

  let getUserStreamManager() =    

    let userStreamManager =
      
      new StreamManager<UserEvent, UserState>(streamFactory, eventProcessorFactory, onError)

    userStreamManager