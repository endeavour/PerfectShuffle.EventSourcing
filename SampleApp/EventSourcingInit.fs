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

  let apply (state:UserState) (eventWithMetadata:EventWithMetadata<SampleApp.Events.UserEvent>) : UserState =
    match eventWithMetadata.Event with
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
        // TODO: Log error
        state

    | UserEvent.PasswordChanged newPw ->
      match state.User with
      | Some user ->
        let updatedUser = {user with Password = newPw}
        {state with User = Some updatedUser }
      | None ->
        // TODO: Log error
        state

  exception EventProcessorException of exn


  let getUserStreamManager() =    

    let credentials = Microsoft.WindowsAzure.Storage.Auth.StorageCredentials("pseventstoretest", "TPrq6CzszWwTpWcHwXTJ7Nc0xCHaSP9SvwdJkCcwcmQcmiPyK9DoIzoo45cfLc1L3HPboksozbMzNsVn3hgL3A==")
              
    let userStreamManager =
      
      let streamFactory =
        
        { new IStreamFactory with
            member __.CreateStream<'event> name =
              let serializer = Serialization.CreateDefaultSerializer<'event>()
              new PerfectShuffle.EventSourcing.AzureTableStorage.AzureTableStream<'event>(credentials, name, "mypartition", serializer) :> Store.IStream<_>}
      
      let serializer = Serialization.CreateDefaultSerializer<UserEvent>()
      
      let eventProcessorFactory (stream:IStream<UserEvent>) : IEventProcessor<UserEvent, UserState> =
        let readModel = SequencedReadModel(UserState.Zero, apply, stream.FirstVersion)
        EventProcessor<UserEvent, UserState>(readModel, stream) :> IEventProcessor<_,_>

      StreamManager<UserEvent, UserState>(streamFactory, eventProcessorFactory)

    userStreamManager