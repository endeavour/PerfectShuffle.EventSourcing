namespace SampleApp
module MyApp =
  open PerfectShuffle.EventSourcing
  open SampleApp.Events
  open SampleApp.Domain
  open SampleApp.Commands
  
  // Optimised for applying events.
  type InternalState = {Users : Set<User>}
  
  // Exposed to outside world, optimised for read access.
  type State = {Users : Map<string,User>}

  let eventStore = EventStore("events.data")
  
  let deserialize (readmodel:IReadModel<_,_>) =
    let events = eventStore.ReadEvents()
    events |> Seq.iteri (fun i evt ->
      printfn "[%d] Applying event: %A" i evt
      readmodel.Apply(events))

  let init() =
     
    /// Converts internal state to the readmodel
    let expose (internalState:InternalState) =
      let users = internalState.Users |> Set.map (fun user -> user.Email, user) |> Map.ofSeq      
      let currentState:State = {Users = users}      
      currentState
    
    let (|Event|) (evtWithMetaData:EventWithMetadata<DomainEvent>) = evtWithMetaData.Event

    /// Folds an event into the current state
    let evtAccumulator (internalState:InternalState) (evt:EventWithMetadata<DomainEvent>) : InternalState =
      match evt with
      | Event(UserCreated(user)) ->
        let newUsers = internalState.Users.Add({Name = user.Name; Company = user.Company; Email = user.Email; Password = user.Password})
        {Users = newUsers}
  
    let initialState:InternalState = {Users = Set.empty}
    let readModel:IReadModel<_,_> = upcast ReadModel<DomainEvent,InternalState,State>(initialState, evtAccumulator, expose)     
      
    deserialize readModel

    let run cmd : Async<CmdOutput<_>> =
      match cmd with
      | SignUp (data) ->
          async {
          let! state = readModel.CurrentStateAsync()
          let email = data.Email.ToLowerInvariant()
          if state.Users.ContainsKey email
            then
              return Failure <| System.Exception("User already exists")
            else
              //send an email to user
              let evts = seq {
                yield
                  UserCreated({Name=data.Name; Email=email; Password=data.Password; Company=data.Company})
                  |> EventMetadata.embellish
              }
              return Success(evts)
          }
   
    let output = new System.IO.StreamWriter("events.data", append=true)

    readModel