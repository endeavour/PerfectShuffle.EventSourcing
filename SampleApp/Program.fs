namespace SampleApp
open System
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store

module Main =

  [<EntryPoint>]
  let main argv = 

    let userStreamManager = MySampleApp.getUserStreamManager()

    let createUser name email pw =

      let evt = SampleApp.Events.UserCreated {Name = name; Email=email; Password=pw; Company = "Acme Corp"} 

      let evts =
        [|          
          {EventToRecord = evt; Metadata = {DeduplicationId = Guid.NewGuid(); EventStamp = DateTime.UtcNow }}                     
        |]

      async {
      let! evtProcessor = userStreamManager.GetEventProcessor email
      let! state = evtProcessor.ExtendedState()

      return!
        match state.State.User with
        | None ->
          async {
          printf "User %s does not exist, creating..." email        
          let batch = {StartVersion = state.NextExpectedStreamVersion; Events = evts }
          let! result = evtProcessor.Persist batch
          return result
          }
        | Some user ->
          async {
          printfn "User %s already exists, skipping..." email
          return Choice1Of2 state
          }
      }

    let changePw email newPw =

      let evt = SampleApp.Events.PasswordChanged newPw

      let evts =
        [|
          {EventToRecord = evt; Metadata = {DeduplicationId = Guid.NewGuid(); EventStamp = DateTime.UtcNow }}          
        |]

      async {
      let! evtProcessor = userStreamManager.GetEventProcessor(email)
      let! state = evtProcessor.ExtendedState()
      let batch = {StartVersion = state.NextExpectedStreamVersion; Events = evts }
      let! result = evtProcessor.Persist batch
      return result
      }

    let createAndChangePw name email =
      let name = name
      let email = sprintf "%s@foo.com" name
      async {
        do! createUser name email "test123" |> Async.Ignore
        do! changePw email "test321" |> Async.Ignore
      }


    let nameAndEmails =
      [0..3]
      |> Seq.map (fun n ->
        let name = sprintf "user%d" n
        let email = sprintf "%s@foo.com" name
        name, email)

    let evts =
      nameAndEmails
      |> Seq.map (fun (name,email) -> createAndChangePw name email)

    let all = MySampleApp.dataProvider :> PerfectShuffle.EventSourcing.Store.IAllEventReader
    all.GetAllEvents 0L |> AsyncSeq.toObservable |> Observable.subscribe (printfn "New Streamed Event: %A") |> ignore<System.IDisposable>
    async {
      
      do! evts |> Async.Parallel |> Async.Ignore

      //let! streams = userStreamManager.Streams()
      let streams = nameAndEmails |> Seq.map snd
      let! processors = streams |> Seq.map userStreamManager.GetEventProcessor |> Async.Parallel
      let! users = processors |> Seq.map (fun x ->
        async {
        let! state = x.State()
        return state.User}) |> Async.Parallel
      for user in users do
        printfn "%A" user

    } |> Async.RunSynchronously

    System.Console.ReadKey() |> ignore
    printfn "%A" argv
    0 // return an integer exit code
