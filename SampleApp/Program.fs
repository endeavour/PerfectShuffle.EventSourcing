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
          {DeduplicationId = Guid.NewGuid(); Timestamp = DateTime.UtcNow; Event = evt }                     
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
          {DeduplicationId = Guid.NewGuid(); Timestamp = DateTime.UtcNow; Event = evt }          
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

      
  //    let eventProcessor = SampleApp.MySampleApp.initialiseEventProcessor()   

  //    while true do
  //      System.Console.ReadKey() |> ignore
  //      async {
  //      let email = sprintf "%d@test.com" System.DateTime.UtcNow.Ticks
  //
  //      let evts =
  //        [|
  //        for i = 1 to 1 do
  //          let name = sprintf "Test %d" i
  //          yield
  //            SampleApp.Events.UserCreated {Name = name; Email=email; Password="letmein"; Company = "Acme Corp"}
  //            |> EventWithMetadata<_>.Wrap 
  //        |]
  //
  //      let sw = System.Diagnostics.Stopwatch.StartNew()
  //
  //      let! state = eventProcessor.ExtendedState()
  //      let batch = { StartVersion = state.NextExpectedStreamVersion; Events = evts }       
  //      let! persistResult = eventProcessor.Persist batch
  //      match persistResult  with
  //      | Choice1Of2 currentState ->
  //        let users = currentState.State.Users
  //          
  //        printfn "Current users: %d" users.Count
  ////        for user in users do
  ////          printfn "%A" user.Value
  //      | Choice2Of2 e ->
  //        printfn "Something went terribly wrong: %A" e
  //      printfn "TIME to insert %d events: %dms" evts.Length sw.ElapsedMilliseconds
  //      } |> Async.RunSynchronously      
