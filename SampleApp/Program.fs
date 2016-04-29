open SampleApp.Commands
open PerfectShuffle.EventSourcing

[<EntryPoint>]
let main argv = 

    let eventSubscription, eventProcessor = SampleApp.MySampleApp.initialiseEventProcessor()
    
//    let evts =
//       let email = sprintf "%d@test.com" System.DateTime.UtcNow.Ticks
//       [|         
//         for i = 1 to 10 do
//           let name = sprintf "Fred %d" i
//           yield             
//             SampleApp.Events.UserCreated({Name = name; Email=email; Password="letmein"; Company = "Acme Corp"})
//             |> EventWithMetadata<_>.Wrap
//       |]

//    printfn "Applying events... %A" evts
    
//    async {
//      // TODO: fix hack
//      do! Async.Sleep 1000 // hack to allow enough time for existing events to be retrieved
//      let! state = eventProcessor.ExtendedState()
//      let streamVersion =
//        match state.NextExpectedStreamVersion with
//        | None -> 1
//        | Some n -> n
//      let batch = { StartVersion = streamVersion; Events = evts }
//      let! persistResult = eventProcessor.Persist batch
//      match persistResult  with
//      | Choice1Of2 currentState ->
//        let users = currentState.State.Users
//          
//        printfn "Current users:"
//        for user in users do
//          printfn "%A" user.Value
//      | Choice2Of2 e ->
//        printfn "Something went terribly wrong: %A" e
//
//    } |> Async.RunSynchronously

    while true do
      System.Console.ReadKey() |> ignore
      async {
      let email = sprintf "%d@test.com" System.DateTime.UtcNow.Ticks

      let evt =
        SampleApp.Events.UserCreated({Name = (sprintf "Test %s" (System.DateTime.UtcNow.ToShortTimeString())); Email=email; Password="letmein"; Company = "Acme Corp"})
        |> EventWithMetadata<_>.Wrap 

      let! state = eventProcessor.ExtendedState()
      let streamVersion =
        match state.NextExpectedStreamVersion with
        | None -> 1
        | Some n -> n
      let batch = { StartVersion = streamVersion; Events = [|evt|] }       
      let! persistResult = eventProcessor.Persist batch
      match persistResult  with
      | Choice1Of2 currentState ->
        let users = currentState.State.Users
          
        printfn "Current users:"
        for user in users do
          printfn "%A" user.Value
      | Choice2Of2 e ->
        printfn "Something went terribly wrong: %A" e
      } |> Async.RunSynchronously

    printfn "%A" argv
    0 // return an integer exit code
