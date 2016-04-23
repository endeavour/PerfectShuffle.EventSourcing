open SampleApp.Commands
open PerfectShuffle.EventSourcing

[<EntryPoint>]
let main argv = 

    let eventSubscription, eventProcessor = SampleApp.MySampleApp.initialiseEventProcessor()
    
    let evts =
       let email = sprintf "%d@test.com" System.DateTime.UtcNow.Ticks
       [|         
         SampleApp.Events.UserCreated({Name="James"; Email=email; Password="letmein"; Company = "Acme Corp"})
         |> EventWithMetadata<_>.Wrap
       |]

    printfn "Applying events... %A" evts
    
    async {
      let! currentState = eventProcessor.Persist evts      
      let users = currentState.State.Users

      printfn "Current users:"
      for user in users do
        printfn "%A" user.Value

    } |> Async.RunSynchronously

    System.Console.ReadKey() |> ignore

    printfn "%A" argv
    0 // return an integer exit code
