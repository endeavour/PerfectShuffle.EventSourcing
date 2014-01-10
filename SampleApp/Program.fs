open SampleApp.Commands
open PerfectShuffle.EventSourcing

[<EntryPoint>]
let main argv = 

    let readModel = SampleApp.MyApp.init()
    
    let evts =
       [
         SampleApp.Events.UserCreated({Name="James"; Email="james@ciseware.com"; Password="letmein"; Company = "Acme Corp"})
         |> EventMetadata.embellish
       ]

    printfn "Applying events... %A" evts
    readModel.Apply(evts)

    async {
      let! currentState = readModel.CurrentStateAsync()
      let users = currentState.Users

      printfn "Current users:"
      for user in users do
        printfn "%A" user.Value

    } |> Async.RunSynchronously

    System.Console.ReadKey() |> ignore

    printfn "%A" argv
    0 // return an integer exit code
