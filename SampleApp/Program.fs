open SampleApp.Commands

[<EntryPoint>]
let main argv = 

    let readModel, commandHandler = SampleApp.MyApp.init()
    
    commandHandler.Process(SignUp({Name="James"; Email="james@ciseware.com"; Password="letmein"; Company = "Acme Corp"}))

    System.Console.ReadKey() |> ignore

    printfn "%A" argv
    0 // return an integer exit code
