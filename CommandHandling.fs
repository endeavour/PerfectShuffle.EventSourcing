namespace PerfectShuffle.EventSourcing

type CmdOutput<'TEvent> =
| Success of seq<'TEvent>
| Failure of exn

type CommandHandler<'TCmd,'TEvent> = 'TCmd -> Async<CmdOutput<'TEvent>>
type EventSerializer<'TEvent> = seq<'TEvent> -> Async<unit>
type CommandProcessor<'TCmd, 'TEvent, 'TExternalState>(readModel:IReadModel<'TEvent,'TExternalState>, cmdHandler:CommandHandler<'TCmd,'TEvent>, serialize:EventSerializer<'TEvent>) =
  
  let processCmd cmd =
    async {
      let! output = cmdHandler cmd
    
      match output with
      | Success evts ->
        do! serialize evts
        readModel.Apply evts
      | Failure ex -> raise ex
    }
    
  member this.Process (cmd:'TCmd) = processCmd cmd |> Async.RunSynchronously