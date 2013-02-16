namespace PerfectShuffle.EventSourcing

type CmdOutput<'TEvent> =
| Success of seq<'TEvent>
| Failure of exn


type CommandHandler<'TCmd,'TEvent> = 'TCmd -> Async<CmdOutput<'TEvent>>

// For C# and autofac friendliness
type ICommandHandler<'TCmd,'TEvent> =
  abstract Handle : cmd:'TCmd -> Async<CmdOutput<'TEvent>>

type EventSerializer<'TEvent> = seq<'TEvent> -> Async<unit>

type ICommandProcessor<'TCmd> =
  abstract Process : cmd:'TCmd -> unit
  abstract ProcessAsync : cmd:'TCmd -> Async<unit>

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
    
  interface ICommandProcessor<'TCmd> with
    member this.Process (cmd:'TCmd) = processCmd cmd |> Async.RunSynchronously
    member this.ProcessAsync (cmd:'TCmd) = processCmd cmd