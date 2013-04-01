namespace PerfectShuffle.EventSourcing

type Id = int64
type EventWithMetadata<'event> = {Id : Id; Timestamp : System.DateTime; Event : 'event}

type CmdOutput<'event> =
| Success of seq<EventWithMetadata<'event>>
| Failure of exn

type CommandHandler<'cmd,'event> = 'cmd -> Async<CmdOutput<'event>>

// For C# and autofac friendliness
type ICommandHandler<'cmd,'event> =
  abstract Handle : cmd:'cmd -> Async<CmdOutput<'event>>

type EventSerializer<'event> = seq<EventWithMetadata<'event>> -> Async<unit>

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
        readModel.Apply (evts |> Seq.map (fun x -> x.Event))
      | Failure ex -> raise ex
    }
    
  interface ICommandProcessor<'TCmd> with
    member this.Process (cmd:'TCmd) = processCmd cmd |> Async.RunSynchronously
    member this.ProcessAsync (cmd:'TCmd) = processCmd cmd