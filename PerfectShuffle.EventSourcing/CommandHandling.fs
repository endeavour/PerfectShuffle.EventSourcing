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
