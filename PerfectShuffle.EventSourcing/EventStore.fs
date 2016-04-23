namespace PerfectShuffle.EventSourcing

module Store =

  open System
  open System.Net

  type WriteConcurrencyCheck =
  | Any
  | NoStream
  | EmptyStream
  | NewEventNumber of int

  type WriteResult =
  | Success
  | ConcurrencyCheckFailed
  | WriteException of exn

  type NewEventArgs<'TEvent> = {EventNumber : int; Event : EventWithMetadata<'TEvent>}

  type IEventRepository<'TEvent> =
    abstract member Events : IObservable<NewEventArgs<'TEvent>>
    abstract member Save : events:EventWithMetadata<'TEvent>[] -> WriteConcurrencyCheck -> Async<WriteResult>

 