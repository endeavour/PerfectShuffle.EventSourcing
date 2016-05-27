#load "Scripts/load-project-debug.fsx"

open PerfectShuffle.EventSourcing.SqlStorage
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control
open System

// Define your library scripting code here

type MyEvent = {Name : string; Age : int64}
let serializer = Serialization.CreateDefaultSerializer<MyEvent>()

let dataProvider = SqlStorage.SqlDataProvider(@"Data Source=(localdb)\mssqllocaldb;Initial Catalog=EventStore;Integrated Security=True")

let stream = SqlStorage.Stream<MyEvent>("Order-123", serializer, dataProvider) :> IStream<_>

let saveEvents () = 

  async {
    for i = 1L to 1000L do
      let evts =
        [|
          {Name = "Bob"; Age=i}
        |]
      let wrappedEvents : EventToRecord<MyEvent>[] = evts |> Array.map (fun e ->
        {Metadata = {DeduplicationId = Guid.NewGuid(); EventStamp = DateTime.UtcNow} ; EventToRecord = e})

      let! result = stream.Save wrappedEvents (WriteConcurrencyCheck.NewEventNumber i) 
      printfn "%A" result
  } |> Async.RunSynchronously


saveEvents ()

let events = stream.EventsFrom 1L |> AsyncSeq.toBlockingSeq

for e in events do
  printfn "%d: %A" e.Metadata.StreamVersion e.RecordedEvent






