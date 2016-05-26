#load "Scripts/load-project-debug.fsx"

open PerfectShuffle.EventSourcing.SqlStorage
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control
open System

// Define your library scripting code here

type MyEvent = {Name : string; Age : int64}
let serializer = Serialization.CreateDefaultSerializer<MyEvent>()

let evtRespository = SqlStorage.EventRepository<MyEvent>(
  @"Data Source=(localdb)\mssqllocaldb;Initial Catalog=EventStore;Integrated Security=True",
  "Order-123",
  serializer
  ) 

let iStream = evtRespository :> IStream<MyEvent>

let saveEvents () = 

  async {
    for i = 1L to 1000L do
      let evts =
        [|
          {Name = "Bob"; Age=i}
        |]
      let wrappedEvents = evts |> Array.map (fun x-> {DeduplicationId = Guid.NewGuid(); Timestamp = DateTime.UtcNow; Event = x })
      let! result = iStream.Save (wrappedEvents) (WriteConcurrencyCheck.NewEventNumber i) 
      printfn "%A" result
  } |> Async.RunSynchronously


saveEvents ()

let events = iStream.EventsFrom 1L |> AsyncSeq.toBlockingSeq

for e in events do
  printfn "%d: %A" e.Metadata.StreamVersion e.Event






