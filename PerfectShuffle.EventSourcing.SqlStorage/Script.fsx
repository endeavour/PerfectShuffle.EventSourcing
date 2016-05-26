// Learn more about F# at http://fsharp.org. See the 'F# Tutorial' project
// for more guidance on F# programming.

//#load "EventRepository.fs"
//#r @"C:\Users\danielr\Source\Repos\perfectshuffle.eventsourcing\PerfectShuffle.EventSourcing.SqlStorage\bin\Debug\PerfectShuffle.EventSourcing.dll"
//#r @"C:\Users\danielr\Source\Repos\perfectshuffle.eventsourcing\PerfectShuffle.EventSourcing.SqlStorage\bin\Debug\PerfectShuffle.EventSourcing.SqlStorage.dll"

#load "Scripts/load-references-debug.fsx"
#load "Scripts/load-project-debug.fsx"

open PerfectShuffle.EventSourcing.SqlStorage
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control

// Define your library scripting code here

type MyEvent = {Name : string; Age : int}
let serializer = Serialization.CreateDefaultSerializer<MyEvent>()

let evtRespository = SqlStorage.EventRepository<MyEvent>(
  @"Data Source=(localdb)\mssqllocaldb;Initial Catalog=EventStore;Integrated Security=True",
  "Order-123",
  serializer
  ) 

let iStream = evtRespository :> IStream<MyEvent>

let saveEvents () = 

  async {
    for i = 1 to 100000 do
      let evts =
        [|
          {Name = "Bob"; Age=i}
        |]
      let wrappedEvents = evts |> Array.map EventWithMetadata<_>.Wrap
      let! result = iStream.Save (wrappedEvents) (WriteConcurrencyCheck.NewEventNumber i) 
      printfn "%A" result
  } |> Async.RunSynchronously


let events = iStream.EventsFrom 1 |> AsyncSeq.toBlockingSeq

for e in events do
  printfn "%d: %A" e.Version e.Event






