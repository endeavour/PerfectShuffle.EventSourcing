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

// Define your library scripting code here

type MyEvent = {Name : string; Age : int}
let serializer = Serialization.CreateDefaultSerializer<MyEvent>()

let evtRespository = SqlStorage.EventRepository<MyEvent>(
  @"Data Source=(localdb)\mssqllocaldb;Initial Catalog=EventStore;Integrated Security=True",
  "Order-123",
  serializer
  ) 

let iStream = evtRespository :> IStream<MyEvent>

let someEvents = [|
  {Name = "Bob"; Age=21}
  {Name = "Bill"; Age=32}
  |]

let wrappedEvents = someEvents |> Array.map EventWithMetadata<_>.Wrap

iStream.Save (wrappedEvents) (WriteConcurrencyCheck.NewEventNumber 29) |> Async.RunSynchronously

