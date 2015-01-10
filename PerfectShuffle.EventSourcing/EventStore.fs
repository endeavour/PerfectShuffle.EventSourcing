namespace PerfectShuffle.EventSourcing
module Store =

  open System
  open System.Net
  open EventStore.ClientAPI

  /// Creates and opens an EventStore connection.
  let conn endPoint =   
      let conn = EventStoreConnection.Create(endPoint)      
      conn.ConnectAsync().RunSynchronously() //TODO: Make async
      conn

  type EventRepository<'TEvent>(conn:IEventStoreConnection, streamId:string, serialize:obj -> string * byte[], deserialize: Type * string * byte array -> obj) =
    let load () = async {
      let! eventsSlice = conn.ReadStreamEventsForwardAsync(streamId, 0, Int32.MaxValue, false) |> Async.AwaitTask
      return
        eventsSlice.Events
        |> Seq.map (fun e -> deserialize(typeof<EventWithMetadata<'TEvent>>, e.Event.EventType, e.Event.Data))    
        |> Seq.cast<EventWithMetadata<'TEvent>>
    }

    let commit (expectedVersion) (e:EventWithMetadata<'e>) = async {
        let eventType,data = serialize e
        let metaData = [||] : byte array
        let eventData = new EventData(Guid.NewGuid(), eventType, true, data, metaData)
        let expectedVersion = if expectedVersion = 0 then ExpectedVersion.NoStream else expectedVersion
        return! conn.AppendToStreamAsync(streamId, expectedVersion, eventData) |> Async.AwaitIAsyncResult |> Async.Ignore
    }

    member __.Load() = load()
    member __.Save(evt) = commit ExpectedVersion.Any evt

