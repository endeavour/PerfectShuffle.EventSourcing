namespace PerfectShuffle.EventSourcing
module Store =

  open System
  open System.Net
  open EventStore.ClientAPI

  /// Creates and opens an EventStore connection.
  let conn endPoint =   
      let conn = EventStoreConnection.Create(endPoint)      
      conn.ConnectAsync().Wait()
      conn

  type EventRepository<'TEvent>(conn:IEventStoreConnection, streamId:string, serialize:obj -> string * byte[], deserialize: Type * string * byte array -> obj) =
    
    let unpack (e:ResolvedEvent) =
      deserialize(typeof<EventWithMetadata<'TEvent>>, e.Event.EventType, e.Event.Data) :?> EventWithMetadata<'TEvent>
      
    
    let load () = async {
      let! eventsSlice = conn.ReadStreamEventsForwardAsync(streamId, 0, Int32.MaxValue, false) |> Async.AwaitTask
      return
        eventsSlice.Events |> Seq.map unpack
    }

    let commit (expectedVersion) (e:EventWithMetadata<'e>) = async {
        let eventType,data = serialize e
        let metaData = [||] : byte array
        let eventData = new EventData(Guid.NewGuid(), eventType, true, data, metaData)
        let expectedVersion = if expectedVersion = 0 then ExpectedVersion.NoStream else expectedVersion
        return! conn.AppendToStreamAsync(streamId, expectedVersion, eventData) |> Async.AwaitIAsyncResult |> Async.Ignore
    }

    let subscribe(observer : (EventStoreSubscription * EventWithMetadata<'TEvent>) -> unit) =
      let f = System.Action<EventStoreSubscription, ResolvedEvent>(fun sub evt ->
          let unpacked = unpack evt
          observer (sub, unpacked)
        )
      let subscription = conn.SubscribeToStreamAsync(streamId, false, f) |> Async.AwaitTask |> Async.RunSynchronously
      
      {new IDisposable with member __.Dispose() = subscription.Dispose()}

    member __.Subscribe(observer) = subscribe observer
    member __.Load() = load()
    member __.Save(evt) = commit ExpectedVersion.Any evt

