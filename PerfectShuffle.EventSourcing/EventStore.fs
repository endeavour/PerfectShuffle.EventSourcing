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

  type EventRepository<'TEvent>(conn:IEventStoreConnection, streamId:string, serialize:obj -> string * byte[], deserialize: Type * string * byte array -> obj) =
    
    let unpack (e:ResolvedEvent) =
      deserialize(typeof<EventWithMetadata<'TEvent>>, e.Event.EventType, e.Event.Data) :?> EventWithMetadata<'TEvent>
    
    let load () = async {
      let! eventsSlice = conn.ReadStreamEventsForwardAsync(streamId, 0, Int32.MaxValue, false) |> Async.AwaitTask
      return
        eventsSlice.Events |> Seq.map unpack
    }

    let commit (concurrencyCheck:WriteConcurrencyCheck) (evts:EventWithMetadata<'e>[]) = async {
        let eventsData =
          evts |> Array.map (fun e ->
            let eventType,data = serialize e
            let metaData = [||] : byte array
            new EventData(Guid.NewGuid(), eventType, true, data, metaData)          
          )

        let expectedVersion =
          match concurrencyCheck with
          | Any -> ExpectedVersion.Any
          | NoStream -> ExpectedVersion.NoStream
          | EmptyStream -> ExpectedVersion.EmptyStream
          | NewEventNumber n -> n - 1

        let! result =
          conn.AppendToStreamAsync(streamId, expectedVersion, eventsData)
          |> Async.AwaitTask
          |> Async.Catch
        
        let rec getInnerException (exn:Exception) =
          match exn with
          | :? System.AggregateException as e ->
            if e.InnerExceptions.Count = 1
              then getInnerException e.InnerException
              else exn
          | e -> e
        
        let r =
          match result with
          | Choice.Choice1Of2(writeResult) -> Success
          | Choice.Choice2Of2(exn) ->
            match getInnerException exn with
            | :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException -> ConcurrencyCheckFailed
            | e -> WriteException(e)            
        return r
    }

    let subscribe (observer : NewEventArgs<'TEvent> -> unit) =
      let f = System.Action<_, ResolvedEvent>(fun _ evt ->
          let unpacked = unpack evt
          let eventNumber = evt.OriginalEventNumber
          observer ({EventNumber = eventNumber; Event = unpacked})
        )
      
      let subscription = conn.SubscribeToStreamFrom(streamId, EventStore.ClientAPI.StreamCheckpoint.StreamStart, false, f)
      
      {new IDisposable with member __.Dispose() = subscription.Stop()}

    member __.Subscribe(observer) = subscribe observer
    member __.Save(evts, concurrencyCheck) = commit concurrencyCheck evts

