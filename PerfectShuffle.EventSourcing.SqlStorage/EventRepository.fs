namespace PerfectShuffle.EventSourcing.SqlStorage

open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control

module SqlStorage =
  open System
  open System.Data
  open System.Data.SqlClient
  open System.Transactions
  open System.Text

  type SqlDataProvider(connectionString:string) =
    
    let eventsFrom (streamName:string) (version:int64) = 
      let connection = new SqlConnection(connectionString)
      
      let batchSize = 1000L

      let rec readBatch (start:int64) =
        asyncSeq {
        use cmd = new SqlCommand("usp_GetStreamEvents", connection, CommandType = CommandType.StoredProcedure)
        cmd.Parameters.AddWithValue("StreamName", streamName) |> ignore
        cmd.Parameters.AddWithValue("FromStreamVersion", start) |> ignore
        cmd.Parameters.AddWithValue("BatchSize", batchSize) |> ignore
        let! reader = cmd.ExecuteReaderAsync() |> Async.AwaitTask
        
        let rec read() =
          asyncSeq {
          let! isMore = reader.ReadAsync() |> Async.AwaitTask
          if isMore then
            let commitVersion = reader.["CommitVersion"] :?> int64
            let payload = reader.["Payload"] :?> byte[]
            let eventType = reader.["EventType"] :?> string
            let streamVersion = reader.["StreamVersion"] :?> int64
            let streamName = reader.["StreamName"] :?> string
            let deduplicationId = reader.["DeduplicationId"] :?> Guid
            let eventTimestamp = reader.["EventStamp"] :?> DateTime
            let commitTimestamp = reader.["CommitStamp"] :?> DateTime
            let metadata = 
              {
                TypeName = eventType
                CommitVersion = commitVersion
                StreamName = streamName
                StreamVersion = streamVersion
                DeduplicationId = deduplicationId
                EventStamp = eventTimestamp
                CommitStamp = commitTimestamp
              }
            
            yield { Payload = payload; Metadata = metadata}
            yield! read()
          }
       
        let! results = read() |> AsyncSeq.toArrayAsync // TODO: Do we really need to do this?
        
        for r in results do
          yield r

        reader.Close()

        if results.Length = 100
          then yield! readBatch (start + batchSize)
          else connection.Dispose()
        }

      asyncSeq {
        do! connection.OpenAsync() |> Async.AwaitTask
        yield! readBatch version
      }

    let rec getInnerException (exn:Exception) =
      match exn with
      | :? System.AggregateException as e ->
        if e.InnerExceptions.Count = 1
          then getInnerException e.InnerException
          else exn
      | e -> e     

    let commit (streamName:string) (concurrencyCheck:WriteConcurrencyCheck) (evts:EventToRecord[]) = async {

      use dt = new DataTable()      
      let cols =
        [|
          "SeqNumber", typeof<int>
          "DeduplicationId", typeof<Guid>
          "EventType", typeof<string>
          "Headers", typeof<byte[]>
          "Payload", typeof<byte[]>
          "EventStamp", typeof<DateTime>
        |]

      cols |> Seq.iter (dt.Columns.Add >> ignore)

      evts
      |> Seq.mapi (fun i evt ->          
          let row = dt.NewRow()
          row.["SeqNumber"] <- i
          row.["DeduplicationId"] <- evt.Metadata.DeduplicationId
          row.["EventType"] <- evt.SerializedEventToRecord.TypeName
          row.["Headers"] <- ([||] : byte[]) // TODO!
          row.["Payload"] <- evt.SerializedEventToRecord.Payload
          row.["EventStamp"] <- evt.Metadata.EventStamp
          row
        )
      |> Seq.iter dt.Rows.Add



      use connection = new SqlConnection(connectionString)
      do! connection.OpenAsync() |> Async.AwaitTask
      let transactionOptions = TransactionOptions(IsolationLevel = IsolationLevel.ReadCommitted)     
      try
        use cmd = new SqlCommand("usp_StoreEvents", connection, CommandType = CommandType.StoredProcedure)
        cmd.Parameters.AddWithValue("StreamName", streamName) |> ignore
          
        let endVersionOutputParam = new SqlParameter("EndVersion", streamName)
        endVersionOutputParam.SqlDbType <- SqlDbType.BigInt
        endVersionOutputParam.Direction <- ParameterDirection.Output
        cmd.Parameters.Add(endVersionOutputParam) |> ignore

        match concurrencyCheck with
        | WriteConcurrencyCheck.NewEventNumber n ->
          cmd.Parameters.AddWithValue("ExpectedStartVersion", n) |> ignore
        | WriteConcurrencyCheck.EmptyStream | WriteConcurrencyCheck.NoStream -> 
          cmd.Parameters.AddWithValue("ExpectedStartVersion", 0) |> ignore // TODO: check this works
        | WriteConcurrencyCheck.Any -> ()
          
        cmd.Parameters.AddWithValue("EventList", dt) |> ignore
        
        let ts = new TransactionScope(TransactionScopeOption.Required, transactionOptions, TransactionScopeAsyncFlowOption.Enabled)
        try
          do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
          ts.Complete()
        finally
          ts.Dispose()

        let endVersion = endVersionOutputParam.Value :?> int64
        return WriteResult.Choice1Of2 (WriteSuccess.StreamVersion endVersion)
      with e ->        
        let innerException = getInnerException e
        match innerException with
        | :? SqlException as e when e.Number = 53001 ->
          return WriteResult.Choice2Of2 (WriteFailure.ConcurrencyCheckFailed)
        | e -> 
          return WriteResult.Choice2Of2 (WriteFailure.WriteException e)
    }

    interface IDataProvider with
      member x.GetAllEvents(fromCommitVersion: int64): AsyncSeq<RawEvent> = 
        failwith "Not implemented yet"
      member x.GetStreamEvents(streamName: string) (fromStreamVersion: int64): AsyncSeq<RawEvent> = 
        eventsFrom streamName fromStreamVersion
      member x.SaveEvents(streamName: string) (concurrencyCheck: WriteConcurrencyCheck) (evts: EventToRecord []): Async<WriteResult> = 
        commit streamName concurrencyCheck evts

  type Stream<'event>(streamName:string, serializer : Serialization.IEventSerializer<'event>, dataProvider:IDataProvider) =
  
    let eventsFrom version =
      dataProvider.GetStreamEvents streamName version
      |> AsyncSeq.map (fun rawEvent ->
            let event = serializer.Deserialize({TypeName = rawEvent.Metadata.TypeName; Payload = rawEvent.Payload})                        
            { RecordedEvent = event; Metadata = rawEvent.Metadata}
          )

    let commit concurrencyCheck (evts:EventToRecord<'event>[]) =
      let rawEvents =
        evts
        |> Array.map(fun evt ->
          let serializedEvent = evt.EventToRecord |> serializer.Serialize         
          let rawEvent : EventToRecord = { SerializedEventToRecord = serializedEvent; Metadata = evt.Metadata}
          rawEvent
          ) 
      
      dataProvider.SaveEvents streamName concurrencyCheck rawEvents

    interface IStream<'event> with
      member __.FirstVersion = 1L
      member __.EventsFrom version = eventsFrom version
      member __.Save evts concurrencyCheck = commit concurrencyCheck evts

