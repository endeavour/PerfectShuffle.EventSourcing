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

  type EventRepository<'event>(connectionString:string, streamId:string, serializer : Serialization.IEventSerializer<'event>) =
    
    let eventsFrom (version:int) = 
      let connection = new SqlConnection(connectionString)
      
      let batchSize = 100

      let rec readBatch (start:int) =
        asyncSeq {
        use cmd = new SqlCommand("usp_GetStreamEvents", connection, CommandType = CommandType.StoredProcedure)
        cmd.Parameters.AddWithValue("StreamName", streamId) |> ignore
        cmd.Parameters.AddWithValue("FromStreamVersion", start) |> ignore
        cmd.Parameters.AddWithValue("BatchSize", batchSize) |> ignore
        let! reader = cmd.ExecuteReaderAsync() |> Async.AwaitTask
        
        let rec read() =
          asyncSeq {
          let! isMore = reader.ReadAsync() |> Async.AwaitTask
          if isMore then
            let payload = reader.["Payload"] :?> byte[]
            let eventType = reader.["EventType"] :?> string
            let version = reader.["StreamVersion"] :?> int64
            let event = serializer.Deserialize({TypeName = eventType; Payload = payload})            
            yield { Event = event.Event; Metadata = event.Metadata; Version = int version }
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

    let commit (concurrencyCheck:WriteConcurrencyCheck) (evts:EventWithMetadata<'event>[]) = async {

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
          let serializedEvent = serializer.Serialize evt
          let row = dt.NewRow()
          row.["SeqNumber"] <- i
          row.["DeduplicationId"] <- evt.Metadata.Id
          row.["EventType"] <- serializedEvent.TypeName
          row.["Headers"] <- ([||] : byte[]) // TODO!
          row.["Payload"] <- serializedEvent.Payload
          row.["EventStamp"] <- evt.Metadata.Timestamp
          row
        )
      |> Seq.iter dt.Rows.Add



      use connection = new SqlConnection(connectionString)
      do! connection.OpenAsync() |> Async.AwaitTask
      let transactionOptions = TransactionOptions(IsolationLevel = IsolationLevel.ReadCommitted)     
      try
        use cmd = new SqlCommand("usp_StoreEvents", connection, CommandType = CommandType.StoredProcedure)
        cmd.Parameters.AddWithValue("StreamName", streamId) |> ignore
          
        let endVersionOutputParam = new SqlParameter("EndVersion", streamId)
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

        let endVersion = Convert.ToInt32(endVersionOutputParam.Value)
        return WriteResult.Choice1Of2 (WriteSuccess.StreamVersion endVersion)
      with e ->        
        let innerException = getInnerException e
        match innerException with
        | :? SqlException as e when e.Number = 53001 ->
          return WriteResult.Choice2Of2 (WriteFailure.ConcurrencyCheckFailed)
        | e -> 
          return WriteResult.Choice2Of2 (WriteFailure.WriteException e)
    }

    interface IStream<'event> with
      member __.FirstVersion = 1
      member __.EventsFrom version = eventsFrom version
      member __.Save evts concurrencyCheck = commit concurrencyCheck evts

