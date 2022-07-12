namespace PerfectShuffle.EventSourcing.SqlStorage

open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control

module SqlStorage =
  open System
  open System.Data
  open Microsoft.Data.SqlClient
  open System.Text

  type SqlDataProvider(connectionString:string, maxPollingDelay:System.TimeSpan) =
    
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
        
        do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore

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

    let readBatched
          startVersion
          batchSize
          (openConn : unit -> Async<'conn>)
          (getReader : int64 -> 'conn -> Async<'reader>)
          (hasMore : 'reader -> Async<bool>)
          (readRow : 'reader -> Async<'row>)
          (closeReader : 'reader -> Async<unit>)
          (getNextStart : int64 -> 'row[] -> Async<Option<int64>>)
          (closeConn : 'conn -> Async<unit>) =
      
      let rec readBatch (start:int64) =
        
        let rec readBatchAux (start:int64) connection =
        
          asyncSeq {
          try
            use! reader = getReader start connection
            let rec read() =
              asyncSeq {
              let! hasMore = hasMore reader
              if hasMore then
                let! row = readRow reader            
                yield row
                yield! read()
              }
       
            let! results = read() |> AsyncSeq.toArrayAsync // TODO: Do we really need to do this?
        
            for r in results do
              yield r

            do! closeReader reader

            let! newStart = getNextStart start results
        
            match newStart with
            | Some n -> yield! readBatchAux n connection
            | None -> do! closeConn connection  
          with e ->
            printfn "Error, retrying..."
            yield!
              asyncSeq {
              do! closeConn connection
              yield! readBatch start     
              }            
          }

        asyncSeq {
          try
            let! connection = openConn()
            yield! readBatchAux start connection
          with e ->
            yield! readBatch start
        }

      readBatch startVersion

    let batchSize = 1000

    let openConn() =
      async {
      let connection = new SqlConnection(connectionString)
      do! connection.OpenAsync() |> Async.AwaitTask
      return connection
      }

    let hasMore (reader:SqlDataReader) = reader.ReadAsync() |> Async.AwaitTask

    let closeReader (reader:SqlDataReader) =
      async {
      reader.Close()
      reader.Dispose()
      }

    let closeConn (conn:SqlConnection) =
      async {
      conn.Close()
      conn.Dispose()
      }    

    let readRow (reader:SqlDataReader) =
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
      
      async.Return { Payload = payload; Metadata = metadata}

    let getAllEvents fromCommitVersion =

      let maxDelay = maxPollingDelay.TotalMilliseconds

      let getReader (start:int64) connection =
        use cmd = new SqlCommand("usp_GetEvents", connection, CommandType = CommandType.StoredProcedure)
        cmd.Parameters.AddWithValue("FromCommitVersion", start) |> ignore
        cmd.Parameters.AddWithValue("BatchSize", batchSize) |> ignore
        cmd.ExecuteReaderAsync() |> Async.AwaitTask      

      let getNextStart (lastStart:int64) (batch:RawEvent[]) =        
        async {
        let delay = maxDelay - (maxDelay / float batchSize) * float batch.Length
        do! Async.Sleep (int delay)
        let newStart = if batch.Length > 0 then batch.[batch.Length - 1].Metadata.CommitVersion + 1L else lastStart 
        return Some newStart
        }

      readBatched fromCommitVersion batchSize openConn getReader hasMore readRow closeReader getNextStart closeConn

    let getStreamEvents streamName fromStreamVersion =
      let getReader (start:int64) connection =
        use cmd = new SqlCommand("usp_GetStreamEvents", connection, CommandType = CommandType.StoredProcedure)
        cmd.Parameters.AddWithValue("StreamName", streamName) |> ignore
        cmd.Parameters.AddWithValue("FromStreamVersion", start) |> ignore
        cmd.Parameters.AddWithValue("BatchSize", batchSize) |> ignore
        cmd.ExecuteReaderAsync() |> Async.AwaitTask  

      let getNextStart (lastStart:int64) (batch:RawEvent[]) =
        async {
        return
          if batch.Length = batchSize
            then
              Some <| batch.[batch.Length - 1].Metadata.StreamVersion + 1L
            else
              None
        }
     
      readBatched fromStreamVersion batchSize openConn getReader hasMore readRow closeReader getNextStart closeConn

    interface IAllEventReader with

      member __.GetAllEvents(fromCommitVersion: int64): AsyncSeq<RawEvent> = 
        getAllEvents fromCommitVersion

    interface IStreamDataProvider with
      
      member __.GetStreamEvents(streamName: string) (fromStreamVersion: int64): AsyncSeq<RawEvent> = 
        getStreamEvents streamName fromStreamVersion
      
      member __.SaveEvents(streamName: string) (concurrencyCheck: WriteConcurrencyCheck) (evts: EventToRecord []): Async<WriteResult> = 
        commit streamName concurrencyCheck evts

      member __.FirstVersion = 1L

  

