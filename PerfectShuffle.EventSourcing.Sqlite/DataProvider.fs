namespace PerfectShuffle.EventSourcing.Sqlite

open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control

module SqliteStorage =
  open System
  open System.Data
  open Microsoft.Data.Sqlite
  open System.Text

  type SqliteDataProvider(connectionString:string, maxPollingDelay:System.TimeSpan) =
    
    let writeGate = new System.Threading.SemaphoreSlim(1)

    let getStreamVersionQuery =
      """SELECT MAX(StreamVersion)
           FROM [Commit]
           WHERE StreamName = $streamName"""

    let getStreamEventsQuery =
      """SELECT
		       c.[CommitVersion],
		       c.[StreamName],
		       c.[StreamVersion],
		       c.[DeduplicationId],
		       c.[EventType],
		       c.[Headers], 
		       c.[Payload], 
		       c.[EventStamp], 
		       c.[CommitStamp] 
	        FROM [Commit] c 
	        WHERE 
	        c.[StreamName] = $streamName and 
	        c.[StreamVersion] >= $streamVersion
          LIMIT $limit"""

    let getEventsQuery =
      """SELECT
		    c.[CommitVersion],
		    c.[StreamName],
		    c.[StreamVersion],
		    c.[DeduplicationId],
		    c.[EventType],
		    c.[Headers], 
		    c.[Payload], 
		    c.[EventStamp], 
		    c.[CommitStamp] 
	      FROM [Commit] c 
	      WHERE 	
	      c.[CommitVersion] >= $commitVersion
        LIMIT $limit"""

    let insertQuery =
      """INSERT INTO [Commit] (
                         StreamName,
                         StreamVersion,
                         DeduplicationId,
                         EventType,
                         Headers,
                         Payload,
                         EventStamp,
                         CommitStamp
                     )
                     VALUES (
                         $streamName,
                         $streamVersion,
                         $deduplicationId,
                         $eventType,
                         $headers,
                         $payload,
                         $eventStamp,
                         $commitStamp
                     );"""

    let rec getInnerException (exn:Exception) =
      match exn with
      | :? System.AggregateException as e ->
        if e.InnerExceptions.Count = 1
          then getInnerException e.InnerException
          else exn
      | e -> e     

    let commit (streamName:string) (concurrencyCheck:WriteConcurrencyCheck) (evts:EventToRecord[]) = async {

      use connection = new SqliteConnection(connectionString)
      do! connection.OpenAsync() |> Async.AwaitTask

      use streamVersionCmd = new SqliteCommand(getStreamVersionQuery, connection)
      streamVersionCmd.Parameters.AddWithValue("$streamName", streamName) |> ignore<SqliteParameter>
      let! result = streamVersionCmd.ExecuteScalarAsync() |> Async.AwaitTask
      let streamVersion =
        match result with
        | :? System.DBNull -> 0L
        | :? int64 as n -> n
        | _ -> failwith "Invalid return type"

      let currentStreamVersion = streamVersion



      try

        match concurrencyCheck with
        | WriteConcurrencyCheck.NewEventNumber n when n <> currentStreamVersion ->
          return WriteResult.Error (WriteFailure.ConcurrencyCheckFailed)
        | WriteConcurrencyCheck.EmptyStream
        | WriteConcurrencyCheck.NoStream when currentStreamVersion <> 0L ->
          return WriteResult.Error (WriteFailure.ConcurrencyCheckFailed)
        | _ ->
          
          let cmds =
            evts
            |> Seq.mapi (fun i evt ->
              let cmd = new SqliteCommand(insertQuery, connection)
              cmd.Parameters.AddWithValue("$streamName", streamName) |> ignore<SqliteParameter>
              cmd.Parameters.AddWithValue("$streamVersion", int64 i + currentStreamVersion + 1L) |> ignore<SqliteParameter>
              cmd.Parameters.AddWithValue("$deduplicationId", evt.Metadata.DeduplicationId) |> ignore<SqliteParameter>
              cmd.Parameters.AddWithValue("$eventType", evt.SerializedEventToRecord.TypeName) |> ignore<SqliteParameter>
              cmd.Parameters.AddWithValue("$headers", ([||] : byte[])) |> ignore<SqliteParameter>
              cmd.Parameters.AddWithValue("$payload", (evt.SerializedEventToRecord.Payload)) |> ignore<SqliteParameter>
              cmd.Parameters.AddWithValue("$eventStamp", evt.Metadata.EventStamp) |> ignore<SqliteParameter>
              cmd.Parameters.AddWithValue("$commitStamp", DateTime.UtcNow) |> ignore<SqliteParameter>
              cmd 
            )
            |> Seq.toArray

          for cmd in cmds do
            do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore

          let endVersion = currentStreamVersion + int64 evts.Length
          return WriteResult.Ok (WriteSuccess.StreamVersion (int64 endVersion))
      with e ->        
        return WriteResult.Error (WriteFailure.WriteException e)
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
      
      let rec readBatch (connection:'conn) (start:int64) =
        asyncSeq {
        let! reader = getReader start connection
        
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
        | Some n -> yield! readBatch connection n
        | None -> do! closeConn connection       
        }

      asyncSeq {
        let! connection = openConn()
        yield! readBatch connection startVersion
      }

    let batchSize = 1000

    let openConn() =
      async {
      let connection = new SqliteConnection(connectionString)
      do! connection.OpenAsync() |> Async.AwaitTask
      return connection
      }
    
    let hasMore (reader:Common.DbDataReader) = reader.ReadAsync() |> Async.AwaitTask

    let closeReader (reader:Common.DbDataReader) =
      async {
      reader.Close()
      reader.Dispose()
      }

    let closeConn (conn:SqliteConnection) =
      async {
      conn.Close()
      conn.Dispose()
      }    

    let readRow (reader:Common.DbDataReader) =
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
        use cmd = new SqliteCommand(getEventsQuery, connection)
        cmd.Parameters.AddWithValue("$commitVersion", start) |> ignore
        cmd.Parameters.AddWithValue("$limit", batchSize) |> ignore
        cmd.ExecuteReaderAsync() |> Async.AwaitTask      

      let getNextStart (lastStart:int64) (batch:RawEvent[]) =        
        async {
        let delay = maxDelay - (maxDelay / float batchSize) * float batch.Length
        do! Async.Sleep (int delay)
        let newStart = if batch.Length > 0 then batch.[batch.Length - 1].Metadata.CommitVersion + 1L else lastStart 
        return Some newStart
        }

      readBatched fromCommitVersion batchSize openConn getReader hasMore readRow closeReader getNextStart closeConn

    let getStreamEvents (streamName:string) (fromStreamVersion:int64) =
      let getReader (start:int64) connection =
        use cmd = new SqliteCommand(getStreamEventsQuery, connection)
        cmd.Parameters.AddWithValue("$streamName", streamName) |> ignore
        cmd.Parameters.AddWithValue("$streamVersion", start) |> ignore
        cmd.Parameters.AddWithValue("$limit", batchSize) |> ignore

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
        async {
        do! writeGate.WaitAsync() |> Async.AwaitTask
        let! result = commit streamName concurrencyCheck evts     
        writeGate.Release() |> ignore<int>
        return result   
        }

      member __.FirstVersion = 1L

  

