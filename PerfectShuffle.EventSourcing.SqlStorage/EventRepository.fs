namespace PerfectShuffle.EventSourcing.SqlStorage

open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open FSharp.Control

module SqlStorage =
  open System
  open System.Data
  open System.Data.SqlClient
  open System.Transactions

  type EventRepository<'event>(connectionString:string, streamId:string, serializer : Serialization.IEventSerializer<'event>) =
    
    let eventsFrom version = 
      AsyncSeq.empty

    let commit (concurrencyCheck:WriteConcurrencyCheck) (evts:EventWithMetadata<'event>[]) = async {

      use dt = new DataTable()      
      let cols =
        [|
          "SeqNumber", typeof<int>
          "DeduplicationId", typeof<Guid>
          "EventType", typeof<string>
          "Headers", typeof<string>
          "Payload", typeof<string>
          "EventStamp", typeof<DateTime>
        |]

      cols |> Seq.iter (dt.Columns.Add >> ignore)

      evts
      |> Seq.mapi (fun i evt ->
          let serializedEvent = serializer.Serialize evt
          let row = dt.NewRow()
          row.["SeqNumber"] <- i
          row.["DeduplicationId"] <- evt.Id
          row.["EventType"] <- serializedEvent.TypeName
          row.["Headers"] <- "" // TODO!
          row.["Payload"] <- serializedEvent.Payload
          row.["EventStamp"] <- evt.Timestamp
          row
        )
      |> Seq.iter dt.Rows.Add



      use connection = new SqlConnection(connectionString)
      connection.Open()
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
        
        using (new TransactionScope(TransactionScopeOption.Required, transactionOptions)) (fun ts ->
          cmd.ExecuteNonQuery() |> ignore<int>
          ts.Complete()
          )
        let endVersion = Convert.ToInt32(endVersionOutputParam.Value)
        return WriteResult.Choice1Of2 (WriteSuccess.StreamVersion endVersion)
      with
        | :? SqlException as e when e.Number = 53001 ->
          return WriteResult.Choice2Of2 (WriteFailure.ConcurrencyCheckFailed)
        | e -> 
          return WriteResult.Choice2Of2 (WriteFailure.WriteException e)
    }

    interface IStream<'event> with
      member __.FirstVersion = 1
      member __.EventsFrom version = eventsFrom version
      member __.Save evts concurrencyCheck = commit concurrencyCheck evts

