namespace PerfectShuffle.EventSourcing.AzureTableStorage

open System
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open FSharp.Control

module EventStore =
  open System.Net
  open Microsoft.WindowsAzure.Storage
  open Microsoft.WindowsAzure.Storage.Table
  /// Creates and opens an EventStore connection.
  let Connect (storageUri : StorageUri, credentials : Auth.StorageCredentials) =   
      let credentials = Auth.StorageCredentials("pseventstoretest", "TPrq6CzszWwTpWcHwXTJ7Nc0xCHaSP9SvwdJkCcwcmQcmiPyK9DoIzoo45cfLc1L3HPboksozbMzNsVn3hgL3A==")
      let cloudStorageAccount = CloudStorageAccount(credentials, true)
      let tableClient = cloudStorageAccount.CreateCloudTableClient()
      tableClient

type EventTypeWrapper(tableEntity:DynamicTableEntity) =

  member this.EventId
    with get() = tableEntity.["EventId"].GuidValue
    and set value = tableEntity.["EventId"].GuidValue <- value

  member this.EventNumber
    with get() = tableEntity.["EventNumber"].Int32Value
    and set value = tableEntity.["EventNumber"].Int32Value <- value

  member this.EventType
    with get() = tableEntity.["EventType"].StringValue
    and set value = tableEntity.["EventType"].StringValue <- value

  member this.Payload
    with get() = tableEntity.["Payload"].StringValue
    and set value = tableEntity.["Payload"].StringValue <- value

  member this.EventTimestamp
    with get() = tableEntity.["EventTimestamp"].DateTime
    and set value = tableEntity.["EventTimestamp"].DateTime <- value


/// Implementation of an event repository that connects to Azure Table Storage
type EventRepository<'TEvent>(azureStorageAccountName:string, storageCredentials:Auth.StorageCredentials, tableName:string, serializer : Serialization.IEventSerializer<'TEvent>) =
    
  let tableClient =
    let cloudStorageAccount = CloudStorageAccount(storageCredentials, true)
    let tableClient = cloudStorageAccount.CreateCloudTableClient()
    tableClient

  let table = tableClient.GetTableReference(tableName)

  let getTableRows () = asyncSeq {
    
    let fetchAll = TableQuery<_>()
    let! firstPage = table.ExecuteQuerySegmentedAsync<DynamicTableEntity>(fetchAll, null) |> Async.AwaitTask
    for r in firstPage.Results do
      yield r    
    let mutable continuationToken : TableContinuationToken = firstPage.ContinuationToken
    while continuationToken <> null do
      let! page = table.ExecuteQuerySegmentedAsync<DynamicTableEntity>(fetchAll, continuationToken) |> Async.AwaitTask
      continuationToken <- page.ContinuationToken
      for r in page.Results do
        yield r
  }

  let load() =
    getTableRows()
    |> AsyncSeq.map (fun x ->
      let wrapped = EventTypeWrapper x
      serializer.Deserialize {TypeName = wrapped.EventType; Payload = System.Text.Encoding.UTF8.GetBytes wrapped.Payload}
      )

  let commit (concurrencyCheck:WriteConcurrencyCheck) (evts:EventWithMetadata<'TEvent>[]) = async {
      let eventsData =
        evts |> Array.map (fun e ->
          let serializedEvent = serializer.Serialize e
          // TODO: Maybe the serializer should return text instead?
          let jsonPayload = System.Text.Encoding.UTF8.GetString serializedEvent.Payload
          Guid.NewGuid(), serializedEvent.TypeName, serializedEvent.Payload
        )

//      let expectedVersion =
//        match concurrencyCheck with
//        | Any -> ExpectedVersion.Any
//        | NoStream -> ExpectedVersion.NoStream
//        | EmptyStream -> ExpectedVersion.EmptyStream
//        | NewEventNumber n -> n - 1

      let saveEvents (firstEventNumber : Option<int>) =
        let batchOperation : TableBatchOperation =
          let operations =
            evts
            |> Seq.mapi (fun i evt ->
              let entity = DynamicTableEntity()
              let wrapped = EventTypeWrapper(entity)
              wrapped.EventId <- Nullable(evt.Id)
              wrapped.EventNumber <- firstEventNumber + i
              wrapped.EventType <- evt.Timestamp
              )
          let op = TableBatchOperation()
          seq {
            yield TableOperation.Insert entity
          } |> Seq.iter op.Add

        table.ExecuteBatchAsync(operation) |> Async.AwaitTask
        table.ExecuteAsync()
        async {
          table
        }

      let foo =
        match concurrencyCheck with
        | NoStream ->
          async {
            let! tableExists = table.ExistsAsync() |> Async.AwaitTask
            if tableExists
              then
                return failwith "Table should not exist already"
              else
                let! created = table.CreateAsync() |> Async.AwaitTask
                return failwith "DIE"
          }
        | EmptyStream | NewEventNumber 0 ->
        | NewEventNumber n ->

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

  let eventsObservable =

    {new System.IObservable<_> with
      member __.Subscribe(observer) =

        let f = System.Action<_, ResolvedEvent>(fun _ evt ->
            let unpacked = unpack evt
            let eventNumber = evt.OriginalEventNumber
            let evt = {EventNumber = eventNumber; Event = unpacked}
            observer.OnNext evt              
          )
      
        let subscription = conn.SubscribeToStreamFrom(streamId, EventStore.ClientAPI.StreamCheckpoint.StreamStart, false, f)

        {new IDisposable with member __.Dispose() = subscription.Stop()}                
      }      

  interface IEventRepository<'TEvent> with
    member __.Events = eventsObservable
    member __.Save evts concurrencyCheck = commit concurrencyCheck evts