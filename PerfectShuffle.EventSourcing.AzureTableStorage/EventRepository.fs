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

/// Implementation of an event repository that connects to Greg Young's EventStore (www.geteventstore.com)
type EventRepository<'TEvent>(azureStorageAccountName:string, storageCredentials:Auth.StorageCredentials, tableName:string, serializer : Serialization.IEventSerializer<'TEvent>) =
    
  let tableClient =
    let cloudStorageAccount = CloudStorageAccount(storageCredentials, true)
    let tableClient = cloudStorageAccount.CreateCloudTableClient()
    tableClient

//  let tableEntity =
//    { new ITableEntity with
//        member __.PartitionKey
//          with get() = failwith "foo"
//          and set (value) = failwith "foo"
//        member __.ETag
//          with get() = failwith "foo"
//          and set (value) = failwith "foo"
//        member __.RowKey
//          with get() = failwith "foo"
//          and set (value) = failwith "foo"
//        member __.Timestamp
//          with get() = failwith "foo"
//          and set (value) = failwith "foo"
//        member __.WriteEntity(ctx) = failwith "foo"
//      }

  let unpack (e:ResolvedEvent) =      
    serializer.Deserialize({TypeName = e.Event.EventType; Payload = e.Event.Data})


  let getTableRows () = asyncSeq {
    let table = tableClient.GetTableReference(tableName)
    let! created = table.CreateIfNotExistsAsync() |> Async.AwaitTask
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

  let commit (concurrencyCheck:WriteConcurrencyCheck) (evts:EventWithMetadata<'TEvent>[]) = async {
      let eventsData =
        evts |> Array.map (fun e ->
          let serializedEvent = serializer.Serialize e
          let metaData = [||] : byte array
          new EventData(Guid.NewGuid(), serializedEvent.TypeName, true, serializedEvent.Payload, metaData)          
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