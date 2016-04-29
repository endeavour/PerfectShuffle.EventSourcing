namespace PerfectShuffle.EventSourcing.AzureTableStorage

open System
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open FSharp.Control
open Streamstone

module EventStore =
  open System.Net
  open Microsoft.WindowsAzure.Storage
  open Microsoft.WindowsAzure.Storage.Table

[<CLIMutable>]
type EventEntity =
  {    
    Id : Guid
    TypeName : string
    Payload : byte[]
    Version : int
  }

/// Implementation of an event repository that connects to Azure Table Storage
type EventRepository<'TEvent>(storageCredentials:Auth.StorageCredentials, tableName:string, partitionName:string, serializer : Serialization.IEventSerializer<'TEvent>) =
    
  let tableClient =
    let cloudStorageAccount = CloudStorageAccount(storageCredentials, true)
    let tableClient = cloudStorageAccount.CreateCloudTableClient()
    tableClient

  let table = tableClient.GetTableReference(tableName)
  
  // TODO: Remove this line from production
//  do table.DeleteIfExists() |> ignore
//  do table.Create() |> ignore
  
  let partition = Partition(table, partitionName)

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

  // TODO: Continue reading stream forever. If we get to end of stream wait some amount of time then try reading again (exponential back-off with a cap?)
  let rawEventStream() =    
    let sliceSize = 1
    let rec rawEventStreamAux startVersion =

      asyncSeq {
        //printfn "Reading from version %d" startVersion
        let! slice = Stream.ReadAsync<EventEntity>(partition, startVersion = startVersion, sliceSize = sliceSize) |> Async.AwaitTask      
        printfn "READING VERSION %d" startVersion
        for evt in slice.Events do
          printfn "\tREAD EVT: %d %A" evt.Version evt.Id
          yield evt
        
        match slice.Events |> Array.tryLast with
        | None ->
          ()
        | Some evt ->
          if slice.IsEndOfStream
            then () //TODO: Sleep and retry?
            else
              yield! rawEventStreamAux (evt.Version + 1)
    }
    rawEventStreamAux 1

  let deserializedEventStream() =
    rawEventStream()
    //TODO: BufferBy version #
    |> AsyncSeq.map (fun x ->
      let serializedEvent : Serialization.SerializedEvent =
        { 
          TypeName = x.TypeName
          Payload = x.Payload
        }
      let evt = serializer.Deserialize(serializedEvent)
      { StartVersion = x.Version; Events = [|evt|]})
    

  let commit (concurrencyCheck:WriteConcurrencyCheck) (evts:EventWithMetadata<'TEvent>[]) : Async<WriteResult> =
    async {      
      if evts.Length = 0
        then
          return Choice2Of2 (WriteFailure.NoItems)
        else
          let batchId = Guid.NewGuid()

          let eventsData =
            evts
            |> Seq.map (fun e ->
              let serializedEvent = serializer.Serialize e              
              e.Id, serializedEvent.TypeName, serializedEvent.Payload
            )
            |> Seq.map (fun (guid, typeName, payload) ->
              let props =
                [|
                  "Id", EntityProperty.GeneratePropertyForGuid(Nullable(guid))
                  "TypeName", EntityProperty.GeneratePropertyForString(typeName)
                  "Payload", EntityProperty.GeneratePropertyForByteArray(payload)
                |]
                |> dict
                |> EventProperties.From          
              EventData(EventId.From(guid),  props)
            )
            |> Seq.toArray

          let write =
            match concurrencyCheck with
            | NoStream | EmptyStream | NewEventNumber 0 ->
              async {
                try
                  let! result = Stream.WriteAsync(partition, 0, eventsData) |> Async.AwaitTask              
                  return Choice1Of2 (StreamVersion (result.Stream.Version))
                with
                  | :? ConcurrencyConflictException as e ->
                    return WriteFailure.ConcurrencyCheckFailed |> Choice2Of2
                  |e ->
                    return WriteFailure.WriteException e |> Choice2Of2    
              }
            | NewEventNumber n ->
              async {
                try
                  let! result = Stream.WriteAsync(partition, n, eventsData) |> Async.AwaitTask              
                  return Choice1Of2 (StreamVersion (result.Stream.Version))                   
                with
                  | :? ConcurrencyConflictException as e ->
                    return WriteFailure.ConcurrencyCheckFailed |> Choice2Of2
                  |e ->
                    return WriteFailure.WriteException e |> Choice2Of2
              }
            | Any ->
              async {
                try
                  let! stream = Stream.TryOpenAsync(partition) |> Async.AwaitTask
                  let stream =
                    match stream.Found, stream.Stream with
                    | true, stream -> stream
                    | false, _ -> Stream(partition)

                  let! result = Stream.WriteAsync(stream, eventsData) |> Async.AwaitTask              
                  return Choice1Of2 (StreamVersion (result.Stream.Version))       
                with
                  | :? ConcurrencyConflictException as e ->
                    return WriteFailure.ConcurrencyCheckFailed |> Choice2Of2
                  |e ->
                    return WriteFailure.WriteException e |> Choice2Of2           
              }

          let! result = write                
          return result
  }

  let eventsObservable =
    deserializedEventStream()
    |> AsyncSeq.toObservable     

  interface IEventRepository<'TEvent> with
    member __.Events = eventsObservable
    member __.Save evts concurrencyCheck = commit concurrencyCheck evts