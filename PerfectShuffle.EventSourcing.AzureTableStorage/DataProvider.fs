namespace PerfectShuffle.EventSourcing.AzureTableStorage

open System
open PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.Store
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open FSharp.Control
open Streamstone

module AzureTableStorage =
  open System.Net
  open Microsoft.WindowsAzure.Storage
  open Microsoft.WindowsAzure.Storage.Table

  [<CLIMutable>]
  type EventEntity =
    {    
      Payload : byte[]
      EventType : string
      StreamName : string
      DeduplicationId : Guid
      EventStamp : DateTimeOffset
      CommitStamp : DateTimeOffset
    }

  /// Implementation of an event repository that connects to Azure Table Storage
  type AzureTableDataProvider(storageCredentials:Auth.StorageCredentials, tableName:string) =
   
    do
      let tableNameVerification = System.Text.RegularExpressions.Regex("^[A-Za-z][A-Za-z0-9]{2,62}$")
      assert (tableNameVerification.IsMatch tableName) // Please see storage naming rules at https://blogs.msdn.microsoft.com/jmstall/2014/06/12/azure-storage-naming-rules/"

    let tableClient =
      let cloudStorageAccount = CloudStorageAccount(storageCredentials, true)
      let tableClient = cloudStorageAccount.CreateCloudTableClient()
      tableClient

    let table = tableClient.GetTableReference(tableName)
  
    let createTableTask =
      async {
      do! table.CreateIfNotExistsAsync() |> Async.AwaitTask |> Async.Ignore
      } |> Async.StartAsTask
  
    let getStreamEvents streamName startIndex =    
      let sliceSize = 100
      let partition = Partition(table, streamName)

      let rec rawEventStreamAux startVersion =
        asyncSeq {
          do! createTableTask |> Async.AwaitTask
          printfn "%s: Reading from version %d" streamName startVersion    
          let! streamExists = Stream.ExistsAsync(partition) |> Async.AwaitTask
          let! stream = Stream.TryOpenAsync(partition)|> Async.AwaitTask
          if stream.Found then
            let! slice = Stream.ReadAsync<EventEntity>(partition, startVersion = startVersion, sliceSize = sliceSize) |> Async.AwaitTask      
            printfn "%s READING VERSION %d" streamName startVersion
            for i = 0 to slice.Events.Length - 1 do
              let evt = slice.Events.[i]
              printfn "%s \tREAD EVT: (%d/%d) Version %d\n\tEvent ID: %A" streamName (i+1) slice.Events.Length (slice.Stream.Version + i) evt.DeduplicationId
              let rawEvent =
                {
                  Payload = evt.Payload
                  Metadata =
                    {
                      TypeName = evt.EventType
                      CommitVersion = -1L
                      StreamName = streamName
                      StreamVersion = int64 (startVersion + i)
                      DeduplicationId = evt.DeduplicationId
                      EventStamp = evt.EventStamp.UtcDateTime
                      CommitStamp = evt.CommitStamp.UtcDateTime
                    }
                }
              yield rawEvent
        
            match slice.Events |> Array.tryLast with
            | Some evt when not slice.IsEndOfStream ->
               let newStart = if slice.Events.Length = 0 then startVersion else startVersion + slice.Events.Length
               yield! rawEventStreamAux newStart
            | _ -> ()
      }
      rawEventStreamAux startIndex

    let commit (streamName:string) (concurrencyCheck:WriteConcurrencyCheck) (evts:EventToRecord[]) : Async<WriteResult> =
      let partition = Partition(table, streamName)
      async {      
        if evts.Length = 0
          then
            return Choice2Of2 (WriteFailure.NoItems)
          else
            let batchId = Guid.NewGuid()

            let eventsData =
              evts
              |> Seq.mapi (fun i e ->
                let props =
                  [|
                    "Payload", EntityProperty.GeneratePropertyForByteArray(e.SerializedEventToRecord.Payload)
                    "EventType", EntityProperty.GeneratePropertyForString(e.SerializedEventToRecord.TypeName)
                    "StreamName", EntityProperty.GeneratePropertyForString(streamName)
                    "DeduplicationId", EntityProperty.GeneratePropertyForGuid(Nullable(e.Metadata.DeduplicationId))
                    "EventStamp", EntityProperty.GeneratePropertyForDateTimeOffset(Nullable(DateTimeOffset(e.Metadata.EventStamp)))
                    "CommitStamp", EntityProperty.GeneratePropertyForDateTimeOffset(Nullable(DateTimeOffset.UtcNow))
                  |]
                  |> dict
                  |> EventProperties.From          
                EventData(EventId.From(e.Metadata.DeduplicationId),  props)
              )
              |> Seq.toArray

            let (|AggregateOrSingleExn|) (e:exn) =
              match e with
              | :? System.AggregateException as e -> (e.InnerExceptions |> Seq.toList)
              | x -> [x]

            let tryWrite =
              let newEventNum =
                match concurrencyCheck with
                | NoStream | EmptyStream | NewEventNumber 0L -> Some 0L
                | NewEventNumber n -> Some n
                | Any -> None
              match newEventNum with
              | Some n ->
                async {
                  try
                    let! result = Stream.WriteAsync(partition, int n, eventsData) |> Async.AwaitTask
                    return Choice1Of2 (StreamVersion (int64 result.Stream.Version))                   
                  with
                    | AggregateOrSingleExn [:? ConcurrencyConflictException as e] ->
                      return WriteFailure.ConcurrencyCheckFailed |> Choice2Of2
                    |e ->
                      return WriteFailure.WriteException e |> Choice2Of2
                }
              | None ->
                async {
                  try
                    let! stream = Stream.TryOpenAsync(partition) |> Async.AwaitTask
                    let stream =
                      match stream.Found, stream.Stream with
                      | true, stream -> stream
                      | false, _ -> Stream(partition)

                    let! result = Stream.WriteAsync(stream, eventsData) |> Async.AwaitTask              
                    return Choice1Of2 (StreamVersion (int64 result.Stream.Version))       
                  with
                    | AggregateOrSingleExn [:? ConcurrencyConflictException as e] ->
                      return WriteFailure.ConcurrencyCheckFailed |> Choice2Of2                    
                    |e ->
                      return WriteFailure.WriteException e |> Choice2Of2           
                }

            let! result = tryWrite                
            return result
    }

    interface IStreamDataProvider with

      member x.GetStreamEvents(streamName: string) (fromStreamVersion: int64): AsyncSeq<RawEvent> = 
        getStreamEvents streamName (int fromStreamVersion)

      member x.SaveEvents(streamName: string) (concurrencyCheck: WriteConcurrencyCheck) (evts: EventToRecord []): Async<WriteResult> = 
        commit streamName concurrencyCheck evts
