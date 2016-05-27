﻿namespace PerfectShuffle.EventSourcing

module Store =

  open System
  open System.Net
  open FSharp.Control

  type WriteConcurrencyCheck =
  /// This disables the optimistic concurrency check.
  | Any
  /// this specifies the expectation that target stream does not yet exist.
  | NoStream
  /// this specifies the expectation that the target stream has been explicitly created, but does not yet have any user events written in it.
  | EmptyStream
  /// Any other integer value	The event number that you expect the stream to currently be at.
  | NewEventNumber of int64

  type WriteSuccess =
  | StreamVersion of int64

  type WriteFailure =
  | NoItems
  | ConcurrencyCheckFailed
  | WriteException of exn

  type WriteResult = Choice<WriteSuccess, WriteFailure>

  type Batch<'event> =
    {
      StartVersion : int64
      Events : EventToRecord<'event>[]
    }

  type Batch =
    {
      StartVersion : int64
      Events : EventToRecord[]
    }

  type RawEvent =
    {
      Payload : byte[] 
      Metadata : RecordedMetadata     
    }

  type IAllEventReader =
    abstract member GetAllEvents : fromCommitVersion:int64 -> AsyncSeq<RawEvent>

  type IStreamReader =
    abstract member GetStreamEvents : streamName:string -> fromStreamVersion:int64 -> AsyncSeq<RawEvent>

  type IStreamWriter =
    abstract member SaveEvents<'event> : streamName:string -> WriteConcurrencyCheck -> EventToRecord[] -> Async<WriteResult>

  type IDataProvider =
    inherit IAllEventReader
    inherit IStreamReader
    inherit IStreamWriter

  type IStream<'event> =
    abstract member FirstVersion : int64
    abstract member EventsFrom : version:int64 -> AsyncSeq<RecordedEvent<'event>>
    abstract member Save : events:EventToRecord<'event>[] -> currencyCheck:WriteConcurrencyCheck -> Async<WriteResult>

 