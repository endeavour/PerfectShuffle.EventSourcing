  namespace PerfectShuffle.EventSourcing
  open System
  
  type Id = System.Guid
    
  type SerializedEvent =
    {
      TypeName : string
      Payload : byte[]
    }

  type EventToRecordMetadata = 
    {
      DeduplicationId : Guid 
      EventStamp : DateTime
    }

  type EventToRecord =
    {
      SerializedEventToRecord : SerializedEvent
      Metadata : EventToRecordMetadata
    }    

  type EventToRecord<'event> = 
    {      
      EventToRecord : 'event
      Metadata : EventToRecordMetadata
    }

  type RecordedMetadata = 
    {
      TypeName : string      
      CommitVersion : int64
      StreamName : string
      StreamVersion : int64
      DeduplicationId : Guid
      EventStamp : DateTime
      CommitStamp : DateTime
    }

  type RecordedEvent<'event> =
    {
      RecordedEvent : 'event
      Metadata : RecordedMetadata
    }

  type RawEvent =
    {
      Payload : byte[]  //TODO: Use SerializedEvent here instead
      Metadata : RecordedMetadata     
    }