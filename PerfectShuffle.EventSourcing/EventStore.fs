namespace PerfectShuffle.EventSourcing
open FsCoreSerializer

  module EventStore =
   
    let private serializer = new BinaryFormatterSerializer() :> ISerializer
    
    let Deserialize<'TEvent>(inputStream:System.IO.Stream) =
      serializer.Deserialize(inputStream) :?> 'TEvent
      
    let Serialize<'TEvent>(outputStream:System.IO.Stream) : 'TEvent -> unit =  
      fun o -> serializer.WriteObj(outputStream, o)
    
    let ReadEvents<'TEvent>(inputStream:System.IO.Stream) =        
        seq {
        while (inputStream.Position < inputStream.Length) do      
          yield Deserialize<'TEvent>(inputStream)
        }