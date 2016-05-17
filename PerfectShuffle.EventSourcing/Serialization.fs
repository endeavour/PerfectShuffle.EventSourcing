namespace PerfectShuffle.EventSourcing

module Serialization =

  type SerializedEvent =
    {
      TypeName : string
      Payload : byte[]
    }

  type IEventSerializer<'event> =
    abstract member Serialize : EventWithMetadata<'event> -> SerializedEvent
    abstract member Deserialize : SerializedEvent -> EventWithMetadata<'event>

  open System
  open System.IO
  open System.Text
  
  module private JsonNet =

      open System.Collections.Generic
      open Newtonsoft.Json
      open Newtonsoft.Json.FSharp
      open Newtonsoft.Json.Serialization
      open Newtonsoft.Json.Converters
      open Microsoft.FSharp.Reflection
          
      let s = new JsonSerializer()
      
      s.ContractResolver <- CamelCasePropertyNamesContractResolver ()      
      s.Formatting <- Formatting.Indented
      [|
        BigIntConverter() :> JsonConverter
        CultureInfoConverter() :> JsonConverter
        GuidConverter() :> JsonConverter
        ListConverter() :> JsonConverter
        MapConverter() :> JsonConverter
        OptionConverter() :> JsonConverter
        TupleArrayConverter() :> JsonConverter
        UnionConverter() :> JsonConverter
        //IdiomaticDuConverter() :> JsonConverter
        UriConverter() :> JsonConverter
      |] |> Seq.iter (s.Converters.Add)
      s.NullValueHandling <- NullValueHandling.Ignore
           
      let serialize o =
        try
          use ms = new MemoryStream()
          (use jsonWriter = new JsonTextWriter(new StreamWriter(ms))
          s.Serialize(jsonWriter, o))
          let data = ms.ToArray()
          (o.GetType().Name),data
        with e ->
          let a = e
          raise e

      let deserialize (t, data:byte array) =
          use ms = new MemoryStream(data)
          use sr = new StreamReader(ms)
          use jsonReader = new JsonTextReader(sr)
          s.Deserialize(jsonReader, t)

  let CreateDefaultSerializer<'event>() = 
    { new IEventSerializer<'event> with
      member __.Serialize e =
        let typ,payload = box e |> JsonNet.serialize
        { TypeName = typ; Payload = payload}
      member __.Deserialize e = 
        JsonNet.deserialize (typeof<EventWithMetadata<'event>>, e.Payload) :?> _
      }