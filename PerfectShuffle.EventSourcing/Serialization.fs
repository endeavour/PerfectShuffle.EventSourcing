namespace PerfectShuffle.EventSourcing

module Serialization =

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
        UriConverter() :> JsonConverter
      |] |> Seq.iter (s.Converters.Add)
      s.NullValueHandling <- NullValueHandling.Ignore
    
      let eventType o =
          let t = o.GetType()
          if FSharpType.IsUnion(t) || (t.DeclaringType <> null && FSharpType.IsUnion(t.DeclaringType)) then
              let cases = FSharpType.GetUnionCases(t)
              let unionCase,_ = FSharpValue.GetUnionFields(o, t)
              unionCase.Name
          else t.Name
        
      let serialize o =
        try
          use ms = new MemoryStream()
          (use jsonWriter = new JsonTextWriter(new StreamWriter(ms))
          s.Serialize(jsonWriter, o))
          let data = ms.ToArray()
          (eventType o),data
        with e ->
          let a = e
          raise e

      let deserialize (t, et:string, data:byte array) =
          use ms = new MemoryStream(data)
          use sr = new StreamReader(ms)
          use jsonReader = new JsonTextReader(sr)
          s.Deserialize(jsonReader, t)

  let serializer = JsonNet.serialize,JsonNet.deserialize

