namespace PerfectShuffle.EventSourcing

module Serialization =

  open System
  open System.IO
  open System.Text
  open Newtonsoft.Json
  open FifteenBelow.Json

  module private JsonNet =

      open System.Collections.Generic
      open Newtonsoft.Json.Serialization
      open Newtonsoft.Json.Converters
      open Microsoft.FSharp.Reflection

      let converters =
        [|
          OptionConverter () :> JsonConverter
          TupleConverter () :> JsonConverter
          ListConverter () :> JsonConverter
          MapConverter () :> JsonConverter
          BoxedMapConverter () :> JsonConverter
          UnionConverter () :> JsonConverter
        |]
     
      let s = new JsonSerializer()
      s.ContractResolver <- CamelCasePropertyNamesContractResolver ()
      converters |> Seq.iter (fun x -> s.Converters.Add(x))
      s.Formatting <- Formatting.Indented
      s.NullValueHandling <- NullValueHandling.Ignore
    
      let eventType o =
          let t = o.GetType()
          if FSharpType.IsUnion(t) || (t.DeclaringType <> null && FSharpType.IsUnion(t.DeclaringType)) then
              let cases = FSharpType.GetUnionCases(t)
              let unionCase,_ = FSharpValue.GetUnionFields(o, t)
              unionCase.Name
          else t.Name
        
      let serialize o =
          use ms = new MemoryStream()
          (use jsonWriter = new JsonTextWriter(new StreamWriter(ms))
          s.Serialize(jsonWriter, o))
          let data = ms.ToArray()
          (eventType o),data

      let deserialize (t, et:string, data:byte array) =
          use ms = new MemoryStream(data)
          use sr = new StreamReader(ms)
          use jsonReader = new JsonTextReader(sr)
          s.Deserialize(jsonReader, t)

  let serializer = JsonNet.serialize,JsonNet.deserialize

  let deserializet<'T> (data:byte array) =
      let json = Encoding.UTF8.GetString(data)
      JsonConvert.DeserializeObject<'T>(json)
    

