namespace PerfectShuffle.EventSourcing

open System
open System.IO
open System.Text

module JsonSerialization =

  open System.Collections.Generic
  open Newtonsoft.Json
  open Newtonsoft.Json.Serialization
  open Newtonsoft.Json.Converters  
  open Microsoft.FSharp.Reflection

  type GuidConverter() =
    inherit JsonConverter()

    override x.CanConvert(t:Type) = t = typeof<Guid>

    override x.WriteJson(writer, value, serializer) =
      let value = value :?> Guid
      if value <> Guid.Empty then writer.WriteValue(value.ToString("N"))
      else writer.WriteValue("")
    
    override x.ReadJson(reader, t, _, serializer) = 
      match reader.TokenType with
      | JsonToken.Null -> Guid.Empty :> obj
      | JsonToken.String ->
        let str = reader.Value :?> string
        if (String.IsNullOrEmpty(str)) then Guid.Empty :> obj
        else new Guid(str) :> obj
      | _ -> failwith "Invalid token when attempting to read Guid."


  type ListConverter() =
    inherit JsonConverter()
  
    override x.CanConvert(t:Type) = 
      t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<list<_>>

    override x.WriteJson(writer, value, serializer) =
      let list = value :?> System.Collections.IEnumerable |> Seq.cast
      serializer.Serialize(writer, list)

    override x.ReadJson(reader, t, _, serializer) = 
      let itemType = t.GetGenericArguments().[0]
      let collectionType = typedefof<IEnumerable<_>>.MakeGenericType(itemType)
      let collection = serializer.Deserialize(reader, collectionType) :?> System.Collections.IEnumerable |> Seq.cast    
      let listType = typedefof<list<_>>.MakeGenericType(itemType)
      let cases = FSharpType.GetUnionCases(listType)
      let rec make = function
        | [] -> FSharpValue.MakeUnion(cases.[0], [||])
        | head::tail -> FSharpValue.MakeUnion(cases.[1], [| head; (make tail); |])          
      make (collection |> Seq.toList)

  type OptionTypeConverter() =
    inherit JsonConverter()
    override x.CanConvert(typ:Type) =
     typ.IsGenericType && 
         typ.GetGenericTypeDefinition() = typedefof<option<OptionTypeConverter>>
  
    override x.WriteJson(writer: JsonWriter, value: obj, serializer: JsonSerializer) =
      if value <> null then
          let t = value.GetType()
          let fieldInfo = t.GetField("value", System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Instance)
          let value = fieldInfo.GetValue(value)
          serializer.Serialize(writer, value)
  
    override x.ReadJson(reader: JsonReader, objectType: Type, existingValue: obj, serializer: JsonSerializer) = 
      let cases = Microsoft.FSharp.Reflection.FSharpType.GetUnionCases(objectType)
      let t = objectType.GetGenericArguments().[0]
      let t = 
        if t.IsValueType then 
            let nullable = typedefof<Nullable<int>> 
            nullable.MakeGenericType [|t|]
          else 
            t
      let value = serializer.Deserialize(reader, t)
      if value <> null then
          FSharpValue.MakeUnion(cases.[1], [|value|])
      else
          FSharpValue.MakeUnion(cases.[0], [||])

  type TupleArrayConverter() =
    inherit JsonConverter()
  
    override x.CanConvert(t:Type) = 
      FSharpType.IsTuple(t)

    override x.WriteJson(writer, value, serializer) =
      let values = FSharpValue.GetTupleFields(value)
      serializer.Serialize(writer, values)

    override x.ReadJson(reader, t, _, serializer) =
      let advance = reader.Read >> ignore
      let deserialize t = serializer.Deserialize(reader, t)
      let itemTypes = FSharpType.GetTupleElements(t)

      let readElements() =
        let rec read index acc =
          match reader.TokenType with
          | JsonToken.EndArray -> acc
          | _ ->
            let value = deserialize(itemTypes.[index])
            advance()
            read (index + 1) (acc @ [value])
        advance()
        read 0 List.empty

      match reader.TokenType with
      | JsonToken.StartArray ->
        let values = readElements()
        FSharpValue.MakeTuple(values |> List.toArray, t)
      | _ -> failwith "invalid token"


  type UnionTypeConverter() =
    inherit JsonConverter()

    override x.CanConvert(typ:Type) =
      FSharpType.IsUnion typ 

    override x.WriteJson(writer: JsonWriter, value: obj, serializer: JsonSerializer) =
      let t = value.GetType()
      let (info, fields) = FSharpValue.GetUnionFields(value, t)
      writer.WriteStartObject()
      writer.WritePropertyName("_tag")
      writer.WriteValue(info.Tag)
      let cases = FSharpType.GetUnionCases(t)
      let case = cases.[info.Tag]
      let fields = case.GetFields()
      for field in fields do
          writer.WritePropertyName(field.Name)
          serializer.Serialize(writer, field.GetValue(value, [||]))
      writer.WriteEndObject()

    override x.ReadJson(reader: JsonReader, objectType: Type, existingValue: obj, serializer: JsonSerializer) =
      reader.Read() |> ignore //pop start obj type label
      reader.Read() |> ignore //pop tag prop name
      let union = FSharpType.GetUnionCases(objectType)
      let case = union.[int(reader.Value :?> int64)]
      let fieldValues =  [| 
             for field in case.GetFields() do
                 reader.Read() |> ignore //pop item name
                 reader.Read() |> ignore
                 yield serializer.Deserialize(reader, field.PropertyType)
       |] 
      
      reader.Read() |> ignore
      FSharpValue.MakeUnion(case, fieldValues)