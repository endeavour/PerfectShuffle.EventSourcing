namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.StringExtensions
open System.IO
open Newtonsoft.Json

module EventStore =
  
  let jsonSerializer = new JsonSerializer()  
  let converters : JsonConverter[] = [|
    new JsonSerialization.GuidConverter()
    new JsonSerialization.ListConverter()
    new JsonSerialization.OptionTypeConverter()
    new JsonSerialization.TupleArrayConverter()
    new JsonSerialization.UnionTypeConverter()  
    |]
  for converter in converters do
    jsonSerializer.Converters.Add(converter)  

  let file = "events.data"

  let nextId = ref 0L

  let ReadEvents<'event>() =
    if System.IO.File.Exists file then
      use rawStream = File.OpenRead file
      use stream = new ConcatenatedStream(["[".ToStream(); rawStream; "]".ToStream()])
      
      use sr = new StreamReader(stream)

      use jsonReader = new JsonTextReader(sr)
      
      let events =
        if not (jsonReader.Read()) || jsonReader.TokenType <> JsonToken.StartArray
          then invalidOp "Unexpected data in events file"
          else
            seq {
            while jsonReader.Read() && jsonReader.TokenType <> JsonToken.EndArray do
              let evt = jsonSerializer.Deserialize<EventWithMetadata<'event>>(jsonReader)
              nextId := evt.Id + 1L
              yield evt
            } |> Seq.cache

      events |> Seq.toArray
           
    else
      Array.empty

  let openOrAppend file =
    let streamWriter =
      if System.IO.File.Exists file then
        System.IO.File.AppendText file
      else
        let file  = System.IO.File.OpenWrite file
        let sw = new System.IO.StreamWriter(file)
        sw
    streamWriter

  let private save evt (jsonWriter:JsonTextWriter) =
    jsonWriter.WriteComment("EVENT: " + string !nextId)
    jsonWriter.WriteWhitespace("\n")
    jsonSerializer.Serialize(jsonWriter, {evt with Id = !nextId})
    jsonWriter.WriteRaw ","
    jsonWriter.WriteWhitespace("\n")    
    nextId := !nextId + 1L

  let Save (evt:EventWithMetadata<'event>) =
    use stream = openOrAppend file
    use jsonWriter = new JsonTextWriter(stream)
    jsonWriter.CloseOutput <- false
    save evt jsonWriter

  let SaveAll (evts:seq<EventWithMetadata<'event>>) =
    use stream = openOrAppend file
    use jsonWriter = new JsonTextWriter(stream)
    jsonWriter.CloseOutput <- false
    for evt in evts do
      save evt jsonWriter
