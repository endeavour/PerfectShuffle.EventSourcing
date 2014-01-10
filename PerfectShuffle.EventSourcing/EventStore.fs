namespace PerfectShuffle.EventSourcing
open PerfectShuffle.EventSourcing.StringExtensions
open System.IO
open Newtonsoft.Json

module EventMetadata =
  let embellish evt =
    {Id = -1L; Timestamp = System.DateTime.UtcNow; Event = evt}

type EventStore(eventsFile:string) =
  //TODO: Support different serialization mechanisms

  let mutable init = false
  
  let nextId = ref 0L
  let jsonSerializer = new JsonSerializer()  
  let converters : JsonConverter[] = [|
    new JsonSerialization.GuidConverter()
    new JsonSerialization.ListConverter()
    new JsonSerialization.OptionTypeConverter()
    new JsonSerialization.TupleArrayConverter()
    new JsonSerialization.UnionTypeConverter()  
    |]
  
  do
    for converter in converters do
      jsonSerializer.Converters.Add(converter)  

  let readEvents() =
    if System.IO.File.Exists eventsFile then
      use rawStream = File.OpenRead eventsFile
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
            }

      let events' = events |> Seq.toArray
      events'
           
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

  let save evt (jsonWriter:JsonTextWriter) =
    jsonWriter.WriteComment("EVENT: " + string !nextId)
    jsonWriter.WriteWhitespace("\n")
    jsonSerializer.Serialize(jsonWriter, {evt with Id = !nextId})
    jsonWriter.WriteRaw ","
    jsonWriter.WriteWhitespace("\n")    
    nextId := !nextId + 1L

  member __.ReadEvents<'event>() : EventWithMetadata<'event>[] = readEvents()
  
  member __.Save (event:EventWithMetadata<'event>) =
    use stream = openOrAppend eventsFile
    use jsonWriter = new JsonTextWriter(stream)
    jsonWriter.CloseOutput <- false
    save event jsonWriter

  member __.SaveAll (events:seq<EventWithMetadata<'event>>) =
    use stream = openOrAppend eventsFile
    use jsonWriter = new JsonTextWriter(stream)
    jsonWriter.CloseOutput <- false
    for event in events do
      save event jsonWriter