namespace PerfectShuffle.EventSourcing
  module StringExtensions =  
    open System.Text
    open System.IO
    
    type System.String with
      member this.ToStream() =
        let bytes = Encoding.Default.GetBytes(this)
        let stream = new MemoryStream(bytes)
        stream
    