namespace PerfectShuffle.EventSourcing

open System
open System.IO

type ConcatenatedStream(streams:seq<Stream>) =
  inherit Stream()
  let mutable hasStarted = false
  let mutable atEnd = false
  let enumerator = streams.GetEnumerator()

  override __.CanRead = true
  override __.CanSeek = false
  override __.CanWrite = false
  override __.Length = raise <| NotSupportedException()
  override __.Position
    with get() = raise <| NotSupportedException()
    and set _ = raise <| NotSupportedException()
  override __.Flush() = raise <| NotSupportedException()
  override __.Seek(_,_) = raise <| NotSupportedException()
  override __.SetLength(_) = raise <| NotSupportedException()  
  
  override __.Write(_,_,_) = raise <| NotSupportedException()
  override this.Read(buffer:byte[], offset:int, count:int) =
    
    if not hasStarted then
      hasStarted <- true
      if not (enumerator.MoveNext()) then
        atEnd <- true

    if not atEnd && enumerator.Current <> null
      then
        let bytesRead = enumerator.Current.Read(buffer, offset, count)
        if bytesRead < count
          then
            enumerator.Current.Close()
            enumerator.Current.Dispose()
            if (enumerator.MoveNext())
                then
                  bytesRead + this.Read(buffer, offset + bytesRead, count - bytesRead)
                else
                  atEnd <- true
                  bytesRead
          else bytesRead       
      else 0
  
  override __.Close() =
    if atEnd = false && enumerator.Current <> null then
      enumerator.Current.Close()

  override __.Dispose(disposing) =
      if atEnd = false && enumerator.Current <> null then
        enumerator.Current.Close()
        enumerator.Current.Dispose()

  interface IDisposable with
    override __.Dispose() =
      if atEnd = false && enumerator.Current <> null then
        enumerator.Current.Dispose()
        