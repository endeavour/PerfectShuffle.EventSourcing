namespace PerfectShuffle.EventSourcing

type ProjectionBuilder() =
  let getEventsFrom initialCommitIndex =
    printfn "test"

  member x.EventsFrom initialCommitIndex = getEventsFrom initialCommitIndex
    
  

