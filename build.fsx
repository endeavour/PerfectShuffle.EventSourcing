// include Fake lib
#r "packages/FAKE/tools/FakeLib.dll"
open Fake

// Properties
let buildDir = FileSystemHelper.currentDirectory @@ "/build"
let outputDir = FileSystemHelper.currentDirectory @@ "/output"

// Targets
Target "Clean" (fun _ ->
  CleanDirs [buildDir; outputDir]
)

Target "BuildApp" (fun _ ->
  !! "**/*.fsproj"
    |> MSBuildRelease buildDir "Build"
    |> Log "AppBuild-Output: "
)

Target "Pack" (fun _ ->
  Paket.Pack (fun settings -> { settings with OutputPath = "./output"; MinimumFromLockFile=true})
  )

Target "Push" (fun _ ->
  Paket.Push (fun settings -> { settings with WorkingDir = "./output"; DegreeOfParallelism = 5; PublishUrl = "https://www.nuget.org/api/v2/package"})
)

Target "Default" (fun _ ->
  ()
)

// Dependencies
"Clean"
  ==> "BuildApp"
  ==> "Pack"
  ==> "Default"

// start build
RunTargetOrDefault "Default"
