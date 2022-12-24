FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["PerfectShuffleEventSourcing.EventStore/PerfectShuffleEventSourcing.EventStore.fsproj", "PerfectShuffleEventSourcing.EventStore/"]
RUN dotnet restore "PerfectShuffleEventSourcing.EventStore/PerfectShuffleEventSourcing.EventStore.fsproj"
COPY . .
WORKDIR "/src/PerfectShuffleEventSourcing.EventStore"
RUN dotnet build "PerfectShuffleEventSourcing.EventStore.fsproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "PerfectShuffleEventSourcing.EventStore.fsproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "PerfectShuffleEventSourcing.EventStore.dll"]
