# Build and publish the broker.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore against the project files alone, so a source-only change does not re-download
# every package.
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/DistMq.Protocol/DistMq.Protocol.csproj src/DistMq.Protocol/
COPY src/DistMq.Core/DistMq.Core.csproj src/DistMq.Core/
COPY src/DistMq.Storage.Abstractions/DistMq.Storage.Abstractions.csproj src/DistMq.Storage.Abstractions/
COPY src/DistMq.Storage.InMemory/DistMq.Storage.InMemory.csproj src/DistMq.Storage.InMemory/
COPY src/DistMq.Storage.Azure/DistMq.Storage.Azure.csproj src/DistMq.Storage.Azure/
COPY src/DistMq.Broker/DistMq.Broker.csproj src/DistMq.Broker/
RUN dotnet restore src/DistMq.Broker/DistMq.Broker.csproj

COPY src/ src/
RUN dotnet publish src/DistMq.Broker/DistMq.Broker.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app .

# Non-root: the broker needs no privileges, and its state lives in Azure Storage.
USER $APP_UID

# Two listeners: a cleartext port cannot carry both HTTP/1.1 and h2c, because without TLS
# there is no ALPN to negotiate with.
#   5000  HTTP/1.1  administration, HTTP data plane, WebSocket bridge
#   5001  HTTP/2    gRPC data plane
EXPOSE 5000 5001
ENV DistMq__HttpPort=5000 \
    DistMq__GrpcPort=5001 \
    DOTNET_gcServer=1

ENTRYPOINT ["dotnet", "DistMq.Broker.dll"]
