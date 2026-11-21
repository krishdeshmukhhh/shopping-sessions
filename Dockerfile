FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Pipeline.csproj *.cs ./
RUN dotnet publish -c Release -r linux-x64 --self-contained false -o /app

FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/ ./
ENV DOTNET_EnableDiagnostics=0 DOTNET_TieredCompilation=0
USER 1000:1000
ENTRYPOINT ["/app/shopping-sessions"]
