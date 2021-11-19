FROM mcr.microsoft.com/dotnet/sdk:6.0 as build
WORKDIR /build
RUN git clone --depth=1 -b separation https://github.com/Coflnet/HypixelSkyblock.git dev
WORKDIR /build/sky
COPY SkySniper.csproj SkySniper.csproj
RUN dotnet restore
COPY . .
RUN dotnet publish -c release

FROM mcr.microsoft.com/dotnet/aspnet:6.0
WORKDIR /app

COPY --from=build /build/sky/bin/release/net6.0/publish/ .

ENTRYPOINT ["dotnet", "SkySniper.dll"]
