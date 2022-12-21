FROM mcr.microsoft.com/dotnet/sdk:7.0 as build
WORKDIR /build
RUN git clone --depth=1 https://github.com/Coflnet/HypixelSkyblock.git dev
WORKDIR /build/sky
COPY SkySniper.csproj SkySniper.csproj
RUN dotnet restore
COPY . .
RUN dotnet test
RUN dotnet publish -c release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app

COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8000

RUN useradd --uid $(shuf -i 2000-65000 -n 1) app
USER app

ENTRYPOINT ["dotnet", "SkySniper.dll", "--hostBuilder:reloadConfigOnChange=false"]
