FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /build
RUN git clone --depth=1 https://github.com/Coflnet/HypixelSkyblock.git dev
WORKDIR /build/sky
RUN git clone --depth=1 https://github.com/NotEnoughUpdates/NotEnoughUpdates-REPO.git NEU-REPO \
    && rm -rf NEU-REPO/.git NEU-REPO/items
COPY SkySniper.csproj SkySniper.csproj
RUN dotnet restore
COPY . .
# --settings ci.runsettings overrides the local-default RunSettingsFilePath (test.runsettings) wired in
# the csproj, so CI runs the FULL suite including the fuzz/parity/bit-exact verification tests.
RUN dotnet test --settings ci.runsettings
RUN dotnet publish -c release -o /app && rm /app/items.json

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8000

RUN useradd --uid $(shuf -i 2000-65000 -n 1) app-user
USER app-user

ENTRYPOINT ["dotnet", "SkySniper.dll", "--hostBuilder:reloadConfigOnChange=false"]
