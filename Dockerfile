FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /build
ARG CACHEBUST=20261025
RUN git init dev && git -C dev remote add origin https://github.com/Coflnet/HypixelSkyblock.git && git -C dev fetch --depth=1 origin 2df0f7c7a34c8c04653e4cf7c02ab6805876eb7a && git -C dev checkout --detach FETCH_HEAD
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

# apt in the base image has CVE-2026-109518 (HIGH); upgrade it until the base image ships the fix.
RUN apt-get update \
    && apt-get install -y --only-upgrade --no-install-recommends apt libapt-pkg6.0t64 \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --uid $(shuf -i 2000-65000 -n 1) app-user
USER app-user

ENTRYPOINT ["dotnet", "SkySniper.dll", "--hostBuilder:reloadConfigOnChange=false"]
