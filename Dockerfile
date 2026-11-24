# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /build
ARG CACHEBUST=20261025
RUN git init dev && git -C dev remote add origin https://github.com/Coflnet/HypixelSkyblock.git && git -C dev fetch --depth=1 origin 91ee97884331758d509d8b2b292a1e7b4b60deb5 && git -C dev checkout --detach FETCH_HEAD
# The AI finder lives in the private repository SkyAIFinder and is built from source next to this one, pinned like the core.
# It is fetched only when the BuildKit secret "skyaifinder_token" holds a read-only deploy key of that repository (the OpenSSH
# private key, base64 on one line):
#   docker build --secret id=skyaifinder_token,env=SKYAIFINDER_TOKEN --build-arg SKYAIFINDER_ENABLED=1 .
# Without the secret the step is skipped and the image is built without the AI finder (the service logs that at startup).
# With the secret, SKYAIFINDER_SHA has to be a full commit id, otherwise the build fails.
# The key only exists in a tmpfs file of this one RUN step and is deleted at its end; it is never an ARG, never in git config,
# never in a layer. The fetch uses GitHub's SSH endpoint on port 443 (CI runners may only reach ports 80 and 443) with
# GitHub's published host keys pinned (https://api.github.com/meta, ssh_keys; the same keys serve ssh.github.com:443).
ARG SKYAIFINDER_SHA=fcd77da7b44f42beda4d81e3d190f447bf0dbde0
# set to 1 together with the secret (container-ci does) so a cached build without the finder is not reused
ARG SKYAIFINDER_ENABLED=0
RUN --mount=type=secret,id=skyaifinder_token,required=false \
    if [ ! -s /run/secrets/skyaifinder_token ]; then \
        echo "No skyaifinder_token secret (SKYAIFINDER_ENABLED=${SKYAIFINDER_ENABLED}): building without the AI finder"; exit 0; \
    fi \
    && if ! echo "${SKYAIFINDER_SHA}" | grep -Eq '^[0-9a-f]{40}$'; then \
        echo "SKYAIFINDER_SHA '${SKYAIFINDER_SHA}' is not a full commit id"; exit 1; \
    fi \
    && apt-get update -qq && apt-get install -y -qq --no-install-recommends openssh-client > /dev/null && rm -rf /var/lib/apt/lists/* \
    && keydir=$(mktemp -d /dev/shm/skyaifinder.XXXXXX) \
    && trap 'rm -rf "$keydir"' EXIT \
    && tr -d ' \r\n' < /run/secrets/skyaifinder_token | base64 -d > "$keydir/key" && chmod 600 "$keydir/key" \
    && printf '%s\n' \
        '[ssh.github.com]:443 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl' \
        '[ssh.github.com]:443 ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=' \
        '[ssh.github.com]:443 ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=' \
        > "$keydir/known_hosts" \
    && git init -q SkyAIFinder && git -C SkyAIFinder remote add origin ssh://git@ssh.github.com:443/Coflnet/SkyAIFinder.git \
    && GIT_SSH_COMMAND="ssh -i $keydir/key -o IdentitiesOnly=yes -o UserKnownHostsFile=$keydir/known_hosts -o StrictHostKeyChecking=yes" \
        git -C SkyAIFinder fetch -q --depth=1 origin "${SKYAIFINDER_SHA}" \
    && git -C SkyAIFinder checkout -q --detach FETCH_HEAD \
    && echo "Building with SkyAIFinder $(git -C SkyAIFinder rev-parse HEAD)"
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
