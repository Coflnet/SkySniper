VERSION=0.5.0

docker run --rm -v "${PWD}:/local" --network host -u $(id -u ${USER}):$(id -g ${USER})  openapitools/openapi-generator-cli generate \
-i http://localhost:5022/swagger/v1/swagger.json \
-g csharp \
-o /local/out --additional-properties=packageName=Coflnet.Sky.Sniper.Client,packageVersion=$VERSION,licenseId=MIT

cd out
sed -i 's/GIT_USER_ID/Coflnet/g' src/Coflnet.Sky.Sniper.Client/Coflnet.Sky.Sniper.Client.csproj
sed -i 's/GIT_REPO_ID/SkyApi/g' src/Coflnet.Sky.Sniper.Client/Coflnet.Sky.Sniper.Client.csproj
sed -i 's/>OpenAPI/>Coflnet/g' src/Coflnet.Sky.Sniper.Client/Coflnet.Sky.Sniper.Client.csproj

dotnet pack
cp src/Coflnet.Sky.Sniper.Client/bin/Debug/Coflnet.Sky.Sniper.Client.*.nupkg ..
