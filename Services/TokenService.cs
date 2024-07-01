using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using JWT.Algorithms;
using JWT.Builder;
using Microsoft.Extensions.Configuration;

namespace Coflnet.Sky.Sniper.Services;

public interface ITokenService
{
    string CreateToken();
    bool HasTokenAccess(string token);
}

public class TokenService : ITokenService
{
    private readonly string secret;
    private readonly string previousSecret;

    private readonly ConcurrentDictionary<long, int> tokenUsages = new();


    public TokenService(IConfiguration config)
    {
        secret = config["JWT_SECRET"];
        previousSecret = config["OLD_JWT_SECRET"];
        if (string.IsNullOrEmpty(previousSecret))
            previousSecret = secret;
    }

    public string CreateToken()
    {
        long id = 0;
        while (tokenUsages.ContainsKey(id) || id == 0)
        {
            id = new Random().Next();
        }
        tokenUsages[id] = 0;
        return new JwtBuilder()
        .WithAlgorithm(new HMACSHA512Algorithm())
        .WithSecret(secret)
        .AddClaim("exp", DateTimeOffset.UtcNow.AddMinutes(100).ToUnixTimeSeconds())
        .AddClaim("id", id)
        .Encode();
    }

    public bool HasTokenAccess(string token)
    {
        var data = VerifyToken(token);
        var expires = ((JsonElement)data["exp"]).GetInt64();
        var secondsLeft = expires - DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var current = tokenUsages.AddOrUpdate(((JsonElement)data["id"]).GetInt64(), 1, (k, v) => v + 1);
        return current < secondsLeft && secondsLeft > 0;
    }

    private IDictionary<string, object> VerifyToken(string token)
    {
        return new JwtBuilder()
             .WithAlgorithm(new HMACSHA512Algorithm())
             .WithSecret(secret, previousSecret)
             .MustVerifySignature()
             .Decode<IDictionary<string, object>>(token);
    }

}
