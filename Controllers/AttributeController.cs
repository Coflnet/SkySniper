using System.Linq;
using Coflnet.Sky.Sniper.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Sniper.Controllers;

[ApiController]
[Route("/api/[controller]")]
public class AttributeController : ControllerBase
{
    private readonly ILogger<AttributeController> _logger;
    private readonly SniperService service;

    public AttributeController(ILogger<AttributeController> logger, SniperService service)
    {
        _logger = logger;
        this.service = service;
    }

    [Route("combo/{leftAttrib}/{rightAttrib}")]
    [HttpGet]
    public List<AttributeComboResult> GetAttributes(string leftAttrib, string rightAttrib, string itemType = "kuudra")
    {
        var allOptions = GetKuudraArmors();
        if (itemType != "kuudra")
            allOptions = service.Lookups.Where(l => l.Key == itemType);
        return allOptions
            .Select(l => (l.Key, l.Value.Lookup.Where(r => r.Value.Lbin.Price > 0 && r.Key.Modifiers.Any(m => m.Key == leftAttrib) && r.Key.Modifiers.Any(m => m.Key == rightAttrib))
                .OrderBy(r => r.Value.Lbin.Price).FirstOrDefault()))
            .Select(l => new AttributeComboResult()
            {
                AuctionUid = l.Item2.Value?.Lbin.AuctionId.ToString(),
                Tag = l.Item1,
                Price = l.Item2.Value?.Lbin.Price ?? -1
            }).ToList();
    }

    private IEnumerable<KeyValuePair<string, PriceLookup>> GetKuudraArmors()
    {
        return service.Lookups.Where(l => service.CrimsonArmors.Any(k => l.Key.StartsWith(k)));
    }

    [Route("cheapest/{itemType}/{attribute}")]
    [HttpGet]
    public Dictionary<string, List<string>> GetCheapest(string itemType, string attribute, int startLevel = 1, int endLevel = 10)
    {
        if (itemType.ToLower() != "kuudra")
        {
            throw new CoflnetException("unsuported", "Only kuudra is supported");
        }
        var allOptions = GetKuudraArmors()
            .SelectMany(l => l.Value.Lookup.Where(r => r.Value.Lbin.Price > 0 && r.Key.Modifiers.Any(m => m.Key == attribute))
                .SelectMany(r => r.Value.Lbins.Select(l => (int.Parse(r.Key.Modifiers.Where(m => m.Key == attribute).First().Value), l.AuctionId.ToString(), l.Price)).ToList()))
            .ToList();
        return GetCheapestPath(startLevel, endLevel, allOptions);
    }

    public static Dictionary<string, List<string>> GetCheapestPath(int startLevel, int endLevel, List<(int, string, long Price)> allOptions)
    {
        var result = new List<(string level, List<string>)>();

        for (int i = startLevel; i < endLevel; i++)
        {
            // find the cheapest for each level
            var neededLevel = i;
            var combo = GetCheapestForLevel(allOptions, i);
            result.Add((i.ToString(), combo.Select(c => c.Item2).ToList()));
            allOptions.RemoveAll(r => combo.Any(c => c.Item2 == r.Item2));
        }
        return result.ToDictionary(r => r.level, r => r.Item2);

        static List<(int, string, long Price)> GetCheapestForLevel(List<(int, string, long Price)> allOptions, int neededLevel)
        {
            var cheapest = allOptions.Where(o => o.Item1 == neededLevel).OrderBy(o => o.Item3).Take(1).ToList();
            if (neededLevel == 1)
            {
                return cheapest;
            }

            // try find cheaper combination
            var lowerLeft = GetCheapestForLevel(allOptions, neededLevel - 1);
            // temp remove the cheapest
            allOptions.RemoveAll(r => lowerLeft.Any(l => l.Item2 == r.Item2));
            var lowerRight = GetCheapestForLevel(allOptions, neededLevel - 1);
            if (lowerRight.Count == 0)
            {
                return cheapest;
            }
            var combined = lowerLeft.Concat(lowerRight).ToList();
            var sum = combined.Sum(c => c.Item3);
            var lowestCost = cheapest.FirstOrDefault().Item3;
            if (cheapest.Count == default || sum < lowestCost)
            {
                cheapest = combined;
            }

            return cheapest;
        }
    }

    public class AttributeComboResult
    {
        public string AuctionUid { get; set; }
        public string Tag { get; set; }
        public long Price { get; set; }
    }
}
