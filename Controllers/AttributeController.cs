using System.Linq;
using Coflnet.Sky.Sniper.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

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
    public List<AttributeComboResult> GetAttributes(string leftAttrib, string rightAttrib)
    {
        return service.Lookups.Where(l => l.Key.StartsWith("CRIMSON"))
            .Select(l => (l.Key, l.Value.Lookup.Where(r => r.Value.Lbin.Price > 0 && r.Key.Modifiers.Any(m => m.Key == leftAttrib) && r.Key.Modifiers.Any(m => m.Key == rightAttrib))
                .OrderByDescending(r => r.Value.Lbin.Price).FirstOrDefault()))
            .Select(l => new AttributeComboResult()
            {
                AuctionUid = l.Item2.Value.Lbin.AuctionId,
                Tag = l.Item1,
                Price = l.Item2.Value.Lbin.Price
            }).ToList();
    }



    [Route("cheapest/{attribute}")]
    [HttpGet]
    public List<(string level, List<string>)> GetCheapest(string attribute, int startLevel = 1, int endLevel = 10)
    {
        var allOptions = service.Lookups.Where(l => l.Key.StartsWith("CRIMSON"))
            .SelectMany(l => l.Value.Lookup.Where(r => r.Value.Lbin.Price > 0 && r.Key.Modifiers.Any(m => m.Key == attribute))
                .SelectMany(r => r.Value.Lbins.Select(l => (int.Parse(r.Key.Modifiers.Where(m => m.Key == attribute).First().Value), l.AuctionId.ToString(), l.Price)).ToList()))
            .ToList();
        var result = new List<(string level, List<string>)>();

        for (int i = startLevel; i < endLevel; i++)
        {
            // find the cheapest for each level
            var cheapest = allOptions.Where(o => o.Item1 == i).OrderBy(o => o.Item3).FirstOrDefault();
            if (cheapest == default)
            {
                // try find two of lower level
                var lower = allOptions.Where(o => o.Item1 == i - 1).OrderBy(o => o.Item3).Take(2).ToList();
                if (lower.Count < 2)
                {
                    // no more options
                    break;
                }
                // add the two lower levels
                result.Add((i.ToString(), lower.Select(l => l.Item2).ToList()));
                continue;
            }
            // remove all other options for this auction
            allOptions.RemoveAll(o => o.Item2 == cheapest.Item2);
            result.Add((i.ToString(), new List<string> { cheapest.Item2 }));
        }
        return result;
    }

    public class AttributeComboResult
    {
        public long AuctionUid { get; set; }
        public string Tag { get; set; }
        public long Price { get; set; }
    }
}
