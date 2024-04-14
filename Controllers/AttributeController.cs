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

    public class AttributeComboResult
    {
        public long AuctionUid { get; set; }
        public string Tag { get; set; }
        public long Price { get; set; }
    }
}
