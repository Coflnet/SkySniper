using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Sniper.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;

namespace Coflnet.Sky.Sniper.Controllers;
[ApiController]
[Route("/api/[controller]")]
public class AuctionController : ControllerBase
{
    private readonly ILogger<AuctionController> _logger;
    private readonly SniperService service;
    private readonly HypixelContext db;

    public AuctionController(ILogger<AuctionController> logger, SniperService service, HypixelContext db)
    {
        _logger = logger;
        this.service = service;
        this.db = db;
    }

    [Route("auction/{auctionUuid}/key")]
    [HttpGet]
    public AuctionKey GetKey(string auctionUuid)
    {
        var uid = AuctionService.Instance.GetId(auctionUuid);
        var auction = db.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments).FirstOrDefault(a => a.UId == uid);
        return service.KeyFromSaveAuction(auction);
    }

    [Route("relevantItems")]
    [HttpGet]
    public List<string> GetRelevantItems()
    {
        return service.Lookups.Where(l => l.Value.Lookup.Any(i => i.Value.Price > 12_000_000 && l.Value.Lookup.Count > 3))
            .OrderByDescending(l => l.Value.Lookup.Sum(i => i.Value.Price * i.Value.Volume))
            .Select(l => l.Key).ToList();
    }

    [Route("lbins")]
    [HttpGet]
    public Dictionary<string, ReferencePrice> GetLowestBins()
    {
        return service.Lookups.Where(l => l.Value.Lookup.Any(li => li.Value.Lbins.Count > 0))
            .ToDictionary(l => l.Key, l => l.Value.Lookup.Select(i => i.Value.Lbin)
                                            .Where(i => i.Price > 0).MinBy(i => i.Price));
    }
}
