using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Sniper.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Coflnet.Sky.Sniper.Controllers;

[ApiController]
[Route("/api/[controller]")]
public class AuctionController : ControllerBase
{
    private readonly ILogger<AuctionController> _logger;
    private readonly SniperService service;
    private readonly HypixelContext db;
    private readonly IAiFinder aiFinder;

    public AuctionController(ILogger<AuctionController> logger, SniperService service, HypixelContext db, IAiFinder aiFinder)
    {
        _logger = logger;
        this.service = service;
        this.db = db;
        this.aiFinder = aiFinder;
    }

    [Route("auction/{auctionUuid}/key")]
    [HttpGet]
    public AuctionKey GetKey(string auctionUuid)
    {
        var uid = AuctionService.Instance.GetId(auctionUuid);
        var auction = db.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments).FirstOrDefault(a => a.UId == uid);
        return service.KeyFromSaveAuction(auction);
    }

    [Route("auction/{auctionUuid}/estimate")]
    [HttpGet]
    public async Task<ActionResult<object>> GetEstimate(string auctionUuid)
    {
        var uid = AuctionService.Instance.GetId(auctionUuid);
        var auction = db.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments).FirstOrDefault(a => a.UId == uid);
        if (auction == null)
            return NotFound();
        return await GetEstimateFromInternal(auction);
    }

    [Route("auction/{auctionUuid}/estimate")]
    [HttpPost]
    public async Task<ActionResult<object>> GetEstimateFromAuction(ApiSaveAuction auction)
    {
        return await GetEstimateFromInternal(auction);
    }

    private async Task<ActionResult<object>> GetEstimateFromInternal(Core.SaveAuction auction)
    {
        try
        {
            var estimate = await aiFinder.Estimate(auction);
            if (estimate == null)
                return StatusCode(503, $"No AI estimate, AI finder: {aiFinder.Variant}");
            _logger.LogInformation(JsonConvert.SerializeObject(estimate.Sample));

            // obtain the value breakdown used by the baseline estimator for visibility
            KeyWithValueBreakdown breakdown = null;
            try
            {
                breakdown = service.ValueKeyForTest(auction);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to compute value breakdown for auction {AuctionUuid}", auction.Uuid);
            }

            // return both the self-learning estimate and the baseline for visibility, including breakdown
            return Ok(new
            {
                Estimated = estimate.EstimatedValue,
                ModelReady = estimate.ModelReady,
                SampleCount = estimate.SampleCount,
                Quality = estimate.Quality,
                Breakdown = breakdown?.ValueBreakdown
            });
        }
        catch (System.Exception ex)
        {
            _logger.LogError(ex, "Failed to estimate price for auction {AuctionUuid}", auction.Uuid);
            return StatusCode(500);
        }
    }

    [Route("relevantItems")]
    [HttpGet]
    public List<string> GetRelevantItems()
    {
        var oldestRef = SniperService.GetDay() - 60;
        return service.Lookups.Where(l => l.Value.Lookup.Any(i => i.Value.Price > 12_000_000) && l.Value.Lookup.Count(i=>i.Key.Count == 1 && i.Value.Price > 5_000_000 && i.Value.OldestRef > oldestRef) > 3 && l.Value.Volume > 10)
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
