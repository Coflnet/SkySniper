using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Sniper.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using ComplicatedFlip = Coflnet.Sky.FlipTracker.Client.Model.ComplicatedFlip;
using Coflnet.Sky.FlipTracker.Client.Model;
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
    private readonly ISelfLearningFlipFinderService flipFinder;

    public AuctionController(ILogger<AuctionController> logger, SniperService service, HypixelContext db, ISelfLearningFlipFinderService flipFinder)
    {
        _logger = logger;
        this.service = service;
        this.db = db;
        this.flipFinder = flipFinder;
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
        try
        {
            // Convert to ComplicatedFlip using the shared helper (lightweight mode)
            var flip = SaveAuctionExtensions.ToComplicatedFlip((object)auction);
            var estimate = await flipFinder.EstimateAsync(flip);

            // obtain the value breakdown used by the baseline estimator for visibility
            KeyWithValueBreakdown breakdown = null;
            try
            {
                breakdown = service.ValueKeyForTest(auction);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to compute value breakdown for auction {AuctionUuid}", auctionUuid);
            }

            // return both the self-learning estimate and the baseline for visibility, including breakdown
            return Ok(new
            {
                Estimated = estimate.EstimatedValue,
                Baseline = estimate.BaselineValue,
                ModelReady = estimate.ModelReady,
                SampleCount = estimate.SampleCount,
                Metrics = estimate.Metrics,
                Breakdown = breakdown?.ValueBreakdown
            });
        }
        catch (System.Exception ex)
        {
            _logger.LogError(ex, "Failed to estimate price for auction {AuctionUuid}", auctionUuid);
            return StatusCode(500);
        }
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
