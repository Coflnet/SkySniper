using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper.Services;
#nullable enable
/// <summary>
/// Adapter that preserves the external API of the old PartialCalcService but delegates
/// predictions and training to the self-learning flip finder service. This keeps
/// callers unchanged while switching logic to the ML-based estimator.
/// </summary>
public class PartialCalcService
{
    private readonly SniperService sniper;
    private readonly ICraftCostService craftCostService;
    private readonly IMayorService mayorService;
    private readonly ISelfLearningFlipFinderService flipFinder;
    private readonly ILogger<PartialCalcService> logger;

    public PartialCalcService(
        SniperService sniper,
        ICraftCostService craftCostService,
        IMayorService mayorService,
        IPersitanceManager persitanceManager,
        ILogger<PartialCalcService> logger,
        Core.Services.HypixelItemService itemService,
    AIFormattingService? aiFormattingService,
    ISelfLearningFlipFinderService flipFinder)
    {
        this.sniper = sniper;
        this.craftCostService = craftCostService;
        this.mayorService = mayorService;
        this.logger = logger;
        this.flipFinder = flipFinder ?? throw new ArgumentNullException(nameof(flipFinder));
        IsPrimary = sniper.ServerDnsName.Contains("manager");
    }

    public bool IsPrimary { get; set; }

    public IEnumerable<string> ItemKeys => flipFinder.GetModelStats().Keys;

    public class PartialResult
    {
        public long Price { get; set; }
        public List<string>? BreakDown { get; set; }
    }

    public Dictionary<string, Dictionary<string, double>> GetAttributeCosts(string tag)
    {
        // Not applicable for adapter; return empty to preserve API
        return new();
    }

    public PartialResult GetPrice(Item originalItem, bool includeBreakDown = false)
    {
        // Build a minimal SaveAuction to use the shared conversion
        var auction = new SaveAuction { Tag = originalItem.Tag, Uuid = Guid.Empty.ToString(), End = DateTime.UtcNow };
        return GetPrice(auction, includeBreakDown);
    }

    public PartialResult GetPrice(SaveAuction auction, bool includeBreakDown = false)
    {
        var result = new PartialResult();
        try
        {
            var flip = SaveAuctionExtensions.ToComplicatedFlip(auction, includeBreakdown: true, sniper: sniper, mayorService: mayorService, craftCostService: craftCostService);
            var estimate = flipFinder.EstimateAsync(flip).GetAwaiter().GetResult();
            result.Price = (long)estimate.EstimatedValue;
            if (includeBreakDown)
            {
                try
                {
                    var breakdown = sniper.ValueKeyForTest(auction);
                    result.BreakDown = breakdown.ValueBreakdown.Select(x =>
                        {
                            if (x.Enchant.Type != default) return $"{x.Enchant.Type}:{x.Enchant.Lvl} -> {(x.IsEstimate ? "(est)" : x.Value.ToString())}";
                            if (!string.IsNullOrEmpty(x.Modifier.Key)) return $"{x.Modifier.Key}:{x.Modifier.Value} -> {(x.IsEstimate ? "(est)" : x.Value.ToString())}";
                            return $"{x.Reforge} -> {(x.IsEstimate ? "(est)" : x.Value.ToString())}";
                        }).ToList();
                }
                catch { }
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "PartialCalc adapter: GetPrice failed");
            result.Price = 0;
        }
        return result;
    }

    public async Task AddSell(SaveAuction auction)
    {
        try
        {
            var flip = SaveAuctionExtensions.ToComplicatedFlip(auction, includeBreakdown: true, sniper: sniper, mayorService: mayorService, craftCostService: craftCostService);
            await flipFinder.TrainAsync(flip);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "PartialCalc adapter: AddSell failed");
        }
    }

    public Task Load()
    {
        // persistence handled by flipFinder; no-op
        return Task.CompletedTask;
    }

    public Task Save()
    {
        // persistence handled by flipFinder; no-op
        return Task.CompletedTask;
    }

    public Task CapAtCraftCost()
    {
        // Not applicable for adapter; no-op
        return Task.CompletedTask;
    }

    public void SetLearningRate(double v)
    {
        // no-op for adapter
    }

    public void Correct(string itemTag, Dictionary<string, Dictionary<string, double>> corrections)
    {
        // no-op for adapter
    }
}

#nullable disable
