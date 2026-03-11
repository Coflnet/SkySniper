using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Crafts.Client.Api;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Sniper.Services;

public interface ICraftCostService
{
    bool TryGetCost(string itemId, out double cost);
    Dictionary<string, double> Costs { get; }
    Dictionary<string, Category> ItemCategories { get; }
    void AddCostForSpecialItems();
}
public class CraftCostService : BackgroundService, ICraftCostService
{
    private readonly ICraftsApi craftsApi;
    private readonly ILogger<CraftCostService> logger;
    public Dictionary<string, double> Costs { get; private set; } = new Dictionary<string, double>();
    public Dictionary<string, Category> ItemCategories { get; private set; } = new Dictionary<string, Category>();


    public CraftCostService(ICraftsApi craftsApi, ILogger<CraftCostService> logger)
    {
        this.craftsApi = craftsApi;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            List<Crafts.Client.Model.ProfitableCraft> all = null;
            try
            {
                all = await craftsApi.GetAllAsync();
            }
            catch (System.Exception e)
            {
                logger.LogError(e, "Error while fetching crafts");
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                continue;
            }

            if (all == null)
            {
                logger.LogError("Crafts api returned null");
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                continue;
            }
            foreach (var craft in all)
            {
                Costs[craft.ItemId] = Math.Min(craft.CraftCost, craft.SellPrice * 2); // cap at 2x clean sell price to avoid unavailability messing with valuations.
                if (craft.Type == "carpentry")
                    Costs[craft.ItemId] = Math.Min(craft.CraftCost, 10_000);
                if (craft.ItemId.EndsWith("DESK"))
                    logger.LogInformation($"Cost for {craft.ItemId} is {craft.CraftCost}, set to {Costs[craft.ItemId]} {craft.Type}");
            }
            logger.LogInformation("Updated craft costs for " + all.Count + " items");
            AddCostForSpecialItems();
            await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
        }
    }

    public void AddCostForSpecialItems()
    {
        foreach (var item in ItemCategories)
        {
            if (item.Value == Category.MISC && item.Key.EndsWith("_PORTAL"))
            {
                // manipulation back stop, travel scrolls top out at ~5m but don't have a 1-1 mapping and there does not seem to be a way to detect new mappings
                Costs[item.Key] = Costs.GetValueOrDefault("PORTALIZER", 5_000_000) + 5_000_000;
            }
        }
    }

    public bool TryGetCost(string itemId, out double cost)
    {
        if (itemId.Contains(':') || Constants.Vanilla.Contains(itemId.Replace("_ITEM", "").ToLower()))
        {
            cost = 10;
            return true;
        }
        return Costs.TryGetValue(itemId, out cost);
    }
}
#nullable disable
