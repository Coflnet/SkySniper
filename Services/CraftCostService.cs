using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Crafts.Client.Api;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System;

namespace Coflnet.Sky.Sniper.Services;

public interface ICraftCostService
{
    bool TryGetCost(string itemId, out double cost);
}

public class CraftCostService : BackgroundService, ICraftCostService
{
    private readonly ICraftsApi craftsApi;
    private readonly ILogger<CraftCostService> logger;
    private Dictionary<string, double> costs = new Dictionary<string, double>();

    public CraftCostService(ICraftsApi craftsApi, ILogger<CraftCostService> logger)
    {
        this.craftsApi = craftsApi;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var all = await craftsApi.CraftsAllGetAsync();
            if(all == null)
            {
                logger.LogError("Crafts api returned null");
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                continue;
            }
            foreach (var craft in all)
            {
                costs[craft.ItemId] = craft.CraftCost;
                if (craft.ItemId.EndsWith("GROWTH_5"))
                    logger.LogInformation("Cost for " + craft.ItemId + " is " + craft.CraftCost);
            }
            logger.LogInformation("Updated craft costs for " + all.Count + " items");
            await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
        }
    }


    public bool TryGetCost(string itemId, out double cost)
    {
        return costs.TryGetValue(itemId, out cost);
    }
}
#nullable disable
