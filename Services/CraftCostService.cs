using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Crafts.Client.Api;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

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
            foreach (var craft in all)
            {
                costs[craft.ItemId] = craft.CraftCost;
            }
            logger.LogInformation("Updated craft costs for " + all.Count + " items");
            await Task.Delay(1000 * 60 * 60 * 24, stoppingToken);
        }
    }


    public bool TryGetCost(string itemId, out double cost)
    {
        return costs.TryGetValue(itemId, out cost);
    }
}
#nullable disable
