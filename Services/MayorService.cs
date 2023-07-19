using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using System.Collections.Generic;
using Coflnet.Sky.Mayor.Client.Api;
using Coflnet.Sky.Core;
using System;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper.Services;

public interface IMayorService
{
    string GetMayor(DateTime time);
}

public class MayorService : BackgroundService, IMayorService
{
    private Dictionary<int, string> YearToMayorName = new();
    private Mayor.Client.Api.IMayorApi mayorApi;
    private IElectionPeriodsApi electionPeriodsApi;
    private readonly ILogger<MayorService> logger;

    public MayorService(IMayorApi mayorApi, IElectionPeriodsApi electionPeriodsApi, ILogger<MayorService> logger)
    {
        this.mayorApi = mayorApi;
        this.electionPeriodsApi = electionPeriodsApi;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await InitMayors();
        while (!stoppingToken.IsCancellationRequested)
        {
            int year = ElectionYear(DateTime.UtcNow);
            await LoadMayorForYear(year);
            await LoadMayorForYear(year - 1);
            await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
        }
    }

    private static int ElectionYear(DateTime time)
    {
        return (int)(Constants.SkyblockYear(time) - 0.2365635);
    }

    private async Task LoadMayorForYear(int year)
    {
        var mayor = await electionPeriodsApi.ElectionPeriodYearGetAsync(year);
        if (mayor?.Winner != null)
            YearToMayorName[mayor.Year] = mayor.Winner.Name;
        logger.LogInformation("Loaded mayor for year " + year + " " + mayor?.Winner?.Name);
    }

    private async Task InitMayors()
    {
        var mayors = await electionPeriodsApi.ElectionPeriodRangeGetAsync(0, System.DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        if (mayors == null)
        {
            logger.LogError("Failed to load mayors");
            return;
        }
        foreach (var mayor in mayors)
        {
            YearToMayorName[mayor.Year] = mayor.Winner.Name;
        }
        logger.LogInformation("Loaded " + mayors.Count + " mayors");
        logger.LogInformation("Current mayor is " + GetMayor(DateTime.UtcNow));
    }

    public string GetMayor(DateTime time)
    {
        if (YearToMayorName.TryGetValue(ElectionYear(time), out var name))
            return name;
        return "Unknown";
    }
}
#nullable disable
