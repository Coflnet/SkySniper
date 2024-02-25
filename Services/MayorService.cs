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
    private readonly Dictionary<int, string> YearToMayorName = new();
    private readonly Mayor.Client.Api.IMayorApi mayorApi;
    private readonly IElectionPeriodsApi electionPeriodsApi;
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
        List<Mayor.Client.Model.ModelElectionPeriod> mayors = null;
        while (mayors == null)
        {
            try
            {
                mayors = await electionPeriodsApi.ElectionPeriodRangeGetAsync(0, DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000);
            }
            catch (System.Exception)
            {

            }
            if (mayors == null)
            {
                logger.LogError("Failed to load mayors");
                await Task.Delay(10000);
            }
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
