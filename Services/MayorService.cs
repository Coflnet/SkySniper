using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using System.Collections.Generic;
using Coflnet.Sky.Mayor.Client.Api;
using Coflnet.Sky.Core;
using System;

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

    public MayorService(IMayorApi mayorApi, IElectionPeriodsApi electionPeriodsApi)
    {
        this.mayorApi = mayorApi;
        this.electionPeriodsApi = electionPeriodsApi;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await InitMayors();
        while (!stoppingToken.IsCancellationRequested)
        {
            var year = (int)Constants.SkyblockYear(DateTime.UtcNow);
            var mayor = await electionPeriodsApi.ElectionPeriodYearGetAsync(year);
            if (mayor?.Winner != null)
                YearToMayorName[year] = mayor.Winner.Name;
            await Task.Delay(1000 * 60 * 60, stoppingToken);
        }
    }

    private async Task InitMayors()
    {
        var mayors = await electionPeriodsApi.ElectionPeriodRangeGetAsync(0, System.DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        if(mayors == null)
            return;
        foreach (var mayor in mayors)
        {
            YearToMayorName[mayor.Year] = mayor.Winner.Name;
        }
    }

    public string GetMayor(DateTime time)
    {
        if (YearToMayorName.TryGetValue((int)Constants.SkyblockYear(time), out var name))
            return name;
        return "Unknown";
    }
}
#nullable disable
