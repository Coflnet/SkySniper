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
    string GetPreviousMayor(DateTime time);
    /// <summary>
    /// Returns true if Diana-related items should be adjusted down.
    /// This is 8 hours before the election year ends (when Diana was mayor) and 48 hours after.
    /// </summary>
    bool IsDianaItemsAdjustmentActive(DateTime time);
}

public class MayorService : BackgroundService, IMayorService
{
    private readonly Dictionary<int, string> YearToMayorName = new();
    private readonly Mayor.Client.Api.IMayorApi mayorApi;
    private readonly IElectionPeriodsApi electionPeriodsApi;
    private readonly ILogger<MayorService> logger;

    /// <summary>
    /// Items related to Diana's mythos event that drop in demand when Diana's term ends.
    /// </summary>
    public static readonly HashSet<string> DianaRelatedItems = new()
    {
        "ANCESTRAL_SPADE",
        "ARCHAIC_SPADE",
        "CROWN_OF_AVARICE",
        "MYTHOS_LEGGINGS",
        "MYTHOS_NECKLACE",
        "MYTHOS_FRAGMENT",
        "MYTHOS_CHESTPLATE",
        "MYTHOS_BRACELET",
        "MYTHOS_BOOTS",
        "MYTHOS_BELT",
        "DIANAS_BOOKSHELF",
        "DAEDALUS_STICK",
        "CHALLENGER_NECKLACE",
        "CHALLENGER_LEGGINGS",
        "CHALLENGER_CHESTPLATE",
        "CHALLENGER_BOOTS",
        "CHALLENGER_BRACELET",
        "CHALLENGER_BELT",
        "CHALLENGER_HELMET",
        "CHALLENGER_CLOAK"
    };

    public MayorService(IMayorApi mayorApi, IElectionPeriodsApi electionPeriodsApi, ILogger<MayorService> logger)
    {
        this.mayorApi = mayorApi;
        this.electionPeriodsApi = electionPeriodsApi;
        this.logger = logger;
    }

    /// <summary>
    /// For testing purposes - sets a mayor for a specific year
    /// </summary>
    public void SetMayorForYear(int year, string mayorName)
    {
        YearToMayorName[year] = mayorName;
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

    /// <summary>
    /// Calculates the election year for a given time.
    /// Elections happen at ~23.65% into the Skyblock year.
    /// </summary>
    public static int ElectionYear(DateTime time)
    {
        return (int)(Constants.SkyblockYear(time) - 0.2365635);
    }

    private async Task LoadMayorForYear(int year)
    {
        try
        {

            var mayor = await electionPeriodsApi.ElectionPeriodYearGetAsync(year);
            if (mayor?.Winner != null)
                YearToMayorName[mayor.Year] = mayor.Winner.Name;
            logger.LogInformation("Loaded mayor for year " + year + " " + mayor?.Winner?.Name);
        }
        catch (System.Exception e)
        {
            logger.LogError(e, "Failed to load mayor for year " + year);
        }
    }

    private async Task InitMayors()
    {
        List<Mayor.Client.Model.CoflnetSkyMayorModelsModelElectionPeriod> mayors = null;
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
            if (mayor == null || mayor.Winner == null)
                continue;
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

    public string GetPreviousMayor(DateTime time)
    {
        var previousYear = ElectionYear(time) - 1;
        if (YearToMayorName.TryGetValue(previousYear, out var name))
            return name;
        return "Unknown";
    }

    /// <summary>
    /// Diana-related items should be adjusted down when Diana was the last mayor
    /// and we are 8 hours before the election year ends OR up to 48 hours after.
    /// A Skyblock year is ~5 days 4 hours (124 hours real time).
    /// </summary>
    public bool IsDianaItemsAdjustmentActive(DateTime time)
    {
        var currentMayor = GetMayor(time);
        var previousMayor = GetPreviousMayor(time);

        // If Diana is the current mayor, check if we're in the last 8 hours of her term
        if (string.Equals(currentMayor, "Diana", StringComparison.OrdinalIgnoreCase))
        {
            // Check if we're within 8 hours of the election year end
            var yearFraction = Constants.SkyblockYear(time) - ElectionYear(time);
            // Election happens at 0.2365635 into the year, so end of mayor term is around 1.0
            // We want the last 8 hours of the term
            // 8 hours is 8/124 = ~0.0645 of a Skyblock year
            const double hoursBeforeEnd = 8.0 / 124.0;
            if (yearFraction >= (1 - hoursBeforeEnd))
            {
                return true;
            }
        }

        // If Diana was the previous mayor, check if we're within 48 hours after election
        if (string.Equals(previousMayor, "Diana", StringComparison.OrdinalIgnoreCase))
        {
            var yearFraction = Constants.SkyblockYear(time) - ElectionYear(time);
            // 48 hours is 48/124 = ~0.387 of a Skyblock year
            const double hoursAfterStart = 48.0 / 124.0;
            // Election happens at 0.2365635, so if we're within 48 hours after that point
            // Year fraction should be near the start (elections happen early in the year)
            if (yearFraction <= hoursAfterStart)
            {
                return true;
            }
        }

        return false;
    }
}
