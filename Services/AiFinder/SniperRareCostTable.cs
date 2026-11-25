using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.AIFinder.Features;
using Coflnet.Sky.AIFinder.Model;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

/// <summary>
/// The finder's current coin costs of rare modifiers (runes by level, reforges, enchantment levels) as this sniper prices them.
/// Reads come from an immutable snapshot that a background refresh swaps in whole, so a read never blocks and never sees a
/// half built table (the finder reads it inside its prediction lock).
/// </summary>
public sealed class SniperRareCostTable : BackgroundService, IRareCostTable
{
    internal const int MaxLevel = 10;
    internal const int HistoryLength = 12;
    private const int MaxRuneLevel = 5;
    private const string UniquePrefix = "UNIQUE_";
    private static readonly TimeSpan DefaultRefreshInterval = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan DefaultRetryInterval = TimeSpan.FromSeconds(10);

    private readonly SniperService sniper;
    private readonly ILogger<SniperRareCostTable> logger;
    private readonly TimeSpan refreshInterval;
    private readonly TimeSpan retryInterval;
    private readonly Func<SniperService, Dictionary<string, double>> build;
    private readonly CostHistory history = new(HistoryLength);
    private volatile IReadOnlyDictionary<string, double> snapshot = new Dictionary<string, double>();
    private volatile bool hasSnapshot;

    public SniperRareCostTable(SniperService sniper, ILogger<SniperRareCostTable> logger)
        : this(sniper, logger, DefaultRefreshInterval, DefaultRetryInterval)
    {
    }

    internal SniperRareCostTable(SniperService sniper, ILogger<SniperRareCostTable> logger, TimeSpan refreshInterval, TimeSpan retryInterval,
        Func<SniperService, Dictionary<string, double>>? build = null)
    {
        this.build = build ?? Build;
        this.sniper = sniper;
        this.logger = logger;
        this.refreshInterval = refreshInterval;
        this.retryInterval = retryInterval;
    }

    /// <summary>
    /// Whether a build with at least one rune or reforge cost was served. Before that every read is null (no cap), so a host
    /// that caps premiums at cost must not value or learn from sales yet.
    /// </summary>
    public bool HasSnapshot => hasSnapshot;

    public double? Cost(string tag, string feature, float value)
    {
        var key = RareKeyRule.IsLevelFeature(feature) ? RareKeyRule.LevelKey(feature, value) : feature;
        return snapshot.TryGetValue(key, out var cost) && cost > 0 ? cost : null;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // runes and reforges come from lookups that may load later than the bazaar, so retry shortly until they are there
        while (!hasSnapshot)
        {
            Refresh();
            if (!hasSnapshot)
                await Task.Delay(retryInterval, stoppingToken);
        }
        using var timer = new PeriodicTimer(refreshInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
            Refresh();
    }

    /// <summary>Builds a reading, folds it into the history and swaps the served snapshot in; a build that fails twice changes nothing</summary>
    internal void Refresh()
    {
        var reading = TryBuild();
        if (reading == null)
            return;
        snapshot = history.Add(reading);
        MarkReady();
    }

    private void MarkReady()
    {
        if (hasSnapshot || !snapshot.Keys.Any(IsRuneOrReforge))
            return;
        hasSnapshot = true;
        logger.LogInformation("Rare cost table is ready with {Count} costs", snapshot.Count);
    }

    private static bool IsRuneOrReforge(string key)
        => key.StartsWith("rune/", StringComparison.Ordinal) || key.StartsWith("reforge/", StringComparison.Ordinal);

    /// <summary>The bazaar prices are written while this reads them, so a failed read is tried once more</summary>
    private Dictionary<string, double>? TryBuild()
    {
        for (var attempt = 1; attempt <= 2; attempt++)
        {
            try
            {
                return build(sniper);
            }
            catch (Exception e)
            {
                logger.LogWarning(e, "Could not build the rare cost table (attempt {Attempt} of 2)", attempt);
            }
        }
        return null;
    }

    internal static Dictionary<string, double> Build(SniperService sniper)
    {
        var costs = new Dictionary<string, double>(StringComparer.Ordinal);
        AddRunes(costs, sniper);
        AddReforges(costs, sniper);
        AddEnchants(costs, sniper);
        return costs;
    }

    /// <summary>Per rune and level the lowest price of a lookup key of that level; a unique rune counts as the rune it is applied as</summary>
    private static void AddRunes(Dictionary<string, double> costs, SniperService sniper)
    {
        foreach (var (tag, lookup) in sniper.Lookups.Where(l => IsRuneItem(l.Key)))
            for (var level = 1; level <= MaxRuneLevel; level++)
                AddLowest(costs, $"rune/{tag.Replace(UniquePrefix, "")}={level}", LevelPrice(lookup, level));
    }

    private static bool IsRuneItem(string tag)
        => tag.StartsWith("RUNE_", StringComparison.Ordinal) || tag.StartsWith(UniquePrefix + "RUNE_", StringComparison.Ordinal);

    /// <summary>The lowest price of the lookup's keys of a level (their first modifier), 0 without one</summary>
    private static long LevelPrice(PriceLookup lookup, int level)
    {
        var text = level.ToString(CultureInfo.InvariantCulture);
        return lookup.Lookup
            .Where(l => l.Value.Price > 0 && l.Key.Modifiers.FirstOrDefault().Value == text)
            .Select(l => l.Value.Price)
            .DefaultIfEmpty(0)
            .Min();
    }

    private static void AddLowest(Dictionary<string, double> costs, string key, long price)
    {
        if (price > 0 && (!costs.TryGetValue(key, out var known) || price < known))
            costs[key] = price;
    }

    private static void AddReforges(Dictionary<string, double> costs, SniperService sniper)
    {
        foreach (var reforge in Enum.GetValues<ItemReferences.Reforge>().Where(r => !ReforgeGroup.Ignored.Contains(r)))
        {
            if (sniper.ExactReforgeValue(reforge) is { } value)
                costs[$"reforge/reforge={reforge}"] = value;
        }
    }

    /// <summary>The sniper's price of every enchantment level it has a real price for; extrapolated levels have no cost</summary>
    private static void AddEnchants(Dictionary<string, double> costs, SniperService sniper)
    {
        foreach (var type in Enum.GetValues<Enchantment.EnchantmentType>())
            AddEnchantLevels(costs, sniper, type);
    }

    private static void AddEnchantLevels(Dictionary<string, double> costs, SniperService sniper, Enchantment.EnchantmentType type)
    {
        for (var level = 1; level <= MaxLevel; level++)
            if (sniper.ExactEnchantCost(type, level) is { } value)
                costs[$"enchant/{type}={level}"] = value;
    }
}

/// <summary>
/// The last readings of each cost. A cost is served as the median of the readings it has, so one odd reading does not move it, and
/// a cost missing from a reading is served from the older ones until it was missing from all of them.
/// </summary>
internal sealed class CostHistory(int length)
{
    private readonly Queue<Dictionary<string, double>> readings = new();

    /// <returns>A new snapshot that is not touched again</returns>
    public IReadOnlyDictionary<string, double> Add(Dictionary<string, double> reading)
    {
        readings.Enqueue(reading);
        if (readings.Count > length)
            readings.Dequeue();
        return Median();
    }

    private Dictionary<string, double> Median()
    {
        var served = new Dictionary<string, double>(StringComparer.Ordinal);
        foreach (var key in readings.SelectMany(r => r.Keys).Distinct(StringComparer.Ordinal))
            served[key] = MedianOf(readings.Where(r => r.ContainsKey(key)).Select(r => r[key]).Order().ToList());
        return served;
    }

    private static double MedianOf(List<double> sorted)
    {
        var middle = sorted.Count / 2;
        return sorted.Count % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    }
}
