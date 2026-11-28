using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>
/// Picks the clean price of each item (group) served by the <c>prices/clean</c> endpoint.
/// Only persisted bucket state (price, references, lowest bins, key) is read, so the same persisted state gives the
/// same prices before and after a restart. In-memory fields such as <see cref="ReferenceAuctions.TimeToSell"/> and
/// <see cref="ReferenceAuctions.Volume"/> are only filled once a bucket is recomputed after a restart and must not
/// decide which bucket is picked.
/// </summary>
public static class CleanPriceSelector
{
    /// <summary>Prices at or below this are placeholders (eg. 1 coin), never a real clean price</summary>
    public const long MaxPlaceholderPrice = 1;
    /// <summary>A bucket with fewer references than this is too thin to stand for the clean price</summary>
    public const int MinReferences = 4;
    /// <summary>A bucket selling less often than once per this many days is too thin to stand for the clean price</summary>
    public const double MinDailyVolume = 0.1;

    /// <summary>
    /// Holds the listings of <see cref="SniperService.HyperionGroup"/> too. Their lbins are stored at their raw price
    /// without the conversion cost and don't say which item they are, so a cheap NECRON_BLADE bin must not cap it.
    /// </summary>
    private const string HyperionGroupTag = "HYPERION";

    private record Candidate(int Extras, long Price, bool WellSupported);

    public static Dictionary<string, long> Select(IEnumerable<KeyValuePair<string, PriceLookup>> lookups)
    {
        var today = SniperService.GetDay();
        var dictionary = lookups
            .SelectMany(l => PricesOf(l.Key, l.Value, today))
            .Where(g => g.price > 0)
            .ToDictionary(g => g.group, g => g.price);
        ExpandHyperionGroup(dictionary);
        return dictionary;
    }

    private static IEnumerable<(string group, long price)> PricesOf(string tag, PriceLookup lookup, short today)
    {
        var capAtLowestBin = tag != HyperionGroupTag;
        return lookup.Lookup
            // placeholders are judged on the bucket total, a cheap stack can still be worth 1 coin per item
            // tier boosted pets are priced as their own item and are never the clean price of a plain pet
            .Where(b => b.Value.Price > MaxPlaceholderPrice && !SniperService.HasTierBoost(b.Key.Modifiers))
            .GroupBy(b => GroupKey(tag, b.Key))
            .Select(g => (g.Key, PickPrice(g.Select(b => ToCandidate(b, today, capAtLowestBin)))));
    }

    /// <summary>
    /// The key an item variant is priced under. Pets are split by level, attribute shards and runes by level and
    /// attribute items by their attributes; everything else is priced by its tag.
    /// </summary>
    internal static string GroupKey(string tag, AuctionKey key)
    {
        if (tag.StartsWith("PET_") && !tag.StartsWith("PET_ITEM_") && !tag.StartsWith("PET_SKIN_"))
            return $"{tag}_{key.Tier}_{PetLevelGroup(key)}";
        if (tag == "ATTRIBUTE_SHARD" && key.Modifiers.First().Value is "7" or "5")
            return $"{tag}+{key.Modifiers.First().Key};{key.Modifiers.First().Value}";
        if (tag.StartsWith("RUNE_"))
            return $"{tag}{(key.Modifiers.Count > 0 ? $"_{key.Modifiers.First().Value}" : "")}";
        if (key.Modifiers.Any(m => m.Key == "virtual"))
            return $"{tag}+{key.Modifiers.First(m => m.Key != "virtual").Key};1";
        var attributes = key.Modifiers?.Where(m => Constants.AttributeKeys.Contains(m.Key)).ToList();
        if (attributes.Count > 0)
            return $"{tag}+{string.Join("_", attributes.Select(m => m.Key))}";
        return tag;
    }

    private static int PetLevelGroup(AuctionKey key) => key.Modifiers?.FirstOrDefault(m => m.Key == "exp").Value switch
    {
        "7" => 100,
        "6" => 100,
        "5" => 90,
        _ => 0,
    };

    /// <summary>
    /// Prefers the bucket with the fewest extras (the clean one) over exotic variants, ignores thin buckets when a
    /// well supported one exists and takes the cheapest of equally clean buckets. Ties only ever share a price, so
    /// the result does not depend on the order buckets are enumerated in.
    /// </summary>
    private static long PickPrice(IEnumerable<Candidate> candidates)
    {
        var priced = candidates.Where(c => c.Price > 0).ToList();
        if (priced.Count == 0)
            return 0;
        var supported = priced.Where(c => c.WellSupported).ToList();
        var pool = supported.Count > 0 ? supported : priced;
        return pool.OrderBy(c => c.Extras).ThenBy(c => c.Price).First().Price;
    }

    private static Candidate ToCandidate(KeyValuePair<AuctionKey, ReferenceAuctions> bucket, short today, bool capAtLowestBin)
    {
        var stackSize = bucket.Key.Count == 0 ? 1 : bucket.Key.Count;
        var total = capAtLowestBin ? CapAtLowestBin(bucket.Value, today) : bucket.Value.Price;
        var price = total / stackSize;
        return new Candidate(Extras(bucket.Key), price, IsWellSupported(bucket.Value, today));
    }

    /// <summary>Nobody pays more than the cheapest active bin of the same variant</summary>
    private static long CapAtLowestBin(ReferenceAuctions bucket, short today)
    {
        // lbins are sorted by price, the first active one is the cheapest; Day is the day the auction ends
        var lowestBin = bucket.Lbins?.FirstOrDefault(l => l.Price > MaxPlaceholderPrice && l.Day >= today) ?? default;
        if (lowestBin.Price == 0)
            return bucket.Price;
        return Math.Min(bucket.Price, lowestBin.Price);
    }

    /// <summary>How much a variant carries on top of the clean item</summary>
    internal static int Extras(AuctionKey key)
    {
        var reforged = key.Reforge is ItemReferences.Reforge.Any or ItemReferences.Reforge.None ? 0 : 1;
        return (key.Enchants?.Count ?? 0) + (key.Modifiers?.Count ?? 0) + reforged;
    }

    /// <summary>Volume from the persisted references (the in-memory volume is only set after a recompute)</summary>
    internal static bool IsWellSupported(ReferenceAuctions bucket, short today)
    {
        var references = bucket.ReferenceSnapshot();
        if (references.Length < MinReferences)
            return false;
        var oldestDay = references.Min(r => r.Day);
        var days = Math.Max(1, today - oldestDay + 1);
        return (double)references.Length / days >= MinDailyVolume;
    }

    private static void ExpandHyperionGroup(Dictionary<string, long> dictionary)
    {
        if (!dictionary.TryGetValue("HYPERION", out var hyperion))
            return;
        foreach (var item in SniperService.HyperionGroup)
            dictionary[item] = hyperion;
    }
}
