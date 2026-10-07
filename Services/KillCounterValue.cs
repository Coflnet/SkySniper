using System;
using System.Collections.Generic;
using System.Linq;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>
/// Lookup key bucket and coin value of the kill counters. The key holds a bucket index, see
/// <see cref="SniperService.NormalizeGeneral"/>. Only counters whose kills showed a price difference
/// in completed sales of independent items have a table, every other counter stays at a small bounded value.
/// </summary>
public static class KillCounterValue
{
    public static readonly HashSet<string> Keys = [
        "blaze_kills",
        "blood_god_kills",
        "bow_kills",
        "eman_kills",
        "expertise_kills",
        "raider_kills",
        "runic_kills",
        "skeletorKills",
        "spider_kills",
        "sword_kills",
        "zombie_kills"
        ];

    /// <summary>
    /// Kills at which a Tarantula, Primordial, Revenant or Reaper piece reaches the next step of its bulwark bonus.
    /// Source: hypixelskyblock.minecraft.wiki/w/Tarantula_Armor and /w/Reaper_Armor, read 2026-10-02
    /// </summary>
    private static readonly int[] BulwarkTiers = [50, 300, 1_000, 2_000, 3_000, 5_000, 7_500, 10_000, 15_000, 25_000, 50_000, 100_000, 200_000, 500_000];
    /// <summary>
    /// The same for a Final Destination piece, the last step is the maximum.
    /// Source: hypixelskyblock.minecraft.wiki/w/Final_Destination_Armor, read 2026-10-02
    /// </summary>
    private static readonly int[] EndermanTiers = [100, 200, 300, 500, 800, 1_200, 1_750, 2_500, 3_500, 5_000, 10_000, 25_000, 50_000, 75_000, 100_000, 125_000, 150_000, 200_000];
    /// <summary>
    /// Final Destination pieces below the tenth tier (5,000 kills) sold at most 2M above one without kills
    /// </summary>
    private const int FirstEndermanTierInKey = 10;
    private const int BulwarkBucketSize = 10_000;

    /// <summary>
    /// Premium over the same item without the counter, indexed by bucket. Buckets 0 to 3 keep the weight they
    /// had. The higher ones are lowered to what completed sales of 2026-08-18 to 2026-10-02 showed in both
    /// compared groups, pieces without upgrades and recombobulated pieces with Ultimate Wise 5, one sale per
    /// item. A bucket without three items in one group repeats the entry below it.
    /// The last entry applies to every higher bucket.
    /// </summary>
    private static readonly Dictionary<string, long[]> SalesBacked = new()
    {
        // one bucket per tier of EndermanTiers from 5,000 kills on
        { "eman_kills", [3_000_000, 6_000_000, 12_000_000, 24_000_000, 30_000_000, 44_000_000, 53_000_000, 53_000_000, 83_000_000] },
    };

    /// <summary>
    /// No sale supported a premium above this bucket for counters without a table
    /// </summary>
    private const int HighestUnprovenBucket = 3;
    private const long UnprovenBucketDifference = 1_000_000;

    /// <summary>
    /// Lookup key bucket of the counters whose bonus follows a tier table, so that kills inside one tier share
    /// a bucket. False for every other counter.
    /// </summary>
    public static bool TryNormalizeByTier(KeyValuePair<string, string> counter, out KeyValuePair<string, string> normalized)
    {
        normalized = default;
        if (counter.Key == "eman_kills")
        {
            var kills = SniperService.GetNumeric(counter);
            var bucket = EndermanTiers.Count(tier => tier <= kills) - FirstEndermanTierInKey;
            normalized = bucket < 0 ? SniperService.Ignore : new(counter.Key, bucket.ToString());
            return true;
        }
        if (counter.Key != "spider_kills" && counter.Key != "zombie_kills")
            return false;
        var counted = SniperService.GetNumeric(counter);
        // the tiers below 10,000 kills sold alike and keep the bucket they had, above the bucket is the one the tier starts in
        if (counted >= BulwarkBucketSize)
            counted = BulwarkTiers.Last(tier => tier <= counted);
        normalized = SniperService.NormalizeNumberTo(new(counter.Key, counted.ToString()), BulwarkBucketSize);
        return true;
    }

    /// <summary>
    /// What the kills of a bucket add to an item. Never negative and at most the highest table entry.
    /// </summary>
    public static long ForBucket(string key, string bucket)
    {
        if (!TryGetBucket(bucket, out var index))
            return 0;
        if (SalesBacked.TryGetValue(key, out var premiums))
            return premiums[Math.Min(index, premiums.Length - 1)];
        return 300_000L * (1 << Math.Min(index, HighestUnprovenBucket)) + 300_000;
    }

    /// <summary>
    /// Coins to take off a reference in <paramref name="referenceBucket"/> to price an item in
    /// <paramref name="ownBucket"/> (null without the counter). Negative when the item has more kills,
    /// only half of that is added.
    /// </summary>
    public static long ReferenceDifference(string key, string referenceBucket, string ownBucket)
    {
        var difference = SalesBacked.ContainsKey(key)
            ? ForBucket(key, referenceBucket) - ForBucket(key, ownBucket)
            : (UnprovenBucket(referenceBucket) - UnprovenBucket(ownBucket)) * UnprovenBucketDifference;
        return difference < 0 ? difference / 2 : difference;
    }

    private static int UnprovenBucket(string bucket)
    {
        return TryGetBucket(bucket, out var index) ? Math.Min(index, HighestUnprovenBucket) : 0;
    }

    private static bool TryGetBucket(string bucket, out int index)
    {
        return int.TryParse(bucket, out index) && index >= 0;
    }
}
