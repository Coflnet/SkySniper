using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Sniper.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace SkySniper.Benchmarks;

/// <summary>
/// Synthetic snipe-path replay: populates a SniperService with a few hundred item tags across the main
/// archetypes (plain, enchanted, pet, attribute gear), then streams probe auctions through TestNewAuction
/// and reports throughput / latency percentiles / allocation per auction. Deterministic (seeded RNG) so
/// runs are comparable across code changes on the same machine.
/// Usage: dotnet run -c Release [--probes N] [--tags N] [--verify]
/// </summary>
public static class Replay
{
    private static long idCounter = 1_000_000;
    private static readonly Random rng = new(42);

    private static readonly (Enchantment.EnchantmentType type, byte lvl)[] EnchantPool =
    {
        (Enchantment.EnchantmentType.sharpness, 6),
        (Enchantment.EnchantmentType.growth, 6),
        (Enchantment.EnchantmentType.protection, 6),
        (Enchantment.EnchantmentType.critical, 6),
        (Enchantment.EnchantmentType.giant_killer, 6),
        (Enchantment.EnchantmentType.ender_slayer, 6),
        (Enchantment.EnchantmentType.smite, 7),
        (Enchantment.EnchantmentType.ultimate_legion, 5),
        (Enchantment.EnchantmentType.ultimate_wise, 5),
        (Enchantment.EnchantmentType.first_strike, 4),
        (Enchantment.EnchantmentType.luck, 6),
        (Enchantment.EnchantmentType.looting, 4),
    };

    public static void Main(string[] args)
    {
        int probes = ArgInt(args, "--probes", 20_000);
        int tags = ArgInt(args, "--tags", 250);
        bool verify = args.Contains("--verify");

        SniperService.MIN_TARGET = 0;
        SniperService.StartTime = DateTime.UtcNow.Date.AddDays(-45);
        var itemService = new HypixelItemService(null, NullLogger<HypixelItemService>.Instance);
        var craftCost = new CraftCostMock();
        var service = new SniperService(itemService, null, NullLogger<SniperService>.Instance, craftCost);
        if (verify)
        {
            SniperService.VerifyClosestIndex = true;
            SniperService.VerifyDominatorIndex = true;
            Console.WriteLine("parity guards ON (plus set SNIPER_VERIFY_CLEANPRICE_MEMO/SNIPER_VERIFY_PARSE_MEMO in env)");
        }

        long snipes = 0;
        service.FoundSnipe += _ => snipes++;

        Console.WriteLine($"populating {tags} tags ...");
        var sw = Stopwatch.StartNew();
        var seeds = Populate(service, tags);
        service.FinishedUpdate();
        service.State = SniperState.FullyLoaded;
        Console.WriteLine($"populated in {sw.ElapsedMilliseconds} ms; lookups={service.Lookups.Count}");

        // one JIT warm-up sweep that is not measured
        foreach (var a in MakeProbes(seeds, 2_000))
            service.TestNewAuction(a);
        service.FinishedUpdate();

        RunPass(service, seeds, probes, "cold ");
        RunPass(service, seeds, probes, "warm ");
        Console.WriteLine($"snipes fired: {snipes}");
    }

    private static void RunPass(SniperService service, List<SaveAuction> seeds, int probes, string label)
    {
        var stream = MakeProbes(seeds, probes);
        var lat = new long[probes];
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);
        long allocStart = GC.GetAllocatedBytesForCurrentThread();
        var total = Stopwatch.StartNew();
        for (int i = 0; i < probes; i++)
        {
            long t0 = Stopwatch.GetTimestamp();
            service.TestNewAuction(stream[i]);
            lat[i] = Stopwatch.GetTimestamp() - t0;
            if (i % 500 == 499)
                service.FinishedUpdate(); // updater cadence; ProcessLbins etc. (included in wall time, not in per-auction latency)
        }
        total.Stop();
        long alloc = GC.GetAllocatedBytesForCurrentThread() - allocStart;
        Array.Sort(lat);
        double ticksPerUs = Stopwatch.Frequency / 1_000_000.0;
        double P(double q) => lat[(int)(q * (probes - 1))] / ticksPerUs;
        Console.WriteLine($"{label}| {probes / total.Elapsed.TotalSeconds,8:F0} auctions/s | " +
            $"p50 {P(0.50),7:F0} us | p90 {P(0.90),7:F0} us | p99 {P(0.99),8:F0} us | p999 {P(0.999),8:F0} us | " +
            $"{alloc / probes,8} B/auction | GC {GC.CollectionCount(0) - g0}/{GC.CollectionCount(1) - g1}/{GC.CollectionCount(2) - g2}");
    }

    /// <summary>Populates buckets and returns one representative seed auction per (tag, key-variant).</summary>
    private static List<SaveAuction> Populate(SniperService service, int tags)
    {
        var seeds = new List<SaveAuction>();
        for (int t = 0; t < tags; t++)
        {
            int archetype = t % 4;
            string tag = archetype switch
            {
                0 => $"BENCH_SWORD_{t}",
                1 => $"BENCH_ARMOR_{t}",
                2 => $"PET_BENCH{t}",
                _ => $"BENCH_CRIMSON_{t}",
            };
            // popular tags carry many key-variant buckets in production; skew: a few heavy tags, many light ones
            int variants = t % 10 == 0 ? 40 : t % 3 == 1 ? 12 : 3;
            for (int v = 0; v < variants; v++)
            {
                var proto = MakeAuction(tag, archetype, v, 5_000_000 + (t * 13 + v * 7) % 40 * 1_000_000);
                int sales = 8 + (t + v) % 12;
                for (int s = 0; s < sales; s++)
                {
                    var sale = Duplicate(proto);
                    sale.End = SniperService.StartTime + TimeSpan.FromDays(40 - s % 12);
                    sale.Start = sale.End - TimeSpan.FromDays(1);
                    service.AddSoldItem(sale);
                }
                service.FinishedUpdate();
                seeds.Add(proto);
            }
        }
        return seeds;
    }

    /// <summary>60% exact re-list, 25% perturbed modifiers (closest/dominator path), 15% enriched key (combined path).</summary>
    private static List<SaveAuction> MakeProbes(List<SaveAuction> seeds, int count)
    {
        var probes = new List<SaveAuction>(count);
        for (int i = 0; i < count; i++)
        {
            var seed = seeds[rng.Next(seeds.Count)];
            var a = Duplicate(seed);
            a.StartingBid = Math.Max(1000, seed.HighestBidAmount / 2); // low ask so the flip path triggers
            a.HighestBidAmount = 0;
            a.Bin = true;
            a.Start = DateTime.UtcNow;
            a.End = DateTime.UtcNow + TimeSpan.FromHours(12);
            int roll = rng.Next(100);
            if (roll >= 60 && roll < 85)
            {
                // perturb: change one nbt value / add an enchant so no exact bucket exists
                if (a.FlatenedNBT.Count > 0)
                {
                    var k = a.FlatenedNBT.Keys.First();
                    a.FlatenedNBT[k] = (rng.Next(1, 9) * 3).ToString();
                }
                else
                    AddEnchant(a, rng.Next(EnchantPool.Length));
            }
            else if (roll >= 85)
            {
                AddEnchant(a, rng.Next(EnchantPool.Length));
                AddEnchant(a, rng.Next(EnchantPool.Length));
                a.FlatenedNBT["rarity_upgrades"] = "1";
            }
            probes.Add(a);
        }
        return probes;
    }

    private static void AddEnchant(SaveAuction a, int poolIdx)
    {
        var (type, lvl) = EnchantPool[poolIdx];
        if (a.Enchantments.Any(e => e.Type == type))
            return;
        a.Enchantments.Add(new Enchantment(type, lvl));
    }

    private static SaveAuction MakeAuction(string tag, int archetype, int variant, long price)
    {
        var a = new SaveAuction
        {
            Tag = tag,
            FlatenedNBT = new Dictionary<string, string>(),
            Enchantments = new List<Enchantment>(),
            StartingBid = price,
            HighestBidAmount = price,
            Category = archetype == 2 ? Category.MISC : archetype == 0 ? Category.WEAPON : Category.ARMOR,
            Count = 1,
            Bin = true,
        };
        switch (archetype)
        {
            case 0: // weapon with a few enchants
                for (int e = 0; e <= variant; e++)
                    AddEnchant(a, (variant * 2 + e) % EnchantPool.Length);
                break;
            case 1: // armor with enchants + a skin sometimes
                AddEnchant(a, (variant * 3) % EnchantPool.Length);
                if (variant % 2 == 0)
                    a.FlatenedNBT["skin"] = "bear";
                break;
            case 2: // pet: tier + exp
                a.Tier = variant % 2 == 0 ? Tier.LEGENDARY : Tier.EPIC;
                a.FlatenedNBT["exp"] = (variant * 5_000_000 + 100_000).ToString();
                a.FlatenedNBT["candyUsed"] = "0";
                break;
            default: // kuudra-style attribute gear
                a.FlatenedNBT["magic_find"] = (2 + variant).ToString();
                a.FlatenedNBT["veteran"] = (3 + variant % 4).ToString();
                break;
        }
        return Duplicate(a); // assign ids
    }

    private static SaveAuction Duplicate(SaveAuction origin)
    {
        var id = idCounter++;
        return new SaveAuction(origin)
        {
            Uuid = id.ToString("x").PadLeft(32, '0'),
            UId = id,
            AuctioneerId = ((short)(id % 20000)).ToString().PadLeft(8, '0'),
            FlatenedNBT = new Dictionary<string, string>(origin.FlatenedNBT),
            Enchantments = origin.Enchantments == null ? null : new List<Enchantment>(origin.Enchantments),
            Bids = new List<SaveBids> { new() { Amount = origin.HighestBidAmount, Bidder = ((short)(id % 17000)).ToString().PadLeft(8, '0') } },
        };
    }

    private static int ArgInt(string[] args, string name, int def)
    {
        var i = Array.IndexOf(args, name);
        return i >= 0 && i + 1 < args.Length ? int.Parse(args[i + 1]) : def;
    }

    private class CraftCostMock : ICraftCostService
    {
        public System.Collections.Concurrent.ConcurrentDictionary<string, Category> ItemCategories { get; } = new();
        public Dictionary<string, double> Costs { get; } = new();
        public void AddCostForSpecialItems() { }
        public bool TryGetCost(string itemId, out double cost)
        {
            cost = 0;
            return false;
        }
    }
}
