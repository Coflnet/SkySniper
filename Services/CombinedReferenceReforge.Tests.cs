using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>
/// Regression test for the "submerged references leak into plain items" bug: the combined-reference finder
/// (<see cref="SniperService.CheckCombined"/>) used to treat an <c>Any</c> query reforge as a wildcard and pool
/// references from buckets with a value-adding reforge (e.g. submerged). A plain, unreforged item then borrowed the
/// much pricier submerged references and got flagged as a large flip. Real-world example: auction edb094ad
/// (a ~50m Submerged Magma Lord Helmet) was referenced when pricing plain fishing_experience helmets.
/// </summary>
public class CombinedReferenceReforgeTests
{
    SniperService service;
    List<LowPricedAuction> found;

    private class MockCraftCostService : ICraftCostService
    {
        public Dictionary<string, double> Costs { get; } = new();
        public ConcurrentDictionary<string, Category> ItemCategories { get; set; } = new();
        public void AddCostForSpecialItems() { }
        public bool TryGetCost(string itemId, out double cost) => Costs.TryGetValue(itemId, out cost);
    }

    [SetUp]
    public void Setup()
    {
        SniperService.MIN_TARGET = 0;
        SniperService.StartTime = DateTime.UtcNow.Date.AddDays(-45);
        service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, new MockCraftCostService());
        found = new List<LowPricedAuction>();
        service.FoundSnipe += found.Add;
    }

    [TearDown]
    public void TearDown() => SniperService.MIN_TARGET = 200_000;

    // A valuable submerged helmet (clean: no attributes) worth ~50m.
    static SaveAuction Submerged(long uid, int day)
    {
        return new SaveAuction
        {
            Tag = "MAGMA_LORD_HELMET",
            Tier = Tier.LEGENDARY,
            Reforge = ItemReferences.Reforge.submerged,
            Count = 1,
            UId = uid,
            StartingBid = 50_000_000,
            HighestBidAmount = 50_000_000,
            End = SniperService.StartTime.AddDays(day),
            AuctioneerId = (0x1000 + uid).ToString("x4") + "00000000000000000000000000",
            Category = Category.ARMOR,
            Enchantments = new List<Enchantment>(),
            FlatenedNBT = new Dictionary<string, string>(),
        };
    }

    // A cheap plain (unreforged) helmet whose only value driver is a fishing_experience attribute.
    static SaveAuction FishingAttr(long uid, int day, long price = 8_000_000)
    {
        return new SaveAuction
        {
            Tag = "MAGMA_LORD_HELMET",
            Tier = Tier.LEGENDARY,
            Reforge = ItemReferences.Reforge.None,
            Count = 1,
            UId = uid,
            StartingBid = price,
            HighestBidAmount = price,
            End = SniperService.StartTime.AddDays(day),
            AuctioneerId = (0x2000 + uid).ToString("x4") + "00000000000000000000000000",
            Category = Category.ARMOR,
            Enchantments = new List<Enchantment>(),
            FlatenedNBT = new Dictionary<string, string> { { "fishing_experience", "1" } },
        };
    }

    // Make an item resolvable by GetCostForItem/GetPriceForItem (both read Lookups) so reforge-value subtraction
    // behaves as it does in production, where the reforge stone has a known price.
    void SeedItemPrice(string tag, long price)
    {
        service.Lookups[tag] = new PriceLookup
        {
            Lookup = new(new Dictionary<AuctionKey, ReferenceAuctions>
            {
                { new AuctionKey { Enchants = new(new List<Models.Enchant>()), Reforge = ItemReferences.Reforge.Any, Modifiers = new(new List<KeyValuePair<string, string>>()), Tier = Tier.LEGENDARY, Count = 1 },
                  new ReferenceAuctions { Price = price } }
            })
        };
    }

    void UpdateAllMedians()
    {
        foreach (var lookup in service.Lookups)
            foreach (var item in lookup.Value.Lookup)
                service.UpdateMedian(item.Value, (lookup.Key, service.GetBreakdownKey(item.Key, lookup.Key)));
    }

    [Test]
    public void PlainItemNotPricedFromValuableReforgeReferences()
    {
        // valuable submerged helmets sell recently and high ...
        for (int i = 0; i < 8; i++)
            service.AddSoldItem(Submerged(200 + i, 37 + i));
        // ... plain fishing_experience helmets sell cheaply and a bit older.
        for (int i = 0; i < 4; i++)
            service.AddSoldItem(FishingAttr(100 + i, 20 + i));
        UpdateAllMedians();
        // Seed the submerged reforge stone price AFTER the median pass (which resets reference-less buckets to 0).
        SeedItemPrice("DEEP_SEA_ORB", 40_000_000);
        service.State = SniperState.FullyLoaded;

        // A plain, unreforged fishing helmet listed cheaply must NOT be flagged as a big flip by borrowing the
        // ~50m submerged references.
        var cheap = FishingAttr(999, 44, price: 8_000_000);
        service.TestNewAuction(cheap);

        var submergedIds = Enumerable.Range(200, 8).Select(i => (long)i).ToHashSet();
        var polluted = found.Where(f =>
        {
            var med = f.AdditionalProps?.GetValueOrDefault("med") ?? "";
            return med.Split(',').Where(s => long.TryParse(s, out _)).Select(long.Parse).Any(submergedIds.Contains);
        }).ToList();

        Assert.That(polluted, Is.Empty,
            "plain item priced using submerged references: " + string.Join("; ", polluted.Select(p =>
                $"{p.Finder} target={p.TargetPrice} med={p.AdditionalProps.GetValueOrDefault("med")}")));
    }
}
