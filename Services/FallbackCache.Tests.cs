using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class FallbackCacheTests
{
    // Canonical purchase fields from the attached cache evidence. No identities are retained.
    // Reference and component prices below are controlled state, not a historical market replay.
    private static SaveAuction Auction(int index) => JsonConvert.DeserializeObject<SaveAuction[]>(Fixtures, new JsonSerializerSettings { ObjectCreationHandling = ObjectCreationHandling.Replace })[index];

    private static SniperService CreateService(SaveAuction auction, bool exactLbin = false)
    {
        var service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance),
            null, NullLogger<SniperService>.Instance, new SniperServiceTests.CraftCostMock());
        SeedComponentPrices(service);
        SeedReferences(service, auction, exactLbin);
        return service;
    }

    private static void SeedComponentPrices(SniperService service)
    {
        foreach (var quality in new[] { "FINE", "FLAWLESS", "PERFECT" })
            foreach (var gem in new[] { "CITRINE", "AMBER" })
                Bazaar(service)[$"{quality}_{gem}_GEM"] = quality == "PERFECT" ? 10_000_000 : quality == "FLAWLESS" ? 2_000_000 : 20_000;
        foreach (var part in new[] { "GOBLIN_OMELETTE_SUNNY_SIDE", "MITHRIL_DRILL_ENGINE", "MITHRIL_FUEL_TANK", "PET_ITEM_QUICK_CLAW" })
            service.Lookups[part] = new PriceLookup { Lookup = new() { [new AuctionKey()] = new ReferenceAuctions { Price = 10_000_000 } } };
        Bazaar(service)["RECOMBOBULATOR_3000"] = 8_000_000;
        Bazaar(service)["ENCHANTMENT_ARCANE_6"] = 100_000;
        service.Lookups["WINTER_FRAGMENT"] = new PriceLookup { Lookup = new() { [new AuctionKey()] = new ReferenceAuctions { Price = 2_000_000 } } };
    }

    private static void SeedReferences(SniperService service, SaveAuction auction, bool exactLbin)
    {
        var key = service.KeyFromSaveAuction(auction);
        var referenceKey = new AuctionKey(key) { Count = 2 };
        var bucket = new ReferenceAuctions { Price = 50_000_000, Volatility = 7, TimeToSell = 42 };
        bucket.References.Enqueue(new ReferencePrice { Price = 50_000_000, Day = SniperService.GetDay() });
        bucket.Lbins.Add(new ReferencePrice { AuctionId = 1, Price = 60_000_000, Day = SniperService.GetDay() });
        service.Lookups[service.GetAuctionGroupTag(auction.Tag).tag] = new PriceLookup { Lookup = new() { [referenceKey] = bucket } };
        if (exactLbin)
            service.Lookups[service.GetAuctionGroupTag(auction.Tag).tag].Lookup[key] = new ReferenceAuctions { Lbins = new() { new ReferencePrice { AuctionId = 2, Price = 70_000_000 } } };
    }

    private static SaveAuction Variant(int index, int variant)
    {
        var auction = Auction(index);
        if (variant == 1)
            foreach (var key in auction.FlatenedNBT.Keys.Where(k => k.StartsWith("drill_part_") || k == "CITRINE_0" || k == "CITRINE_1" || k == "AMBER_0").ToArray())
                auction.FlatenedNBT.Remove(key);
        if (variant == 2)
            foreach (var key in auction.FlatenedNBT.Keys.Where(k => k == "CITRINE_0" || k == "CITRINE_1" || k == "AMBER_0").ToArray())
                auction.FlatenedNBT[key] = "FINE";
        return auction;
    }

    [TestCase(0, false)]
    [TestCase(1, false)]
    [TestCase(2, false)]
    [TestCase(0, true)]
    [TestCase(2, true)]
    public void VariantsMatchFreshEvaluationInBothOrders(int index, bool exactLbin)
    {
        var expected = Enumerable.Range(0, 3).Select(v => CreateService(Auction(index), exactLbin).GetPrice(Variant(index, v))).ToArray();
        Assert.That(expected[0].Median, Is.GreaterThan(expected[1].Median),  $"fixture must exercise removable value: nbt={Auction(index).FlatenedNBT.Count}, key={expected[0].ItemKey}");
        foreach (var order in new[] { new[] { 0, 1, 2 }, new[] { 1, 0, 2 }, new[] { 2, 1, 0 } })
        {
            var service = CreateService(Auction(index), exactLbin);
            var keys = order.Select(v => service.KeyFromSaveAuction(Variant(index, v))).ToArray();
            Assert.That(keys[0].Equals(keys[1]) && keys[1].Equals(keys[2]), Is.True, "variants share the cache key");
            for (int repeat = 0; repeat < 3; repeat++)
                foreach (var variant in order)
                    AssertEstimate(service.GetPrice(Variant(index, variant)), expected[variant]);
            Assert.That(CacheCount(service, "ClosetMedianMapLookup"), Is.EqualTo(1), "reuse candidate selection across variants");
            if (!exactLbin)
                Assert.That(CacheCount(service, "ClosetLbinMapLookup"), Is.EqualTo(1));
            var returned = service.GetPrice(Variant(index, 0));
            returned.Median = 1;
            returned.MedianKey = "changed by caller";
            AssertEstimate(service.GetPrice(Variant(index, 0)), expected[0]);
        }
    }

    [Test]
    public void DroppedValuesMatchFreshEvaluation()
    {
        var full = Auction(0);
        var stripped = Auction(0);
        stripped.Enchantments.RemoveAll(e => e.Type == Enchantment.EnchantmentType.arcane);
        var service = CreateService(full);
        var fullKey = service.KeyFromSaveAuction(full);
        var strippedKey = service.KeyFromSaveAuction(stripped);
        Assert.That(fullKey.Equals(strippedKey), Is.True);
        Assert.That(fullKey.ValueSubstract, Is.Not.EqualTo(strippedKey.ValueSubstract));
        VerifyBothOrders(full, stripped);
    }

    [Test]
    public void HeldItemsMatchFreshEvaluation()
    {
        var full = new SaveAuction { Tag = "PET_TIGER", Tier = Tier.LEGENDARY, Count = 1,
            FlatenedNBT = new() { ["exp"] = "10000000", ["heldItem"] = "PET_ITEM_QUICK_CLAW" } };
        var stripped = new SaveAuction { Tag = full.Tag, Tier = full.Tier, Count = 1,
            FlatenedNBT = new() { ["exp"] = "10000000" } };
        var service = CreateService(full);
        Assert.That(service.KeyFromSaveAuction(full).Equals(service.KeyFromSaveAuction(stripped)), Is.True);
        Assert.That(CreateService(full).GetPrice(full).Median, Is.GreaterThan(CreateService(full).GetPrice(stripped).Median));
        VerifyBothOrders(full, stripped);
    }

    [Test]
    public void FallbackLbinUsesTheCurrentVariantsMedianForAttributes()
    {
        var full = new SaveAuction { Tag = "AURORA_CHESTPLATE", Tier = Tier.LEGENDARY, Count = 1,
            FlatenedNBT = new() { ["mana_pool"] = "5", ["AMBER_0"] = "FLAWLESS" } };
        var stripped = new SaveAuction { Tag = full.Tag, Tier = full.Tier, Count = 1,
            FlatenedNBT = new() { ["mana_pool"] = "5" } };
        var variants = new[] { full, stripped };
        var expected = variants.Select(v => CreateAttributeService(full).GetPrice(v)).ToArray();
        Assert.That(expected[0].Lbin.Price, Is.Not.EqualTo(expected[1].Lbin.Price), "attribute correction depends on the caller's median");
        foreach (var order in new[] { new[] { 0, 1 }, new[] { 1, 0 } })
        {
            var service = CreateAttributeService(full);
            Assert.That(service.KeyFromSaveAuction(full).Equals(service.KeyFromSaveAuction(stripped)), Is.True);
            for (int repeat = 0; repeat < 3; repeat++)
                foreach (var index in order)
                    Assert.That(service.GetPrice(variants[index]).Lbin.Price, Is.EqualTo(expected[index].Lbin.Price));
            Assert.That(CacheCount(service, "ClosetLbinMapLookup"), Is.EqualTo(1));
        }
    }

    private static SniperService CreateAttributeService(SaveAuction auction)
    {
        var service = CreateService(auction);
        var lookup = service.Lookups[auction.Tag];
        var reference = lookup.Lookup.Single();
        var key = new AuctionKey(reference.Key) { Modifiers = new(new[] { new KeyValuePair<string, string>("mana_pool", "6") }) };
        lookup.Lookup = new() { [key] = reference.Value };
        return service;
    }

    private static void VerifyBothOrders(SaveAuction full, SaveAuction stripped)
    {
        var variants = new[] { full, stripped };
        var expected = variants.Select(v => CreateService(full).GetPrice(v)).ToArray();
        foreach (var order in new[] { new[] { 0, 1 }, new[] { 1, 0 } })
        {
            var service = CreateService(full);
            for (int repeat = 0; repeat < 3; repeat++)
                foreach (var index in order)
                    AssertEstimate(service.GetPrice(variants[index]), expected[index]);
        }
    }

    [Test]
    public void CombinedStarredAdjustmentDoesNotAccumulate()
    {
        const string tag = "STARRED_YETI_SWORD";
        var starred = (ConcurrentDictionary<string, byte>)typeof(SniperService)
            .GetField("CombinableStarred", BindingFlags.NonPublic | BindingFlags.Static).GetValue(null);
        bool added = starred.TryAdd(tag, 0);
        try
        {
            var auction = new SaveAuction { Tag = tag, Tier = Tier.LEGENDARY, Count = 1,
                FlatenedNBT = new() { ["upgrade_level"] = "5" } };
            var service = CreateService(auction);
            Assert.That(service.GetAuctionGroupTag(tag).costSubstract, Is.EqualTo(14_000_000));
            var expected = CreateService(auction).GetPrice(auction);
            for (int repeat = 0; repeat < 4; repeat++)
                AssertEstimate(service.GetPrice(auction), expected);
        }
        finally
        {
            if (added)
                starred.TryRemove(tag, out _);
        }
    }

    private static Dictionary<string, double> Bazaar(SniperService service) =>
        (Dictionary<string, double>)typeof(SniperService).GetField("BazaarPrices", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(service);

    private static int CacheCount(SniperService service, string name)
    {
        var cache = typeof(SniperService).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance).GetValue(service);
        return (int)cache.GetType().GetProperty("Count").GetValue(cache);
    }

    private static void AssertEstimate(PriceEstimate actual, PriceEstimate expected)
    {
        Assert.Multiple(() =>
        {
            Assert.That(actual.Median, Is.EqualTo(expected.Median));
            Assert.That(actual.Lbin, Is.EqualTo(expected.Lbin));
            Assert.That(actual.SLbin, Is.EqualTo(expected.SLbin));
            Assert.That(actual.MedianKey, Is.EqualTo(expected.MedianKey));
            Assert.That(actual.LbinKey, Is.EqualTo(expected.LbinKey));
            Assert.That(actual.Volume, Is.EqualTo(expected.Volume));
            Assert.That(actual.Volatility, Is.EqualTo(expected.Volatility));
            Assert.That(actual.AvgSellTime, Is.EqualTo(expected.AvgSellTime));
            Assert.That(actual.LastSale, Is.EqualTo(expected.LastSale));
        });
    }

    private const string Fixtures = """
[
  {
    "tag": "HELIX_CHOPPER",
    "itemName": "Moonglade Helix Chopper \u272a\u272a\u272a\u272a\u272a",
    "tier": "LEGENDARY",
    "reforge": "moonglade",
    "category": "MISC",
    "bin": true,
    "count": 1,
    "startingBid": 45000000,
    "highestBidAmount": 45000000,
    "start": "2026-09-09T22:38:02",
    "end": "2026-09-09T22:38:52",
    "itemCreatedAt": "2026-04-27T19:50:35",
    "flatNbt": {
      "rarity_upgrades": "1",
      "absorb_logs_chopped": "3118325",
      "sweep": "1",
      "fighting": "1",
      "foraging_wisdom": "1",
      "foraging_fortune": "1",
      "unlocked_slots": "CITRINE_0,CITRINE_1",
      "CITRINE_0": "PERFECT",
      "CITRINE_1": "PERFECT",
      "logs_cut": "342986",
      "upgrade_level": "5",
      "wood_singularity_count": "1"
    },
    "Enchantments": [
      {
        "Type": "absorb",
        "level": 8
      },
      {
        "Type": "ultimate_first_impression",
        "level": 5
      },
      {
        "Type": "arcane",
        "level": 6
      },
      {
        "Type": "unknown",
        "level": 1
      },
      {
        "Type": "efficiency",
        "level": 5
      },
      {
        "Type": "unknown",
        "level": 1
      }
    ]
  },
  {
    "tag": "HELIX_CHOPPER",
    "itemName": "Moonglade Helix Chopper \u272a\u272a\u272a\u272a\u272a",
    "tier": "LEGENDARY",
    "reforge": "moonglade",
    "category": "MISC",
    "bin": true,
    "count": 1,
    "startingBid": 30000000,
    "highestBidAmount": 30000000,
    "start": "2026-09-09T21:01:36",
    "end": "2026-09-09T21:02:54",
    "itemCreatedAt": "2026-09-06T03:36:05",
    "flatNbt": {
      "rarity_upgrades": "1",
      "absorb_logs_chopped": "4001",
      "sweep": "1",
      "foraging_wisdom": "1",
      "foraging_fortune": "1",
      "unlocked_slots": "CITRINE_0,CITRINE_1",
      "CITRINE_0": "FINE",
      "CITRINE_1": "FLAWLESS",
      "logs_cut": "48103",
      "upgrade_level": "5"
    },
    "Enchantments": [
      {
        "Type": "absorb",
        "level": 2
      },
      {
        "Type": "ultimate_first_impression",
        "level": 5
      },
      {
        "Type": "arcane",
        "level": 6
      },
      {
        "Type": "unknown",
        "level": 1
      },
      {
        "Type": "efficiency",
        "level": 5
      },
      {
        "Type": "unknown",
        "level": 1
      },
      {
        "Type": "silk_touch",
        "level": 1
      }
    ]
  },
  {
    "tag": "TITANIUM_DRILL_2",
    "itemName": "Auspicious Titanium Drill DR-X455",
    "tier": "LEGENDARY",
    "reforge": "Auspicious",
    "category": "UNKNOWN",
    "bin": true,
    "count": 1,
    "startingBid": 100000000,
    "highestBidAmount": 100000000,
    "start": "2026-09-09T19:37:22",
    "end": "2026-09-09T19:50:26",
    "itemCreatedAt": "2021-04-15T00:26:00",
    "flatNbt": {
      "rarity_upgrades": "1",
      "drill_fuel": "8072",
      "compact_blocks": "710741",
      "drill_part_upgrade_module": "goblin_omelette_sunny_side",
      "drill_part_engine": "mithril_drill_engine",
      "drill_part_fuel_tank": "mithril_fuel_tank",
      "AMBER_0": "FLAWLESS"
    },
    "Enchantments": [
      {
        "Type": "compact",
        "level": 9
      },
      {
        "Type": "experience",
        "level": 4
      },
      {
        "Type": "fortune",
        "level": 4
      },
      {
        "Type": "ultimate_flowstate",
        "level": 3
      },
      {
        "Type": "efficiency",
        "level": 5
      },
      {
        "Type": "smelting_touch",
        "level": 1
      },
      {
        "Type": "prismatic",
        "level": 5
      },
      {
        "Type": "telekinesis",
        "level": 1
      }
    ]
  }
]
""";
}
