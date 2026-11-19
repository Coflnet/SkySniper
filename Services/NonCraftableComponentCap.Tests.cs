using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>
/// A key holding a component that can not be bought and applied to a clean item (an enchant that only levels up
/// through use, or an enchant without a book price) is not bounded by the sum of its component prices. When the
/// bucket's own references sell clearly above that sum, the bucket is capped at their lower quartile instead.
/// Each fixture holds a completed BIN auction as listed, the bucket references that existed before its listing day,
/// the bazaar prices of its enchant books and the price the same item sold for afterwards.
/// </summary>
public class NonCraftableComponentCapTests
{
    [TearDown]
    public void TearDown()
    {
        SniperService.MIN_TARGET = 200_000;
        SniperService.StartTime = new DateTime(2021, 9, 25);
    }

    [TestCase("HelixChopperAbsorb9.json", 117_355_291)]
    [TestCase("HelixChopperAbsorb8.json", 55_311_327)]
    [TestCase("HelixHelmetForestPledgeA.json", 28_011_452)]
    [TestCase("HelixHelmetForestPledgeB.json", 28_040_359)]
    [TestCase("HelixHelmetForestPledgeScuba.json", 21_000_000)]
    public void FindsListingBelowReferencesWithoutExceedingLaterSale(string fixtureName, long expectedTarget)
    {
        var fixture = Fixture.Load(fixtureName);
        var service = CreateService(fixture);
        var found = new List<LowPricedAuction>();
        service.FoundSnipe += found.Add;

        service.TestNewAuction(fixture.Auction);

        var best = found.Where(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN || f.Finder == LowPricedAuction.FinderType.SNIPER)
            .OrderByDescending(f => f.TargetPrice).FirstOrDefault();
        best.Should().NotBeNull("the listing is far below what the same items sold for");
        best.TargetPrice.Should().BeGreaterThan(fixture.Auction.StartingBid);
        best.TargetPrice.Should().BeLessThanOrEqualTo(fixture.LaterSalePrice, "the target must not exceed what the item sold for afterwards");
        best.TargetPrice.Should().BeInRange(expectedTarget * 99 / 100, expectedTarget * 101 / 100, "the target is pinned to its current value so a later change of the valuation shows up");
    }

    [Test]
    public void UsageLeveledEnchantAboveLevelOneCountsAsNonCraftable()
    {
        SniperService.HasNonCraftableComponent(Breakdown(new Enchant { Type = Enchantment.EnchantmentType.absorb, Lvl = 9 })).Should().BeTrue();
        SniperService.HasNonCraftableComponent(Breakdown(new Enchant { Type = Enchantment.EnchantmentType.absorb, Lvl = 1 })).Should().BeFalse("the level 1 book can be bought");
        SniperService.HasNonCraftableComponent(Breakdown(new Enchant { Type = Enchantment.EnchantmentType.sharpness, Lvl = 6 })).Should().BeFalse();
    }

    [Test]
    public void EnchantWithoutBookPriceCountsAsNonCraftable()
    {
        var enchant = new Enchant { Type = Enchantment.EnchantmentType.forest_pledge, Lvl = 6 };
        var key = Breakdown(enchant);
        key.ValueBreakdown.Add(new SniperService.RankElem(enchant, -1));
        SniperService.HasNonCraftableComponent(key).Should().BeTrue();
    }

    [Test]
    public void NegativeModifierEstimateDoesNotCountAsNonCraftable()
    {
        var key = Breakdown(new Enchant { Type = Enchantment.EnchantmentType.sharpness, Lvl = 6 });
        key.ValueBreakdown.Add(new SniperService.RankElem(new KeyValuePair<string, string>("baseStatBoost", "30"), -8_000_000) { IsEstimate = true });
        SniperService.HasNonCraftableComponent(key).Should().BeFalse("a below average stat roll lowers the value, its price is known");
    }

    [Test]
    public void PricedComponentsKeepCraftCostCap()
    {
        var service = CreateService(new Fixture { Day = 1000, Bazaar = WithBookTable(("ENCHANTMENT_SHARPNESS_6", 1_000_000)) });
        var bucket = AddBucket(service, "CRAFTABLE_SWORD", Key(Tier.EPIC, Enchantment.EnchantmentType.sharpness, 6),
            BucketWithReferences(1000, 50_000_000, 52_000_000, 54_000_000, 56_000_000, 58_000_000, 60_000_000));

        service.RefreshLookup("CRAFTABLE_SWORD");

        bucket.Price.Should().BeLessThan(10_000_000, "a clean item plus one bought book costs about 6M");
    }

    [Test]
    public void LowLevelUsageEnchantOnPricedKeyKeepsCraftCostCap()
    {
        var service = CreateService(new Fixture { Day = 1000, Bazaar = WithBookTable(("ENCHANTMENT_CULTIVATING_1", 1_000_000), ("ENCHANTMENT_HARVESTING_6", 1_000_000)) });
        var key = new AuctionKey(new List<Enchant>
        {
            new() { Type = Enchantment.EnchantmentType.cultivating, Lvl = 6 },
            new() { Type = Enchantment.EnchantmentType.harvesting, Lvl = 6 }
        }, ItemReferences.Reforge.Any, new List<KeyValuePair<string, string>>(), Tier.EPIC, 1);
        var bucket = AddBucket(service, "USAGE_HOE", key,
            BucketWithReferences(1000, 17_000_000, 17_500_000, 18_000_000, 19_000_000, 20_000_000, 21_000_000, 22_000_000, 23_000_000));

        service.RefreshLookup("USAGE_HOE");

        bucket.Price.Should().BeLessThan(16_500_000, "the references do not sell clearly above the component sum, so it stays the cap");
    }

    [Test]
    public void UnpricedEnchantIsCappedAtLowerQuartileOfReferences()
    {
        var service = CreateService(new Fixture { Day = 1000, Bazaar = WithBookTable(("RECOMBOBULATOR_3000", 8_000_000)) });
        var bucket = AddUnpricedHelmet(service);

        service.RefreshLookup("UNPRICED_HELMET");

        bucket.Price.Should().Be(22_000_000, "the lower quartile of the eight references, above the 14M component sum");
    }

    [Test]
    public void PartialBazaarKeepsCraftCostCap()
    {
        var service = CreateService(new Fixture { Day = 1000, Bazaar = new() { ["RECOMBOBULATOR_3000"] = 8_000_000 } });
        var bucket = AddUnpricedHelmet(service);

        service.RefreshLookup("UNPRICED_HELMET");

        bucket.Price.Should().Be(13_799_998, "without enchant book prices a missing book price says nothing");
    }

    /// <summary>
    /// Without any bazaar price no component has a value and the craft cost cap does not apply, as before.
    /// </summary>
    [Test]
    public void EmptyBazaarKeepsOldPrice()
    {
        var service = CreateService(new Fixture { Day = 1000 });
        var bucket = AddUnpricedHelmet(service);

        service.RefreshLookup("UNPRICED_HELMET");

        bucket.Price.Should().Be(EmptyBazaarHelmetPrice);
    }

    [Test]
    public void QuartileAboveInputKeepsInput()
    {
        var service = CreateService(new Fixture { Day = 1000, Bazaar = WithBookTable(("RECOMBOBULATOR_3000", 8_000_000)) });
        AddUnpricedHelmet(service);
        var key = service.GetBreakdownKey(UnpricedHelmetKey, "UNPRICED_HELMET");
        var references = BucketWithReferences(1000, 40_000_000, 41_000_000, 42_000_000, 43_000_000).References.ToList();

        var price = service.CapAtCraftCostOrReferences("UNPRICED_HELMET", key, 25_000_000, 0, references, out var raised);

        price.Should().Be(25_000_000);
        raised.Should().BeTrue();
    }

    /// <summary>
    /// 54 sales around 35M during the last week, then 6 at 25M today and a 26M listing: the craft cost (about 17M)
    /// is raised to the price before the cap (25M), and the steps after the cap must not lift it any further.
    /// </summary>
    [Test]
    public void RaisedCapIsNotLiftedFurtherByRecentSales()
    {
        const short today = 998;
        var service = CreateService(new Fixture { Day = today, Bazaar = WithBookTable(("ENCHANTMENT_ABSORB_1", 4_000_000), ("RECOMBOBULATOR_3000", 8_000_000)) });
        var bucket = new ReferenceAuctions();
        for (int i = 0; i < 60; i++)
        {
            var isToday = i >= 54;
            bucket.EnqueueReference(new ReferencePrice
            {
                AuctionId = i + 1,
                Price = (isToday ? 25_000_000 : 35_000_000) + i,
                Day = (short)(isToday ? today : 991 + i / 8),
                Seller = (short)(i + 1)
            });
        }
        bucket.Lbins.Add(new ReferencePrice { AuctionId = 1_000, Price = 26_000_000, Day = today, Seller = 1_000 });
        var key = new AuctionKey(new List<Enchant> { new() { Type = Enchantment.EnchantmentType.absorb, Lvl = 6 } }, ItemReferences.Reforge.Any,
            new List<KeyValuePair<string, string>> { new("rarity_upgrades", "1") }, Tier.LEGENDARY, 1);
        AddBucket(service, "USAGE_AXE", key, bucket, today);

        service.RefreshLookup("USAGE_AXE");

        bucket.Price.Should().BeLessThanOrEqualTo(25_100_000, "the newest sales were at 25M");
        bucket.RiskyEstimate.Should().BeLessThanOrEqualTo(25_100_000);
    }

    private static readonly AuctionKey UnpricedHelmetKey = new(new List<Enchant> { new() { Type = Enchantment.EnchantmentType.forest_pledge, Lvl = 6 } },
        ItemReferences.Reforge.Any, new List<KeyValuePair<string, string>> { new("rarity_upgrades", "1") }, Tier.LEGENDARY, 1);

    private const long EmptyBazaarHelmetPrice = 31_000_000;

    private static ReferenceAuctions AddUnpricedHelmet(SniperService service)
    {
        return AddBucket(service, "UNPRICED_HELMET", UnpricedHelmetKey,
            BucketWithReferences(1000, 20_000_000, 22_000_000, 30_000_000, 31_000_000, 32_000_000, 33_000_000, 34_000_000, 35_000_000));
    }

    private static ReferenceAuctions AddBucket(SniperService service, string tag, AuctionKey key, ReferenceAuctions bucket, short day = 1000)
    {
        var lookup = service.Lookups.GetOrAdd(tag, _ => new PriceLookup());
        lookup.Lookup[key] = bucket;
        lookup.Lookup[new AuctionKey(new(), ItemReferences.Reforge.Any, new(), Tier.EPIC, 1)] =
            BucketWithReferences(day, 5_000_000, 5_000_000, 5_000_000, 5_000_000, 5_000_000, 5_000_000);
        return bucket;
    }

    private static AuctionKey Key(Tier tier, Enchantment.EnchantmentType type, byte level)
    {
        return new AuctionKey(new List<Enchant> { new() { Type = type, Lvl = level } }, ItemReferences.Reforge.Any, new List<KeyValuePair<string, string>>(), tier, 1);
    }

    /// <summary>
    /// The enchant book table of a full bazaar update plus the given prices.
    /// </summary>
    private static Dictionary<string, double> WithBookTable(params (string id, double price)[] extra)
    {
        var prices = JsonConvert.DeserializeObject<Dictionary<string, double>>(File.ReadAllText(Path.Combine("Mock", "NonCraftable", "EnchantBookPrices.json")));
        foreach (var (id, price) in extra)
            prices[id] = price;
        return prices;
    }

    private static KeyWithValueBreakdown Breakdown(Enchant enchant)
    {
        return new KeyWithValueBreakdown
        {
            Key = new AuctionKeyWithValue { Enchants = new List<Enchant> { enchant }.AsReadOnly(), Modifiers = new List<KeyValuePair<string, string>>().AsReadOnly() },
            ValueBreakdown = new List<SniperService.RankElem>()
        };
    }

    private static ReferenceAuctions BucketWithReferences(short day, params long[] prices)
    {
        var bucket = new ReferenceAuctions();
        for (int i = 0; i < prices.Length; i++)
            bucket.EnqueueReference(new ReferencePrice { AuctionId = i + 1, Price = prices[i], Day = (short)(day - i % 5), Seller = (short)(i + 1) });
        return bucket;
    }

    private static SniperService CreateService(Fixture fixture)
    {
        SniperService.MIN_TARGET = 0;
        SniperService.StartTime = DateTime.UtcNow.AddDays(-fixture.Day - 0.5);
        var service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null,
            NullLogger<SniperService>.Instance, new SniperServiceTests.CraftCostMock());
        if (fixture.Bazaar.Count > 0)
            service.UpdateBazaar(new() { Timestamp = DateTime.UtcNow, Products = fixture.Bazaar.Select(BazaarProduct).ToList() });
        if (fixture.Auction == null)
            return service;
        service.Lookups[fixture.Auction.Tag] = fixture.CreateLookup();
        service.RefreshLookup(fixture.Auction.Tag);
        service.State = SniperState.FullyLoaded;
        return service;
    }

    private static dev.ProductInfo BazaarProduct(KeyValuePair<string, double> price)
    {
        return new dev.ProductInfo
        {
            ProductId = price.Key,
            SellSummary = new() { new() { PricePerUnit = price.Value, Amount = 10 } },
            BuySummery = new()
        };
    }

    private class Fixture
    {
        public short Day;
        public long LaterSalePrice;
        public Dictionary<Tier, long> CleanPricePerTier = new();
        public Dictionary<string, double> Bazaar = new();
        public ApiSaveAuction Auction;
        public Dictionary<string, ReferenceAuctions> Lookup = new();

        public static Fixture Load(string name)
        {
            var fixture = JsonConvert.DeserializeObject<Fixture>(File.ReadAllText(Path.Combine("Mock", "NonCraftable", name)));
            fixture.Bazaar = WithBookTable(fixture.Bazaar.Select(p => (p.Key, p.Value)).ToArray());
            fixture.Auction.AuctioneerId = "00000000000000000000000000000001";
            fixture.Auction.UId = 1;
            return fixture;
        }

        public PriceLookup CreateLookup()
        {
            var lookup = new PriceLookup { CleanPricePerTier = new(CleanPricePerTier) };
            foreach (var bucket in Lookup)
                lookup.Lookup[ParseKey(bucket.Key)] = bucket.Value;
            return lookup;
        }
    }

    /// <summary>
    /// Parses the string form of an <see cref="AuctionKey"/> as written by the bucket dump.
    /// </summary>
    private static AuctionKey ParseKey(string text)
    {
        var parts = text.Replace(", ", ",").Split(' ');
        return new AuctionKey(
            ParseEnchants(parts[0]),
            Enum.Parse<ItemReferences.Reforge>(parts[1]),
            ParseModifiers(string.Join(" ", parts.Skip(2).Take(parts.Length - 4))),
            Enum.Parse<Tier>(parts[^2]),
            byte.Parse(parts[^1]));
    }

    private static List<Enchant> ParseEnchants(string text)
    {
        return text.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(e => e.Split('='))
            .Select(e => new Enchant { Type = Enum.Parse<Enchantment.EnchantmentType>(e[0]), Lvl = byte.Parse(e[1]) }).ToList();
    }

    private static List<KeyValuePair<string, string>> ParseModifiers(string text)
    {
        return text.Split("],[", StringSplitOptions.RemoveEmptyEntries).Select(m => m.Trim('[', ']').Split(',', 2))
            .Where(m => m[0].Length > 0).Select(m => new KeyValuePair<string, string>(m[0], m[1])).ToList();
    }
}
