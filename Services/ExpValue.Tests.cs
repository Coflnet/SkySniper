using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class ExpValueTests
{
    [TestCase(0, 243_662_568, 0)]
    [TestCase(233_743_354, 234_103_584, 12_850)]
    [TestCase(233_743_354, 0, 0)]
    public void WitchExpRequiresPricedLegendaryEndpoints(long basePrice, long maxPrice, long expected)
    {
        var key = new AuctionKey([], ItemReferences.Reforge.Any,
            [new("exp", "0"), new("candyUsed", "0")], Tier.COMMON, 1);
        var lookup = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>();
        lookup[new AuctionKey(key) { Tier = Tier.LEGENDARY }] = new() { Price = basePrice };
        lookup[new AuctionKey([], ItemReferences.Reforge.Any,
            [new("exp", "6")], Tier.LEGENDARY, 1)] = new() { Price = maxPrice };
        var auction = new SaveAuction
        {
            Tag = "PET_WITCH",
            Tier = Tier.COMMON,
            Count = 1,
            FlatenedNBT = new() { ["exp"] = "904451.3828946403", ["candyUsed"] = "0" }
        };

        var method = typeof(SniperService).GetMethod("GetValueDifferenceForExp", BindingFlags.NonPublic | BindingFlags.Static);
        var expValue = (long)method!.Invoke(null, new object[] { auction, key, lookup })!;

        expValue.Should().Be(expected, "a zero bucket price means unavailable pricing, not a free level-1 pet");
    }

    [Test]
    public void InvertedLegendaryBucketsDoNotAddExpValueToLvl1Pet()
    {
        var legendaryLvl1 = new AuctionKey([], ItemReferences.Reforge.Any,
            [new("exp", "0"), new("candyUsed", "0")], Tier.LEGENDARY, 1);
        var legendaryLvl100 = new AuctionKey([], ItemReferences.Reforge.Any,
            [new("exp", "6")], Tier.LEGENDARY, 1);
        var lookup = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>(new Dictionary<AuctionKey, ReferenceAuctions>
        {
            // This 42.57M inversion reproduces the reported expvalue of 9,122,143.
            [legendaryLvl1] = new() { Price = 102_570_000 },
            [legendaryLvl100] = new() { Price = 60_000_000 }
        });
        var auction = new SaveAuction
        {
            Tag = "PET_ENDERMAN",
            Tier = Tier.EPIC,
            Count = 1,
            FlatenedNBT = new() { ["exp"] = "0", ["candyUsed"] = "0" }
        };
        var exactBucketKey = new AuctionKey([], ItemReferences.Reforge.Any,
            [new("exp", "0"), new("candyUsed", "0")], Tier.EPIC, 1);

        var method = typeof(SniperService).GetMethod("GetValueDifferenceForExp", BindingFlags.NonPublic | BindingFlags.Static);
        var expValue = (long)method!.Invoke(null, new object[] { auction, exactBucketKey, lookup })!;

        expValue.Should().Be(0, "a negative level-1-to-level-100 market slope cannot make zero pet exp valuable");
    }

    [TestCase(Tier.RARE, "BEJEWELED_COLLAR")]
    [TestCase(Tier.EPIC, "PET_ITEM_TIER_BOOST")] // tier boosted rare is priced from rare buckets
    public void AiExpAttributeUsesPetTierBuckets(Tier tier, string heldItem)
    {
        // bucket medians from the PET_SCATHA pricing state; the legendary level 1 bucket had a single unpriced reference
        var service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, null);
        var lookup = new PriceLookup();
        foreach (var bucketTier in new[] { Tier.RARE, Tier.EPIC, Tier.LEGENDARY })
        {
            lookup.Lookup[new AuctionKey([], ItemReferences.Reforge.Any, [new("exp", "0"), new("candyUsed", "0")], bucketTier, 1)] = new()
            { Price = bucketTier switch { Tier.RARE => 86_000_000, Tier.EPIC => 183_583_332, _ => 0 } };
            lookup.Lookup[new AuctionKey([], ItemReferences.Reforge.Any, [new("exp", "6")], bucketTier, 1)] = new()
            { Price = bucketTier switch { Tier.RARE => 93_078_304, Tier.EPIC => 197_539_376, _ => 400_000_000 } };
        }
        service.Lookups["PET_SCATHA"] = lookup;
        // reported [Lvl 100] Scatha bought for 90M with an AI target of 134M
        var auction = new SaveAuction
        {
            Tag = "PET_SCATHA",
            Tier = tier,
            Category = Category.MISC,
            Reforge = ItemReferences.Reforge.None,
            Bin = true,
            Count = 1,
            StartingBid = 90_000_000,
            HighestBidAmount = 90_000_000,
            Enchantments = [],
            FlatenedNBT = new()
            {
                ["active"] = "False", ["candyUsed"] = "0", ["exp"] = "47496807.22508164", ["heldItem"] = heldItem,
                ["hideInfo"] = "False", ["hideRightClick"] = "False", ["noMove"] = "False", ["petSoulbound"] = "False",
                ["tier"] = tier.ToString(), ["type"] = "SCATHA"
            }
        };

        var flip = auction.ToComplicatedFlip(includeBreakdown: true, sniper: service);

        flip.AttributeValues["exp:6"].Should().BeLessThanOrEqualTo(93_078_304 - 86_000_000,
            "exp on a rare pet is worth at most the rare level 1 to level 100 spread, not a share of the legendary price");
        // attribute sum cap as computed by InternalDataLoader.CheckForPartial
        var cap = flip.AttributeValues.Where(a => !a.Key.StartsWith("candyUsed:")).Sum(a => a.Value);
        cap.Should().BeGreaterThanOrEqualTo(86_000_000, "the cap keeps the rare level 1 pet value so undervalued listings still flip");
        cap.Should().BeLessThan((long)(auction.StartingBid * 1.1), "the reported 90M purchase is not a flip");
    }

    [TestCase(80_000_000, 183_583_332, 197_539_376)]
    [TestCase(165_000_000, 183_583_332, 197_539_376)] // worth more than the epic pet is above the rare one
    [TestCase(80_000_000, 86_000_000, 93_078_304)] // epic priced like rare leaves only the presence flag
    public void AiTierFlagIgnoresTierBoost(long boostPrice, long epicBasePrice, long epicMaxPrice)
    {
        var service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, null);
        var lookup = new PriceLookup();
        // bucket medians from the PET_SCATHA pricing state, as in AiExpAttributeUsesPetTierBuckets
        foreach (var bucketTier in new[] { Tier.RARE, Tier.EPIC })
        {
            lookup.Lookup[new AuctionKey([], ItemReferences.Reforge.Any, [new("exp", "0"), new("candyUsed", "0")], bucketTier, 1)] = new()
            { Price = bucketTier == Tier.RARE ? 86_000_000 : epicBasePrice };
            lookup.Lookup[new AuctionKey([], ItemReferences.Reforge.Any, [new("exp", "6")], bucketTier, 1)] = new()
            { Price = bucketTier == Tier.RARE ? 93_078_304 : epicMaxPrice };
        }
        service.Lookups["PET_SCATHA"] = lookup;
        SaveAuction Scatha(Tier tier, string heldItem) => new()
        {
            Tag = "PET_SCATHA",
            Tier = tier,
            Category = Category.MISC,
            Reforge = ItemReferences.Reforge.None,
            Bin = true,
            Count = 1,
            StartingBid = 90_000_000,
            HighestBidAmount = 90_000_000,
            Enchantments = [],
            FlatenedNBT = new()
            {
                ["active"] = "False", ["candyUsed"] = "0", ["exp"] = "47496807.22508164", ["heldItem"] = heldItem,
                ["hideInfo"] = "False", ["hideRightClick"] = "False", ["noMove"] = "False", ["petSoulbound"] = "False",
                ["tier"] = tier.ToString(), ["type"] = "SCATHA"
            }
        };

        var plain = Scatha(Tier.RARE, "BEJEWELED_COLLAR").ToComplicatedFlip(includeBreakdown: true, sniper: service).AttributeValues;
        var realEpic = Scatha(Tier.EPIC, "BEJEWELED_COLLAR").ToComplicatedFlip(includeBreakdown: true, sniper: service).AttributeValues;
        var unpricedBoost = Scatha(Tier.EPIC, "PET_ITEM_TIER_BOOST").ToComplicatedFlip(includeBreakdown: true, sniper: service).AttributeValues;
        // attribute sum cap as computed by InternalDataLoader.CheckForPartial
        var tierSpread = realEpic.Values.Sum() - plain.Values.Sum();
        service.UpdateBazaar(new() { Products = [new() { ProductId = "PET_ITEM_TIER_BOOST", SellSummary = [new() { PricePerUnit = boostPrice }], BuySummery = [] }] });
        var boosted = Scatha(Tier.EPIC, "PET_ITEM_TIER_BOOST").ToComplicatedFlip(includeBreakdown: true, sniper: service).AttributeValues;

        boosted.Keys.Should().Contain("tier:RARE", "a tier boosted rare is still a rare pet").And.NotContain("tier:EPIC");
        plain.Keys.Should().Contain("tier:RARE").And.NotContain("petItem:TIER_BOOST");
        unpricedBoost.Should().Contain("petItem:TIER_BOOST", 1L, "without a known price the boost is only a presence flag");
        var boostValue = Math.Max(Math.Min(boostPrice, tierSpread), 1L); // never 0, that reads as no boost
        boosted.Should().Contain("petItem:TIER_BOOST", boostValue, "the boost carries its item price, bounded by the tier spread");
        boosted.Values.Sum().Should().Be(plain.Values.Sum() + boostValue, "the cap is the plain pet of the real tier plus the boost")
            .And.BeLessThanOrEqualTo(Math.Max(realEpic.Values.Sum(), plain.Values.Sum() + 1L), "a boosted rare is not capped above a real epic, except by the presence flag");
    }
}
