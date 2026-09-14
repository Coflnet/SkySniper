using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
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
}
