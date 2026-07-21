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
