using System.Collections.Generic;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class AuctionListenersTests
{
    private static SniperService Sniper() => new(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, null);

    private static SaveAuction Auction(string uuid) => new()
    {
        Uuid = uuid, Tag = "HYPERION", Tier = Tier.MYTHIC, Bin = true, Count = 1, StartingBid = 1_000_000, FlatenedNBT = new(), Enchantments = []
    };

    [Test]
    public void TestedAuctionsReachTheListenerOnlyOnTheEventPath()
    {
        var sniper = Sniper();
        var tested = new List<string>();
        sniper.AuctionTested += a => tested.Add(a.Uuid);

        sniper.TestNewAuction(Auction("00000000000000000000000000000001"));
        sniper.TestNewAuction(Auction("00000000000000000000000000000002"), triggerEvents: false);

        tested.Should().Equal("00000000000000000000000000000001");
    }
}
