using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class ValueDetailsTests
{
    /// <summary>Items of a group are priced with their group's lookup, so their clean price is the group's too</summary>
    [Test]
    public void CleanPriceComesFromTheLookupTheItemIsPricedWith()
    {
        var sniper = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, null);
        sniper.Lookups["HYPERION"] = new PriceLookup { CleanPricePerTier = new() { [Tier.MYTHIC] = 900_000_000 } };
        var auction = new SaveAuction
        {
            Uuid = "00000000000000000000000000000001", Tag = "VALKYRIE", Tier = Tier.MYTHIC, Bin = true, Count = 1, StartingBid = 1, FlatenedNBT = new(), Enchantments = []
        };

        sniper.GetValueDetails(auction).CleanPrice.Should().Be(900_000_000);
    }
}
