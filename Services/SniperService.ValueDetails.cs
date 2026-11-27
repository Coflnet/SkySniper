using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services;

public partial class SniperService
{
    /// <summary>
    /// The values of an auction beyond its lookup key, see <see cref="ItemValueDetails"/>. Reads the same state the
    /// price lookup reads and changes nothing but the category of an auction that has none (as <see cref="GetPrice"/> does).
    /// </summary>
    public ItemValueDetails GetValueDetails(SaveAuction auction)
    {
        var detailed = DetailedKeyFromSaveAuction(auction);
        var estimate = GetPrice(auction);
        return new ItemValueDetails(AllValuedModifiers(auction), detailed.SubstractedValue,
            GetFullRemovableValue(auction, detailed.Key), CleanPriceOf(auction, detailed.Key), estimate?.Median ?? 0, LowestBinPrice(estimate));
    }

    private List<RankElem> AllValuedModifiers(SaveAuction auction)
    {
        var (enchants, modifiers) = SelectValuable(auction);
        return CapKeyLength(enchants, modifiers, auction, 500_000, int.MaxValue).ranked.ToList();
    }

    /// <summary>The clean price of the lookup the auction is priced with (items of a group share their group's lookup).
    /// A tier boosted pet has none and reports 0.</summary>
    private long CleanPriceOf(SaveAuction auction, AuctionKey key)
    {
        if (auction.Tag == null || !Lookups.TryGetValue(GetAuctionGroupTag(auction.Tag).tag, out var lookup))
            return 0;
        return TryGetStoredCleanPrice(lookup, key, auction.Tier, out var price) ? price : 0;
    }

    private static long LowestBinPrice(PriceEstimate estimate)
    {
        if (estimate == null || estimate.Lbin.AuctionId == default)
            return 0;
        return estimate.Lbin.Price;
    }
}
