using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Amazon.S3;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.AIFinder;
using Coflnet.Sky.AIFinder.Model;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using TrackerFlip = Coflnet.Sky.FlipTracker.Client.Model.ComplicatedFlip;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

/// <summary>The sniper's values as the AI finder reads them</summary>
public sealed class SniperItemValueSource : IItemValueSource
{
    private readonly SniperService sniper;
    private readonly ICraftCostService? craftCost;

    public SniperItemValueSource(SniperService sniper, ICraftCostService? craftCost = null)
    {
        this.sniper = sniper ?? throw new ArgumentNullException(nameof(sniper));
        this.craftCost = craftCost;
    }

    public SniperValues GetValues(SaveAuction auction)
    {
        var details = sniper.GetValueDetails(auction);
        var cost = craftCost != null && craftCost.TryGetCost(auction.Tag, out var craft) ? (long)craft : 0;
        return new SniperValues(details.AllModifiers.Select(ToValuedAttribute).ToList(), details.ValueSubstracted, details.RemovableValue,
            details.CleanPrice, details.Median, details.LowestBin, cost);
    }

    private static ValuedAttribute ToValuedAttribute(SniperService.RankElem element)
        => new(element.Enchant.Type, element.Enchant.Lvl, element.Modifier, element.Reforge, element.Value, element.IsEstimate);
}

/// <summary>Model blobs live next to the sniper's persisted state</summary>
public sealed class PersistenceModelBlobStore : IModelBlobStore
{
    private readonly IPersitanceManager persistence;

    public PersistenceModelBlobStore(IPersitanceManager persistence)
    {
        this.persistence = persistence;
    }

    public Task SaveBlob(string key, Stream data) => persistence.SaveBlob(key, data);

    /// <returns>Null when there is no such blob; other failures throw</returns>
    public async Task<Stream?> LoadBlob(string key)
    {
        try
        {
            return await persistence.LoadBlob(key);
        }
        catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }
}

public sealed class MayorServiceLookup : IMayorLookup
{
    private readonly IMayorService mayors;

    public MayorServiceLookup(IMayorService mayors)
    {
        this.mayors = mayors;
    }

    /// <returns>Null while the mayor service has no mayor: it answers "Unknown" then, which the finder must not take for a mayor</returns>
    public string? GetMayor(DateTime time)
    {
        var mayor = mayors.GetMayor(time);
        return SharedContextMayors.IsKnown(mayor) ? mayor : null;
    }
}

/// <summary>Sold BIN auctions of a tag from the auction database in pages ordered by end and id, one context per retrain</summary>
public sealed class HypixelSoldAuctionSource : ISoldAuctionSource, IDisposable
{
    private readonly HypixelContext context;
    private readonly int itemId;

    public HypixelSoldAuctionSource(HypixelContext context, int itemId)
    {
        this.context = context;
        this.itemId = itemId;
    }

    public Task<List<SaveAuction>> LoadSoldBins(string tag, DateTime start, DateTime end, SoldPageStart? after, int limit, CancellationToken cancellationToken)
    {
        var query = context.Auctions.Where(a => a.End >= start && a.End < end && a.Bin && a.HighestBidAmount > 0 && itemId == a.ItemId);
        if (after is { } page)
            query = query.Where(a => a.End > page.End || a.End == page.End && a.Id > page.Id);
        return query.OrderBy(a => a.End).ThenBy(a => a.Id)
            .Take(limit)
            .Include(a => a.NbtData).Include(a => a.Enchantments)
            .AsNoTracking()
            .ToListAsync(cancellationToken);
    }

    public void Dispose() => context.Dispose();
}

/// <summary>Conversions between the AI finder's samples and the flip tracker's complicated flips</summary>
public static class AiFlipSampleConversions
{
    public static AiFlipSample ToSample(this TrackerFlip flip) => new()
    {
        AuctionId = flip.AuctionId,
        ItemTag = flip.ItemTag,
        EndedAt = flip.EndedAt,
        SoldFor = flip.SoldFor,
        AttributeValues = flip.AttributeValues ?? new Dictionary<string, long>()
    };
}
