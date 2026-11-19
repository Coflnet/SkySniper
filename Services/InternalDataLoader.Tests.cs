using NUnit.Framework;
using Coflnet.Sky.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Configuration;

namespace Coflnet.Sky.Sniper.Services;
public class InternalDataLoaderTest
{
    [Test]
    public void ComparesToOldest()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string>()).Build();
        var loader = new InternalDataLoader(null, config, null, null, null, null, null, null, null);
        var references = new ConcurrentQueue<ReferencePrice>();
        var sample = new ReferencePrice() { Day = SniperService.GetDay(DateTime.UtcNow - TimeSpan.FromDays(5)), Price = 1000, Seller = 1, AuctionId = 1 };
        for (int i = 0; i < 15; i++)
        {
            references.Enqueue(sample);
        }
        Assert.That(!loader.ShouldAuctionBeIncluded(new SaveAuction() { End = DateTime.UtcNow - TimeSpan.FromDays(10) }, references));
        Assert.That(loader.ShouldAuctionBeIncluded(new SaveAuction() { End = DateTime.UtcNow - TimeSpan.FromDays(1) }, references));
    }

    /// <summary>Listeners that stand down while new auctions are processed see each batch as in flight until it finished</summary>
    [Test]
    public void BatchIsInFlightWhileItIsProcessed()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string>()).Build();
        var loader = new InternalDataLoader(null, config, null, null, null, null, null, null, null);
        var during = false;

        loader.ProcessInFlight(() => during = loader.IsProcessingNewAuctions());

        Assert.That(during, Is.True);
        Assert.That(loader.IsProcessingNewAuctions(), Is.False);
        Assert.Throws<InvalidOperationException>(() => loader.ProcessInFlight(() => throw new InvalidOperationException()));
        Assert.That(loader.IsProcessingNewAuctions(), Is.False, "a failed batch is not in flight any more");
    }
}
