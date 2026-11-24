using NUnit.Framework;
using Coflnet.Sky.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Coflnet.Sky.Sniper.Models;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using AwesomeAssertions;

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

    /// <summary>Each tag's replay scope ends with its own replay, also when that replay failed, and the next tag still replays</summary>
    [Test]
    public async Task AiHistoryReplayEndsEachTagsScopeAfterItsReplay()
    {
        var finder = new RecordingAiFinder("HYPERION", "TERMINATOR") { FailingTag = "HYPERION" };
        using var scopes = new AiReplayScopes(finder);

        await Loader(finder).ReplayAiHistory(scopes, CancellationToken.None);

        finder.Calls.Should().Equal("begin HYPERION", "begin TERMINATOR", "replay HYPERION", "end HYPERION", "replay TERMINATOR", "end TERMINATOR");
    }

    [Test]
    public async Task CancelledAiHistoryReplayStillEndsEveryScope()
    {
        var finder = new RecordingAiFinder("HYPERION", "TERMINATOR");
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        using (var scopes = new AiReplayScopes(finder))
            await FluentActions.Awaiting(() => Loader(finder).ReplayAiHistory(scopes, cancelled.Token)).Should().ThrowAsync<OperationCanceledException>();

        finder.Calls.Where(c => c.StartsWith("end")).Should().BeEquivalentTo("end HYPERION", "end TERMINATOR");
        finder.Calls.Should().NotContain("replay TERMINATOR");
    }

    [Test]
    public void ScopesOpenedBeforeAFailedOneAreEnded()
    {
        var finder = new RecordingAiFinder("HYPERION", "TERMINATOR") { FailingTag = "TERMINATOR", FailToBegin = true };

        Assert.Throws<InvalidOperationException>(() => new AiReplayScopes(finder));

        finder.Calls.Should().Equal("begin HYPERION", "end HYPERION");
    }

    private static InternalDataLoader Loader(IAiFinder finder)
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string>()).Build();
        return new InternalDataLoader(null, config, null, NullLogger<InternalDataLoader>.Instance, null, null, null, null, finder);
    }
}

/// <summary>An AI finder that records replay scopes and history replays in order and estimates a fixed value</summary>
internal sealed class RecordingAiFinder(params string[] tags) : IAiFinder
{
    public List<string> Calls { get; } = new();
    public string FailingTag { get; init; }
    public bool FailToBegin { get; init; }
    public AiEstimate Estimated { get; set; }
    public string Variant => "recording";
    public bool Active => true;
    public IEnumerable<string> ModelTags => tags;
    public IReadOnlyList<string> ActiveTags => tags;

    public IDisposable BeginReplay(string tag)
    {
        if (tag == FailingTag && FailToBegin)
            throw new InvalidOperationException("scope failed");
        Calls.Add("begin " + tag);
        return new Scope(() => Calls.Add("end " + tag));
    }

    public Task ReplayHistory(string tag, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Calls.Add("replay " + tag);
        if (tag == FailingTag)
            throw new InvalidOperationException("replay failed");
        return Task.CompletedTask;
    }

    public Task<AiEstimate> Estimate(SaveAuction auction) => Task.FromResult(Estimated);
    public void Start(Action<LowPricedAuction> publish, Func<bool> pause = null) { }
    public Task AddSale(SaveAuction auction) => Task.CompletedTask;
    public Task Retrain(string tag, CancellationToken cancellationToken, DateTime totalStart, Func<SaveAuction, Task> addSale) => Task.CompletedTask;
    public Task TrainFromTracker(IEnumerable<FlipTracker.Client.Model.ComplicatedFlip> flips) => Task.CompletedTask;
    public IEnumerable<object> GetModelStats() => [];
    public Task PersistModel(string tag) => Task.CompletedTask;

    private sealed class Scope(Action ended) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
                ended();
        }
    }
}
