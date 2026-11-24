using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Coflnet.Sky.AIFinder.Model;
using Coflnet.Sky.AIFinder;
using AwesomeAssertions;
using Newtonsoft.Json;
using NUnit.Framework;
using TrackerFlip = Coflnet.Sky.FlipTracker.Client.Model.ComplicatedFlip;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

public class AiFinderAdaptersTests
{
    /// <summary>
    /// Flips carry the sample as JSON in their cflip property; it has to read the same as the flip tracker's type it replaced
    /// </summary>
    [TestCase(0)]
    [TestCase(925_000_000)]
    public void SampleSerialisesLikeTheTrackerFlip(long soldFor)
    {
        var sample = new AiFlipSample
        {
            AuctionId = Guid.Parse("00000000000000000000000000000001"),
            ItemTag = "HYPERION",
            EndedAt = new DateTime(2025, 10, 1, 2, 0, 0, DateTimeKind.Utc),
            SoldFor = soldFor,
            AttributeValues = new Dictionary<string, long> { ["cleancost"] = 870_000_000, ["ultimate_wise:5"] = 3_100_000 }
        };

        var trackerFlip = new TrackerFlip
        {
            AuctionId = sample.AuctionId, ItemTag = sample.ItemTag, EndedAt = sample.EndedAt, SoldFor = sample.SoldFor, AttributeValues = sample.AttributeValues
        };

        JsonConvert.SerializeObject(sample).Should().Be(JsonConvert.SerializeObject(trackerFlip));
    }

    [Test]
    public void TrackerFlipWithoutAttributesBecomesAnEmptySample()
    {
        var sample = new TrackerFlip { ItemTag = "HYPERION", SoldFor = 1 }.ToSample();

        sample.AttributeValues.Should().BeEmpty();
        sample.ItemTag.Should().Be("HYPERION");
    }

    private sealed class FixedMayors(string mayor) : IMayorService
    {
        public string GetMayor(DateTime time) => mayor;
        public string GetPreviousMayor(DateTime time) => mayor;
        public bool IsDianaItemsAdjustmentActive(DateTime time) => false;
    }

    private sealed class ContextRecorder : Coflnet.Sky.AIFinder.ISelfLearningFlipFinderService
    {
        public List<string?> Shared { get; } = new();
        public Task ShareContextAsync(string? mayor) { Shared.Add(mayor); return Task.CompletedTask; }
        public bool Trains => true;
        public Coflnet.Sky.AIFinder.Model.SharedContext? Context => null;
        public void StartLoading(Func<bool>? pause = null, Action<TimeSpan>? loaded = null) { }
        public bool IsRelevantItem(string tag) => false;
        public Task TrainAsync(AiFlipSample flip, System.Threading.CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task TrainBatchAsync(IEnumerable<AiFlipSample> flips, System.Threading.CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<SelfLearningFlipEstimate?> EstimateAsync(AiFlipSample flip, System.Threading.CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IReadOnlyDictionary<string, SelfLearningFlipFinderService.ModelStats> GetModelStats() => throw new NotSupportedException();
        public Task PersistModelAsync(string? tag = null) => throw new NotSupportedException();
    }

    [TestCase("Unknown")]
    [TestCase("unknown")]
    [TestCase("")]
    [TestCase("  ")]
    public void MayorLookupTreatsAnUnsetMayorAsMissing(string mayor)
        => new MayorServiceLookup(new FixedMayors(mayor)).GetMayor(DateTime.UtcNow).Should().BeNull();

    [Test]
    public void MayorLookupPassesARealMayorThrough()
        => new MayorServiceLookup(new FixedMayors("Diana")).GetMayor(DateTime.UtcNow).Should().Be("Diana");

    [Test]
    public void ContextIsNotSharedWithoutAMayor()
    {
        var model = new ContextRecorder();

        SkyAiFinderAdapter.ShareContext(model, () => null, NullLogger.Instance);
        SkyAiFinderAdapter.ShareContext(model, () => "Diana", NullLogger.Instance);

        model.Shared.Should().Equal("Diana");
    }

    [TestCase("TARGET_SHARE", "5")]
    [TestCase("HAIRCUT", "abc")]
    [TestCase("MAX_CONCURRENT_FITS", "99999999999999999999")]
    public void InvalidSettingKeepsTheFinderOffAndTheStartGoing(string key, string value)
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { [$"AI_FINDER:{key}"] = value }).Build();
        var error = new StringWriter();
        var previous = Console.Error;
        Console.SetError(error);
        try
        {
            var services = new ServiceCollection().AddAiFinder(config, trains: false).BuildServiceProvider();

            var finder = services.GetRequiredService<IAiFinder>();
            finder.Active.Should().BeFalse();
            finder.Variant.Should().Be(NoAiFinder.InvalidName);
            AiFinderRegistration.Describe(config).Should().Be((false, NoAiFinder.InvalidName));
            error.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries).Should().ContainSingle().Which.Should().Contain(key);
        }
        finally
        {
            Console.SetError(previous);
        }
    }

    [Test]
    public async Task LoadOnlyInstanceObservesSalesOfRelevantItems()
    {
        var model = new RecordingModel(trains: false);

        await Adapter(model).AddSale(Sale("HYPERION"));
        await Adapter(model).AddSale(Sale("DIRT"));

        model.Calls.Should().Equal("observe HYPERION");
    }

    /// <summary>A sale older than any model a load-only instance serves cannot move one, so it is not even valued</summary>
    [Test]
    public async Task LoadOnlyInstanceSkipsSalesOlderThanAServedModel()
    {
        var model = new RecordingModel(trains: false);

        await Adapter(model).AddSale(Sale("HYPERION", DateTime.UtcNow.AddDays(-4)));

        model.Calls.Should().BeEmpty();
    }

    [Test]
    public async Task TrainingInstanceTrainsOnSales()
    {
        var model = new RecordingModel(trains: true);

        await Adapter(model).AddSale(Sale("HYPERION"));

        model.Calls.Should().Equal("train HYPERION");
    }

    [Test]
    public async Task HistoryReplayHoldsTheTagsReplayScope()
    {
        var model = new RecordingModel(trains: true);

        await Adapter(model, _ => new FixedSales(Sale("HYPERION", DateTime.UtcNow.AddDays(-1)))).ReplayHistory("HYPERION", CancellationToken.None);

        model.Calls.Should().Equal("begin HYPERION", "train HYPERION", "end HYPERION");
    }

    [Test]
    public async Task RetrainHoldsTheReplayScopeAroundItsSales()
    {
        var model = new RecordingModel(trains: true);
        var sales = new FixedSales(Sale("HYPERION", DateTime.UtcNow.AddDays(-1)));

        await Adapter(model, _ => sales).Retrain("HYPERION", CancellationToken.None, DateTime.UtcNow.AddDays(-3), sale =>
        {
            model.Calls.Add("sale");
            return Task.CompletedTask;
        });

        model.Calls.Should().Equal("begin HYPERION", "sale", "end HYPERION");
    }

    [Test]
    public async Task ReplayScopeEndsWhenTheReplayFails()
    {
        var model = new RecordingModel(trains: true);
        var adapter = Adapter(model, _ => new FixedSales(new InvalidOperationException("database down")));

        await FluentActions.Awaiting(() => adapter.ReplayHistory("HYPERION", CancellationToken.None)).Should().ThrowAsync<InvalidOperationException>();

        model.Calls.Should().Equal("begin HYPERION", "end HYPERION");
    }

    [Test]
    public async Task ReplayScopeEndsWhenTheReplayIsCancelled()
    {
        var model = new RecordingModel(trains: true);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        await FluentActions.Awaiting(() => Adapter(model, _ => new FixedSales()).ReplayHistory("HYPERION", cancelled.Token))
            .Should().ThrowAsync<OperationCanceledException>();

        model.Calls.Should().Equal("begin HYPERION", "end HYPERION");
    }

    [Test]
    public async Task OnlyTheTrainingInstanceReplaysRelevantItems()
    {
        var loading = new RecordingModel(trains: false);
        var training = new RecordingModel(trains: true);

        await Adapter(loading, _ => new FixedSales()).ReplayHistory("HYPERION", CancellationToken.None);
        await Adapter(training, _ => new FixedSales()).ReplayHistory("DIRT", CancellationToken.None);

        loading.Calls.Should().BeEmpty();
        training.Calls.Should().BeEmpty();
    }

    [Test]
    public async Task OnlyTheTrainingInstanceRetrainsRelevantItems()
    {
        var loading = new RecordingModel(trains: false);
        var training = new RecordingModel(trains: true);
        Func<Coflnet.Sky.Core.SaveAuction, Task> fed = _ => Task.CompletedTask;

        await Adapter(loading, _ => new FixedSales()).Retrain("HYPERION", CancellationToken.None, DateTime.UtcNow.AddDays(-3), fed);
        await Adapter(training, _ => new FixedSales()).Retrain("DIRT", CancellationToken.None, DateTime.UtcNow.AddDays(-3), fed);

        loading.Calls.Should().BeEmpty();
        training.Calls.Should().BeEmpty();
    }

    [Test]
    public void ActiveTagsFollowTheAllowlist()
    {
        var settings = AiFinderSettings.Defaults(trains: true);
        settings = settings with { Model = settings.Model with { Tags = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "TERMINATOR", "HYPERION" } } };

        Adapter(new RecordingModel(trains: true), settings: settings).ActiveTags.Should().Equal("HYPERION", "TERMINATOR");
    }

    private static SkyAiFinderAdapter Adapter(RecordingModel model, Func<string, ISoldAuctionSource>? sales = null, AiFinderSettings? settings = null)
    {
        var extractor = new AiFeatureExtractor(null);
        var flipFinder = new AiFlipFinder(model, extractor, NullLogger<AiFlipFinder>.Instance);
        return new SkyAiFinderAdapter(model, flipFinder, extractor, null!, null, NullLogger<SkyAiFinderAdapter>.Instance,
            settings ?? AiFinderSettings.Defaults(model.Trains), soldAuctions: sales);
    }

    private static Coflnet.Sky.Core.SaveAuction Sale(string tag, DateTime? end = null) => new()
    {
        Tag = tag,
        Uuid = Guid.NewGuid().ToString("N"),
        Bin = true,
        Start = (end ?? DateTime.UtcNow).AddHours(-2),
        End = end ?? DateTime.UtcNow,
        HighestBidAmount = 900_000_000,
        FlatenedNBT = new Dictionary<string, string>(),
        Enchantments = new List<Coflnet.Sky.Core.Enchantment>()
    };

    /// <summary>Sold auctions of a fixed list, or a failure, for whatever is asked</summary>
    private sealed class FixedSales(params Coflnet.Sky.Core.SaveAuction[] sales) : ISoldAuctionSource
    {
        private readonly Exception? failure;

        public FixedSales(Exception failure) : this()
        {
            this.failure = failure;
        }

        public Task<List<Coflnet.Sky.Core.SaveAuction>> LoadSoldBins(string tag, DateTime start, DateTime end, SoldPageStart? after, int limit,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (failure != null)
                throw failure;
            return Task.FromResult(sales.Where(s => after == null && s.End >= start && s.End < end).ToList());
        }
    }

    /// <summary>Records what the adapter asks of the model, in order</summary>
    private sealed class RecordingModel(bool trains) : ISelfLearningFlipFinderService
    {
        public List<string> Calls { get; } = new();
        public bool Trains => trains;
        public bool IsRelevantItem(string tag) => tag is "HYPERION" or "TERMINATOR";

        public Task TrainAsync(AiFlipSample flip, CancellationToken cancellationToken = default)
        {
            Calls.Add("train " + flip.ItemTag);
            return Task.CompletedTask;
        }

        public void ObserveSale(AiFlipSample flip) => Calls.Add("observe " + flip.ItemTag);

        public IDisposable BeginReplay(string tag)
        {
            Calls.Add("begin " + tag);
            return new Scope(() => Calls.Add("end " + tag));
        }

        public Coflnet.Sky.AIFinder.Model.SharedContext? Context => null;
        public Task ShareContextAsync(string? mayor) => Task.CompletedTask;
        public void StartLoading(Func<bool>? pause = null, Action<TimeSpan>? loaded = null) { }
        public Task TrainBatchAsync(IEnumerable<AiFlipSample> flips, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<SelfLearningFlipEstimate?> EstimateAsync(AiFlipSample flip, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IReadOnlyDictionary<string, SelfLearningFlipFinderService.ModelStats> GetModelStats() => throw new NotSupportedException();
        public Task PersistModelAsync(string? tag = null) => throw new NotSupportedException();
    }

    private sealed class Scope(Action ended) : IDisposable
    {
        public void Dispose() => ended();
    }
}
