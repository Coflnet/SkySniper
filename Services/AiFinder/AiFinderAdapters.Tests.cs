using System;
using System.Collections.Generic;
using System.IO;
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
}
