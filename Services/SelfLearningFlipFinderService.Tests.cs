using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Coflnet.Sky.FlipTracker.Client.Model;
using FluentAssertions;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services.Tests;

[TestFixture]
public class SelfLearningFlipFinderServiceTests
{
    private class TestPersistence : IPersitanceManager
    {
        private readonly Dictionary<string, byte[]> store = new();
        public Task LoadLookups(SniperService service) => Task.CompletedTask;
        public Task SaveLookup(System.Collections.Concurrent.ConcurrentDictionary<string, PriceLookup> lookups) => Task.CompletedTask;
        public Task<System.Collections.Concurrent.ConcurrentDictionary<string, AttributeLookup>> GetWeigths() => Task.FromResult(new System.Collections.Concurrent.ConcurrentDictionary<string, AttributeLookup>());
        public Task SaveWeigths(System.Collections.Concurrent.ConcurrentDictionary<string, AttributeLookup> lookups) => Task.CompletedTask;
        public Task<List<KeyValuePair<string, PriceLookup>>> LoadGroup(int groupId) => Task.FromResult(new List<KeyValuePair<string, PriceLookup>>());
        public Task<Dictionary<string,double>> LoadCraftCost() => Task.FromResult(new Dictionary<string,double>());
        public Task SaveBlob(string key, System.IO.Stream data)
        {
            using var ms = new System.IO.MemoryStream();
            data.Position = 0;
            data.CopyTo(ms);
            store[key] = ms.ToArray();
            return Task.CompletedTask;
        }
        public Task<System.IO.Stream> LoadBlob(string key)
        {
            if (store.TryGetValue(key, out var b))
                return Task.FromResult<System.IO.Stream>(new System.IO.MemoryStream(b));
            throw new System.IO.FileNotFoundException();
        }
    }
    [Test]
    public async Task EstimateWithoutTraining_UsesBaselineFromCleanCost()
    {
    using var service = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, new TestPersistence(), minSamplesForTraining: 6);
        var flip = new ComplicatedFlip
        {
            AuctionId = Guid.NewGuid(),
            ItemTag = "HYPERION",
            EndedAt = DateTime.UtcNow,
            SoldFor = 0,
            AttributeValues = new Dictionary<string, long>
            {
                ["cleancost"] = 1_500_000_000,
                ["strength"] = 120
            }
        };

        var result = await service.EstimateAsync(flip);

        result.ModelReady.Should().BeFalse();
        result.EstimatedValue.Should().BeApproximately(1_500_000_000d, 1);
        result.BaselineValue.Should().BeApproximately(1_500_000_000d, 1);
    }

    [Test]
    public async Task TrainingSamplesEnablePredictionsAboveBaseline()
    {
    using var service = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, new TestPersistence(), minSamplesForTraining: 12);

        for (var i = 0; i < 12; i++)
        {
            var cleanCost = 800_000_000 + (i * 10_000_000);
            var gemstones = 40_000_000 + (i * 1_000_000);
            var bonus = 100_000_000 + (i * 500_000);
            var attributes = new Dictionary<string, long>
            {
                ["cleancost"] = cleanCost,
                ["gemstones"] = gemstones,
                ["stat_strength"] = 120 + (i * 2),
                ["stars"] = 5 + (i % 4)
            };

            var sample = new ComplicatedFlip
            {
                AuctionId = Guid.NewGuid(),
                ItemTag = "TERMINATOR",
                EndedAt = DateTime.UtcNow,
                SoldFor = cleanCost + gemstones + bonus,
                AttributeValues = attributes
            };

            await service.TrainAsync(sample);
        }

    // sanity-check: snapshot should show training samples present
    var snap = service.GetSnapshot();
    Console.WriteLine($"Snapshot: samples={snap.SampleCount}, features={snap.FeatureNames.Count}");
    snap.SampleCount.Should().BeGreaterOrEqualTo(12);

    // ensure model is trained from in-memory samples (tests run faster with explicit rebuild)
    var trained = await service.EnsureTrainedModelAsync("TERMINATOR");
    Console.WriteLine($"EnsureTrainedModelAsync returned: {trained}");
    trained.Should().BeTrue();

        var estimateAttributes = new Dictionary<string, long>
        {
            ["cleancost"] = 860_000_000,
            ["gemstones"] = 60_000_000,
            ["stat_strength"] = 138,
            ["stars"] = 6
        };

        var estimateFlip = new ComplicatedFlip
        {
            AuctionId = Guid.NewGuid(),
            ItemTag = "TERMINATOR",
            EndedAt = DateTime.UtcNow,
            SoldFor = 0,
            AttributeValues = estimateAttributes
        };

        var result = await service.EstimateAsync(estimateFlip);

        result.ModelReady.Should().BeTrue();
        result.SampleCount.Should().BeGreaterOrEqualTo(12);
        result.BaselineValue.Should().BeApproximately(estimateAttributes["cleancost"], 1);

        var expected = estimateAttributes["cleancost"] + estimateAttributes["gemstones"] + 103_000_000d;
        result.EstimatedValue.Should().BeApproximately(expected, 50_000_000d);
        result.EstimatedValue.Should().BeGreaterThan(result.BaselineValue);
    }
}
