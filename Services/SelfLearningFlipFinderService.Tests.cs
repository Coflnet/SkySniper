using System;
using System.Collections.Generic;
using System.Linq;
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
    public async Task EstimateWithoutTraining_IsNull()
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

        result.Should().BeNull();
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

        // With L2=0.1 regularization, the model is more conservative than L2=0.01
        // The prediction should still be reasonable (between baseline and ideal)
        var baseline = estimateAttributes["cleancost"];
        var ideal = estimateAttributes["cleancost"] + estimateAttributes["gemstones"] + 103_000_000d;
        
        result.EstimatedValue.Should().BeGreaterThan(result.BaselineValue, "model should predict value above baseline");
        result.EstimatedValue.Should().BeInRange(baseline, ideal + 100_000_000d, "prediction should be reasonable");
    }

    [Test]
    [Category("Slow")]
    [Explicit("This test takes ~6 seconds and is used for model validation, not regular CI runs")]
    public async Task HyperionPredictionAccuracy_Within4Percent()
    {
        // Load real HYPERION samples from JSON
        var jsonPath = System.IO.Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "Mock", "HyperionSamples.json");
        var json = await System.IO.File.ReadAllTextAsync(jsonPath);
        var options = new System.Text.Json.JsonSerializerOptions 
        { 
            PropertyNameCaseInsensitive = true 
        };
        var flips = System.Text.Json.JsonSerializer.Deserialize<List<ComplicatedFlip>>(json, options);
        
        flips.Should().NotBeNullOrEmpty("HyperionSamples.json should contain test data");
        flips.Count.Should().BeGreaterThan(1000, "need sufficient samples for training and validation");

        TestContext.WriteLine($"Loaded {flips.Count} flips from JSON");

        // Split 80/20 for training/validation
        var trainingCount = (int)(flips.Count * 0.8);
        var training = flips.Take(trainingCount).ToList();
        var validation = flips.Skip(trainingCount).ToList();

        TestContext.WriteLine($"Training: {training.Count}, Validation: {validation.Count}");

        // FastTree configuration (now used instead of SDCA)
        var config = new { Name = "FastTree" };

        TestContext.WriteLine($"\nTesting configuration: {config.Name}");

        var persistence = new TestPersistence();
        using var service = new SelfLearningFlipFinderService(
            NullLogger<SelfLearningFlipFinderService>.Instance,
            persistence,
            minSamplesForTraining: 100
        );

        // Train with all samples using batch method
        await service.TrainBatchAsync(training);

        // Check snapshot
        var snapshot = service.GetSnapshot();
        TestContext.WriteLine($"After batch training: SampleCount={snapshot.SampleCount}, Features={snapshot.FeatureNames.Count}");

        // Ensure model is trained
        var trained = await service.EnsureTrainedModelAsync("HYPERION");
        trained.Should().BeTrue("model should train with sufficient samples");

        // Validate predictions
        var errors = new List<double>();
        foreach (var flip in validation)
        {
            // Create a copy without soldFor to simulate prediction scenario
            var testFlip = new ComplicatedFlip
            {
                AuctionId = flip.AuctionId,
                ItemTag = flip.ItemTag,
                EndedAt = flip.EndedAt,
                SoldFor = 0,
                AttributeValues = flip.AttributeValues
            };
            var estimate = await service.EstimateAsync(testFlip);
            
            var actualPrice = flip.SoldFor;
            var predictedPrice = estimate.EstimatedValue;
            var percentError = Math.Abs(predictedPrice - actualPrice) / actualPrice * 100.0;
            
            errors.Add(percentError);
        }

        var avgError = errors.Average();
        var medianError = errors.OrderBy(e => e).ElementAt(errors.Count / 2);
        var maxError = errors.Max();
        var errorsOver10Percent = errors.Count(e => e > 10);
        
        TestContext.WriteLine($"\n=== RESULTS ===");
        TestContext.WriteLine($"Average Error: {avgError:F2}%");
        TestContext.WriteLine($"Median Error: {medianError:F2}%");
        TestContext.WriteLine($"Max Error: {maxError:F2}%");
        TestContext.WriteLine($"Errors >10%: {errorsOver10Percent} / {errors.Count} ({100.0 * errorsOver10Percent / errors.Count:F1}%)");

        // Test scroll_count:3 items are valued correctly (>1.3B)
        TestContext.WriteLine($"\n=== SCROLL_COUNT:3 VALIDATION ===");
        var scrollCount3Items = validation.Where(f => 
            f.AttributeValues.Any(kv => kv.Key == "scroll_count:3")).ToList();
        TestContext.WriteLine($"Found {scrollCount3Items.Count} items with scroll_count:3");
        
        var scrollCount3Predictions = new List<(long Actual, long Predicted)>();
        foreach (var flip in scrollCount3Items.Take(10)) // Sample first 10
        {
            var testFlip = new ComplicatedFlip
            {
                AuctionId = flip.AuctionId,
                ItemTag = flip.ItemTag,
                EndedAt = flip.EndedAt,
                SoldFor = 0,
                AttributeValues = flip.AttributeValues
            };
            var estimate = await service.EstimateAsync(testFlip);
            scrollCount3Predictions.Add((flip.SoldFor, (long)estimate.EstimatedValue));
            TestContext.WriteLine($"Auction {flip.AuctionId}: Actual={flip.SoldFor:N0}, Predicted={estimate.EstimatedValue:N0}");
        }
        
        // All scroll_count:3 items should be predicted >1.3B
        foreach (var (actual, predicted) in scrollCount3Predictions)
        {
            predicted.Should().BeGreaterThan(1_300_000_000L, 
                "scroll_count:3 should make HYPERION worth more than 1.3 billion");
        }

        // Median error should be <4% (robust to outliers)
        medianError.Should().BeLessThan(4.0, 
            $"configuration '{config.Name}' should achieve <4% median prediction error (average was {avgError:F2}% due to outliers)");
        
        // Most predictions should be reasonable (not >90% with >10% error)
        (100.0 * errorsOver10Percent / errors.Count).Should().BeLessThan(15.0,
            "most predictions should be within 10% of actual price");
    }
}
