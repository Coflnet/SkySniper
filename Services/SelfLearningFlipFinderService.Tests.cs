using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.FlipTracker.Client.Model;
using AwesomeAssertions;
using Coflnet.Sky.Sniper.Models;
using MessagePack;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.ML;
using Microsoft.ML.Data;
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
        public Task FlushDueGroups(System.Collections.Concurrent.ConcurrentDictionary<string, PriceLookup> lookups, TimeSpan maxAge, System.Threading.CancellationToken cancellationToken = default) => Task.CompletedTask;
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
    private sealed class RecordingLogger : ILogger<SelfLearningFlipFinderService>
    {
        public List<string> Messages { get; } = new();
        public IDisposable BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            => Messages.Add(formatter(state, exception));
    }

    private sealed class LegacySample
    {
        public float[] Features { get; set; }
        public float Label { get; set; }
    }

    private static ComplicatedFlip Sale(string tag, long soldFor, Dictionary<string, long> attributes, DateTime? endedAt = null) => new()
    {
        AuctionId = Guid.NewGuid(),
        ItemTag = tag,
        EndedAt = endedAt ?? DateTime.UtcNow,
        SoldFor = soldFor,
        AttributeValues = attributes
    };

    /// <summary>Price around <paramref name="median"/> with the spread of a liquid item, deterministic per seed.</summary>
    private static long SpreadPrice(Random random, double median, double sigma = 0.15)
    {
        var gaussian = Math.Sqrt(-2 * Math.Log(1 - random.NextDouble())) * Math.Cos(2 * Math.PI * random.NextDouble());
        return (long)(median * Math.Exp(sigma * gaussian));
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
    snap.SampleCount.Should().BeGreaterThanOrEqualTo(12);

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
        result.SampleCount.Should().BeGreaterThanOrEqualTo(12);
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

    [Test]
    public async Task PetRock_WithCandyUsedFlag_NotOvervalued()
    {
        // PET_ROCK is a cheap pet (~150K-300K). The old system assigned candyUsed:0 = 10M
        // as a weight, which inflated the attribute sum and caused the ML model to predict
        // millions. After the fix, candyUsed is a presence flag (1) and should not inflate values.
        using var service = new SelfLearningFlipFinderService(
            NullLogger<SelfLearningFlipFinderService>.Instance,
            new TestPersistence(),
            minSamplesForTraining: 12);

        // Train with realistic PET_ROCK samples at actual market prices (150K-350K)
        var rng = new Random(42);
        for (var i = 0; i < 20; i++)
        {
            var soldFor = 150_000 + rng.Next(200_000); // 150K-350K
            var sample = new ComplicatedFlip
            {
                AuctionId = Guid.NewGuid(),
                ItemTag = "PET_ROCK",
                EndedAt = DateTime.UtcNow.AddHours(-i),
                SoldFor = soldFor,
                AttributeValues = new Dictionary<string, long>
                {
                    // candyUsed:0 should now be a presence flag (1), not 10M
                    ["candyUsed:0"] = 1,
                    ["exp:0"] = rng.Next(0, 500_000),
                    ["tier:UNCOMMON"] = 1
                }
            };
            await service.TrainAsync(sample);
        }

        var trained = await service.EnsureTrainedModelAsync("PET_ROCK");
        trained.Should().BeTrue();

        // Estimate a PET_ROCK with the same attribute pattern
        var estimateFlip = new ComplicatedFlip
        {
            AuctionId = Guid.NewGuid(),
            ItemTag = "PET_ROCK",
            EndedAt = DateTime.UtcNow,
            SoldFor = 0,
            AttributeValues = new Dictionary<string, long>
            {
                ["candyUsed:0"] = 1,
                ["exp:0"] = 0,
                ["tier:UNCOMMON"] = 1
            }
        };

        var result = await service.EstimateAsync(estimateFlip);

        result.Should().NotBeNull();
        result!.ModelReady.Should().BeTrue();
        // The prediction must not wildly overvalue the pet. A PET_ROCK is worth ~300K max.
        // The old system predicted ~12.8M due to the inflated candyUsed attribute.
        result.EstimatedValue.Should().BeLessThan(1_000_000,
            "PET_ROCK should not be valued above 1M; it sells for 150K-350K");
    }

    [Test]
    public async Task TrainAsync_SkipsNonRelevantItems()
    {
        // Verify that TrainAsync does not accumulate training data for items
        // not in the RelevantItems set (e.g. a made-up tag).
        using var service = new SelfLearningFlipFinderService(
            NullLogger<SelfLearningFlipFinderService>.Instance,
            new TestPersistence(),
            minSamplesForTraining: 6);

        for (var i = 0; i < 20; i++)
        {
            var sample = new ComplicatedFlip
            {
                AuctionId = Guid.NewGuid(),
                ItemTag = "FAKE_NONEXISTENT_ITEM",
                EndedAt = DateTime.UtcNow,
                SoldFor = 100_000,
                AttributeValues = new Dictionary<string, long>
                {
                    ["some_attr"] = 42
                }
            };
            await service.TrainAsync(sample);
        }

        var stats = service.GetModelStats();
        stats.Should().NotContainKey("FAKE_NONEXISTENT_ITEM",
            "non-relevant items should not have training data");

        var estimate = await service.EstimateAsync(new ComplicatedFlip
        {
            AuctionId = Guid.NewGuid(),
            ItemTag = "FAKE_NONEXISTENT_ITEM",
            EndedAt = DateTime.UtcNow,
            SoldFor = 0,
            AttributeValues = new Dictionary<string, long> { ["some_attr"] = 42 }
        });
        estimate.Should().BeNull("non-relevant items should return null estimate");
    }

    /// <summary>
    /// A tag sold almost only clean (like the production FERMENTO_HELMET and WARDEN_HELMET sets) has no column that
    /// separates enough sales for a leaf. FastTree then returns an ensemble without trees, which scored every sale
    /// at 0 coins: R² on the training data was far below zero and the estimate fell back to the craft cost.
    /// </summary>
    [Test]
    public async Task TagWithoutUsableSplit_PredictsMedianInsteadOfNothing()
    {
        const int sales = 1400;
        using var service = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, new TestPersistence(), minSamplesForTraining: sales);
        var random = new Random(7);
        for (var i = 0; i < sales; i++)
        {
            var attributes = new Dictionary<string, long> { ["cleancost"] = 2_500_000 };
            // three sparse coin valued columns, each on three sales only
            if (i < 9)
                attributes[$"enchant_{i / 3}:5"] = 300_000 + i * 250_000L;
            var price = i switch
            {
                500 => 100_000_000, // coin transfer
                900 => 200_000, // junk price
                _ => SpreadPrice(random, 5_000_000)
            };
            await service.TrainAsync(Sale("FERMENTO_HELMET", price, attributes));
        }

        var metrics = service.GetModelStats()["FERMENTO_HELMET"].Metrics;
        metrics.RSquared.Should().BeGreaterThan(-0.05, "a model must not be worse than a constant on its own training data");

        var estimate = await service.EstimateAsync(Sale("FERMENTO_HELMET", 0, new() { ["cleancost"] = 2_500_000 }));
        estimate.ModelReady.Should().BeTrue();
        estimate.EstimatedValue.Should().BeInRange(4_500_000, 5_500_000, "without a usable split the tag's median sale is the estimate");
    }

    /// <summary>
    /// Two coin transfers among the 70 sales of an upgrade moved the whole leaf when the label was the raw price.
    /// On a log scale a single sale for 100 coins does the same downwards, so neither is trained on.
    /// </summary>
    [Test]
    public async Task TransferAndJunkPricesDoNotMoveTheirGroup()
    {
        const int sales = 1400;
        using var service = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, new TestPersistence(), minSamplesForTraining: sales);
        var random = new Random(11);
        var upgraded = new Dictionary<string, long> { ["cleancost"] = 11_000_000, ["ultimate_soul_eater:5"] = 15_000_000 };
        for (var i = 0; i < sales; i++)
        {
            var hasUpgrade = i % 20 == 0;
            var price = i switch
            {
                400 or 800 => 2_000_000_000, // coin transfers
                1200 => 100, // junk price
                _ => SpreadPrice(random, hasUpgrade ? 42_000_000 : 30_000_000)
            };
            await service.TrainAsync(Sale("JUJU_SHORTBOW", price, hasUpgrade ? upgraded : new() { ["cleancost"] = 11_000_000 }));
        }

        var estimate = await service.EstimateAsync(Sale("JUJU_SHORTBOW", 0, upgraded));

        estimate.EstimatedValue.Should().BeInRange(42_000_000 * 0.9, 42_000_000 * 1.15, "67 of the 70 upgraded bows sold around 42M");
    }

    /// <summary>
    /// Upgrades that are each too rare for a leaf could not be learned at all. Their summed value is a column of
    /// its own now, so an item with an upgrade the model never saw is priced by what its attributes add up to.
    /// </summary>
    [Test]
    public async Task RareUpgradesArePricedThroughTheirSum()
    {
        const int sales = 1500;
        using var service = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, new TestPersistence(), minSamplesForTraining: sales);
        var random = new Random(13);
        for (var i = 0; i < sales; i++)
        {
            var attributes = new Dictionary<string, long> { ["cleancost"] = 20_000_000 };
            // 150 different upgrades worth 5M to 80M, each on three sales, selling for 80% of their value
            var upgradeValue = i % 10 < 3 ? 5_000_000 + i / 10 * 500_000L : 0;
            if (upgradeValue > 0)
                attributes[$"gem_{i / 10}:1"] = upgradeValue;
            await service.TrainAsync(Sale("DIVAN_HELMET", SpreadPrice(random, 20_000_000 + upgradeValue * 0.8, 0.05), attributes));
        }

        var estimate = await service.EstimateAsync(Sale("DIVAN_HELMET", 0, new() { ["cleancost"] = 20_000_000, ["gem_unseen:1"] = 60_000_000 }));

        estimate.EstimatedValue.Should().BeInRange(58_000_000, 78_000_000, "upgrades worth 60M sold for about 68M in total");
    }

    /// <summary>
    /// Sales converted in one batch share the coin value of a modifier, a sale converted on its own can be one coin
    /// apart. Two such values are neighbouring floats; FastTree rounds the threshold between them onto the upper
    /// one and predicts the whole batch on the outlier's side of the split it trained them on the other side of.
    /// </summary>
    [Test]
    public async Task SalesNextToAnOutlierKeepTheirOwnPrice()
    {
        const int sales = 1400;
        const int batchSize = 25;
        using var service = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, new TestPersistence(), minSamplesForTraining: sales);
        var random = new Random(3);
        long BatchValue(int i) => 10_753_000 + 3 * (i / batchSize);
        const int outlier = 20 * batchSize + 12;
        for (var i = 0; i < sales; i++)
        {
            var attributes = new Dictionary<string, long> { ["rarity_upgrades:1"] = i == outlier ? BatchValue(i) - 1 : BatchValue(i) };
            if (i % 3 == 0)
                attributes["hotpc:1"] = 3_000_000;
            await service.TrainAsync(Sale("BONE_NECKLACE", i == outlier ? 1_000_000_000 : SpreadPrice(random, 29_000_000, 0.2), attributes));
        }

        var estimate = await service.EstimateAsync(Sale("BONE_NECKLACE", 0, new() { ["rarity_upgrades:1"] = BatchValue(outlier) }));

        estimate.EstimatedValue.Should().BeInRange(20_000_000, 40_000_000, "the 24 other sales of that batch went for about 29M");
    }

    /// <summary>
    /// The refit log line only had the fit on the training samples. It now also carries the error of the serving
    /// model on sales it had not seen; a sale replayed by a retrain is not counted again.
    /// </summary>
    [Test]
    public async Task RefitLogsErrorOnSalesTheModelHadNotSeen()
    {
        const int sales = 300;
        var logger = new RecordingLogger();
        using var service = new SelfLearningFlipFinderService(logger, new TestPersistence(), minSamplesForTraining: sales);
        var history = GogglesHistory(sales + 40);
        foreach (var sale in history.Take(sales))
            await service.TrainAsync(sale);

        foreach (var sale in history.Skip(sales))
            await service.TrainAsync(sale);
        foreach (var replayed in history.Take(40))
            await service.TrainAsync(replayed);
        await service.PersistModelAsync("WITHER_GOGGLES");

        var refit = logger.Messages.Last(m => m.StartsWith("Trained FastTree model for WITHER_GOGGLES"));
        refit.Should().Contain("heldOutSales=40");
        var medianError = double.Parse(System.Text.RegularExpressions.Regex.Match(refit, @"heldOutMedianError=([0-9.]+)").Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
        medianError.Should().BeLessThan(0.1, "prices spread about 5% around what the model learned");
    }

    /// <summary>
    /// After a restart the history is replayed while the persisted model serves. That model was trained on those
    /// sales, so its error on them is not a held-out error.
    /// </summary>
    [Test]
    public async Task ReplayAfterRestartIsNotCountedAsHeldOut()
    {
        const int sales = 150;
        var persistence = new TestPersistence();
        var history = GogglesHistory(sales);
        using (var beforeRestart = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, persistence, minSamplesForTraining: sales))
        {
            foreach (var sale in history)
                await beforeRestart.TrainAsync(sale);
        }

        var logger = new RecordingLogger();
        using var restarted = new SelfLearningFlipFinderService(logger, persistence, minSamplesForTraining: sales);
        await restarted.TrainAsync(history[0]);
        await restarted.EstimateAsync(Sale("WITHER_GOGGLES", 0, history[0].AttributeValues));
        logger.Messages.Should().Contain(m => m.StartsWith("Loaded persisted model for WITHER_GOGGLES"));
        foreach (var replayed in history.Skip(1))
            await restarted.TrainAsync(replayed);

        logger.Messages.Last(m => m.StartsWith("Trained FastTree model for WITHER_GOGGLES")).Should().EndWith("heldOutSales=0");
    }

    private static List<ComplicatedFlip> GogglesHistory(int sales)
    {
        var random = new Random(17);
        var start = DateTime.UtcNow.AddDays(-1);
        return Enumerable.Range(0, sales).Select(i =>
        {
            var recombobulated = i % 2 == 0;
            return Sale("WITHER_GOGGLES", SpreadPrice(random, recombobulated ? 18_000_000 : 8_000_000, 0.05),
                new() { ["cleancost"] = 6_000_000, ["rarity_upgrades:1"] = recombobulated ? 10_000_000 : 0 }, start.AddMinutes(i));
        }).ToList();
    }

    /// <summary>
    /// Rolling deployment, instances of the previous version still running: their raw-coin model and the shared
    /// metadata object it is paired with are read but never rewritten, so that pair stays consistent for them.
    /// </summary>
    [Test]
    public async Task LegacyModelIsReadButNeverRewritten()
    {
        const int sales = 120;
        var persistence = new TestPersistence();
        var legacyFeatures = new[] { "rarity_upgrades:1", "cleancost" };
        // a raw price model exactly as the previous version stored it
        var ml = new MLContext(seed: 1);
        var schema = SchemaDefinition.Create(typeof(LegacySample));
        schema[nameof(LegacySample.Features)].ColumnType = new VectorDataViewType(NumberDataViewType.Single, legacyFeatures.Length);
        var data = ml.Data.LoadFromEnumerable(Enumerable.Range(0, sales).Select(i => new LegacySample { Features = [i % 2 * 8_000_000, 900_000_000], Label = 1_000_000_000 + i % 2 * 8_000_000 }), schema);
        var legacyModel = new System.IO.MemoryStream();
        ml.Model.Save(ml.Regression.Trainers.FastTree(nameof(LegacySample.Label), nameof(LegacySample.Features), numberOfTrees: 50, minimumExampleCountPerLeaf: 5).Fit(data), data.Schema, legacyModel);
        var legacyMeta = MessagePackSerializer.Serialize(new Dictionary<string, SelfLearningFlipFinderService.PersistMeta>
        {
            ["HYPERION"] = new() { FeatureNames = legacyFeatures, SampleCount = sales, Rmse = 1_000_000, RSquared = 0.9 }
        });
        await persistence.SaveBlob("selflearning/model/HYPERION", legacyModel);
        await persistence.SaveBlob("selflearning/meta/all", new System.IO.MemoryStream(legacyMeta));

        var logger = new RecordingLogger();
        using var service = new SelfLearningFlipFinderService(logger, persistence, minSamplesForTraining: sales);
        var random = new Random(19);
        ComplicatedFlip NextSale(int i) => Sale("HYPERION", SpreadPrice(random, 1_000_000_000), new() { ["cleancost"] = 900_000_000, ["rarity_upgrades:1"] = 8_000_000 + i });
        await service.TrainAsync(NextSale(0));
        await service.EstimateAsync(Sale("HYPERION", 0, new() { ["cleancost"] = 900_000_000 }));
        logger.Messages.Should().Contain(m => m.StartsWith("Loaded persisted model for HYPERION with 2 features"));
        service.GetModelStats()["HYPERION"].FeatureNames.Should().Equal(legacyFeatures);

        for (var i = 1; i < sales; i++)
            await service.TrainAsync(NextSale(i));

        (await StoredBytes(persistence, "selflearning/logmodel/HYPERION")).Should().NotBeEmpty("the refit model is stored under its own key");
        (await StoredBytes(persistence, "selflearning/model/HYPERION")).Should().Equal(legacyModel.ToArray());
        (await StoredBytes(persistence, "selflearning/meta/all")).Should().Equal(legacyMeta);
    }

    /// <summary>
    /// Rolling deployment, the other direction: an instance of the previous version saves the shared metadata
    /// object after a log model was stored. The log model must come back with the feature order it was fit on.
    /// </summary>
    [Test]
    public async Task LogModelLoadsWithItsOwnFeatureOrder()
    {
        const int sales = 120;
        var persistence = new TestPersistence();
        var random = new Random(19);
        ComplicatedFlip NextSale(int i) => Sale("HYPERION", SpreadPrice(random, 1_000_000_000), new() { ["cleancost"] = 900_000_000, ["rarity_upgrades:1"] = 8_000_000 + i });
        IReadOnlyCollection<string> fitFeatures;
        using (var trainer = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, persistence, minSamplesForTraining: sales))
        {
            for (var i = 0; i < sales; i++)
                await trainer.TrainAsync(NextSale(i));
            fitFeatures = trainer.GetModelStats()["HYPERION"].FeatureNames;
        }
        await persistence.SaveBlob("selflearning/meta/all", new System.IO.MemoryStream(MessagePackSerializer.Serialize(new Dictionary<string, SelfLearningFlipFinderService.PersistMeta>
        {
            ["HYPERION"] = new() { FeatureNames = ["rarity_upgrades:1", "cleancost"], SampleCount = sales }
        })));

        var logger = new RecordingLogger();
        using var service = new SelfLearningFlipFinderService(logger, persistence, minSamplesForTraining: sales);
        await service.TrainAsync(NextSale(0));
        await service.EstimateAsync(Sale("HYPERION", 0, new() { ["cleancost"] = 900_000_000 }));

        logger.Messages.Should().Contain(m => m.StartsWith("Loaded persisted model for HYPERION with 3 features"));
        service.GetModelStats()["HYPERION"].FeatureNames.Should().Equal(fitFeatures);
    }

    private static async Task<byte[]> StoredBytes(TestPersistence persistence, string key)
    {
        var copy = new System.IO.MemoryStream();
        (await persistence.LoadBlob(key)).CopyTo(copy);
        return copy.ToArray();
    }
}
