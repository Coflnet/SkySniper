using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.FlipTracker.Client.Model;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services.Tests;

public class SelfLearningReadyInferenceTests
{
    private sealed class Persistence : IPersitanceManager
    {
        private readonly ConcurrentDictionary<string, byte[]> blobs = new();
        public Action<string> Saving;
        public int Loads;
        public Task SaveBlob(string key, Stream data)
        {
            Saving?.Invoke(key);
            using var copy = new MemoryStream();
            data.CopyTo(copy);
            blobs[key] = copy.ToArray();
            return Task.CompletedTask;
        }
        public Task<Stream> LoadBlob(string key)
        {
            Interlocked.Increment(ref Loads);
            return Task.FromResult<Stream>(blobs.TryGetValue(key, out var bytes) ? new MemoryStream(bytes) : null);
        }
        public Task LoadLookups(SniperService service) => Task.CompletedTask;
        public Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups) => Task.CompletedTask;
        public Task<ConcurrentDictionary<string, AttributeLookup>> GetWeigths() => Task.FromResult(new ConcurrentDictionary<string, AttributeLookup>());
        public Task SaveWeigths(ConcurrentDictionary<string, AttributeLookup> lookups) => Task.CompletedTask;
        public Task<List<KeyValuePair<string, PriceLookup>>> LoadGroup(int groupId) => Task.FromResult(new List<KeyValuePair<string, PriceLookup>>());
        public Task<Dictionary<string, double>> LoadCraftCost() => Task.FromResult(new Dictionary<string, double>());
        public Task FlushDueGroups(ConcurrentDictionary<string, PriceLookup> lookups, TimeSpan maxAge, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private static ComplicatedFlip Sample(int i = 0) => new()
    {
        ItemTag = "SPEED_WITHER_BOOTS", SoldFor = 30_000_000 + i * 500_000,
        AttributeValues = new() { ["cleancost"] = 2_600_000, ["dye_item:DYE_CHARCOAL"] = 17_999_000 + i * 100_000 }
    };

    private static async Task<SelfLearningFlipFinderService> Trained(Persistence persistence)
    {
        var service = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, persistence, 8);
        var samples = new List<ComplicatedFlip>();
        for (var i = 0; i < 8; i++) samples.Add(Sample(i));
        await service.TrainBatchAsync(samples);
        return service;
    }

    private static ReaderWriterLockSlim TrainingGate(SelfLearningFlipFinderService service) =>
        (ReaderWriterLockSlim)typeof(SelfLearningFlipFinderService).GetField("gate", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(service);

    // Reflection keeps these tests compilable when overlaid onto the unchanged base.
    private static SelfLearningFlipEstimate Ready(SelfLearningFlipFinderService service, ComplicatedFlip sample) =>
        (SelfLearningFlipEstimate)typeof(SelfLearningFlipFinderService).GetMethod("EstimateReady")?.Invoke(service, new object[] { sample, CancellationToken.None });

    [Test]
    public async Task WarmEstimateDoesNotEnterTrainingGate()
    {
        using var service = await Trained(new Persistence());
        var expected = await service.EstimateAsync(Sample());
        Assert.That(expected.ModelReady, Is.True);
        var gate = TrainingGate(service);
        gate.EnterWriteLock();
        try
        {
            // A read attempt fails deterministically with NoRecursion on the base. This proves
            // the ready path doesn't use the lock, without relying on scheduler timing/timeouts.
            var actual = service.EstimateAsync(Sample()).GetAwaiter().GetResult();
            Assert.That(actual, Is.EqualTo(expected));
        }
        finally { gate.ExitWriteLock(); }
    }

    [Test]
    public void ReadyInferenceWithoutModelDoesNotLoadOrTrain()
    {
        var persistence = new Persistence();
        using var service = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, persistence, 8);
        var gate = TrainingGate(service);
        gate.EnterWriteLock();
        try { Assert.That(Ready(service, Sample()), Is.Null); }
        finally { gate.ExitWriteLock(); }
        Assert.That(persistence.Loads, Is.Zero);
        Assert.That(service.GetSnapshot().SampleCount, Is.Zero);
    }

    [Test]
    public async Task FeatureExpansionAndBlockedPersistenceKeepPublishedModelCoherent()
    {
        var persistence = new Persistence();
        using var service = await Trained(persistence);
        var before = await service.EstimateAsync(Sample());
        var expanded = Sample(8);
        expanded.AttributeValues["new_feature"] = 3_000_000;
        await service.TrainAsync(expanded); // appends a feature; five-minute refit throttle retains old model
        var unchanged = await service.EstimateAsync(Sample());
        Assert.That(unchanged, Is.EqualTo(before), "sample metadata and schema must describe the published model, not pending training data");

        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        persistence.Saving = key =>
        {
            if (key != "selflearning/meta/all") return;
            entered.Set();
            release.Wait();
        };
        var refit = Task.Run(() => service.PersistModelAsync("SPEED_WITHER_BOOTS"));
        SelfLearningFlipEstimate during;
        try
        {
            Assert.That(entered.Wait(TimeSpan.FromSeconds(30)), Is.True, "maintenance reached deliberately blocked persistence");
            Assert.That(refit.IsCompleted, Is.False);
            during = Ready(service, expanded);
            Assert.That(during, Is.Not.Null);
            Assert.That(during.ModelReady, Is.True);
            Assert.That(during.SampleCount, Is.EqualTo(9));
        }
        finally
        {
            release.Set();
            await refit;
        }
        Assert.That(Ready(service, expanded), Is.EqualTo(during));
    }
    [Test]
    public async Task PersistedModelKeepsItsOwnMetadataAndFeatureMap()
    {
        var persistence = new Persistence();
        SelfLearningFlipEstimate expected;
        using (var trained = await Trained(persistence))
            expected = await trained.EstimateAsync(Sample());
        using var service = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, persistence, 8);
        var pending = Sample(9);
        pending.AttributeValues["new_feature"] = 1_000_000;
        await service.TrainAsync(pending);
        Assert.That(Ready(service, Sample()), Is.Null, "ready-only inference never initiates a model load");
        // The compatibility API may explicitly restore a persisted model. The ready path
        // then serves its schema/count/label cap, not those of the one pending sample.
        var restored = await service.EstimateAsync(Sample());
        Assert.That(restored, Is.EqualTo(expected));
        Assert.That(Ready(service, Sample()), Is.EqualTo(expected));
        Assert.That(service.GetSnapshot().SampleCount, Is.EqualTo(1));
    }

}
