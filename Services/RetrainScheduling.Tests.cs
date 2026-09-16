using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.FlipTracker.Client.Model;
using LowPricedAuction = Coflnet.Sky.Core.LowPricedAuction;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using StackExchange.Redis;

namespace Coflnet.Sky.Sniper.Services.Tests;

public class RetrainSchedulingTests
{
    private sealed class Finder : ISelfLearningFlipFinderService
    {
        public bool IsRelevantItem(string tag) => tag == "SPEED_WITHER_BOOTS";
        public IReadOnlyDictionary<string, SelfLearningFlipFinderService.ModelStats> GetModelStats() =>
            throw new InvalidOperationException("Scheduling must not acquire training state through GetModelStats.");
        public SelfLearningFlipModelSnapshot GetSnapshot() => throw new NotSupportedException();
        public Task TrainAsync(ComplicatedFlip flip, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task TrainBatchAsync(IEnumerable<ComplicatedFlip> flips, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<SelfLearningFlipEstimate> EstimateAsync(ComplicatedFlip flip, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task PersistModelAsync(string tag = null) => throw new NotSupportedException();
    }

    public class RedisCalls : DispatchProxy
    {
        public IDatabase Database;
        public readonly List<string> Tags = new();
        protected override object Invoke(MethodInfo method, object[] args)
        {
            if (method.Name == "GetDatabase") return Database;
            if (method.Name == "StreamAdd")
            {
                Assert.That(args[0].ToString(), Is.EqualTo("retrain"));
                var entries = (NameValueEntry[])args[1];
                Assert.That(entries.Length, Is.EqualTo(1));
                Assert.That(entries[0].Name.ToString(), Is.EqualTo("tag"));
                Tags.Add(entries[0].Value.ToString());
                return (RedisValue)"1-0";
            }
            throw new NotSupportedException(method.Name);
        }
    }

    private static (RetrainService Service, InternalDataLoader Loader, RedisCalls Calls) Create()
    {
        var finder = new Finder();
        var sniper = new SniperService(null, null, NullLogger<SniperService>.Instance, new SniperServiceTests.CraftCostMock());
        var config = new ConfigurationBuilder().Build();
        var partial = new PartialCalcService(sniper, null, null, null, NullLogger<PartialCalcService>.Instance, null, null, finder);
        var loader = new InternalDataLoader(sniper, config, null, NullLogger<InternalDataLoader>.Instance,
            null, null, null, partial, null, null, finder, null);
        var database = DispatchProxy.Create<IDatabase, RedisCalls>();
        var connection = DispatchProxy.Create<IConnectionMultiplexer, RedisCalls>();
        ((RedisCalls)(object)connection).Database = database;
        var service = new RetrainService(partial, loader, sniper, connection, NullLogger<RetrainService>.Instance, config);
        return (service, loader, (RedisCalls)(object)database);
    }

    [TestCase("SPEED_WITHER_BOOTS", true)]
    [TestCase("UNSUPPORTED_TEST_ITEM", false)]
    public void SchedulingUsesRelevanceWithoutReadingTrainingStats(string tag, bool expected)
    {
        var (service, loader, calls) = Create();
        using (service)
        using (loader)
        {
            service.SheduleRetrain(tag);
            Assert.That(calls.Tags, Is.EqualTo(expected ? new[] { tag } : Array.Empty<string>()));
        }
    }

    [Test]
    public void RegisteredAiFlipCallbackQueuesWithoutReadingTrainingStats()
    {
        var (service, loader, calls) = Create();
        using (service)
        using (loader)
        {
            // Invoke the real handler registered by RetrainService, without Kafka or a model.
            var callback = (Action<LowPricedAuction>)typeof(InternalDataLoader)
                .GetField("FoundPartialFlip", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(loader);
            callback(new LowPricedAuction
            {
                Auction = new() { Tag = "SPEED_WITHER_BOOTS", StartingBid = 1_000_000 },
                TargetPrice = 3_000_000
            });
            Assert.That(calls.Tags, Is.EqualTo(new[] { "SPEED_WITHER_BOOTS" }));
        }
    }
}
