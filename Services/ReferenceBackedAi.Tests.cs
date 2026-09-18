using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using ComplicatedFlip = Coflnet.Sky.FlipTracker.Client.Model.ComplicatedFlip;
using Coflnet.Sky.Sniper.Models;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Sniper.Services.Tests;

[NonParallelizable]
public class ReferenceBackedAiTests
{
    private SniperService sniper;
    private readonly List<LowPricedAuction> found = new();
    private int previousMinimum;

    private sealed class CraftCost : ICraftCostService
    {
        public Dictionary<string, double> Costs { get; } = new();
        public ConcurrentDictionary<string, Category> ItemCategories { get; } = new();
        public void AddCostForSpecialItems() { }
        public bool TryGetCost(string tag, out double cost) { cost = 2_600_000; return tag == "SPEED_WITHER_BOOTS"; }
    }

    [SetUp]
    public void Setup()
    {
        previousMinimum = SniperService.MIN_TARGET;
        SniperService.MIN_TARGET = 0;
        sniper = new(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null,
            NullLogger<SniperService>.Instance, new CraftCost()) { State = SniperState.FullyLoaded };
        found.Clear();
        sniper.FoundSnipe += found.Add;
        foreach (var (tag, price) in new[] { ("DYE_CHARCOAL", 17_999_000L), ("RECOMBOBULATOR_3000", 9_989_376L),
            ("ENCHANTMENT_ULTIMATE_LEGION_3", 8_518_142L), ("ENCHANTMENT_GROWTH_6", 2_341_986L) })
        {
            ((Dictionary<string, double>)typeof(SniperService).GetField("BazaarPrices", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(sniper))[tag] = price;
            sniper.Lookups[tag] = new() { Lookup = new(new[] { new KeyValuePair<AuctionKey, ReferenceAuctions>(
                new(new(), ItemReferences.Reforge.Any, new(), Tier.COMMON, 1), new() { Price = price }) }) };
        }
    }

    [TearDown]
    public void Cleanup() => SniperService.MIN_TARGET = previousMinimum;

    // Pricing-relevant subject fields; identities below are synthetic test identifiers.
    private static SaveAuction Subject() => new()
    {
        Uuid = "00000000000000000000000000000001", UId = 1,
        AuctioneerId = "11111111111111111111111111111111", Tag = "SPEED_WITHER_BOOTS",
        Bin = true, Count = 1, StartingBid = 27_000_000, Tier = Tier.MYTHIC,
        Reforge = ItemReferences.Reforge.ancient, Start = DateTime.UtcNow, End = DateTime.UtcNow.AddDays(14),
        FlatenedNBT = new() { ["rarity_upgrades"] = "1", ["is_shiny"] = "1", ["hpc"] = "10",
            ["upgrade_level"] = "5", ["dye_item"] = "DYE_CHARCOAL", ["color"] = "33:38:42", ["cc"] = "1" },
        Enchantments = new() { new(EnchantmentType.ultimate_legion, 3), new(EnchantmentType.growth, 6),
            new(EnchantmentType.protection, 6), new(EnchantmentType.sugar_rush, 3), new(EnchantmentType.rejuvenate, 5),
            new(EnchantmentType.feather_falling, 10), new(EnchantmentType.depth_strider, 3) }
    };

    [TestCase(0L, 0L, false)]
    [TestCase(1L, 0L, false)]
    [TestCase(9L, 0L, true)]
    [TestCase(0L, 10L, true)]
    [TestCase(9L, 10L, true)]
    public void SparseSniperRequiresAnIndependentListingAndReportsTheListingUsed(long ownId, long higherId, bool expected)
    {
        var key = new AuctionKey(new() { new() { Type = EnchantmentType.ultimate_legion, Lvl = 3 } }, ItemReferences.Reforge.Any,
            new() { new("rarity_upgrades", "1"), new("upgrade_level", "5"), new("dye_item", "DYE_CHARCOAL") }, Tier.MYTHIC, 1);
        var breakdown = new KeyWithValueBreakdown { Key = key, ValueBreakdown = new()
        {
            new(new KeyValuePair<string, string>("dye_item", "DYE_CHARCOAL"), 17_999_000),
            new(new KeyValuePair<string, string>("rarity_upgrades", "1"), 9_989_376),
            new(new Enchant { Type = EnchantmentType.ultimate_legion, Lvl = 3 }, 8_518_142),
            new(new KeyValuePair<string, string>("upgrade_level", "5"), 2_907_600),
            new(new Enchant { Type = EnchantmentType.growth, Lvl = 6 }, 2_341_986)
        } };
        // Controlled sparse state: median and deduplicated count are zero, with four raw sales.
        // This tests the reported failure mechanism, not a historical state replay.
        // Price without identity models the unsupported synthesized LBIN; borrowed sales must not legitimize it.
        var bucket = new ReferenceAuctions { Lbins = new() { new() { AuctionId = ownId, Price = 99_323_338 } } };
        for (var i = 0; i < 4; i++) bucket.EnqueueReference(new() { AuctionId = 20 + i, Price = 79_266_116, Day = SniperService.GetDay() });
        var betterKey = new AuctionKey(key) { Modifiers = new(new List<KeyValuePair<string, string>>() { new("rarity_upgrades", "1"),
            new("upgrade_level", "7"), new("dye_item", "DYE_CHARCOAL") }) };
        var betterBucket = new ReferenceAuctions();
        if (higherId != 0) betterBucket.Lbins.Add(new() { AuctionId = higherId, Price = 70_000_000 });
        for (var i = 0; i < 8; i++) betterBucket.EnqueueReference(new() { AuctionId = 30 + i, Price = 79_266_116, Day = SniperService.GetDay() });
        var lookup = new PriceLookup { CleanPricePerTier = new() { [Tier.LEGENDARY] = 2_600_000 } };
        lookup.Lookup[key] = bucket;
        lookup.Lookup[betterKey] = betterBucket;
        var sixStarKey = new AuctionKey(key) { Modifiers = new(new List<KeyValuePair<string, string>>() {
            new("rarity_upgrades", "1"), new("upgrade_level", "6"), new("dye_item", "DYE_CHARCOAL") }) };
        var sixStarBucket = new ReferenceAuctions();
        for (var i = 0; i < 8; i++) sixStarBucket.EnqueueReference(new() { AuctionId = 40 + i, Price = 79_266_116, Day = SniperService.GetDay() });
        lookup.Lookup[sixStarKey] = sixStarBucket;
        sniper.Lookups["SPEED_WITHER_BOOTS"] = lookup;

        Invoke(sniper, "PotentialSnipe", Subject(), ("SPEED_WITHER_BOOTS", 0L), 27_810_000d,
            bucket, key, lookup, 0L, breakdown);
        Assert.That(found.Any(f => f.Finder == LowPricedAuction.FinderType.SNIPER), Is.EqualTo(expected));
        if (expected)
        {
            Assert.That(found.Single().AdditionalProps["reference"], Is.EqualTo(AuctionService.Instance.GetUuid(higherId != 0 ? higherId : ownId)));
            if (higherId == 0) Assert.That(found.Single().TargetPrice, Is.EqualTo(50_958_299));
        }
    }

    [Test]
    public void IngestionUsesReadyPredictionWithoutCallingTheLoadingOrTrainingPath()
    {
        var finder = new Finder { ThrowOnLegacy = true };
        var loader = Loader(finder);
        Assert.DoesNotThrow(() => Invoke(loader, "CheckForPartial", Subject()));
        Assert.That(finder.ReadyCalls, Is.EqualTo(1));
        Assert.That(finder.LegacyCalls, Is.Zero);
        Assert.That(found.Single().Finder, Is.EqualTo(LowPricedAuction.FinderType.AI));
        Assert.That(found.Single().TargetPrice, Is.EqualTo((long)(31_893_528 * .9)));
        Assert.That(found.Single().AdditionalProps["server"], Is.EqualTo(sniper.ServerDnsName));
    }

    [Test]
    public void NoProduceAlsoDisablesAi()
    {
        var finder = new Finder();
        Invoke(Loader(finder, true), "CheckForPartial", Subject());
        Assert.That(found, Is.Empty);
        Assert.That(finder.ReadyCalls + finder.LegacyCalls, Is.Zero);
    }

    [TestCase(false, 31_893_528d)]
    [TestCase(true, double.PositiveInfinity)]
    [TestCase(true, double.NaN)]
    [TestCase(true, -1d)]
    public void UntrainedOrInvalidPredictionDoesNotBecomeAnAiFlip(bool ready, double price)
    {
        Invoke(Loader(new Finder { Result = new(price, 2_600_000, ready, 1000, null) }), "CheckForPartial", Subject());
        Assert.That(found, Is.Empty);
    }

    [Test]
    public void MissingReadyModelAbstainsWithoutLoading()
    {
        var finder = new Finder { Result = null, ThrowOnLegacy = true };
        Assert.DoesNotThrow(() => Invoke(Loader(finder), "CheckForPartial", Subject()));
        Assert.That(found, Is.Empty);
        Assert.That(finder.LegacyCalls, Is.Zero);
    }

    private InternalDataLoader Loader(Finder finder, bool noProduce = false)
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string>
            { ["NO_PRODUCE"] = noProduce.ToString(), ["TOPICS:LOW_PRICED"] = "test-flips" }).Build();
        var loader = new InternalDataLoader(sniper, config, null, NullLogger<InternalDataLoader>.Instance,
            null, null, null, null, null, null, finder, new CraftCost());
        var producer = DispatchProxy.Create<IProducer<string, LowPricedAuction>, Producer>();
        ((Producer)(object)producer).Produced = found.Add;
        typeof(InternalDataLoader).GetField("FlipProducer", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(loader, producer);
        return loader;
    }

    private static void Invoke(object target, string name, params object[] args) => target.GetType()
        .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance).Invoke(target, args);

    public class Producer : DispatchProxy
    {
        public Action<LowPricedAuction> Produced;
        protected override object Invoke(MethodInfo method, object[] args)
        {
            if (method.Name == "Produce") Produced(((Message<string, LowPricedAuction>)args[1]).Value);
            return null;
        }
    }

    private sealed class Finder : ISelfLearningFlipFinderService
    {
        public SelfLearningFlipEstimate Result = new(31_893_528, 2_600_000, true, 1000, new(18_323_976, .5));
        public bool ThrowOnLegacy;
        public int ReadyCalls, LegacyCalls;
        public SelfLearningFlipEstimate EstimateReady(ComplicatedFlip flip, CancellationToken ct = default) { ReadyCalls++; return Result; }
        public Task<SelfLearningFlipEstimate> EstimateAsync(ComplicatedFlip flip, CancellationToken ct = default)
        {
            LegacyCalls++;
            if (ThrowOnLegacy) throw new InvalidOperationException("Loading/training must not run on auction ingestion.");
            return Task.FromResult(Result);
        }
        public bool IsRelevantItem(string tag) => true;
        public Task TrainAsync(ComplicatedFlip flip, CancellationToken ct = default) => Task.CompletedTask;
        public Task TrainBatchAsync(IEnumerable<ComplicatedFlip> flips, CancellationToken ct = default) => Task.CompletedTask;
        public Task PersistModelAsync(string tag = null) => Task.CompletedTask;
        public SelfLearningFlipModelSnapshot GetSnapshot() => new(Array.Empty<string>(), 0, null);
        public IReadOnlyDictionary<string, SelfLearningFlipFinderService.ModelStats> GetModelStats() => new Dictionary<string, SelfLearningFlipFinderService.ModelStats>();
    }
}
