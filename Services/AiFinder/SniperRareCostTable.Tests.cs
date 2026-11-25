using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using Coflnet.Sky.AIFinder.Model;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

public class SniperRareCostTableTests
{
    private SniperService sniper = null!;
    private SniperRareCostTable table = null!;

    [SetUp]
    public void Setup()
    {
        sniper = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, null);
        table = new SniperRareCostTable(sniper, NullLogger<SniperRareCostTable>.Instance);
    }

    private static KeyValuePair<AuctionKey, ReferenceAuctions> LevelKeyAt(int level, long price, Tier tier = Tier.COMMON)
        => new(new AuctionKey { Tier = tier, Modifiers = new ReadOnlyCollection<KeyValuePair<string, string>>([new("rune", level.ToString())]) },
            new ReferenceAuctions { Price = price });

    private void SetRune(string tag, params (int level, long price)[] prices)
        => sniper.Lookups[tag] = new PriceLookup { Lookup = new(prices.Select((p, i) => LevelKeyAt(p.level, p.price, (Tier)i))) };

    private void SetBazaarPrice(string tag, int price)
        => sniper.UpdateBazaar(new() { Products = [new() { ProductId = tag, SellSummary = [new() { PricePerUnit = price }], BuySummery = [] }] });

    private double? Refreshed(string feature, float value)
    {
        table.Refresh();
        return table.Cost("ANY_TAG", feature, value);
    }

    [Test]
    public void RuneLevelCostsTheLowestPriceOfThatLevel()
    {
        SetRune("RUNE_LIGHTNING", (3, 9_000_000), (3, 7_000_000), (4, 30_000_000));

        Refreshed("rune/RUNE_LIGHTNING", 3).Should().Be(7_000_000);
        table.Cost("ANY_TAG", "rune/RUNE_LIGHTNING", 4).Should().Be(30_000_000);
    }

    [Test]
    public void UniqueRuneCountsAsTheRuneItIsAppliedAs()
    {
        SetRune("RUNE_LIGHTNING", (2, 5_000_000));
        SetRune("UNIQUE_RUNE_LIGHTNING", (2, 3_000_000), (1, 400_000));

        Refreshed("rune/RUNE_LIGHTNING", 2).Should().Be(3_000_000);
        table.Cost("ANY_TAG", "rune/RUNE_LIGHTNING", 1).Should().Be(400_000);
    }

    [Test]
    public void RuneLevelWithoutAPriceHasNoCost()
    {
        SetRune("RUNE_LIGHTNING", (3, 7_000_000));

        Refreshed("rune/RUNE_LIGHTNING", 2).Should().BeNull();
        table.Cost("ANY_TAG", "rune/RUNE_UNKNOWN", 3).Should().BeNull();
    }

    [Test]
    public void ReforgeCostsTheSniperValueOfItsStone()
    {
        SetRune("BLESSED_FRUIT", (1, 2_000_000));
        var expected = sniper.GetReforgeValue(ItemReferences.Reforge.blessed);
        expected.Should().BeGreaterThan(0);

        Refreshed("reforge/reforge=blessed", 1).Should().Be(expected);
    }

    [Test]
    public void IgnoredReforgeHasNoCost()
        => Refreshed("reforge/reforge=bloodshot", 1).Should().BeNull();

    [Test]
    public void EnchantLevelCostsTheModifierValueOfThatLevel()
    {
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_FATAL_TEMPO_1", 1_000_000);
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_FATAL_TEMPO_3", 5_000_000);

        Refreshed("enchant/ultimate_fatal_tempo", 3).Should().Be(5_000_000);
        table.Cost("OTHER_TAG", "enchant/ultimate_fatal_tempo", 3).Should().Be(5_000_000, "the value does not depend on the item");
    }

    [Test]
    public void UnknownFeaturesHaveNoCost()
    {
        table.Refresh();

        table.Cost("ANY_TAG", "enchant/not_an_enchant", 1).Should().BeNull();
        table.Cost("ANY_TAG", "skin/skin=SKELETON", 1).Should().BeNull();
        table.Cost("ANY_TAG", "attribute/mana_pool", 2).Should().BeNull();
    }

    [Test]
    public void TableIsEmptyBeforeTheFirstRefresh()
        => table.Cost("ANY_TAG", "rune/RUNE_LIGHTNING", 3).Should().BeNull();

    [Test]
    public void CostThatDisappearedIsStillServedFromTheHistory()
    {
        SetRune("RUNE_LIGHTNING", (3, 7_000_000));
        Refreshed("rune/RUNE_LIGHTNING", 3).Should().Be(7_000_000);

        sniper.Lookups.TryRemove("RUNE_LIGHTNING", out _);

        Refreshed("rune/RUNE_LIGHTNING", 3).Should().Be(7_000_000);
    }

    [Test]
    public async Task ReadsDuringARefreshSeeAWholeSnapshot()
    {
        SetRune("RUNE_LIGHTNING", (3, 7_000_000));
        SetRune("RUNE_FIRE", (3, 8_000_000));
        table.Refresh();
        using var stop = new CancellationTokenSource();
        var torn = 0;
        var reader = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                var first = table.Cost("T", "rune/RUNE_LIGHTNING", 3);
                var second = table.Cost("T", "rune/RUNE_FIRE", 3);
                if (first == null || second == null)
                    Interlocked.Increment(ref torn);
            }
        });

        for (var i = 0; i < 200; i++)
            table.Refresh();
        stop.Cancel();
        await reader;

        torn.Should().Be(0, "the table is built aside and swapped in, never filled in place");
    }

    [Test]
    public void ReadDoesNotWaitForARunningRefresh()
    {
        table.Refresh();
        var refreshing = Task.Run(() => { for (var i = 0; i < 50; i++) table.Refresh(); });

        while (!refreshing.IsCompleted)
            table.Cost("T", "reforge/reforge=blessed", 1);

        refreshing.IsCompletedSuccessfully.Should().BeTrue();
    }

    [Test]
    public void ExtrapolatedEnchantLevelHasNoCost()
    {
        SetBazaarPrice("ENCHANTMENT_SMITE_1", 100_000);
        SetBazaarPrice("ENCHANTMENT_SMITE_5", 900_000);

        Refreshed("enchant/smite", 1).Should().Be(100_000);
        table.Cost("T", "enchant/smite", 5).Should().Be(900_000);
        table.Cost("T", "enchant/smite", 6).Should().BeNull("level 6 has no price of its own, EnchantValue would only extrapolate it");
    }

    [Test]
    public void ReforgeWithoutAPricedStoneHasNoCost()
        => Refreshed("reforge/reforge=blessed", 1).Should().BeNull("only the apply cost is known, not the stone");

    [Test]
    public void HasNoSnapshotUntilARuneOrReforgeCostIsKnown()
    {
        SetBazaarPrice("ENCHANTMENT_SMITE_1", 100_000);
        table.Refresh();
        table.HasSnapshot.Should().BeFalse("bazaar prices alone do not make the table usable");

        SetRune("RUNE_LIGHTNING", (3, 7_000_000));
        table.Refresh();

        table.HasSnapshot.Should().BeTrue();
    }

    [Test]
    public async Task BuildIsRetriedUntilTheLookupsAreLoaded()
    {
        var fast = new SniperRareCostTable(sniper, NullLogger<SniperRareCostTable>.Instance, TimeSpan.FromHours(1), TimeSpan.FromMilliseconds(10));
        await fast.StartAsync(CancellationToken.None);
        try
        {
            await Task.Delay(100);
            fast.HasSnapshot.Should().BeFalse();
            SetRune("RUNE_LIGHTNING", (3, 7_000_000));
            await WaitFor(() => fast.HasSnapshot);
            fast.Cost("T", "rune/RUNE_LIGHTNING", 3).Should().Be(7_000_000);
        }
        finally
        {
            await fast.StopAsync(CancellationToken.None);
        }
    }

    private static async Task WaitFor(Func<bool> condition)
    {
        for (var i = 0; i < 200 && !condition(); i++)
            await Task.Delay(25);
    }

    [Test]
    public void FailedBuildIsRetriedOnce()
    {
        var calls = 0;
        var flaky = new SniperRareCostTable(sniper, NullLogger<SniperRareCostTable>.Instance, TimeSpan.FromHours(1), TimeSpan.FromHours(1),
            _ => ++calls == 1 ? throw new InvalidOperationException("written while read") : new() { ["rune/RUNE_X=1"] = 5 });

        flaky.Refresh();

        calls.Should().Be(2);
        flaky.Cost("T", "rune/RUNE_X", 1).Should().Be(5);
    }

    [Test]
    public void BuildThatFailsTwiceKeepsThePreviousSnapshot()
    {
        var calls = 0;
        var broken = new SniperRareCostTable(sniper, NullLogger<SniperRareCostTable>.Instance, TimeSpan.FromHours(1), TimeSpan.FromHours(1),
            _ => ++calls == 1 ? new() { ["rune/RUNE_X=1"] = 5 } : throw new InvalidOperationException("written while read"));

        broken.Refresh();
        broken.Refresh();

        broken.Cost("T", "rune/RUNE_X", 1).Should().Be(5);
    }

    [Test]
    public void OneOddReadingDoesNotMoveTheServedCost()
    {
        var history = new CostHistory(12);
        history.Add(new() { ["rune/RUNE_X=1"] = 100 });
        history.Add(new() { ["rune/RUNE_X=1"] = 100 });

        var served = history.Add(new() { ["rune/RUNE_X=1"] = 1 });

        served["rune/RUNE_X=1"].Should().Be(100);
    }

    [Test]
    public void CostMissingFromOneReadingIsStillServed()
    {
        var history = new CostHistory(12);
        history.Add(new() { ["rune/RUNE_X=1"] = 100 });

        var served = history.Add(new() { ["rune/RUNE_Y=1"] = 7 });

        served.Should().ContainKey("rune/RUNE_X=1").WhoseValue.Should().Be(100);
    }

    [Test]
    public void CostMissingFromTheWholeWindowIsDropped()
    {
        var history = new CostHistory(3);
        history.Add(new() { ["rune/RUNE_X=1"] = 100 });
        IReadOnlyDictionary<string, double> served = new Dictionary<string, double>();
        for (var i = 0; i < 3; i++)
            served = history.Add(new() { ["rune/RUNE_Y=1"] = 7 });

        served.Should().NotContainKey("rune/RUNE_X=1");
    }

    [Test]
    public void EvenNumberOfReadingsServesTheMiddleAverage()
    {
        var history = new CostHistory(12);
        history.Add(new() { ["k"] = 100 });

        history.Add(new() { ["k"] = 200 })["k"].Should().Be(150);
    }
}
