using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Sniper.Services;
using dev;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Controllers;

/// <summary>
/// The list price suggestion uses a non-zero self-learning value instead of the median, so it is only set where the AI is the
/// better source: a ready model and an item carrying more than one value modifier
/// </summary>
public class SniperControllerPriceTests
{
    private const double AiValue = 950_000_000;
    private SniperService service = null!;
    private RecordingAiFinder finder = null!;

    [SetUp]
    public void Setup()
    {
        service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null,
            NullLogger<SniperService>.Instance, new SniperServiceTests.CraftCostMock());
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_LEGION_1", 1_800_000);
        finder = new RecordingAiFinder("HYPERION");
    }

    [Test]
    public async Task ReadyModelAndSeveralModifiersSetTheValue()
    {
        finder.Estimated = Estimate(ready: true);

        (await SelfLearningValue(TwoModifiers())).Should().Be(AiValue);
    }

    [Test]
    public async Task ModelThatIsNotReadyLeavesItAtZero()
    {
        finder.Estimated = Estimate(ready: false);

        (await SelfLearningValue(TwoModifiers())).Should().Be(0);
    }

    [Test]
    public async Task SingleModifierLeavesItAtZero()
    {
        finder.Estimated = Estimate(ready: true);

        (await SelfLearningValue(OneModifier())).Should().Be(0);
    }

    [Test]
    public async Task CleanItemLeavesItAtZero()
    {
        finder.Estimated = Estimate(ready: true);
        var clean = new ApiSaveAuction() { Tag = "HYPERION", Uuid = Guid.NewGuid().ToString("N"), FlatenedNBT = new() };

        (await SelfLearningValue(clean)).Should().Be(0);
    }

    /// <summary>Regression: the median key of this item keeps one modifier, the item itself carries many</summary>
    [Test]
    public async Task ManyModifiersOnTheItemSetTheValue()
    {
        finder.Estimated = Estimate(ready: true);
        var terminator = new ApiSaveAuction()
        {
            Tag = "TERMINATOR",
            Uuid = Guid.NewGuid().ToString("N"),
            Tier = Tier.MYTHIC,
            Reforge = ItemReferences.Reforge.Precise,
            Enchantments = SniperServiceTests.TerminatorEnchants(),
            FlatenedNBT = new() { { "rarity_upgrades", "1" } },
        };

        (await SelfLearningValue(terminator)).Should().Be(AiValue);
    }

    [TestCase(0)]
    [TestCase(-5)]
    [TestCase(double.NaN)]
    public async Task NonPositiveEstimateLeavesItAtZero(double value)
    {
        finder.Estimated = Estimate(ready: true, value);

        (await SelfLearningValue(TwoModifiers())).Should().Be(0);
    }

    [Test]
    public async Task NoEstimateLeavesItAtZero()
    {
        finder.Estimated = null!;

        (await SelfLearningValue(TwoModifiers())).Should().Be(0);
    }

    [Test]
    public async Task NotAskedForLeavesItAtZero()
    {
        finder.Estimated = Estimate(ready: true);

        var prices = await Controller().GetPrices(new List<ApiSaveAuction> { TwoModifiers() }, includeSelfLearning: false);

        prices.Should().ContainSingle().Which.SelfLearningEstimatedValue.Should().Be(0);
    }

    private async Task<double> SelfLearningValue(ApiSaveAuction auction)
    {
        var prices = await Controller().GetPrices(new List<ApiSaveAuction> { auction }, includeSelfLearning: true);
        return prices.Should().ContainSingle().Subject.SelfLearningEstimatedValue;
    }

    private SniperController Controller()
        => new(NullLogger<SniperController>.Instance, service, null, null, null, null, null, finder, null);

    private static AiEstimate Estimate(bool ready, double value = AiValue) => new(value, ready, 500, null, new object());

    /// <summary>One enchant and a reforge</summary>
    private static ApiSaveAuction TwoModifiers() => Hyperion(ItemReferences.Reforge.Fabled);

    /// <summary>One enchant and no reforge</summary>
    private static ApiSaveAuction OneModifier() => Hyperion(ItemReferences.Reforge.None);

    private static ApiSaveAuction Hyperion(ItemReferences.Reforge reforge) => new()
    {
        Tag = "HYPERION",
        Uuid = Guid.NewGuid().ToString("N"),
        Enchantments = new List<Enchantment>() { new(Enchantment.EnchantmentType.ultimate_legion, 1) },
        FlatenedNBT = new(),
        Reforge = reforge,
    };

    private void SetBazaarPrice(string tag, int value) => service.UpdateBazaar(new()
    {
        Products = new()
        {
            new()
            {
                ProductId = tag,
                SellSummary = new List<SellOrder>() { new() { PricePerUnit = value } },
                BuySummery = new List<BuyOrder>()
            }
        }
    });
}
