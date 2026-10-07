using System;
using System.Collections.Generic;
using System.Linq;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>
/// The AI finder only knows what <see cref="SaveAuctionExtensions.ToComplicatedFlip"/> tells it.
/// Fixtures are sold auctions of 2026-10-02 without identifiers, priced with the bazaar and lowest bin of that day.
/// </summary>
public class SaveAuctionExtensionsTests
{
    private class CraftCost : ICraftCostService
    {
        public Dictionary<string, double> Costs { get; } = new();
        public System.Collections.Concurrent.ConcurrentDictionary<string, Category> ItemCategories { get; } = new();
        public void AddCostForSpecialItems() { }
        public bool TryGetCost(string itemId, out double cost) => Costs.TryGetValue(itemId, out cost);
    }

    private static SniperService CreateService(Dictionary<string, long> bazaar, Dictionary<string, long> lowestBins = null)
    {
        var itemService = new HypixelItemService(null, NullLogger<HypixelItemService>.Instance);
        itemService.GetItemsAsync().Wait(); // star and gemstone slot costs
        var service = new SniperService(itemService, null, NullLogger<SniperService>.Instance, null);
        service.UpdateBazaar(new()
        {
            Timestamp = new DateTime(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc),
            Products = bazaar.Select(p => new dev.ProductInfo
            {
                ProductId = p.Key,
                SellSummary = new() { new() { PricePerUnit = p.Value, Amount = 10 } },
                BuySummery = new()
            }).ToList()
        });
        foreach (var item in lowestBins ?? [])
        {
            var lookup = new PriceLookup();
            lookup.Lookup[new AuctionKey()] = new() { Price = item.Value };
            service.Lookups[item.Key] = lookup;
        }
        return service;
    }

    private static SaveAuction Sold(string tag, long price, string enchants, Dictionary<string, string> nbt) => new()
    {
        Tag = tag,
        Tier = Tier.MYTHIC,
        Count = 1,
        Bin = true,
        StartingBid = price,
        HighestBidAmount = price,
        End = new DateTime(2026, 10, 2, 11, 49, 5, DateTimeKind.Utc),
        ItemCreatedAt = new DateTime(2026, 1, 1),
        FlatenedNBT = nbt,
        Enchantments = enchants.Split(',').Select(e => e.Split(':'))
            .Select(e => new Enchantment(Enum.Parse<Enchantment.EnchantmentType>(e[0]), byte.Parse(e[1]))).ToList()
    };

    /// <summary>
    /// The sum <see cref="InternalDataLoader"/> caps the model estimate with
    /// </summary>
    private static long Cap(Dictionary<string, long> attributes)
        => attributes.Where(a => !a.Key.StartsWith("candyUsed:")).Sum(a => a.Value);

    [Test]
    public void HyperionKeepsModifiersBeyondTheFiveInTheLookupKey()
    {
        var service = CreateService(new()
        {
            ["IMPLOSION_SCROLL"] = 178_409_181, ["SHADOW_WARP_SCROLL"] = 172_003_945, ["WITHER_SHIELD_SCROLL"] = 187_666_959,
            ["RECOMBOBULATOR_3000"] = 10_661_112, ["FUMING_POTATO_BOOK"] = 1_718_698, ["HOT_POTATO_BOOK"] = 79_966,
            ["THE_ART_OF_WAR"] = 12_544_591, ["PERFECT_SAPPHIRE_GEM"] = 13_858_060, ["ESSENCE_WITHER"] = 2_450,
            ["FIRST_MASTER_STAR"] = 14_536_952, ["SECOND_MASTER_STAR"] = 18_222_241, ["THIRD_MASTER_STAR"] = 27_642_645,
            ["FOURTH_MASTER_STAR"] = 68_119_231, ["ENCHANTMENT_TRIPLE_STRIKE_5"] = 27_915_998, ["ENCHANTMENT_LUCK_7"] = 26_776_492,
            ["ENCHANTMENT_CLEAVE_6"] = 17_870_987, ["ENCHANTMENT_EXPERIENCE_5"] = 16_727_496, ["ENCHANTMENT_TABASCO_3"] = 10_891_666,
            ["ENCHANTMENT_TITAN_KILLER_7"] = 8_663_243, ["ENCHANTMENT_SMOLDERING_1"] = 5_504_742,
            ["ENCHANTMENT_THUNDERLORD_7"] = 5_443_273, ["ENCHANTMENT_DRAGON_HUNTER_6"] = 4_458_776,
            ["ENCHANTMENT_VAMPIRISM_6"] = 1_175_135,
        });
        service.Lookups["HYPERION"] = new() { CleanPricePerTier = new() { [Tier.MYTHIC] = 472_770_766, [Tier.LEGENDARY] = 477_000_000 } };
        var auction = Sold("HYPERION", 1_499_999_999,
            "venomous:7,ultimate_wise:5,execute:5,dragon_hunter:6,titan_killer:7,experience:5,bane_of_arthropods:7,triple_strike:5,"
            + "impaling:5,lethality:6,fire_aspect:3,magmarizer:5,mana_steal:3,luck:7,looting:4,cleave:6,smoldering:1,scavenger:5,"
            + "knockback:2,critical:6,thunderlord:7,sharpness:6,cubism:5,champion:10,vampirism:6,smite:7,ender_slayer:6,tabasco:3",
            new()
            {
                ["rarity_upgrades"] = "1", ["art_of_war_count"] = "1", ["upgrade_level"] = "9", ["hpc"] = "15",
                ["power_ability_scroll"] = "SAPPHIRE_POWER_SCROLL",
                ["ability_scroll"] = "IMPLOSION_SCROLL SHADOW_WARP_SCROLL WITHER_SHIELD_SCROLL",
                ["COMBAT_0"] = "PERFECT", ["COMBAT_0_gem"] = "SAPPHIRE", ["SAPPHIRE_0"] = "PERFECT",
                ["unlocked_slots"] = "COMBAT_0,SAPPHIRE_0", ["champion_combat_xp"] = "4668746.685500018", ["RUNE_SPIRIT"] = "3"
            });

        var features = auction.ToComplicatedFlip(includeBreakdown: true, sniper: service,
            craftCostService: new CraftCost { Costs = { ["HYPERION"] = 479_000_000 } }).AttributeValues;

        features.Should().Contain("rarity_upgrades:1", 10_661_112, "a recombobulator is not one of the five most valuable modifiers");
        features.Should().Contain("art_of_war_count:1", 12_544_591);
        features.Should().Contain("cleave:6", 17_870_987);
        features.Should().Contain("gems", 2 * (13_858_060 - 500_000), "perfect gems are taken out of the lookup key");
        features.Keys.Should().NotContain("scavenger:5", "scavenger does nothing on dungeon items");
        Cap(features).Should().BeGreaterThan(1_300_000_000,
            "the clean item and the five modifiers of the lookup key only make 1.23B of the 1.5B it sold for");
    }

    [Test]
    public void EnchantWithoutEffectDoesNotHideTheOtherEnchants()
    {
        var service = CreateService(new()
        {
            // Scavenger 6 is made with a Golden Bounty, which keeps it in the lookup key until it is dropped as useless
            ["GOLDEN_BOUNTY"] = 42_191_940, ["ENDSTONE_IDOL"] = 47_009_881, ["ENCHANTMENT_EXECUTE_6"] = 29_050_000,
            ["ENCHANTMENT_MAGMARIZER_6"] = 28_871_976, ["ENCHANTMENT_TRIPLE_STRIKE_5"] = 27_915_998,
            ["ENCHANTMENT_LUCK_7"] = 26_776_492, ["ENCHANTMENT_CLEAVE_6"] = 17_870_987, ["ENCHANTMENT_EXPERIENCE_5"] = 16_727_496,
            ["ENCHANTMENT_TABASCO_3"] = 10_891_666, ["RECOMBOBULATOR_3000"] = 10_661_112, ["THE_ART_OF_WAR"] = 12_544_591,
            ["FUMING_POTATO_BOOK"] = 1_718_698, ["HOT_POTATO_BOOK"] = 79_966, ["PERFECT_SAPPHIRE_GEM"] = 13_858_060,
            ["ESSENCE_WITHER"] = 2_450, ["FIRST_MASTER_STAR"] = 14_536_952, ["SECOND_MASTER_STAR"] = 18_222_241,
        });
        service.Lookups["HYPERION"] = new() { CleanPricePerTier = new() { [Tier.MYTHIC] = 472_770_766, [Tier.LEGENDARY] = 477_000_000 } };
        var auction = Sold("HYPERION", 1_250_000_000,
            "lethality:6,bane_of_arthropods:7,triple_strike:5,mana_steal:3,luck:7,fire_aspect:3,magmarizer:6,smoldering:1,scavenger:6,"
            + "looting:4,cleave:6,champion:1,thunderlord:7,knockback:2,critical:6,tabasco:3,vampirism:6,sharpness:6,ender_slayer:7,"
            + "execute:6,smite:7,experience:5,venomous:7,ultimate_wise:5,impaling:5,dragon_hunter:6,titan_killer:7",
            new()
            {
                ["rarity_upgrades"] = "1", ["hpc"] = "15", ["art_of_war_count"] = "1", ["upgrade_level"] = "7",
                ["COMBAT_0"] = "PERFECT", ["COMBAT_0_gem"] = "SAPPHIRE", ["SAPPHIRE_0"] = "PERFECT",
                ["unlocked_slots"] = "COMBAT_0,SAPPHIRE_0", ["champion_combat_xp"] = "146.48999999999998", ["RUNE_LIGHTNING"] = "3"
            });

        var features = auction.ToComplicatedFlip(includeBreakdown: true, sniper: service).AttributeValues;

        features.Keys.Should().NotContain("scavenger:6", "scavenger does nothing on dungeon items");
        // both are among the five most valuable modifiers but too cheap to stay in the lookup key
        features.Should().Contain("execute:6", 29_050_000);
        features.Should().Contain("magmarizer:6", 28_871_976);
    }

    [Test]
    public void DrillPartsAreFeatures()
    {
        var service = CreateService(new()
        {
            ["RECOMBOBULATOR_3000"] = 10_661_112, ["DIVAN_POWDER_COATING"] = 63_399_230, ["POLARVOID_BOOK"] = 2_732_999,
            ["SIL_EX"] = 2_891_673, ["ENCHANTMENT_COMPACT_1"] = 4_240_840, ["ENCHANTMENT_LAPIDARY_5"] = 4_666_743,
            ["PERFECT_JADE_GEM"] = 13_460_578, ["PERFECT_TOPAZ_GEM"] = 14_432_197, ["PERFECT_AMBER_GEM"] = 14_507_088,
        }, new()
        {
            ["AMBER_POLISHED_DRILL_ENGINE"] = 227_315_841, ["PERFECTLY_CUT_FUEL_TANK"] = 85_191_243,
            ["GOBLIN_OMELETTE_SUNNY_SIDE"] = 4_613_777,
        });
        var auction = Sold("TITANIUM_DRILL_4", 830_000_000,
            "smelting_touch:1,efficiency:10,fortune:4,compact:7,paleontologist:5,prismatic:5,ultimate_flowstate:3,lapidary:5,experience:4",
            new()
            {
                ["rarity_upgrades"] = "1", ["polarvoid"] = "5", ["drill_fuel"] = "96497", ["compact_blocks"] = "71760",
                ["divan_powder_coating"] = "1", ["JADE_0"] = "PERFECT", ["MINING_0"] = "PERFECT", ["MINING_0_gem"] = "TOPAZ",
                ["AMBER_0"] = "PERFECT", ["unlocked_slots"] = "JADE_0,MINING_0",
                ["drill_part_engine"] = "amber_polished_drill_engine", ["engine.id"] = "AMBER_POLISHED_DRILL_ENGINE",
                ["drill_part_fuel_tank"] = "perfectly_cut_fuel_tank", ["fuel_tank.id"] = "PERFECTLY_CUT_FUEL_TANK",
                ["drill_part_upgrade_module"] = "goblin_omelette_sunny_side", ["upgrade_module.id"] = "GOBLIN_OMELETTE_SUNNY_SIDE"
            });

        var features = auction.ToComplicatedFlip(includeBreakdown: true, sniper: service).AttributeValues;

        // what the sniper adds back for a part: its price after auction house tax and the removal fee
        features.Should().Contain("drill_part_engine:AMBER_POLISHED_DRILL_ENGINE", 227_315_841L * 97 / 100 - 50_000);
        features.Should().Contain("drill_part_fuel_tank:PERFECTLY_CUT_FUEL_TANK", 85_191_243L * 97 / 100 - 50_000);
        features.Should().Contain("drill_part_upgrade_module:GOBLIN_OMELETTE_SUNNY_SIDE", 4_613_777L * 97 / 100 - 50_000);
        features.Should().Contain("gems", 13_460_578 + 14_432_197 + 14_507_088 - 3 * 500_000);
    }

    [Test]
    public void FivePerfectGemsCountWithTheirPriceInsteadOfTheRankingWeight()
    {
        var gemPrices = new Dictionary<string, long>() { ["PERFECT_JADE_GEM"] = 13_460_578, ["PERFECT_AMBER_GEM"] = 14_507_088, ["PERFECT_TOPAZ_GEM"] = 14_432_197 };
        var service = CreateService(new(gemPrices) { ["RECOMBOBULATOR_3000"] = 10_661_112 });
        var auction = Sold("DIVAN_CHESTPLATE", 159_000_000, "growth:5,protection:5,ultimate_wisdom:5", new()
        {
            ["rarity_upgrades"] = "1", ["unlocked_slots"] = "AMBER_0,AMBER_1,JADE_0,JADE_1,TOPAZ_0",
            ["JADE_1"] = "PERFECT", ["JADE_0"] = "PERFECT", ["AMBER_0"] = "PERFECT", ["AMBER_1"] = "PERFECT", ["TOPAZ_0"] = "PERFECT"
        });

        var features = auction.ToComplicatedFlip(includeBreakdown: true, sniper: service).AttributeValues;

        var gemValue = 2 * gemPrices["PERFECT_JADE_GEM"] + 2 * gemPrices["PERFECT_AMBER_GEM"] + gemPrices["PERFECT_TOPAZ_GEM"] - 5 * 500_000;
        features.Should().Contain("gems", gemValue);
        features.Should().Contain("pgems:5", 1, "the flat 100M of pgems ranks the modifier in the lookup key, it is not what the gems are worth");
    }

    [Test]
    public void RunningAuctionGetsTheMayorOfToday()
    {
        var mayors = new MayorService(null, null, NullLogger<MayorService>.Instance);
        mayors.SetMayorForYear(MayorService.ElectionYear(DateTime.UtcNow), "Diana");
        var auction = Sold("HYPERION", 0, "sharpness:5", new());
        auction.End = DateTime.UtcNow.AddDays(14); // a fresh listing ends under a mayor that is not elected yet

        var features = auction.ToComplicatedFlip(includeBreakdown: true, sniper: CreateService(new()), mayorService: mayors).AttributeValues;

        features.Should().Contain("m:diana", 1);
    }
}
