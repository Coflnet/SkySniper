using System;
using System.Collections.Generic;
using System.Linq;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using dev;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>
/// Pins the values the AI finder gets from this sniper for a few real auctions (anonymised, timestamps
/// rounded to the hour) against a fixed pricing state. What the finder makes of it is pinned in SkyAIFinder.
/// Regenerate with <see cref="DISCOVER"/> = true and paste the printed lines; never hand-edit them.
/// </summary>
public class AiValueSourceCharacterisationTests
{
    private static readonly bool DISCOVER = false;
    private SniperService service = null!;
    private SniperServiceTests.CraftCostMock craftCost = null!;

    [SetUp]
    public void Setup()
    {
        SniperService.StartTime = DateTime.UtcNow.Date.AddDays(-45);
        craftCost = new SniperServiceTests.CraftCostMock();
        craftCost.Costs["HYPERION"] = 870_000_000;
        craftCost.Costs["TITANIUM_DRILL_4"] = 95_000_000;
        service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, craftCost);
        foreach (var (tag, price) in BazaarPrices)
            SetBazaarPrice(tag, price);
        AddPetBuckets("PET_SCATHA", "6", (Tier.RARE, 86_000_000, 93_078_304), (Tier.EPIC, 183_583_332, 197_539_376), (Tier.LEGENDARY, 0, 400_000_000));
        AddPetBuckets("PET_GOLDEN_DRAGON", "7", (Tier.LEGENDARY, 520_000_000, 690_000_000));
    }

    private static readonly (string tag, int price)[] BazaarPrices =
    [
        ("FLAWLESS_JASPER_GEM", 9_100_000), ("FLAWLESS_SAPPHIRE_GEM", 2_400_000), ("FLAWLESS_AMBER_GEM", 2_300_000),
        ("FLAWLESS_JADE_GEM", 2_600_000), ("FLAWLESS_TOPAZ_GEM", 3_900_000), ("PET_ITEM_TIER_BOOST", 82_000_000),
        ("MINOS_RELIC", 31_000_000), ("RECOMBOBULATOR_3000", 7_400_000), ("HOT_POTATO_BOOK", 81_269), ("FUMING_POTATO_BOOK", 1_250_000),
        ("THE_ART_OF_WAR", 9_800_000), ("GOBLIN_OMELETTE_SPICY", 19_000_000), ("GEMSTONE_FUEL_TANK", 11_500_000),
        ("ENCHANTMENT_ULTIMATE_WISE_5", 3_100_000), ("ENCHANTMENT_PROSECUTE_5", 4_200_000), ("ENCHANTMENT_COMPACT_1", 5_000_000),
        ("ENCHANTMENT_ULTIMATE_FLOWSTATE_3", 2_000_000), ("ENCHANTMENT_REJUVENATE_5", 394_491), ("WITHER_CATALYST", 2_100_000),
    ];

    [TestCase("1")]
    [TestCase("2")]
    [TestCase("3")]
    [TestCase("4")]
    [TestCase("kills")]
    public void ValuesOfRealAuctions(string id)
    {
        var auction = id == "kills" ? ReaperChestplateWithKills() : Auctions[id];

        var values = new SniperItemValueSource(service, craftCost).GetValues(auction);

        var described = Describe(values);
        if (DISCOVER)
            Console.WriteLine($"[GOLD {id}] " + string.Join(", ", described.Select(p => $"\"{p}\"")));
        described.Should().BeEquivalentTo(Golden[id]);
    }

    /// <summary>
    /// The values of exp and candy depend on state other tests leave in the sniper's shared caches, only their presence is pinned
    /// </summary>
    private static List<string> Describe(Coflnet.Sky.AIFinder.SniperValues values)
    {
        var parts = values.AllModifiers.Select(part =>
        {
            var value = part.Modifier.Key is "exp" or "candyUsed" ? "varies" : part.Value.ToString();
            if (part.EnchantType != default)
                return $"enchant {part.EnchantType} {part.EnchantLevel} = {value}";
            if (!string.IsNullOrEmpty(part.Modifier.Key))
                return $"modifier {part.Modifier.Key} {part.Modifier.Value} = {value}";
            return $"reforge {part.Reforge} = {value}";
        }).ToList();
        parts.Add($"craft cost {values.CraftCost}, clean {values.CleanPrice}, removable {values.RemovableValue}");
        return parts;
    }

    private static readonly Dictionary<string, string[]> Golden = new()
    {
        ["1"] = ["modifier art_of_war_count 1 = 9800000", "modifier rarity_upgrades 1 = 7400000", "modifier hotpc 1 = 7062690", "enchant ultimate_wise 5 = 3100000", "reforge withered = 50000", "enchant ender_slayer 6 = -1", "enchant luck 7 = -1", "enchant scavenger 5 = -1", "enchant vampirism 6 = -1", "enchant dragon_hunter 6 = -1", "enchant bane_of_arthropods 7 = -1", "enchant fire_aspect 3 = -1", "enchant sharpness 6 = -1", "enchant smite 7 = -1", "enchant titan_killer 7 = -1", "craft cost 870000000, clean 0, removable 11300000"],
        ["2"] = ["enchant compact 10 = 11000000", "enchant efficiency 6 = 8000000", "modifier rarity_upgrades 1 = 7400000", "enchant ultimate_flowstate 3 = 2000000", "enchant prismatic 5 = -1", "craft cost 95000000, clean 0, removable 39000000"],
        ["3"] = ["modifier exp 6 = varies", "craft cost 0, clean 0, removable 82000000"],
        ["4"] = ["modifier candyUsed 1 = varies", "modifier exp 1 = varies", "craft cost 0, clean 0, removable 31000000"],
        ["kills"] = ["modifier zombie_kills 10 = 2700000", "modifier hotpc 0 = 812690", "enchant rejuvenate 5 = 394491", "craft cost 0, clean 0, removable 0"],
    };

    private void AddPetBuckets(string tag, string maxExp, params (Tier tier, long levelOne, long maxLevel)[] buckets)
    {
        var lookup = new PriceLookup();
        foreach (var (tier, levelOne, maxLevel) in buckets)
        {
            lookup.Lookup[new AuctionKey([], ItemReferences.Reforge.Any, [new("exp", "0"), new("candyUsed", "0")], tier, 1)] = new() { Price = levelOne };
            lookup.Lookup[new AuctionKey([], ItemReferences.Reforge.Any, [new("exp", maxExp)], tier, 1)] = new() { Price = maxLevel };
        }
        service.Lookups[tag] = lookup;
    }

    private void SetBazaarPrice(string tag, int value)
    {
        service.UpdateBazaar(new() { Products = [new() { ProductId = tag, SellSummary = [new() { PricePerUnit = value }], BuySummery = [] }] });
    }

    private static SaveAuction ReaperChestplateWithKills() => new()
    {
        Uuid = "00000000000000000000000000000005",
        Tag = "REAPER_CHESTPLATE",
        Tier = Tier.LEGENDARY,
        Reforge = ItemReferences.Reforge.ancient,
        Category = Category.ARMOR,
        Bin = true,
        Count = 1,
        StartingBid = 10_000_000,
        HighestBidAmount = 10_000_000,
        Start = new DateTime(2026, 9, 27, 9, 0, 0, DateTimeKind.Utc),
        End = new DateTime(2026, 9, 27, 10, 0, 0, DateTimeKind.Utc),
        Enchantments = [new(Enchantment.EnchantmentType.rejuvenate, 5), new(Enchantment.EnchantmentType.thorns, 3),
            new(Enchantment.EnchantmentType.growth, 5), new(Enchantment.EnchantmentType.protection, 5)],
        FlatenedNBT = new() { { "cc", "1" }, { "hpc", "10" }, { "zombie_kills", "146666" } }
    };

    private static readonly Dictionary<string, SaveAuction> Auctions = new()
    {
        // Withered Hyperion ✪✪✪✪✪➎
        ["1"] = new SaveAuction
        {
            Uuid = "00000000000000000000000000000001", Tag = "HYPERION", Reforge = ItemReferences.Reforge.withered, ItemName = "Withered Hyperion ✪✪✪✪✪➎", Tier = Tier.MYTHIC, Category = Category.UNKNOWN,
            Bin = true, Count = 1, StartingBid = 925000000, HighestBidAmount = 925000000,
            Start = new DateTime(2025, 9, 30, 23, 0, 0, DateTimeKind.Utc), End = new DateTime(2025, 10, 1, 2, 0, 0, DateTimeKind.Utc),
            Enchantments = [new() { Type = Enchantment.EnchantmentType.bane_of_arthropods, Level = 7 }, new() { Type = Enchantment.EnchantmentType.impaling, Level = 5 }, new() { Type = Enchantment.EnchantmentType.luck, Level = 7 }, new() { Type = Enchantment.EnchantmentType.critical, Level = 6 }, new() { Type = Enchantment.EnchantmentType.thunderbolt, Level = 7 }, new() { Type = Enchantment.EnchantmentType.cleave, Level = 5 }, new() { Type = Enchantment.EnchantmentType.syphon, Level = 4 }, new() { Type = Enchantment.EnchantmentType.smite, Level = 7 }, new() { Type = Enchantment.EnchantmentType.looting, Level = 4 }, new() { Type = Enchantment.EnchantmentType.ender_slayer, Level = 6 }, new() { Type = Enchantment.EnchantmentType.scavenger, Level = 5 }, new() { Type = Enchantment.EnchantmentType.knockback, Level = 2 }, new() { Type = Enchantment.EnchantmentType.experience, Level = 4 }, new() { Type = Enchantment.EnchantmentType.vampirism, Level = 6 }, new() { Type = Enchantment.EnchantmentType.fire_aspect, Level = 3 }, new() { Type = Enchantment.EnchantmentType.dragon_hunter, Level = 6 }, new() { Type = Enchantment.EnchantmentType.venomous, Level = 5 }, new() { Type = Enchantment.EnchantmentType.triple_strike, Level = 4 }, new() { Type = Enchantment.EnchantmentType.ultimate_wise, Level = 5 }, new() { Type = Enchantment.EnchantmentType.sharpness, Level = 6 }, new() { Type = Enchantment.EnchantmentType.titan_killer, Level = 7 }, new() { Type = Enchantment.EnchantmentType.cubism, Level = 5 }, new() { Type = Enchantment.EnchantmentType.lethality, Level = 6 }, new() { Type = Enchantment.EnchantmentType.prosecute, Level = 5 }],
            FlatenedNBT = new() { ["COMBAT_0"] = "FLAWLESS", ["COMBAT_0_gem"] = "JASPER", ["RUNE_BLOOD_2"] = "3", ["SAPPHIRE_0"] = "FLAWLESS", ["art_of_war_count"] = "1", ["hpc"] = "15", ["rarity_upgrades"] = "1", ["unlocked_slots"] = "COMBAT_0,SAPPHIRE_0", ["upgrade_level"] = "10" }
        },
        // Auspicious Titanium Drill DR-X655
        ["2"] = new SaveAuction
        {
            Uuid = "00000000000000000000000000000002", Tag = "TITANIUM_DRILL_4", Reforge = ItemReferences.Reforge.Auspicious, ItemName = "Auspicious Titanium Drill DR-X655", Tier = Tier.MYTHIC, Category = Category.MISC,
            Bin = true, Count = 1, StartingBid = 270000000, HighestBidAmount = 270000000,
            Start = new DateTime(2025, 9, 1, 2, 0, 0, DateTimeKind.Utc), End = new DateTime(2025, 9, 1, 2, 0, 0, DateTimeKind.Utc),
            Enchantments = [new() { Type = Enchantment.EnchantmentType.efficiency, Level = 6 }, new() { Type = Enchantment.EnchantmentType.lapidary, Level = 1 }, new() { Type = Enchantment.EnchantmentType.fortune, Level = 4 }, new() { Type = Enchantment.EnchantmentType.compact, Level = 10 }, new() { Type = Enchantment.EnchantmentType.prismatic, Level = 5 }, new() { Type = Enchantment.EnchantmentType.ultimate_flowstate, Level = 3 }, new() { Type = Enchantment.EnchantmentType.experience, Level = 4 }],
            FlatenedNBT = new() { ["AMBER_0"] = "FLAWLESS", ["JADE_0"] = "FLAWLESS", ["MINING_0"] = "FLAWLESS", ["MINING_0_gem"] = "TOPAZ", ["compact_blocks"] = "3054963", ["drill_fuel"] = "26003", ["drill_part_fuel_tank"] = "gemstone_fuel_tank", ["drill_part_upgrade_module"] = "goblin_omelette_spicy", ["rarity_upgrades"] = "1", ["unlocked_slots"] = "JADE_0,MINING_0" }
        },
        // [Lvl 100] Scatha
        ["3"] = new SaveAuction
        {
            Uuid = "00000000000000000000000000000003", Tag = "PET_SCATHA", Reforge = ItemReferences.Reforge.None, ItemName = "[Lvl 100] Scatha", Tier = Tier.LEGENDARY, Category = Category.UNKNOWN,
            Bin = true, Count = 1, StartingBid = 335000000, HighestBidAmount = 335000000,
            Start = new DateTime(2025, 10, 1, 2, 0, 0, DateTimeKind.Utc), End = new DateTime(2025, 10, 1, 8, 0, 0, DateTimeKind.Utc),
            Enchantments = [],
            FlatenedNBT = new() { ["active"] = "False", ["candyUsed"] = "0", ["exp"] = "100313767.20759104", ["heldItem"] = "PET_ITEM_TIER_BOOST", ["hideInfo"] = "False", ["hideRightClick"] = "True", ["noMove"] = "False", ["tier"] = "EPIC", ["type"] = "SCATHA" }
        },
        // [Lvl 111] Golden Dragon
        ["4"] = new SaveAuction
        {
            Uuid = "00000000000000000000000000000004", Tag = "PET_GOLDEN_DRAGON", Reforge = ItemReferences.Reforge.None, ItemName = "[Lvl 111] Golden Dragon", Tier = Tier.LEGENDARY, Category = Category.MISC,
            Bin = true, Count = 1, StartingBid = 612000000, HighestBidAmount = 612000000,
            Start = new DateTime(2025, 10, 1, 0, 0, 0, DateTimeKind.Utc), End = new DateTime(2025, 10, 1, 1, 0, 0, DateTimeKind.Utc),
            Enchantments = [],
            FlatenedNBT = new() { ["active"] = "False", ["candyUsed"] = "10", ["exp"] = "42641430.94144653", ["heldItem"] = "MINOS_RELIC", ["hideInfo"] = "False", ["hideRightClick"] = "True", ["noMove"] = "False", ["tier"] = "LEGENDARY", ["type"] = "GOLDEN_DRAGON" }
        },
    };
}
