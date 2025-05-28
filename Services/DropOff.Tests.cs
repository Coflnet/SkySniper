using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using dev;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;
public class DropOffTests
{
    string loaded = null;
    SniperService sniperService;
    List<LowPricedAuction> found;
    private class MockCraftCostService : ICraftCostService
    {
        public Dictionary<string, double> Costs { get; } = new();
        public bool TryGetCost(string itemId, out double cost)
        {
            return Costs.TryGetValue(itemId, out cost);
        }
    }
    private ICraftCostService craftCostService = new MockCraftCostService();

    [SetUp]
    public void Setup()
    {
        SniperService.StartTime = new DateTime(2021, 9, 25);
        if (loaded == null)
        {
            loaded = File.ReadAllText("Mock/boots.json");
        }
        sniperService = new SniperService(new(null, null), null, NullLogger<SniperService>.Instance, craftCostService);
        var parsed = JsonConvert.DeserializeObject<LookupLoad>(loaded);
        var xy =
                parsed.Lookup.Where(l => l.Key.Contains("BLACK")).ToDictionary(l => ParseKey(l), l => l.Value);
        var converted = new PriceLookup()
        {
            Lookup = new System.Collections.Concurrent.ConcurrentDictionary<AuctionKey, ReferenceAuctions>(xy)
        };
        SniperService.StartTime += TimeSpan.FromDays(10000);
        sniperService.AddLookupData("WISE_WITHER_BOOTS", converted);

        found = new List<LowPricedAuction>();
        sniperService.AddLookupData("RUNE_TIDAL", new PriceLookup()
        {
            Lookup = new(new Dictionary<AuctionKey, ReferenceAuctions>()
            {
                {
                    new AuctionKey()
                    {
                        Enchants = new(new List<Enchant>()),
                        Reforge = ItemReferences.Reforge.Any,
                        Tier = Tier.COMMON,
                        Count = 1,
                        Modifiers = new(new List<KeyValuePair<string, string>>()
                        {
                            new("RUNE_TIDAL", "3")
                        })
                    },
                    new ReferenceAuctions()
                    {
                        References = new(),
                        Price = 10_000_000
                    }
                }
            })
        });
        sniperService.FoundSnipe += (a) =>
        {
            found.Add(a);
        };
    }

    [Test]
    public void DownwardTrendLimit()
    {
        SniperService.StartTime += DateTime.UtcNow - new DateTime(2024, 12, 30);
        var converted = LoadLookupMock("trend.json");
        sniperService.AddLookupData("HYPERION", converted);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("HYPERION", sniperService.GetBreakdownKey(item.Key, "HYPERION")));
        }
        var price = sniperService.Lookups["HYPERION"].Lookup.First().Value.Price;
        price.Should().Be(1029_293_224L, "Trend anylsis shows a downward trend, compared to long time its the smallest");
    }
    [Test]
    public void LowResellPullsdown()
    {
        SniperService.StartTime += DateTime.UtcNow - new DateTime(2024, 12, 30) - TimeSpan.FromDays(10000);
        var converted = LoadLookupMock("skindrop.json");
        SetBazaarPrice("PET_SKIN_CAT_SPACE_KITTY", 40_000_000);
        sniperService.AddLookupData("PET_BLACK_CAT", converted);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("PET_BLACK_CAT", sniperService.GetBreakdownKey(item.Key, "PET_BLACK_CAT")));
        }
        var price = sniperService.Lookups["PET_BLACK_CAT"].Lookup.First().Value.Price;
        price.Should().Be(117000008L, "Low resell rate should pull down the price");
    }
    /// <summary>
    /// Flip from https://sky.coflnet.com/auction/3513bd5932a2413183059fe636867d92
    /// to https://sky.coflnet.com/auction/01456d41316046dc9bf99ccc17b30d95
    /// got undervalued because the median was 250m (matching with risky estimate at the time)
    /// </summary>
    [Test]
    public void JerryArtifact()
    {
        SetBazaarPrice("RECOMBOBULATOR_3000", 8_000_000);
        AddLookupAndUpdateMeidans("jerry.json", "JERRY_TALISMAN_GOLDEN", new DateTime(2025, 1, 5));
        var price = sniperService.Lookups["JERRY_TALISMAN_GOLDEN"].Lookup.First(l => l.Key.Modifiers.Count == 0 && l.Key.Count == 1).Value;
        price.RiskyEstimate.Should().BeGreaterThanOrEqualTo(262678900L, "Half of the risky estimate should be applied");
    }

    [Test]
    public void InfernalSortOrderLowerValue()
    {
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_HABANERO_TACTICS_5", 187_477_552);
        SetBazaarPrice("ENCHANTMENT_MANA_VAMPIRE_10", 32_899_992);
        SetBazaarPrice("DYE_BLACK_ICE", 26800000);
        SetBazaarPrice("RECOMBOBULATOR_3000", 9_000_000);
        AddLookupAndUpdateMeidans("Infernal.json", "INFERNAL_CRIMSON_LEGGINGS", new DateTime(2025, 4, 4));
        var testAuction = new SaveAuction()
        {
            Tag = "INFERNAL_CRIMSON_LEGGINGS",
            FlatenedNBT = new Dictionary<string, string>() { { "magic_find", "10" },{ "veteran", "10" }, 
                { "dye_item", "DYE_BLACK_ICE" }, {"rarity_upgrades", "1" } },
            Enchantments = [new(){Type = Enchantment.EnchantmentType.mana_vampire, Level = 10 }, new(){Type = Enchantment.EnchantmentType.ultimate_habanero_tactics, Level = 5 }],
            StartingBid = 830_000_000,
            HighestBidAmount = 0,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.MYTHIC,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(testAuction);
        var flip = found.Last(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN);
        flip.TargetPrice.Should().Be(877592576L);
    }

    [Test]
    public void PestVestSniperFind()
    {
        AddLookupAndUpdateMeidans("pest_vest.json", "PEST_VEST", new DateTime(2025, 5, 12));
        var testAuction = new SaveAuction()
        {
            Tag = "PEST_VEST",
            FlatenedNBT = new(),
            Enchantments = [],
            StartingBid = 5_200_000,
            HighestBidAmount = 0,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.EPIC,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(testAuction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER);
        flip.TargetPrice.Should().Be(6989400L);
    }

    [Test]
    public void HermitCrabLevel100NotLimited()
    {
        AddLookupAndUpdateMeidans("HermitCrab.json", "PET_HERMIT_CRAB", new DateTime(2025, 1, 5));
        var price = sniperService.Lookups["PET_HERMIT_CRAB"].Lookup.First(l => l.Key.Modifiers.Count == 1 && l.Key.Tier == Tier.LEGENDARY).Value;
        price.Price.Should().Be(42_000_000, "Level 100 should not be limited by craft cost");
    }
    [Test]
    public void PerfectGem()
    {
        SniperService.StartTime += DateTime.UtcNow - new DateTime(2025, 3, 30) - TimeSpan.FromDays(10000);
        var converted = LoadLookupMock("GEM.json");
        sniperService.AddLookupData("PERFECT_JASPER_GEM", converted);
        craftCostService.Costs["PERFECT_JASPER_GEM"] = 20_000_000_000;
        SetBazaarPrice("PERFECT_JASPER_GEM", 30_000_000);
        foreach (var item in sniperService.Lookups)
            {
                foreach (var bucket in item.Value.Lookup)
                {
                    if (bucket.Value.References.Count < 4)
                        continue; // can't have a median
                    // make sure all medians are up to date
                    sniperService.UpdateMedian(bucket.Value, (item.Key, sniperService.GetBreakdownKey(bucket.Key, item.Key)));
                }
            }
        var price = sniperService.Lookups["PERFECT_JASPER_GEM"].Lookup.First().Value;
        price.Price.Should().Be(30491442L, "Median bazaar price");
    }

    private void AddLookupAndUpdateMeidans(string fileName, string itemTag, DateTime simulatedTime)
    {
        SniperService.StartTime += DateTime.UtcNow - simulatedTime - TimeSpan.FromDays(10000);
        var converted = LoadLookupMock(fileName);
        sniperService.AddLookupData(itemTag, converted);
        UpdateMedian(itemTag, converted);
    }

    private void UpdateMedian(string itemTag, PriceLookup converted)
    {
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, (itemTag, sniperService.GetBreakdownKey(item.Key, itemTag)));
        }
    }

    [Test]
    public void DropPriceSameSeller()
    {
        SniperService.StartTime += DateTime.UtcNow - new DateTime(2025, 1, 8);
        var converted = LoadLookupMock("sameSellerPriceDrop.json");
        sniperService.AddLookupData("GLOSSY_MINERAL_BOOTS", converted);
        craftCostService.Costs["GLOSSY_MINERAL_BOOTS"] = 37_000_000;
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("GLOSSY_MINERAL_BOOTS", sniperService.GetBreakdownKey(item.Key, "GLOSSY_MINERAL_BOOTS")));
        }
        var price = sniperService.Lookups["GLOSSY_MINERAL_BOOTS"].Lookup.First(l => l.Key.Modifiers.Count == 0 && l.Key.Count == 1).Value;
        price.Price.Should().Be(40_710_000, "craft cost limited from 41m");
        price.RiskyEstimate.Should().Be(44_781_000, "*11/10 above median because it is already limite by craft cost");
    }

    [Test]
    public void SpeedTest()
    {
        Assert.Ignore("This test is only for local testing");
        SetBazaarPrice("RECOMBOBULATOR_3000", 8_000_000);
        SetBazaarPrice("ENCHANTMENT_GROWTH_6", 44_000_000);
        var testAuction = new SaveAuction()
        {
            Tag = "WISE_WITHER_BOOTS",
            FlatenedNBT = new Dictionary<string, string>() { { "rarity_upgrades", "1" } },
            Enchantments = new List<Enchantment>() { new() { Type = Enchantment.EnchantmentType.growth, Level = 6 } },
            StartingBid = 900_000,
            HighestBidAmount = 0,
            UId = 4,
            AuctioneerId = "aab123",
            Tier = Tier.MYTHIC,
            ItemCreatedAt = DateTime.UtcNow,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.AllocatedDicts = new();
        for (int i = 0; i < 2000; i++)
        {
            sniperService.AllocatedDicts.Enqueue(new(10));
        }
        var startTime = DateTime.UtcNow;
        for (int i = 0; i < 1000; i++)
        {
            sniperService.TestNewAuction(testAuction, true, true);
        }
        var took = DateTime.UtcNow - startTime;
        took.Should().BeLessThan(TimeSpan.FromMilliseconds(90));
    }


    [Test]
    public void ScavengerArtifact()
    {
        var converted = LoadLookupMock("ScavengerArtifact.json");
        SniperService.StartTime += (DateTime.UtcNow - new DateTime(2024, 9, 22));
        SetBazaarPrice("RECOMBOBULATOR_3000", 0);
        sniperService.AddLookupData("SCAVENGER_ARTIFACT", converted);
        foreach (var item in converted.Lookup)
        {
            try
            {
                sniperService.UpdateMedian(item.Value, ("SCAVENGER_ARTIFACT", sniperService.GetBreakdownKey(item.Key, "SCAVENGER_ARTIFACT")));
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        sniperService.Lookups["SCAVENGER_ARTIFACT"].Lookup.Where(l => l.Key.Modifiers.Count == 1).First().Value.Price.Should().BeLessThan(200_000_000);
    }
    [Test]
    public void DeskMedian()
    {
        var converted = LoadLookupMock("DESK.json");
        SniperService.StartTime += -TimeSpan.FromDays(10_000);
        sniperService.AddLookupData("DESK", converted);
        foreach (var item in converted.Lookup)
        {
            try
            {
                sniperService.UpdateMedian(item.Value, ("DESK", sniperService.GetBreakdownKey(item.Key, "DESK")));
                craftCostService.Costs["DESK"] = 10_000;
                sniperService.UpdateMedian(item.Value, ("DESK", sniperService.GetBreakdownKey(item.Key, "DESK")));
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
        var desk = sniperService.Lookups["DESK"].Lookup.Where(l => l.Key.Count == 1).First();
        desk.Value.Price.Should().Be(21_600 + 750_000);
    }
    [Test]
    public void ScarfUnderValue()
    {
        var converted = LoadLookupMock("SCARF.json");
        SniperService.StartTime += -TimeSpan.FromDays(10_000) + (DateTime.UtcNow - new DateTime(2024, 10, 14));
        sniperService.AddLookupData("SCARF", converted);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("SCARF", sniperService.GetBreakdownKey(item.Key, "SCARF")));
        }
        var scarf = sniperService.Lookups["SCARF"].Lookup.Where(l => l.Key.Count == 1 && l.Key.Modifiers.Count == 0).First();
        scarf.Value.Price.Should().Be(1450000L);
    }

    [Test]
    public void AspectOfTheVoidCraftCostCapSniperHigh()
    {
        var converted = LoadLookupMock("aspect_of_the_void.json");
        SniperService.StartTime += -TimeSpan.FromDays(10_000) + (DateTime.UtcNow - new DateTime(2025, 02, 09));
        sniperService.AddLookupData("ASPECT_OF_THE_VOID", converted);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("ASPECT_OF_THE_VOID", sniperService.GetBreakdownKey(item.Key, "ASPECT_OF_THE_VOID")));
        }
        SetBazaarPrice("ETHERWARP_MERGER", 260_000);
        SetBazaarPrice("ETHERWARP_CONDUIT", 15_500_000);
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_WISE_5", 2_000_000);
        var testAuction = new SaveAuction()
        {
            Tag = "ASPECT_OF_THE_VOID",
            FlatenedNBT = new() { { "ethermerge", "1" } },
            Enchantments = [new Enchantment() { Type = Enchantment.EnchantmentType.ultimate_wise, Level = 5 }],
            StartingBid = 23_000_000,
            HighestBidAmount = 0,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.EPIC,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(testAuction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER);
        flip.TargetPrice.Should().BeGreaterThan(26_000_000L);
    }
    /// <summary>
    /// Real world example, craft cost did not use the correct clean price
    /// </summary>
    [Test]
    public void PickaxeCraftCost()
    {
        var converted = LoadLookupMock("Pickaxe.json");
        SniperService.StartTime += -TimeSpan.FromDays(10_000) + (DateTime.UtcNow - new DateTime(2024, 10, 17));
        sniperService.AddLookupData("DIAMOND_PICKAXE", converted);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("DIAMOND_PICKAXE", sniperService.GetBreakdownKey(item.Key, "DIAMOND_PICKAXE")));
        }
        SetBazaarPrice("SIL_EX", 5_500_000);
        var testAuction = new SaveAuction()
        {
            Tag = "DIAMOND_PICKAXE",
            FlatenedNBT = [],
            Enchantments = [new Enchantment() { Type = Enchantment.EnchantmentType.efficiency, Level = 10 }],
            StartingBid = 900_000,
            HighestBidAmount = 0,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(testAuction);
        var pickaxe = found.First(f => f.Finder == LowPricedAuction.FinderType.CraftCost);
        pickaxe.TargetPrice.Should().Be(23377000L);
    }

    [Test]
    public void Manip()
    {
        PriceLookup converted = LoadLookupMock("manipulation.json");
        SniperService.StartTime += TimeSpan.FromDays(10000);
        SetBazaarPrice("RECOMBOBULATOR_3000", 7_000_000);
        sniperService.AddLookupData("PET_SKIN_RAT_HIDE_AND_SQUEAK", converted);
        var testAuction = new SaveAuction()
        {
            Tag = "PET_SKIN_RAT_HIDE_AND_SQUEAK",
            FlatenedNBT = new Dictionary<string, string>() { { "rarity_upgrades", "1" } },
            StartingBid = 900_000,
            HighestBidAmount = 0,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.Ready;
        sniperService.TestNewAuction(testAuction);
        Assert.That(found.All(f => f.TargetPrice < 8_550_000), JsonConvert.SerializeObject(found, Formatting.Indented));
    }
    /// <summary>
    /// When price is dropping like for this Travel Scroll the median price should be adjusted downwards
    /// Context: https://discord.com/channels/267680588666896385/1296462463956680724/1297352116427423795
    /// </summary>
    [Test]
    public void PriceDroppingForwardAdjust()
    {
        PriceLookup converted = LoadLookupMock("TravelScroll.json");
        SniperService.StartTime -= TimeSpan.FromDays(10000) + (DateTime.UtcNow - new DateTime(2024, 10, 20));
        sniperService.UpdateMedian(converted.Lookup.Last().Value);
        sniperService.AddLookupData("HUB_DA_TRAVEL_SCROLL", converted);
        var testAuction = new SaveAuction()
        {
            Tag = "HUB_DA_TRAVEL_SCROLL",
            FlatenedNBT = [],
            StartingBid = 20_900_000,
            Enchantments = [],
            HighestBidAmount = 0,
            UId = 1234,
            AuctioneerId = "12aaa",
            Tier = Tier.EPIC,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(testAuction);
        var medianSnipe = found.FirstOrDefault(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN);
        medianSnipe?.TargetPrice.Should().Be(28000000L, JsonConvert.SerializeObject(found, Formatting.Indented));
    }
    [Test]
    public void UseRiskyEstimateOnLowVolumeWithLowVolatility()
    {
        PriceLookup converted = LoadLookupMock("RiskyLowVolume.json");
        SniperService.StartTime -= TimeSpan.FromDays(10000) - (DateTime.UtcNow - new DateTime(2024, 12, 22));
        var element = converted.Lookup.Last(l => l.Key.Modifiers.Count > 0);
        SetBazaarPrice("ENCHANTMENT_SMITE_7", 8_000_000);
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_WISE_5", 3_000_000);
        SetBazaarPrice("ENCHANTMENT_VAMPIRISM_5", 3_000_000);
        SetBazaarPrice("RECOMBOBULATOR_3000", 8_000_000);
        var key = ("SCAVENGER_ARTIFACT", sniperService.GetBreakdownKey(element.Key, "SCAVENGER_ARTIFACT"));
        sniperService.UpdateMedian(element.Value, key);
        element.Value.Price.Should().Be(62312557L);
        element.Value.RiskyEstimate.Should().Be(66554664L);
    }

    [Test]
    public void DyeItemCraftCostChange()
    {
        PriceLookup converted = LoadLookupMock("DYE.json");
        SniperService.StartTime -= TimeSpan.FromDays(10000) - (DateTime.UtcNow - new DateTime(2024, 11, 09));
        sniperService.UpdateMedian(converted.Lookup.Last().Value);
        sniperService.AddLookupData("DYE_H", converted);
        var testAuction = new SaveAuction()
        {
            Tag = "CHESTPLATE",
            FlatenedNBT = new() { { "dye_item", "DYE_H" } },
            StartingBid = 20_900_000,
            HighestBidAmount = 0,
            UId = 1234,
            AuctioneerId = "12aaa",
            Tier = Tier.EPIC,
            Count = 1
        };
        sniperService.AddSoldItem(testAuction.Dupplicate());
        sniperService.AddSoldItem(testAuction.Dupplicate());
        sniperService.AddSoldItem(testAuction.Dupplicate());
        sniperService.AddSoldItem(testAuction.Dupplicate());
        sniperService.AddSoldItem(testAuction.Dupplicate());
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(testAuction);
        var medianSnipe = found.First(f => f.Finder == LowPricedAuction.FinderType.CraftCost);
        medianSnipe.TargetPrice.Should().Be(97399150, JsonConvert.SerializeObject(found, Formatting.Indented));
    }

    [TestCase(9, 50000000L)]
    [TestCase(10, 63360000L)] // lbin based up to 99% at 10 volume
    public void SniperEstimate(byte volumeOverride, long target)
    {
        PriceLookup converted = LoadLookupMock("potato-talisman.json");
        SniperService.StartTime += TimeSpan.FromDays(10000);
        sniperService.AddLookupData("POTATO_TALISMAN", converted);
        var testAuction = new SaveAuction()
        {
            Tag = "POTATO_TALISMAN",
            FlatenedNBT = new(),
            StartingBid = 900_000,
            HighestBidAmount = 0,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.COMMON,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        converted.Lookup.Where(l => l.Key.Modifiers.Count == 0).First().Value.Volume = volumeOverride;
        sniperService.TestNewAuction(testAuction);
        var sniper = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER);
        sniper.TargetPrice.Should().Be(target);
    }

    [Test]
    public void SkinLowVolumeMedianTimeLimit()
    {
        PriceLookup converted = LoadLookupMock("skin-adjustment.json");
        var Difference = DateTime.UtcNow - new DateTime(2024, 8, 12);
        SniperService.StartTime = new DateTime(2021, 9, 25) + Difference;
        sniperService.AddLookupData("PET_SKIN_BAT_VAMPIRE", converted);
        sniperService.UpdateMedian(converted.Lookup.Last().Value);
        var testAuction = new SaveAuction()
        {
            Tag = "PET_SKIN_BAT_VAMPIRE",
            StartingBid = 900_000,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.MYTHIC,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(testAuction);
        found.Where(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN)
            .First().TargetPrice.Should().Be(150000000);
    }

    /// <summary>
    /// Snipes were recommended with target 42m because a higher value key matched
    /// </summary>
    [Test]
    public void AvoidVeryRiskySnipes()
    {
        var converted = LoadLookupMock("wisewitherboots.json");
        SniperService.StartTime += DateTime.UtcNow - new DateTime(2025, 1, 20);
        sniperService.AddLookupData("WISE_WITHER_BOOTS", converted);
        SetBazaarPrice("RECOMBOBULATOR_3000", 10_700_000);
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_WISDOM_5", 4_000_000);
        SetBazaarPrice("WITHER_ESSENCE", 3_200);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("WISE_WITHER_BOOTS", sniperService.GetBreakdownKey(item.Key, "WISE_WITHER_BOOTS")));
        }
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("WISE_WITHER_BOOTS", sniperService.GetBreakdownKey(item.Key, "WISE_WITHER_BOOTS")));
        }
        var withEnchant = converted.Lookup.First(l => l.Key.ToString() == "ultimate_wisdom=5 Any [rarity_upgrades, 1],[upgrade_level, 5] MYTHIC 1").Value;
        var keyOrder = string.Join('\n', converted.Lookup.Keys);
        Console.WriteLine(keyOrder);
        withEnchant.Price.Should().Be(31970000L);
        withEnchant.RiskyEstimate.Should().Be(35167000L);
        var withoutEnchant = converted.Lookup.First(l => l.Key.ToString() == " Any [rarity_upgrades, 1],[upgrade_level, 5] MYTHIC 1").Value;
        withoutEnchant.Price.Should().Be(27570000L);
        withoutEnchant.RiskyEstimate.Should().BeGreaterThanOrEqualTo(28347000L);
        // maybe test a snipe auction
    }

    [Test]
    public void LimitLowReferceSnipe()
    {
        AddLookupAndUpdateMeidans("gauntlet.json", "VANQUISHED_GLOWSTONE_GAUNTLET", new DateTime(2025, 4, 14));
        var auction = new SaveAuction()
        {
            Tag = "VANQUISHED_GLOWSTONE_GAUNTLET",
            StartingBid = 160_000_000,
            UId = 4,
            FlatenedNBT = new() { { "rarity_upgrades", "1" }, { "mana_regeneration", "7" }, { "mana_pool", "10" } },
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        found.Should().BeEmpty();
        var cheaper = auction.Dupplicate();
        cheaper.StartingBid = 20_000_000;
        sniperService.TestNewAuction(cheaper);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER);
        flip.TargetPrice.Should().Be(31000000L);
    }

    [Test]
    public async Task AddBackValuetoMedianManyModifiers()
    {
        await sniperService.Init();
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_SOUL_EATER_5", 20_000_000);
        SetBazaarPrice("ENCHANTMENT_OVERLOAD_5", 19_000_000);
        SetBazaarPrice("ENCHANTMENT_DRAGON_HUNTER_5", 18_000_000);
        SetBazaarPrice("ENCHANTMENT_TABASCO_3", 10_000_000);
        SetBazaarPrice("ENCHANTMENT_TOXOPHILITE_10", 8_000_000);
        SetBazaarPrice("FUMING_POTATO_BOOK", 2_000_000);
        SetBazaarPrice("RECOMBOBULATOR_3000", 8_000_000);
        SetBazaarPrice("FIRST_MASTER_STAR", 18_000_000);
        SetBazaarPrice("SECOND_MASTER_STAR", 22_000_000);
        SetBazaarPrice("THIRD_MASTER_STAR", 45_000_000);
        SetBazaarPrice("FOURTH_MASTER_STAR", 80_000_000);
        SetBazaarPrice("FIFTH_MASTER_STAR", 95_000_000);
        SetBazaarPrice("ESSENCE_DRAGON", 4800);
        
        AddLookupAndUpdateMeidans("Terminator.json", "TERMINATOR", new DateTime(2025, 5, 12));
        var auction = new SaveAuction()
        {
            Tag = "TERMINATOR",
            StartingBid = 720_000_000,
            UId = 4,
            Enchantments = [new (){Type=Enchantment.EnchantmentType.ultimate_soul_eater, Level=5 },
                new (){Type=Enchantment.EnchantmentType.overload, Level=5 },
                new (){Type=Enchantment.EnchantmentType.dragon_hunter, Level=5 },
                new (){Type=Enchantment.EnchantmentType.tabasco, Level=3 },
                new (){Type=Enchantment.EnchantmentType.toxophilite, Level=10 },
                new (){Type=Enchantment.EnchantmentType.infinite_quiver, Level=10 },
                new (){Type=Enchantment.EnchantmentType.chance, Level=4 },
                new (){Type=Enchantment.EnchantmentType.power, Level=6 }],
            FlatenedNBT = new() { { "rarity_upgrades", "1" }, { "hpc", "15" }, { "upgrade_level", "10" } },
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN);
        flip.TargetPrice.Should().BeGreaterThanOrEqualTo(844_000_000L, "median of 781m + craft cost partial");
    }
    [Test]
    public void Level1SheepNotFoundOnStonks()
    {
        SniperService.MIN_TARGET = 0; // test mode
        AddLookupAndUpdateMeidans("Sheep.json", "PET_SHEEP", new DateTime(2025, 5, 2));
        var auction = new SaveAuction()
        {
            Tag = "PET_SHEEP",
            StartingBid = 300_000,
            UId = 4,
            FlatenedNBT = new() { { "candyUsed", "0" }, { "exp", "0" } },
            AuctioneerId = "12aaa",
            Tier = Tier.EPIC,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        SniperService.MIN_TARGET = 200_000; // disable test mode
        found.Should().BeEmpty();
    }

    [Test]
    public void MolteNecklaceFind()
    {
        SetBazaarPrice("RECOMBOBULATOR_3000", 9_000_000);
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_THE_ONE_5", 100000000);
        SetBazaarPrice("ENCHANTMENT_CAYENNE_5", 20000000);
        SetBazaarPrice("ENCHANTMENT_QUANTUM_5", 1500000);
        AddLookupAndUpdateMeidans("Molten_Necklace.json", "MOLTEN_NECKLACE", new DateTime(2025, 5, 28));
        // based on 20e2c27983a0460094e92819fb41fd06
        var auction = new SaveAuction()
        {
            Tag = "MOLTEN_NECKLACE",
            StartingBid = 15_000_000,
            UId = 4,
            Enchantments = [new Enchantment() { Type = Enchantment.EnchantmentType.ultimate_the_one, Level = 5 },
                new Enchantment() { Type = Enchantment.EnchantmentType.cayenne, Level = 5 },
                new Enchantment() { Type = Enchantment.EnchantmentType.quantum, Level = 5 }],
            FlatenedNBT = new() { { "dominance", "10" }, { "speed", "9" }, { "rarity_upgrades", "1" }, { "boss_tier", "3" } },
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.CraftCost);
        flip.TargetPrice.Should().BeGreaterThanOrEqualTo(100_734999L, "based on 20e2c27983a0460094e92819fb41fd06"); // it sold for 390m https://sky.coflnet.com/auction/8acb03a605b34fb8936eececffd8f63c
    }

    /// <summary>
    /// Craft cost estimate was to low because median samples were too low
    /// After calculating 5th percentile to use for value of attribute estimation is mostly correct (could be up to ~9m)
    /// </summary>
    [Test]
    public void MolteBeltMedian()
    {
        AddLookupAndUpdateMeidans("Molten_Belt.json", "MOLTEN_BELT", new DateTime(2025, 5, 4));
        var price = sniperService.Lookups["MOLTEN_BELT"].Lookup.First(l => l.Key.Modifiers.Count == 1 && l.Key.Modifiers.First().Key == "mana_pool" && l.Key.Modifiers.First().Value == "5");
        sniperService.UpdateMedian(price.Value, ("MOLTEN_BELT", sniperService.GetBreakdownKey(price.Key, "MOLTEN_BELT")));
        price.Value.Price.Should().Be(6800000L);
    }
    /// <summary>
    /// comparison combination extra value for "godroll" was partially added to median if the attributes were dropped
    /// sample suggested (sell): https://sky.coflnet.com/auction/fb84cd7fd0fc4eca80d58f26faae1082
    /// </summary>
    [Test]
    public void MolteBeltMedianAttributeNotReaddingComparisonvalue()
    {
        AddLookupAndUpdateMeidans("Molten_Belt.json", "MOLTEN_BELT", new DateTime(2025, 5, 4));
        var auction = new SaveAuction()
        {
            Tag = "MOLTEN_BELT",
            StartingBid = 500_000,
            UId = 4,
            FlatenedNBT = new() { { "dominance", "3" }, { "mana_pool", "3" } },
            AuctioneerId = "12aaa",
            Tier = Tier.EPIC,
            Count = 1
        };
        var dupplicate = auction.Dupplicate();
        dupplicate.FlatenedNBT.Clear();
        dupplicate.HighestBidAmount = 1_200_000;
        sniperService.AddSoldItem(dupplicate);
        sniperService.AddSoldItem(dupplicate.Dupplicate());
        sniperService.AddSoldItem(dupplicate.Dupplicate());
        sniperService.AddSoldItem(dupplicate.Dupplicate());

        var clean = sniperService.Lookups["MOLTEN_BELT"].Lookup.First(l => l.Key.Modifiers.Count == 0 && l.Key.Count == 1).Value;
        clean.Price.Should().BeGreaterThanOrEqualTo(700_000L);
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.OrderByDescending(f => f.TargetPrice).First(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN);
        flip.TargetPrice.Should().BeLessThan(5_000_000L);
    }

    [Test]
    public void LowerToLbinIfLowVolume()
    {
        var converted = LoadLookupMock("limitByLbin.json");
        SniperService.StartTime += DateTime.UtcNow - new DateTime(2025, 1, 21);
        sniperService.AddLookupData("ITEM", converted);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("ITEM", sniperService.GetBreakdownKey(item.Key, "ITEM")));
        }
        var auction = new SaveAuction()
        {
            Tag = "ITEM",
            StartingBid = 900_000,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN);
        flip.TargetPrice.Should().Be(2_990_000_000L);
    }

    [Test]
    public void AllowHigherEstimateOnCleanHighVolumeLbin()
    {
        var converted = LoadLookupMock("DRILL_ENGINE.json");
        SniperService.StartTime += DateTime.UtcNow - new DateTime(2025, 3, 10) - TimeSpan.FromDays(10000);
        sniperService.AddLookupData("DRILL_ENGINE", converted);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("DRILL_ENGINE", sniperService.GetBreakdownKey(item.Key, "DRILL_ENGINE")));
        }
        var auction = new SaveAuction()
        {
            Tag = "DRILL_ENGINE",
            StartingBid = 270_000_000,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.RARE,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        var lbin = auction.Dupplicate();
        lbin.StartingBid = 305_000_000;
        sniperService.TestNewAuction(lbin);
        sniperService.FinishedUpdate();
        sniperService.TestNewAuction(auction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER);
        flip.TargetPrice.Should().Be(305_000_000L * 99 / 100);

    }

    [Test]
    public async Task CleanValueCorrect()
    {
        await sniperService.Init();
        SetBazaarPrice("RECOMBOBULATOR_3000", 9_000_000);
        SetBazaarPrice("THE_ART_OF_WAR", 8_000_000);
        SetBazaarPrice("FUMING_POTATO_BOOK", 3_000_000);
        SetBazaarPrice("IMPLOSION_SCROLL", 250_000_000);
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_CHIMERA_5", 300_000_000);
        SetBazaarPrice("FIRST_MASTER_STAR", 18_000_000);
        AddLookupAndUpdateMeidans("HYPERION.json", "HYPERION", new DateTime(2025, 3, 10));
        var cleanPrices = sniperService.Lookups["HYPERION"].Lookup.Where(p => p.Value.Price > 0 && p.Value.TimeToSell > 3).ToList().OrderBy(v => v.Value.Price).Skip(1).Take(5);
        cleanPrices.First().Value.Price.Should().BeGreaterThan(897_000_000, cleanPrices.First().Key.ToString());
        var auction = new SaveAuction()
        {
            Tag = "HYPERION",
            StartingBid = 270_000_000,
            UId = 4,
            AuctioneerId = "12aaa",
            FlatenedNBT = new() { { "rarity_upgrades", "1" }, { "upgrade_level", "10" } },
            Tier = Tier.MYTHIC,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        for (int i = 0; i < 5; i++)
        {
            sniperService.TestNewAuction(auction.Dupplicate());
        }
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN && f.TargetPrice >= 994589999);
        flip.Should().NotBeNull();
    }

    /// <summary>
    /// Median tended to sometimes limit itself to below clean value, assert that thats not the case
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task MinValueAboveClean()
    {
        await sniperService.Init();
        SetBazaarPrice("RECOMBOBULATOR_3000", 9_000_000);
        SetBazaarPrice("GEMSTONE_CHAMBER", 6_000_000);
        SetBazaarPrice("JADERALD", 4_400_000);
        AddLookupAndUpdateMeidans("divan.json", "DIVAN_BOOTS", new DateTime(2025, 4, 15));
        UpdateMedian("DIVAN_BOOTS", sniperService.Lookups["DIVAN_BOOTS"]);
        var pgems= sniperService.Lookups["DIVAN_BOOTS"].Lookup.Where(p => p.Key.ToString() == " Any [pgems, 5],[rarity_upgrades, 1],[unlocked_slots, AMBER_0,AMBER_1,JADE_0,JADE_1,TOPAZ_0] MYTHIC 1").First();
        pgems.Value.Price.Should().BeGreaterThan(42_000_000, "unlocked gem slots are expensive");
        var cheapest = sniperService.Lookups["DIVAN_BOOTS"].Lookup.Where(p => p.Value.Price > 0 && p.Value.TimeToSell > 3).ToList().OrderBy(v => v.Value.Price).Take(15).ToList();
        cheapest.Skip(1).First().Value.Price.Should().BeGreaterThan(20_000_000, string.Join('\n', cheapest.Select(c => c.Key.ToString() + " " + c.Value.Price)));
    }
    [Test]
    public async Task SellTimeIsNotTooLow()
    {
        await sniperService.Init();
        AddLookupAndUpdateMeidans("selltime.json", "AURORA_CHESTPLATE", new DateTime(2025, 4, 19));
        var updated = sniperService.Lookups["AURORA_CHESTPLATE"].Lookup.First();
        updated.Value.TimeToSell.Should().BeGreaterThan(60, "at least 60 minutes estimate");
    }
    [Test]
    public async Task SniperLimitShouldBeModerate()
    {
        await sniperService.Init();
        SetBazaarPrice("RECOMBOBULATOR_3000", 9_000_000);
        SetBazaarPrice("TALISMAN_ENRICHMENT_FEROCITY", 7_000_000);
        AddLookupAndUpdateMeidans("relict.json", "WITHER_RELIC", new DateTime(2025, 4, 22));
        // {"enchantments":[],"uuid":"261c6b1ae9144c59a4899743b3c4c598","count":1,"startingBid":100999999,"tag":"WITHER_RELIC","itemName":"Wither Relic","start":"2025-04-22T08:43:52","end":"2025-04-22T08:44:23","auctioneerId":"4617b0253ac54726b1e8a087a8c6b0d2","profileId":"e5b698baf5e94b3d86c9a59cca3552f1","coop":null,"coopMembers":null,"highestBidAmount":100999999,"bids":[{"bidder":"fbcc09e738b5404f9a534497f7748ade","profileId":"unknown","amount":100999999,"timestamp":"2025-04-22T08:44:23"}],"anvilUses":0,"nbtData":{"data":{"rarity_upgrades":1,"talisman_enrichment":"critical_chance","uid":"fd7387ac1ccd","uuid":"6ec00445-3ba6-4168-b87d-fd7387ac1ccd"}},"itemCreatedAt":"2020-08-28T01:56:00","reforge":"None","category":"UNKNOWN","tier":"MYTHIC","bin":true,"flatNbt":{"rarity_upgrades":"1","talisman_enrichment":"critical_chance","uid":"fd7387ac1ccd","uuid":"6ec00445-3ba6-4168-b87d-fd7387ac1ccd"}}
        var auction = new SaveAuction()
        {
            Tag = "WITHER_RELIC",
            StartingBid = 100_000_000,
            UId = 4,
            FlatenedNBT = new Dictionary<string, string>() { { "rarity_upgrades", "1" }, { "talisman_enrichment", "critical_chance" } },
            AuctioneerId = "12aaa",
            Tier = Tier.MYTHIC,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER);
        flip.TargetPrice.Should().BeGreaterThan(188_000_000L, "should be not much limited by the starting bid");
    }


    /// <summary>
    /// Price drop protection should adjust to volume and not miss good flips
    /// </summary>
    [Test]
    public void PetPriceDropIgnore()
    {
        AddLookupAndUpdateMeidans("petHigherRisk.json", "PET_G", new DateTime(2025, 1, 31));
        var auction = new SaveAuction()
        {
            Tag = "PET_G",
            StartingBid = 16_000_000,
            UId = 4,
            FlatenedNBT = new Dictionary<string, string>() { { "exp", "26000000" } },
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN);
        flip.TargetPrice.Should().Be(21_780_000);
    }
    /// <summary>
    /// Golden dragon should have craft cost based on level (exp) and rarity
    /// </summary>
    [Test]
    public void PetCleanExpBased()
    {
        SetBazaarPrice("MINOS_RELIC", 44_000_000);
        var converted = LoadLookupMock("dragon.json");
        SniperService.StartTime = new DateTime(2021, 9, 25) + (DateTime.UtcNow - new DateTime(2025, 3, 21));
        sniperService.AddLookupData("PET_GOLDEN_DRAGON", converted);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("PET_GOLDEN_DRAGON", sniperService.GetBreakdownKey(item.Key, "PET_GOLDEN_DRAGON")));
        }
        var auction = new SaveAuction()
        {
            Tag = "PET_GOLDEN_DRAGON",
            StartingBid = 16_000_000,
            UId = 4,
            FlatenedNBT = new Dictionary<string, string>() { { "exp", "260000000" }, { "candyUsed", "0" }, { "heldItem", "MINOS_RELIC" } },
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN);
        flip.TargetPrice.Should().Be(972428570L);
        flip.AdditionalProps["breakdown"].Should().StartWith("[{\"Value\":371999999,");
        var sniper = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER);
        sniper.TargetPrice.Should().Be(734264300L); // should be limited by a little bit over craft cost and not target 1.1b

        // check that exp is not dropped on lvl 1
        auction.FlatenedNBT["exp"] = "1";
        var lowKey = sniperService.ValueKeyForTest(auction);
        lowKey.Key.Modifiers.Count.Should().Be(3);
    }

    [Test]
    public void ReduceForSameSellerSells()
    {
        var converted = LoadLookupMock("hoverious.json");
        SniperService.StartTime = new DateTime(2021, 9, 25) + (DateTime.UtcNow - new DateTime(2025, 2, 3));
        sniperService.AddLookupData("INFINI_VACUUM_HOOVERIUS", converted);
        foreach (var item in converted.Lookup)
        {
            sniperService.UpdateMedian(item.Value, ("INFINI_VACUUM_HOOVERIUS", sniperService.GetBreakdownKey(item.Key, "INFINI_VACUUM_HOOVERIUS")));
        }
        var auction = new SaveAuction()
        {
            Tag = "INFINI_VACUUM_HOOVERIUS",
            StartingBid = 16_000_000,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN);
        flip.TargetPrice.Should().Be(25_000_000L, "pulled down by 66th percentile on last 12 sales (5th highest)");
    }
    /// <summary>
    /// if manipulation is detected within references the time window should be longer
    /// </summary>
    [Test]
    public void SkinManipulatedLongerTimeLimit()
    {
        PriceLookup converted = LoadLookupMock("SKIN-antimanip.json");
        var Difference = DateTime.UtcNow - new DateTime(2024, 12, 07);
        SniperService.StartTime = new DateTime(2021, 9, 25) + Difference;
        var testAuction = new SaveAuction()
        {
            Tag = "PET_SKIN_BAT_VAMPIRE",
            StartingBid = 900_000,
            UId = 4,
            AuctioneerId = "12aaa",
            Tier = Tier.EPIC,
            Count = 1
        };
        var sampleLbin = testAuction.Dupplicate();
        sampleLbin.StartingBid = 2_000_000_000;
        sniperService.TestNewAuction(sampleLbin);
        sniperService.AddLookupData("PET_SKIN_BAT_VAMPIRE", converted);
        sniperService.UpdateMedian(sniperService.Lookups.Last().Value.Lookup.Last().Value);
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(testAuction);
        found.Where(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN)
            .First().TargetPrice.Should().Be(750000000);
    }

    /// <summary>
    /// Real world example, craft cost capped at tierboosted pet
    /// </summary>
    [Test]
    public void ScathaNotUnvervalued()
    {
        PriceLookup converted = LoadLookupMock("SCATHA.json");
        var Difference = DateTime.UtcNow - new DateTime(2024, 11, 24);
        SniperService.StartTime = new DateTime(2021, 9, 25) + Difference;
        sniperService.AddLookupData("PET_SCATHA", converted);
        foreach (var item in sniperService.Lookups["PET_SCATHA"].Lookup)
        {
            if (item.Key.Modifiers.Count == 1 && item.Key.Modifiers.First().Value == "6" && item.Key.Tier == Tier.LEGENDARY)
            {
                Console.WriteLine(item.Key);
            }
            sniperService.UpdateMedian(item.Value);
        }
        var testAuction = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            StartingBid = 900_000,
            UId = 4,
            FlatenedNBT = new Dictionary<string, string>() { { "exp", "26000000" } },
            AuctioneerId = "12aaa",
            Tier = Tier.LEGENDARY,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(testAuction);
        found.Where(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN)
            .First().TargetPrice.Should().Be(409_000_000);
    }

    [Test]
    public void EndermanStonksLevelComparison()
    {
        SetBazaarPrice("ENDERMAN_SLAYER", 400_000); // not on bazaar but for price test enough
        AddLookupAndUpdateMeidans("Enderman.json","PET_ENDERMAN", new DateTime(2025, 3, 23));
        var auction = new SaveAuction()
        {
            Tag = "PET_ENDERMAN",
            StartingBid = 500_000,
            UId = 4,
            FlatenedNBT = new Dictionary<string, string>() { { "exp", "1" }, { "candyUsed", "0" } },
            AuctioneerId = "12aaa",
            Tier = Tier.EPIC,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.FirstOrDefault(f => f.Finder == LowPricedAuction.FinderType.STONKS);
        flip.TargetPrice.Should().Be(712500L, "exp adjusted");
    }
    [Test]
    public void PetCraftCostCandyNegative()
    {
        AddLookupAndUpdateMeidans("Enderman.json","PET_ENDERMAN", new DateTime(2025, 3, 23));
        var auction = new SaveAuction()
        {
            Tag = "PET_ENDERMAN",
            StartingBid = 100_000,
            UId = 4,
            FlatenedNBT = new Dictionary<string, string>() { { "exp", "1166478" }, { "candyUsed", "0" } },
            AuctioneerId = "12aaa",
            Tier = Tier.COMMON,
            Count = 1
        };
        sniperService.State = SniperState.FullyLoaded;
        sniperService.TestNewAuction(auction);
        var flip = found.FirstOrDefault(f => f.Finder == LowPricedAuction.FinderType.SNIPER);
        flip.TargetPrice.Should().BeLessThan(3307238L, "exp value adjusted"); // by default was 5m+
    }

    private static PriceLookup LoadLookupMock(string mockFileName)
    {
        var text = File.ReadAllText($"Mock/{mockFileName}");
        var parsed = JsonConvert.DeserializeObject<LookupLoad>(text);
        var xy =
                parsed.Lookup.ToDictionary(l => ParseKey(l), l => l.Value);
        var converted = new PriceLookup()
        {
            Lookup = new System.Collections.Concurrent.ConcurrentDictionary<AuctionKey, ReferenceAuctions>(xy),
            CleanPricePerDay = parsed.CleanPricePerDay,
            CleanKey = parsed.CleanKey
        };
        return converted;
    }

    private static AuctionKey ParseKey(KeyValuePair<string, ReferenceAuctions> e)
    {
        var parts = e.Key.Replace(", ", ",").Split(' ');
        var key = new AuctionKey()
        {
            Enchants = new(parts[0].Split(',').Where(e => !string.IsNullOrWhiteSpace(e)).Select(e =>
            {
                var eparts = e.Split('=');
                return new Enchant() { Type = Enum.Parse<Enchantment.EnchantmentType>(eparts.First()), Lvl = byte.Parse(eparts.Last()) };
            }).ToArray()),
            Reforge = Enum.Parse<ItemReferences.Reforge>(parts[1]),
            Tier = Enum.Parse<Tier>(parts.Reverse().Skip(1).First()),
            Count = byte.Parse(parts.Last()),
            Modifiers = new(parts.Skip(2).First().Split("],[").Select(e =>
            {
                var mparts = e.Split(',', 2);
                return new KeyValuePair<string, string>(mparts.First().TrimStart('['), mparts.Last().TrimEnd(']'));
            }).Where(m => !string.IsNullOrEmpty(m.Value)).ToList())
        };
        return key;
    }

    public class LookupLoad
    {
        public Dictionary<string, ReferenceAuctions> Lookup;
        public Dictionary<short, long> CleanPricePerDay = new();
        public AuctionKey CleanKey;
    }
    [Test]
    public async Task Test1()
    {
        var json = """
        {
        "uuid": "ece41041a8e944e08f514fecb2bb6169",
        "count": 1,
        "startingBid": 1000,
        "tag": "WISE_WITHER_BOOTS",
        "itemName": "✿ Necrotic Storm's Boots ✪✪✪✪✪➎",
        "start": "2024-02-08T19:26:23.471Z",
        "end": "2024-02-09T01:26:23.471Z",
        "auctioneerId": "5279f862b2db4b76b86bc574a6788119",
        "highestBidAmount": 0,
        "bids": [],
        "anvilUses": 0,
        "enchantments": [
          {
            "type": "depth_strider",
            "level": 3
          },
          {
            "type": "sugar_rush",
            "level": 3
          },
          {
            "type": "thorns",
            "level": 3
          },
          {
            "type": "ultimate_refrigerate",
            "level": 5
          },
          {
            "type": "mana_vampire",
            "level": 10
          },
          {
            "type": "feather_falling",
            "level": 10
          },
          {
            "type": "rejuvenate",
            "level": 5
          },
          {
            "type": "growth",
            "level": 7
          },
          {
            "type": "protection",
            "level": 7
          }
        ],
        "nbtData": {
          "Data": {
            "rarity_upgrades": 1,
            "color": "28:212:228",
            "runes": {
              "TIDAL": 3
            },
            "dungeon_item_level": 5,
            "upgrade_level": 10,
            "uid": "ea2bde7b8323",
            "uuid": "c7ca7340-a561-4b13-8355-ea2bde7b8323",
            "dye_item": "DYE_PURE_BLACK",
            "hpc": 15,
            "gems": {
              "COMBAT_0": {
                "uuid": "7256ca25-f711-482e-a406-1397309b6c9b",
                "quality": "PERFECT"
              },
              "COMBAT_0_gem": "AMETHYST",
              "SAPPHIRE_0": "PERFECT"
            },
            "artOfPeaceApplied": 1
          }
        },
        "itemCreatedAt": "2021-09-11T03:21:00Z",
        "reforge": "Necrotic",
        "category": "ARMOR",
        "tier": "MYTHIC",
        "bin": true,
        "flatNbt": {
          "rarity_upgrades": "1",
          "color": "28:212:228",
          "dungeon_item_level": "5",
          "upgrade_level": "10",
          "uid": "ea2bde7b8323",
          "uuid": "c7ca7340-a561-4b13-8355-ea2bde7b8323",
          "dye_item": "DYE_PURE_BLACK",
          "hpc": "15",
          "COMBAT_0_gem": "AMETHYST",
          "SAPPHIRE_0": "PERFECT",
          "artOfPeaceApplied": "1",
          "COMBAT_0": "PERFECT",
          "COMBAT_0.uuid": "7256ca25-f711-482e-a406-1397309b6c9b",
          "RUNE_TIDAL": "3"
        }}
        """;
        await sniperService.Init();
        var item = JsonConvert.DeserializeObject<ApiSaveAuction>(json);
        // ultimate_refrigerate=5,growth=7,protection=7,mana_vampire=10 Any [artOfPeaceApplied, 1],[COMBAT_0, PERFECT],[dye_item, DYE_PURE_BLACK],[hotpc, 1],[rarity_upgrades, 1],[RUNE_TIDAL, 3]
        SetBazaarPrice("FIRST_MASTER_STAR", 13_000_000);
        SetBazaarPrice("DYE_PURE_BLACK", 337_000_000);
        SetBazaarPrice("PERFECT_SAPPHIRE_GEM", 12_000_000);
        SetBazaarPrice("PERFECT_AMETHYST_GEM", 12_000_000);
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_REFRIGERATE_5", 26_000_000);
        SetBazaarPrice("ENCHANTMENT_GROWTH_7", 144_000_000);
        SetBazaarPrice("ENCHANTMENT_PROTECTION_7", 40_000_000);
        SetBazaarPrice("ENCHANTMENT_MANA_VAMPIRE_10", 36_000_000);
        SetBazaarPrice("THE_ART_OF_PEACE", 48_000_000);
        SetBazaarPrice("RUNE_TIDAL", 8_000_000);
        SetBazaarPrice("RECOMBOBULATOR_3000", 8_000_000);
        SetBazaarPrice("HOT_POTATO_BOOK", 80_000);
        sniperService.State = SniperState.Ready;
        sniperService.TestNewAuction(item);
        Assert.That(found.Count, Is.GreaterThanOrEqualTo(1));
        // combines buckets to reach estimation
        found.Last(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN).TargetPrice.Should().Be(324872335);
    }

    [Test]
    public void UsesLessSpiderKills()
    {
        // ultimate_last_stand=5 Any [color, 0:0:0],[hotpc, 1],[rarity_upgrades, 1],[spider_kills, 2]
        var auction = new SaveAuction()
        {
            Tag = "FIRST_MASTER_STAR",
            Tier = Tier.LEGENDARY,
            Enchantments = new(new List<Enchantment>()
              {
                  new() { Type = Enchantment.EnchantmentType.ultimate_last_stand, Level = 5 }
              }),
            FlatenedNBT = new(new Dictionary<string, string>()
            {
                { "color", "0:0:0" },
                { "spider_kills", "100000" }
            }),
        };
        var lowerKills = auction.Dupplicate();
        lowerKills.FlatenedNBT["spider_kills"] = "61000";
        sniperService.AddSoldItem(lowerKills.Dupplicate(30760795, DateTime.UtcNow - TimeSpan.FromDays(30)));
        sniperService.AddSoldItem(lowerKills.Dupplicate(24716394, DateTime.UtcNow - TimeSpan.FromDays(25)));
        sniperService.AddSoldItem(lowerKills.Dupplicate(21760795, DateTime.UtcNow - TimeSpan.FromDays(6)));

        sniperService.AddSoldItem(auction.Dupplicate(49742295, DateTime.UtcNow - TimeSpan.FromDays(2)));

        sniperService.TestNewAuction(auction.Dupplicate());
        Assert.That(2, Is.EqualTo(found.Count));
        Assert.That(24222066 - 3, Is.EqualTo(found.First().TargetPrice));
    }


    private void SetBazaarPrice(string tag, int value, int buyValue = 0)
    {
        var sellOrder = new List<SellOrder>();
        if (value > 0)
            sellOrder.Add(new SellOrder() { PricePerUnit = value });
        sniperService.UpdateBazaar(new()
        {
            Products = new(){
                new (){
                    ProductId =  tag,
                    SellSummary = sellOrder,
                    BuySummery = new(){
                        new (){
                            PricePerUnit = buyValue
                        }
                    }
                }
            },
            Timestamp = DateTime.UtcNow
        });
    }
}
