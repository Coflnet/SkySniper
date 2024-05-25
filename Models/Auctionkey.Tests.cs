using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Services;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using NUnit.Framework;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Sniper.Models;

public class AuctionkeyTests
{
    private SniperService service = null!;
    private HypixelItemService itemService = null!;
    [SetUp]
    public void Setup()
    {
        itemService = new HypixelItemService(null, null);
        service = new SniperService(itemService, null, NullLogger<SniperService>.Instance, null);
    }

    [Test]
    public void SkinsWithValue()
    {
        var withSkin = new AuctionKey()
        {
            Reforge = ItemReferences.Reforge.Any,
            Enchants = new List<Enchant>().AsReadOnly(),
            Modifiers = new(new List<KeyValuePair<string, string>>() { new("skin", "1"), new("exp", "6") }),
            Tier = Tier.LEGENDARY
        };
        var withoutSkin = new AuctionKey()
        {
            Reforge = ItemReferences.Reforge.Any,
            Enchants = new List<Enchant>().AsReadOnly(),
            Modifiers = new(new List<KeyValuePair<string, string>>() { new("exp", "6") }),
            Tier = Tier.LEGENDARY
        };
        var exp = new SniperService.RankElem(new KeyValuePair<string, string>("exp", "6"), 6);
        var value = new List<SniperService.RankElem>() { new(new KeyValuePair<string, string>("skin", "1"), 0), exp };
        var emtpyValue = new List<SniperService.RankElem>() { exp };
        var similarity = withoutSkin.Similarity(withSkin, service, value, emtpyValue);
        Assert.That(0, Is.GreaterThan(similarity));
    }

    [Test]
    public void DifferentModifiersDecrease()
    {
        var key = new AuctionKey();
        var keyB = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new("test", "test") }.AsReadOnly() };
        // by default reforge and tier match
        Assert.That(key.Similarity(key), Is.GreaterThan(keyB.Similarity(key)));
    }
    [Test]
    public void SameModsMatch()
    {
        var key = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new("test", "test") }.AsReadOnly() };
        var keyB = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new("test", "test") }.AsReadOnly() };
        // by default reforge and tier match
        Assert.That(key.Similarity(key), Is.EqualTo(keyB.Similarity(key)));
    }
    [Test]
    public void SameModsDecreaseFurther()
    {
        var key = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new("mending", "1"), new("veteran", "1") }.AsReadOnly() };
        var further = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new("dominance", "1"), new("veteran", "1") }.AsReadOnly() };
        var closer = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new("mending", "4"), new("veteran", "4") }.AsReadOnly() };
        // by default reforge and tier match
        Assert.That(key.Similarity(closer), Is.GreaterThan(key.Similarity(further)));
    }
    [Test]
    public void PreferMatchingKind()
    {
        var key = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new("test", "test") }.AsReadOnly() };
    }
    [Test]
    public void NoModsNoError()
    {
        var key = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>().AsReadOnly() };
        var keyB = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>().AsReadOnly() };
        // by default reforge and tier match
        Assert.That(key.Similarity(key), Is.EqualTo(keyB.Similarity(key)));
    }

    [Test]
    public void DifferentEnchantsDecrease()
    {
        var key = new AuctionKey();
        var keyB = new AuctionKey() { Enchants = new List<Enchant>() { new Enchant() { Lvl = 1, Type = EnchantmentType.angler } }.AsReadOnly() };
        // by default reforge and tier match
        Assert.That(key.Similarity(key), Is.GreaterThan(keyB.Similarity(key)), "extra enchants should decrease");
    }
    [Test]
    public void RecombCadyRelicLbinSimilarity()
    {
        // the issue likely has something to do with enrichments, TODO: add enrichments
        var auctionA = new SaveAuction() { FlatenedNBT = new(), Tag = "CANDY_RELIC", Tier = Tier.LEGENDARY, Category = Category.ACCESSORIES };
        var b = SniperServiceTests.Dupplicate(auctionA);
        b.FlatenedNBT.Add("rarity_upgrades", "1");
        b.Tier = Tier.MYTHIC;
        var keyA = service.KeyFromSaveAuction(auctionA);
        var keyB = service.KeyFromSaveAuction(b);
        Assert.That(keyA.Similarity(keyB), Is.LessThan(keyA.Similarity(keyA)));
    }
    [Test]
    public void IncludesLavaShellNecklaceSpecialAttribs()
    {
        var auction = new SaveAuction()
        {
            Tag = "LAVA_SHELL_NECKLACE",
            FlatenedNBT = new() { { "lifeline", "1" }, { "mana_regeneration", "1" } },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(1, Is.EqualTo(key.Modifiers.Count));
        Assert.That(key.Modifiers.Any(x => x.Key == "lifeline" && x.Value == "1"));
    }
    [Test]
    public void IncludesLavaShellNecklaceSpecialAttribCombo()
    {
        var auction = new SaveAuction()
        {
            Tag = "LAVA_SHELL_NECKLACE",
            FlatenedNBT = new() { { "lifeline", "1" }, { "mana_pool", "1" } },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(2, Is.EqualTo(key.Modifiers.Count));
    }
    [Test]
    public void IgnoresBadEnchants()
    {
        var key = new AuctionKey()
        {
            Reforge = ItemReferences.Reforge.Any,
            Enchants = new List<Enchant>() { new() { Type = EnchantmentType.execute, Lvl = 8 } }.AsReadOnly(),
            Modifiers = new(new List<KeyValuePair<string, string>>())
        };
        System.Console.WriteLine(key);
        var auction = new SaveAuction()
        {
            Enchantments = new() {
                new() { Level = 6, Type = EnchantmentType.luck },
                 new() { Level = 8, Type = EnchantmentType.execute }
             }
        };
        // by default reforge and tier match
        Assert.That(key, Is.EqualTo(service.KeyFromSaveAuction(auction)));
    }
    [Test]
    public async Task UnlockedSlotsVsLegianSimilarity()
    {
        await itemService.GetItemsAsync();
        var baseAuction = new SaveAuction()
        {
            Enchantments = new() { new(EnchantmentType.ultimate_legion, 5) },
            FlatenedNBT = new() { { "rarity_upgrades", "1" }, { "unlocked_slots", "UNIVERSAL_0" } },
            Tier = Tier.MYTHIC
        };
        var targetAuction = new SaveAuction()
        {
            Enchantments = new() { new(EnchantmentType.ultimate_legion, 5) },
            FlatenedNBT = new() { { "rarity_upgrades", "1" } },
            Tier = Tier.MYTHIC
        };
        var badAuction = new SaveAuction()
        {
            Enchantments = new() { new(EnchantmentType.growth, 5) },
            FlatenedNBT = new() { { "rarity_upgrades", "1" } },
            Tier = Tier.MYTHIC
        };
        var originkey = service.KeyFromSaveAuction(baseAuction);
        var targetKey = service.KeyFromSaveAuction(targetAuction);
        var badKey = service.KeyFromSaveAuction(badAuction);
        // by default reforge and tier match
        Assert.That(originkey.Similarity(targetKey), Is.GreaterThan(originkey.Similarity(badKey)));
    }

    [Test]
    public void Lvl103isNotbucket0()
    {
        var dragon = new SaveAuction()
        {
            Enchantments = [],
            FlatenedNBT = new() { { "exp", 27245497.685185183.ToString() }, { "candyUsed", "0" } },
            Tag = "PET_GOLDEN_DRAGON"
        };
        var key = service.KeyFromSaveAuction(dragon);
        Assert.That(key.Modifiers.First(m => m.Key == "candyUsed").Value, Is.EqualTo("0"));
        Assert.That(key.Modifiers.First(m => m.Key == "exp").Value, Is.EqualTo("0.6"));
    }

    /// <summary>
    /// Slots only accessible when converting to another item are usually not worth it
    /// and almost never reach high enough volume
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task RemovesUnaccessibleUnlockedSlots()
    {
        await itemService.GetItemsAsync();
        var baseAuction = new SaveAuction()
        {
            Tag = "POWER_WITHER_CHESTPLATE",
            Enchantments = new() { new(EnchantmentType.ultimate_legion, 5) },
            FlatenedNBT = new() { { "unlocked_slots", "COMBAT_0,DEFENSIVE_0,JASPER_0,SAPPHIRE_0" } },
            Tier = Tier.MYTHIC
        };
        var key = service.KeyFromSaveAuction(baseAuction);
        Assert.That("COMBAT_0,JASPER_0", Is.EqualTo(key.Modifiers.First().Value));
    }

    [Test]
    public void ShinyOnlyOnChestPlateAndHyperion()
    {
        var auction = new SaveAuction()
        {
            Tag = "POWER_WITHER_CHESTPLATE",
            FlatenedNBT = new() { { "is_shiny", "1" } },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(1, Is.EqualTo(key.Modifiers.Count));
        Assert.That(key.Modifiers.Any(x => x.Key == "is_shiny" && x.Value == "1"));
        auction.Tag = "HYPERION";
        key = service.KeyFromSaveAuction(auction);
        Assert.That(1, Is.EqualTo(key.Modifiers.Count));
        Assert.That(key.Modifiers.Any(x => x.Key == "is_shiny" && x.Value == "1"));
        auction.Tag = "POWER_WITHER_LEGGINGS";
        key = service.KeyFromSaveAuction(auction);
        Assert.That(0, Is.EqualTo(key.Modifiers.Count));
    }

    [Test]
    public void HigherExpIsFurther()
    {
        AuctionKey originkey = CreateFromExp("1");
        var closerKey = CreateFromExp("0");
        var furtherKey = CreateFromExp("3");
        var closerValue = originkey.Similarity(closerKey);
        System.Console.WriteLine("computing");
        Assert.That(closerValue, Is.GreaterThan(originkey.Similarity(furtherKey)));

        static AuctionKey CreateFromExp(string amount)
        {
            return new AuctionKey(null, ItemReferences.Reforge.Any, new() { new("exp", amount) }, Tier.UNKNOWN, 1);
        }
    }

    [TestCase("SWORD", EnchantmentType.ender_slayer, 6, true)]
    [TestCase("ATOMSPLIT_KATANA", EnchantmentType.ender_slayer, 6, false)]
    [TestCase("SWORD", EnchantmentType.ender_slayer, 7, false)]
    public void EnderSlayer6IgnoredOnEverythingExceptKatana(string tag, Core.Enchantment.EnchantmentType type, byte level, bool shouldIgnore)
    {
        var auction = new SaveAuction()
        {
            Tag = tag,
            Enchantments = new() { new(type, level) },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(shouldIgnore, Is.EqualTo(key.Enchants.Count == 0));
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    public void CheckTierBoostNotDropped(int level)
    {
        var auction = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            FlatenedNBT = new() { { "heldItem", "PET_ITEM_TIER_BOOST" },
                                {"candyUsed", "0"} },
        };
        var modifierList = service.KeyFromSaveAuction(auction, level).Modifiers;
        Assert.That(modifierList.Any(x => x.Value == SniperService.TierBoostShorthand));
        Assert.That(modifierList.Any(x => x.Value == "0" && x.Key == "candyUsed"));
    }

    [TestCase(0, 0)]
    [TestCase(1, 1)]
    [TestCase(2, 1)]
    public void NormalizedCandy(int amount, int target)
    {
        var auction = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            FlatenedNBT = new() { { "heldItem", "PET_ITEM_TIER_BOOST" },
                                {"candyUsed", amount.ToString()} },
        };
        var modifierList = service.KeyFromSaveAuction(auction).Modifiers;
        Assert.That(modifierList.Any(x => x.Value == SniperService.TierBoostShorthand));
        Assert.That(modifierList.Any(x => x.Value == target.ToString() && x.Key == "candyUsed"));
    }

    [Test]
    public void CheckFishingSpeedAttributeInclude()
    {
        var auction = new SaveAuction()
        {
            Tag = "FISHING_ROD",
            FlatenedNBT = new() { { "fishing_speed", "9" } },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(key.Modifiers.Any(x => x.Value == "9" && x.Key == "fishing_speed"));
    }

    [Test]
    public void LowLevelPetDifference()
    {
        var auctionHigh = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            FlatenedNBT = new() { { "exp", "1500000" },
                                {"candyUsed", "0"} },
        };
        var auction = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            FlatenedNBT = new() { { "exp", "100000" },
                                {"candyUsed", "0"} },
        };
        var keyHigh = service.KeyFromSaveAuction(auctionHigh);
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(-3, Is.GreaterThan(keyHigh.Similarity(key)), $"{keyHigh}\n{key}");
    }

    [Test]
    public void HigherEditionIsCloser()
    {
        var auction = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            FlatenedNBT = new() { { "edition", "1" } },
        };
        var key = service.KeyFromSaveAuction(auction);
        var auction2 = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            FlatenedNBT = new() { { "edition", "2000" } },
        };
        var key2 = service.KeyFromSaveAuction(auction2);
        var noEdition = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            FlatenedNBT = new(),
        };
        var key3 = service.KeyFromSaveAuction(noEdition);
        var similarity = key3.Similarity(key2);
        var lessSimilar = key3.Similarity(key);
        Assert.That(similarity, Is.GreaterThan(lessSimilar));
    }

    [Test]
    public void RunesAppart()
    {
        var clean = CreateFromLevel("0");
        clean = new AuctionKey() { Modifiers = AuctionKey.EmptyModifiers };

        var lvl1 = CreateFromLevel("1");
        var lvl2 = CreateFromLevel("2");

        Assert.That(clean.Similarity(lvl1), Is.GreaterThan(clean.Similarity(lvl2)));
        static AuctionKey CreateFromLevel(string amount)
        {
            return new AuctionKey(null, ItemReferences.Reforge.Any, new() { new("DRAGON", amount) }, Tier.EPIC, 1);
        }
    }
    [Test]
    public void IncludeRuneLevelsOnRunes()
    {
        var auction = new SaveAuction()
        {
            Tag = "RUNE_SNOW",
            FlatenedNBT = new() { { "RUNE_SNOW", "3" } },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(key.Modifiers.Any(x => x.Value == "3" && x.Key == "RUNE_SNOW"));
        auction.Tag = "ARMOR";
        key = service.KeyFromSaveAuction(auction);
        Assert.That(key.Modifiers.Count, Is.EqualTo(0), "Ignore valueless rune on armor");
    }

    [Test]
    public void EnderDragonAppart()
    {
        var clean = CreateFromExp("0", true);
        //clean.Modifiers.Clear();

        var lvl1 = CreateFromExp("1", true);
        var lvl2 = CreateFromExp("0", false);

        var simValue = clean.Similarity(lvl1);
        System.Console.WriteLine(simValue);

        Assert.That(simValue, Is.GreaterThan(clean.Similarity(lvl2)));
        static AuctionKey CreateFromExp(string amount, bool boost)
        {
            if (boost)
            {
                return new AuctionKey(null, ItemReferences.Reforge.Any, new() { new("exp", amount), new(SniperService.PetItemKey, SniperService.TierBoostShorthand) }, Tier.EPIC, 1);
            }
            return new AuctionKey(null, ItemReferences.Reforge.Any, new() { new("exp", amount) }, Tier.EPIC, 1); ;
        }
    }

    [Test]
    public void PickaxeEfficiency()
    {
        var clean = Create();

        var close = Create();
        close = new() { Tier = Tier.SPECIAL };
        var far = Create();
        far = far.WithEnchants(new List<Enchant>() { new() { Type = EnchantmentType.efficiency, Lvl = 10 } });

        var simValue = clean.Similarity(close);
        System.Console.WriteLine(simValue);

        Assert.That(simValue, Is.GreaterThan(clean.Similarity(far)));
        static AuctionKey Create()
        {
            return new AuctionKey(new(), ItemReferences.Reforge.Any, new(), Tier.VERY_SPECIAL, 1);
        }
    }

    static AuctionKey CreateWithEnchant(Core.Enchantment.EnchantmentType type, byte level)
    {
        var key = new AuctionKey(new(){new Enchant(){
                    Type = type,
                    Lvl = level
                }}, ItemReferences.Reforge.Any, null, Tier.EPIC, 1);
        return key;
    }

    [Test]
    public void SameEnchantIsCloser()
    {
        var baseKey = CreateWithEnchant(EnchantmentType.ultimate_legion, 5);

        var closer = CreateWithEnchant(EnchantmentType.ultimate_legion, 4);
        var lvl2 = CreateWithEnchant(EnchantmentType.ultimate_duplex, 4);

        var simValue = baseKey.Similarity(closer);
        System.Console.WriteLine(simValue);

        Assert.That(simValue, Is.GreaterThan(baseKey.Similarity(lvl2)));
    }
    [Test]
    public void LowerEnchantIsCloser()
    {
        var baseKey = CreateWithEnchant(EnchantmentType.ultimate_legion, 5);

        var closer = CreateWithEnchant(EnchantmentType.ultimate_legion, 4);
        var lvl2 = CreateWithEnchant(EnchantmentType.ultimate_legion, 6);

        var simValue = baseKey.Similarity(closer);
        System.Console.WriteLine(simValue);

        Assert.That(simValue, Is.GreaterThan(baseKey.Similarity(lvl2)));
    }

    [Test]
    public void SameValuableEnchantIsCloser()
    {
        var differentEnchants = new List<Enchant>(){
                new Enchant(){
                    Type = EnchantmentType.luck,
                    Lvl = 6
                },
                new Enchant(){
                    Type = EnchantmentType.critical,
                    Lvl = 6
                }
            };

        var baseKey = CreateWithEnchant(EnchantmentType.ultimate_legion, 5);
        //   baseKey.Enchants.AddRange(differentEnchants); TODO REMOVE

        var closer = CreateWithEnchant(EnchantmentType.ultimate_legion, 5);

        var lvl2 = CreateWithEnchant(EnchantmentType.ultimate_duplex, 5);
        //   lvl2.Enchants.AddRange(differentEnchants);

        var simValue = baseKey.Similarity(closer);
        System.Console.WriteLine(simValue);

        Assert.That(simValue, Is.GreaterThan(baseKey.Similarity(lvl2)));
    }

    [Test]
    public void TakeAttributeCombo()
    {
        var auction = new SaveAuction()
        {
            Tag = "MOLTEN_CLOAK",
            FlatenedNBT = new() { { "dominance", "2" },
                                {"mending", "2"} },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(2, Is.EqualTo(key.Modifiers.Count));
        Assert.That(key.Modifiers.Any(x => x.Key == "dominance" && x.Value == "2"));
    }

    [TestCase("ANYTHING", 1)]
    [TestCase("CASHMERE_JACKET", 0)]
    public void DropColorOnCashmereJacket(string tag, int expected)
    {
        var auction = new SaveAuction()
        {
            Tag = tag,
            FlatenedNBT = new() { { "color", "01:01:01" } },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(expected, Is.EqualTo(key.Modifiers.Count));
    }

    [Test]
    public void SpecialItemCombo()
    {
        var auction = new SaveAuction()
        {
            Tag = "MOLTEN_CLOAK",
            FlatenedNBT = new() { { "lifeline", "1" },
                                {"mana_pool", "2"} },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(1, Is.EqualTo(key.Modifiers.Count));
        auction.Tag = "TERROR_LEGGINGS";
        key = service.KeyFromSaveAuction(auction);
        Assert.That(2, Is.EqualTo(key.Modifiers.Count));
    }

    [Test]
    public void IngoreUltWiseOnGauntlet()
    {
        var auction = new SaveAuction()
        {
            Enchantments = new() { new(EnchantmentType.ultimate_wise, 5) },
            FlatenedNBT = new() { { "rarity_upgrades", "1" } },
            Tier = Tier.MYTHIC,
            Tag = "DEMONLORD_GAUNTLET"
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(0, Is.EqualTo(key.Enchants.Count));
    }

    [Test]
    public void InclusedsEfficiency10()
    {
        var auction = new SaveAuction()
        {
            Enchantments = new() { new(EnchantmentType.efficiency, 10) },
            Tag = "DEMONLORD_GAUNTLET"
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(1, Is.EqualTo(key.Enchants.Count));
    }

    [Test]
    public void ExcludesPower6()
    {
        var auction = new SaveAuction()
        {
            Enchantments = new() { new(EnchantmentType.power, 6) },
            Tag = "DEMONLORD_GAUNTLET"
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(0, Is.EqualTo(key.Enchants.Count));
    }

    [Test]
    public void DeseralisedMatch()
    {
        var json = "{\"Enchants\":[{\"Type\":\"cultivating\",\"Lvl\":8}],\"Reforge\":\"Any\",\"Modifiers\":[{\"Key\":\"rarity_upgrades\",\"Value\":\"1\"}],\"Tier\":\"MYTHIC\",\"Count\":1}";
        var deserilaizedKey = JsonConvert.DeserializeObject<AuctionKey>(json);
        var originalKey = new AuctionKey(new() { new() { Type = EnchantmentType.cultivating, Lvl = 8 } }, ItemReferences.Reforge.Any, new() { new("rarity_upgrades", 1.ToString()) }, Tier.MYTHIC, 1);
        var key = MessagePackSerializer.Deserialize<AuctionKey>(MessagePackSerializer.Serialize(originalKey));
        Assert.That(key, Is.EqualTo(deserilaizedKey), JsonConvert.SerializeObject(key));
        Assert.That(key, Is.EqualTo(originalKey));
        Assert.That(key.GetHashCode(), Is.EqualTo(deserilaizedKey.GetHashCode()));
        Assert.That(key.GetHashCode(), Is.EqualTo(originalKey.GetHashCode()));
    }

    [Test]
    public void IncludesFarmingForDummies()
    {
        var auction = new SaveAuction()
        {
            FlatenedNBT = new() { { "farming_for_dummies_count", "1" } },
            Tag = "FARMING_FOR_DUMMIES"
        };
        SetBazaarPrice("FARMING_FOR_DUMMIES", 1_000_000);
        var key = service.ValueKeyForTest(auction);
        Assert.That(key.ValueBreakdown.First().Value, Is.EqualTo(1_000_000));
    }
    [Test]
    public void ValuesExpertiseCorrectly()
    {
        var auction = new SaveAuction()
        {
            FlatenedNBT = new() { { "expertise_kills", "11000" } },
            Enchantments = new() { new(EnchantmentType.expertise, 9) },
            Tag = "S"
        };
        SetBazaarPrice("ENCHANTMENT_EXPERTISE_1", 4_000_000);
        var key = service.ValueKeyForTest(auction);
        Assert.That(key.ValueBreakdown.First().Value, Is.EqualTo(6_000_000), JsonConvert.SerializeObject(key.ValueBreakdown));
    }
    [Test]
    public void ValueAssignedtoRecombobulator()
    {
        var price = Random.Shared.Next(1_000_000, 10_000_000);
        var auction = new SaveAuction()
        {
            FlatenedNBT = new() { { "rarity_upgrades", "1" } },
            Tag = "KEVIN"
        };
        SetBazaarPrice("RECOMBOBULATOR_3000", price);
        var key = service.ValueKeyForTest(auction);
        Assert.That(key.ValueBreakdown.First().Value, Is.EqualTo(price));
    }

    private void SetBazaarPrice(string tag, int value, int buyValue = 0)
    {
        var sellOrder = new List<dev.SellOrder>();
        if (value > 0)
            sellOrder.Add(new dev.SellOrder() { PricePerUnit = value });
        service.UpdateBazaar(new()
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
            }
        });
    }
    //[Test]
    public void HyperionMostSimilar()
    {
        var baseAuction = new SaveAuction()
        {
            Enchantments = new() {
                    new(EnchantmentType.impaling, 3),
                    new(EnchantmentType.luck, 6),
                    new(EnchantmentType.critical, 6),
                    new(EnchantmentType.cleave, 5),
                    new(EnchantmentType.looting, 4),
                    new(EnchantmentType.smite, 7),
                    new(EnchantmentType.ender_slayer, 6),
                    new(EnchantmentType.scavenger, 4),
                    new(EnchantmentType.experience, 4),
                    new(EnchantmentType.vampirism, 6),
                    new(EnchantmentType.fire_aspect, 2),
                    new(EnchantmentType.life_steal, 4),
                    new(EnchantmentType.giant_killer, 6),
                    new(EnchantmentType.first_strike, 4),
                    new(EnchantmentType.thunderlord, 6),
                    new(EnchantmentType.ultimate_wise, 5),
                    new(EnchantmentType.cubism, 5),
                    new(EnchantmentType.champion, 4),
                    new(EnchantmentType.lethality, 6),
                    new(EnchantmentType.prosecute, 5),
                },
            FlatenedNBT = new() { { "rarity_upgrades", "1" },
                { "hpc", "15" },
                { "champion_combat_xp", "437497.3933520025" },
                { "upgrade_level", "5" },
                {"ability_scroll", "IMPLOSION_SCROLL SHADOW_WARP_SCROLL WITHER_SHIELD_SCROLL" } },
            Tier = Tier.MYTHIC,
            Reforge = ItemReferences.Reforge.withered,
            Tag = "HYPERION"
        };
        var comparedTo = SniperServiceTests.Dupplicate(baseAuction);
        comparedTo.Enchantments = new List<Core.Enchantment>() { new(EnchantmentType.ultimate_legion, 5) };
        comparedTo.HighestBidAmount = 1_000_000;
        service.AddSoldItem(SniperServiceTests.Dupplicate(comparedTo));
        service.AddSoldItem(SniperServiceTests.Dupplicate(comparedTo));
        service.AddSoldItem(SniperServiceTests.Dupplicate(comparedTo));
        service.AddSoldItem(SniperServiceTests.Dupplicate(comparedTo));
        service.FinishedUpdate();
        LowPricedAuction flip = null;
        service.FoundSnipe += (f) =>
        {
            flip = f;
        };
        var toExpensive = SniperServiceTests.Dupplicate(baseAuction);
        toExpensive.StartingBid = 890_000;
        service.TestNewAuction(toExpensive);
        // Non exact matches have to have higher profit
        Assert.That(flip, Is.Null);
        service.TestNewAuction(baseAuction);
        // uses median of the different most similar sells
        Assert.That(1_000_000, Is.EqualTo(flip.TargetPrice));
    }
}