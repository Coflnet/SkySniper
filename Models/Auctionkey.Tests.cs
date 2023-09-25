using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;
using MessagePack;
using Newtonsoft.Json;
using NUnit.Framework;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Sniper.Models;
public class AuctionkeyTests
{
    [Test]
    public void DifferentModifiersDecrease()
    {
        var key = new AuctionKey();
        var keyB = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("test", "test") } };
        // by default reforge and tier match
        Assert.Greater(key.Similarity(key), keyB.Similarity(key));
    }
    [Test]
    public void SameModsMatch()
    {
        var key = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("test", "test") } };
        var keyB = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("test", "test") } };
        // by default reforge and tier match
        Assert.AreEqual(key.Similarity(key), keyB.Similarity(key));
    }
    [Test]
    public void SameModsDecreaseFurther()
    {
        var key = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("test", "testxy") } };
        var keyB = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("test", "test") } };
        // by default reforge and tier match
        Assert.Greater(key.Similarity(key), keyB.Similarity(key));
    }
    [Test]
    public void NoModsNoError()
    {
        var key = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() };
        var keyB = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>() };
        // by default reforge and tier match
        Assert.AreEqual(key.Similarity(key), keyB.Similarity(key));
    }

    [Test]
    public void DifferentEnchantsDecrease()
    {
        var key = new AuctionKey();
        var keyB = new AuctionKey() { Enchants = new List<Enchantment>() { new Enchantment() { Lvl = 1, Type = Core.Enchantment.EnchantmentType.angler } } };
        // by default reforge and tier match
        Assert.Greater(key.Similarity(key), keyB.Similarity(key), "extra enchants should decrease");
    }
    [Test]
    public void RecombCadyRelicLbinSimilarity()
    {
        // the issue likely has something to do with enrichments, TODO: add enrichments
        var auctionA = new SaveAuction() { FlatenedNBT = new(), Tag = "CANDY_RELIC", Tier = Tier.LEGENDARY };
        var b = Services.SniperServiceTests.Dupplicate(auctionA);
        b.FlatenedNBT.Add("rarity_upgrades", "1");
        b.Tier = Tier.MYTHIC;
        var sniperService = new SniperService();
        var keyA = sniperService.KeyFromSaveAuction(auctionA);
        var keyB = sniperService.KeyFromSaveAuction(b);
        Assert.Less(keyA.Similarity(keyB), keyA.Similarity(keyA));
    }
    [Test]
    public void IgnoresBadEnchants()
    {
        var key = new AuctionKey() { Reforge = ItemReferences.Reforge.Any, Enchants = new List<Enchantment>(), Modifiers = new() };
        key.Enchants.Add(new() { Type = Core.Enchantment.EnchantmentType.execute, Lvl = 8 });
        System.Console.WriteLine(key);
        var auction = new SaveAuction()
        {
            Enchantments = new() {
                new() { Level = 6, Type = Core.Enchantment.EnchantmentType.luck },
                 new() { Level = 8, Type = Core.Enchantment.EnchantmentType.execute }
             }
        };
        var service = new SniperService();
        // by default reforge and tier match
        Assert.AreEqual(key, service.KeyFromSaveAuction(auction));
    }
    [Test]
    public void UnlockedSlotsVsLegianSimilarity()
    {
        var baseAuction = new SaveAuction()
        {
            Enchantments = new() { new(Core.Enchantment.EnchantmentType.ultimate_legion, 5) },
            FlatenedNBT = new() { { "rarity_upgrades", "1" }, { "unlocked_slots", "UNIVERSAL_0" } },
            Tier = Tier.MYTHIC
        };
        var targetAuction = new SaveAuction()
        {
            Enchantments = new() { new(Core.Enchantment.EnchantmentType.ultimate_legion, 5) },
            FlatenedNBT = new() { { "rarity_upgrades", "1" } },
            Tier = Tier.MYTHIC
        };
        var badAuction = new SaveAuction()
        {
            Enchantments = new() { new(Core.Enchantment.EnchantmentType.growth, 5) },
            FlatenedNBT = new() { { "rarity_upgrades", "1" } },
            Tier = Tier.MYTHIC
        };
        var service = new SniperService();
        var originkey = service.KeyFromSaveAuction(baseAuction);
        var targetKey = service.KeyFromSaveAuction(targetAuction);
        var badKey = service.KeyFromSaveAuction(badAuction);
        // by default reforge and tier match
        Assert.Greater(originkey.Similarity(targetKey), originkey.Similarity(badKey));
    }

    [Test]
    public void HigherExpIsFurther()
    {
        AuctionKey originkey = CreateFromExp("1");
        var closerKey = CreateFromExp("0");
        var furtherKey = CreateFromExp("3");
        var closerValue = originkey.Similarity(closerKey);
        System.Console.WriteLine("computing");
        Assert.Greater(closerValue, originkey.Similarity(furtherKey));

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
        var service = new SniperService();
        var auction = new SaveAuction()
        {
            Tag = tag,
            Enchantments = new() { new(type, level) },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.AreEqual(shouldIgnore, key.Enchants.Count == 0);
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(4)]
    [TestCase(5)]
    public void CheckTierBoostNotDropped(int level)
    {
        var service = new SniperService();
        var auction = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            FlatenedNBT = new() { { "heldItem", "PET_ITEM_TIER_BOOST" },
                                {"candyUsed", "0"} },
        };
        var modifierList = service.KeyFromSaveAuction(auction, level).Modifiers;
        Assert.IsTrue(modifierList.Any(x => x.Value == SniperService.TierBoostShorthand));
        Assert.IsTrue(modifierList.Any(x => x.Value == "0" && x.Key == "candyUsed"));
    }

    [TestCase(0, 0)]
    [TestCase(1, 1)]
    [TestCase(2, 1)]
    public void NormalizedCandy(int amount, int target)
    {
        var service = new SniperService();
        var auction = new SaveAuction()
        {
            Tag = "PET_SCATHA",
            FlatenedNBT = new() { { "heldItem", "PET_ITEM_TIER_BOOST" },
                                {"candyUsed", amount.ToString()} },
        };
        var modifierList = service.KeyFromSaveAuction(auction).Modifiers;
        Assert.IsTrue(modifierList.Any(x => x.Value == SniperService.TierBoostShorthand));
        Assert.IsTrue(modifierList.Any(x => x.Value == target.ToString() && x.Key == "candyUsed"));
    }

    [Test]
    public void CheckFishingSpeedAttributeInclude()
    {
        var service = new SniperService();
        var auction = new SaveAuction()
        {
            Tag = "FISHING_ROD",
            FlatenedNBT = new() { { "fishing_speed", "9" } },
        };
        var key = service.KeyFromSaveAuction(auction);
        Assert.IsTrue(key.Modifiers.Any(x => x.Value == "9" && x.Key == "fishing_speed"));
    }

    [Test]
    public void LowLevelPetDifference()
    {
        var service = new SniperService();
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
        Assert.Greater(-3, keyHigh.Similarity(key), $"{keyHigh}\n{key}");
    }

    [Test]
    public void HigherEditionIsCloser()
    {
        var service = new SniperService();
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
        Assert.Greater(similarity, lessSimilar);
    }

    [Test]
    public void RunesAppart()
    {
        var clean = CreateFromLevel("0");
        clean.Modifiers.Clear();

        var lvl1 = CreateFromLevel("1");
        var lvl2 = CreateFromLevel("2");

        Assert.Greater(clean.Similarity(lvl1), clean.Similarity(lvl2));
        static AuctionKey CreateFromLevel(string amount)
        {
            return new AuctionKey(null, ItemReferences.Reforge.Any, new() { new("DRAGON", amount) }, Tier.EPIC, 1);
        }
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

        Assert.Greater(simValue, clean.Similarity(lvl2));
        static AuctionKey CreateFromExp(string amount, bool boost)
        {
            var key = new AuctionKey(null, ItemReferences.Reforge.Any, new() { new("exp", amount) }, Tier.EPIC, 1);
            if (boost)
            {
                key.Modifiers.Add(new(SniperService.PetItemKey, SniperService.TierBoostShorthand));
            }
            return key;
        }
    }

    [Test]
    public void PickaxeEfficiency()
    {
        var clean = Create();

        var close = Create();
        close.Tier = Tier.SPECIAL;
        var far = Create();
        far.Enchants.Add(new Enchantment() { Type = Core.Enchantment.EnchantmentType.efficiency, Lvl = 10 });

        var simValue = clean.Similarity(close);
        System.Console.WriteLine(simValue);

        Assert.Greater(simValue, clean.Similarity(far));
        static AuctionKey Create()
        {
            return new AuctionKey(new(), ItemReferences.Reforge.Any, new(), Tier.VERY_SPECIAL, 1);
        }
    }

    static AuctionKey CreateWithEnchant(Core.Enchantment.EnchantmentType type, byte level)
    {
        var key = new AuctionKey(new(){new Enchantment(){
                    Type = type,
                    Lvl = level
                }}, ItemReferences.Reforge.Any, null, Tier.EPIC, 1);
        return key;
    }

    [Test]
    public void SameEnchantIsCloser()
    {
        var baseKey = CreateWithEnchant(Core.Enchantment.EnchantmentType.ultimate_legion, 5);

        var closer = CreateWithEnchant(Core.Enchantment.EnchantmentType.ultimate_legion, 4);
        var lvl2 = CreateWithEnchant(Core.Enchantment.EnchantmentType.ultimate_duplex, 4);

        var simValue = baseKey.Similarity(closer);
        System.Console.WriteLine(simValue);

        Assert.Greater(simValue, baseKey.Similarity(lvl2));
    }
    [Test]
    public void LowerEnchantIsCloser()
    {
        var baseKey = CreateWithEnchant(Core.Enchantment.EnchantmentType.ultimate_legion, 5);

        var closer = CreateWithEnchant(Core.Enchantment.EnchantmentType.ultimate_legion, 4);
        var lvl2 = CreateWithEnchant(Core.Enchantment.EnchantmentType.ultimate_legion, 6);

        var simValue = baseKey.Similarity(closer);
        System.Console.WriteLine(simValue);

        Assert.Greater(simValue, baseKey.Similarity(lvl2));
    }

    [Test]
    public void SameValuableEnchantIsCloser()
    {
        var differentEnchants = new List<Enchantment>(){
                new Enchantment(){
                    Type = Core.Enchantment.EnchantmentType.luck,
                    Lvl = 6
                },
                new Enchantment(){
                    Type = Core.Enchantment.EnchantmentType.critical,
                    Lvl = 6
                }
            };

        var baseKey = CreateWithEnchant(Core.Enchantment.EnchantmentType.ultimate_legion, 5);
        baseKey.Enchants.AddRange(differentEnchants);

        var closer = CreateWithEnchant(Core.Enchantment.EnchantmentType.ultimate_legion, 5);

        var lvl2 = CreateWithEnchant(Core.Enchantment.EnchantmentType.ultimate_duplex, 5);
        lvl2.Enchants.AddRange(differentEnchants);

        var simValue = baseKey.Similarity(closer);
        System.Console.WriteLine(simValue);

        Assert.Greater(simValue, baseKey.Similarity(lvl2));
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
        var service = new SniperService();
        var key = service.KeyFromSaveAuction(auction);
        Assert.AreEqual(2, key.Modifiers.Count);
        Assert.IsTrue(key.Modifiers.Any(x => x.Key == "dominance" && x.Value == "2"));
    }

    [Test]
    public void IngoreUltWiseOnGauntlet()
    {
        var auction = new SaveAuction()
        {
            Enchantments = new() { new(Core.Enchantment.EnchantmentType.ultimate_wise, 5) },
            FlatenedNBT = new() { { "rarity_upgrades", "1" } },
            Tier = Tier.MYTHIC,
            Tag = "DEMONLORD_GAUNTLET"
        };
        var service = new SniperService();
        var key = service.KeyFromSaveAuction(auction);
        Assert.AreEqual(0, key.Enchants.Count);
    }

    [Test]
    public void InclusedsEfficiency10()
    {
        var auction = new SaveAuction()
        {
            Enchantments = new() { new(Core.Enchantment.EnchantmentType.efficiency, 10) },
            Tag = "DEMONLORD_GAUNTLET"
        };
        var service = new SniperService();
        var key = service.KeyFromSaveAuction(auction);
        Assert.AreEqual(1, key.Enchants.Count);
    }

    [Test]
    public void ExcludesPower6()
    {
        var auction = new SaveAuction()
        {
            Enchantments = new() { new(Core.Enchantment.EnchantmentType.power, 6) },
            Tag = "DEMONLORD_GAUNTLET"
        };
        var service = new SniperService();
        var key = service.KeyFromSaveAuction(auction);
        Assert.AreEqual(0, key.Enchants.Count);
    }

    [Test]
    public void DeseralisedMatch()
    {
        var json = "{\"Enchants\":[{\"Type\":\"cultivating\",\"Lvl\":8}],\"Reforge\":\"Any\",\"Modifiers\":[{\"Key\":\"rarity_upgrades\",\"Value\":\"1\"}],\"Tier\":\"MYTHIC\",\"Count\":1}";
        var deserilaizedKey = JsonConvert.DeserializeObject<AuctionKey>(json);
        var originalKey = new AuctionKey(new(){new(){Type=EnchantmentType.cultivating, Lvl=8}}, ItemReferences.Reforge.Any, new() { new("rarity_upgrades", "1") }, Tier.MYTHIC, 1);
        var key = MessagePackSerializer.Deserialize<AuctionKey>(MessagePackSerializer.Serialize(originalKey));
        Assert.AreEqual(key, deserilaizedKey, JsonConvert.SerializeObject(key));
        Assert.AreEqual(key, originalKey);
        Assert.AreEqual(key.GetHashCode(), deserilaizedKey.GetHashCode());
        Assert.AreEqual(key.GetHashCode(), originalKey.GetHashCode());
    }
    //[Test]
    public void HyperionMostSimilar()
    {
        var baseAuction = new SaveAuction()
        {
            Enchantments = new() {
                    new(Core.Enchantment.EnchantmentType.impaling, 3),
                    new(Core.Enchantment.EnchantmentType.luck, 6),
                    new(Core.Enchantment.EnchantmentType.critical, 6),
                    new(Core.Enchantment.EnchantmentType.cleave, 5),
                    new(Core.Enchantment.EnchantmentType.looting, 4),
                    new(Core.Enchantment.EnchantmentType.smite, 7),
                    new(Core.Enchantment.EnchantmentType.ender_slayer, 6),
                    new(Core.Enchantment.EnchantmentType.scavenger, 4),
                    new(Core.Enchantment.EnchantmentType.experience, 4),
                    new(Core.Enchantment.EnchantmentType.vampirism, 6),
                    new(Core.Enchantment.EnchantmentType.fire_aspect, 2),
                    new(Core.Enchantment.EnchantmentType.life_steal, 4),
                    new(Core.Enchantment.EnchantmentType.giant_killer, 6),
                    new(Core.Enchantment.EnchantmentType.first_strike, 4),
                    new(Core.Enchantment.EnchantmentType.thunderlord, 6),
                    new(Core.Enchantment.EnchantmentType.ultimate_wise, 5),
                    new(Core.Enchantment.EnchantmentType.cubism, 5),
                    new(Core.Enchantment.EnchantmentType.champion, 4),
                    new(Core.Enchantment.EnchantmentType.lethality, 6),
                    new(Core.Enchantment.EnchantmentType.PROSECUTE, 5),
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
        var comparedTo = Services.SniperServiceTests.Dupplicate(baseAuction);
        comparedTo.Enchantments = new List<Core.Enchantment>() { new(Core.Enchantment.EnchantmentType.ultimate_legion, 5) };
        comparedTo.HighestBidAmount = 1_000_000;
        var service = new SniperService();
        service.AddSoldItem(Services.SniperServiceTests.Dupplicate(comparedTo));
        service.AddSoldItem(Services.SniperServiceTests.Dupplicate(comparedTo));
        service.AddSoldItem(Services.SniperServiceTests.Dupplicate(comparedTo));
        service.AddSoldItem(Services.SniperServiceTests.Dupplicate(comparedTo));
        service.FinishedUpdate();
        LowPricedAuction flip = null;
        service.FoundSnipe += (f) =>
        {
            flip = f;
        };
        var toExpensive = Services.SniperServiceTests.Dupplicate(baseAuction);
        toExpensive.StartingBid = 890_000;
        service.TestNewAuction(toExpensive);
        // Non exact matches have to have higher profit
        Assert.IsNull(flip);
        service.TestNewAuction(baseAuction);
        // uses median of the different most similar sells
        Assert.AreEqual(1_000_000, flip.TargetPrice);
    }
}