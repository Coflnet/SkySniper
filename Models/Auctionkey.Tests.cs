using System.Collections.Generic;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Models
{
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
                FlatenedNBT = new() { { "rarity_upgrades", "1" }, { "unlocked_slots", "UNIVERSAL_0" } },
                Tier = Tier.MYTHIC
            };
            var service = new SniperService();
            var originkey = service.KeyFromSaveAuction(baseAuction);
            var targetKey = service.KeyFromSaveAuction(targetAuction);
            var badKey = service.KeyFromSaveAuction(badAuction);
            // by default reforge and tier match
            Assert.Greater(originkey.Similarity(targetKey), originkey.Similarity(badKey));
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

}