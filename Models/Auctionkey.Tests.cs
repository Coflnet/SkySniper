using System.Collections.Generic;
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
            var keyB = new AuctionKey() { Modifiers = new List<KeyValuePair<string, string>>()  };
            // by default reforge and tier match
            Assert.AreEqual(key.Similarity(key), keyB.Similarity(key));
        }
        [Test]
        public void DifferentEnchantsDecrease()
        {
            var key = new AuctionKey();
            var keyB = new AuctionKey() { Enchants = new List<Enchantment>() { new Enchantment() { Lvl = 1, Type = Core.Enchantment.EnchantmentType.angler } } };
            // by default reforge and tier match
            Assert.Greater(key.Similarity(key), keyB.Similarity(key));
        }
    }

}