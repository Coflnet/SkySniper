using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Sniper.Models;
using hypixel;
using Microsoft.VisualBasic;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services
{
    public class SniperServiceTests
    {
        SaveAuction firstAuction;
        SaveAuction secondAuction;
        SaveAuction highestValAuction;
        SniperService service;
        List<LowPricedAuction> found = new ();
        [SetUp]
        public void Setup()
        {
            firstAuction = new hypixel.SaveAuction()
            {
                Tag = "1",
                FlatenedNBT = new System.Collections.Generic.Dictionary<string, string>() { { "skin", "bear" } },
                StartingBid = 900,
                HighestBidAmount = 900,
                UId = 4,
                AuctioneerId = "12aaa"
            };
            secondAuction = new hypixel.SaveAuction()
            {
                Tag = "1",
                FlatenedNBT = new System.Collections.Generic.Dictionary<string, string>() { { "skin", "bear" } },
                StartingBid = 700,
                HighestBidAmount = 700,
                UId = 3,
                AuctioneerId = "12bbb"
            };


            highestValAuction = new hypixel.SaveAuction()
            {
                Tag = "1",
                FlatenedNBT = new System.Collections.Generic.Dictionary<string, string>() { { "skin", "bear" } },
                StartingBid = 1000,
                HighestBidAmount = 1000,
                UId = 5,
                AuctioneerId = "12c144"

            };
            SniperService.MIN_TARGET = 0;
            service = new SniperService();
            
            found = new List<LowPricedAuction>();
            service.FoundSnipe += found.Add;
        }
        [Test]
        public void UsesLbinFirst()
        {
            var found = new List<LowPricedAuction>();
            service.FoundSnipe += found.Add;

            service.AddSoldItem(highestValAuction);
            service.AddSoldItem(Dupplicate(highestValAuction));
            service.AddSoldItem(Dupplicate(firstAuction));


            service.TestNewAuction(firstAuction);
            service.TestNewAuction(secondAuction);
            Assert.AreEqual(1000, found.First().TargetPrice);
            Assert.AreEqual(LowPricedAuction.FinderType.SNIPER_MEDIAN, found.First().Finder);
            Assert.AreEqual(900, found.Last().TargetPrice);
            Assert.AreEqual(LowPricedAuction.FinderType.SNIPER, found.Last().Finder);
            // first is sold
            service.AddSoldItem(firstAuction);
            service.TestNewAuction(secondAuction);
            Assert.AreEqual(900, found.Last().TargetPrice);
            Assert.AreEqual(LowPricedAuction.FinderType.SNIPER_MEDIAN, found.Last().Finder);
        }

        private static SaveAuction Dupplicate(SaveAuction origin)
        {
            return new SaveAuction(origin)
            {
                Uuid = new System.Random().Next().ToString(),
                UId = new System.Random().Next(),
                AuctioneerId = new System.Random().Next().ToString(),
                FlatenedNBT = origin.FlatenedNBT
            };
        }

        [Test]
        public void FallbackOnNomatch()
        {
            service.AddSoldItem(highestValAuction);
            service.AddSoldItem(Dupplicate(highestValAuction));
            service.AddSoldItem(Dupplicate(highestValAuction));
            service.TestNewAuction(highestValAuction);
            var anotherAuction = new SaveAuction(highestValAuction)
            { UId = 563, StartingBid = 500, AuctioneerId = "00000", FlatenedNBT = highestValAuction.FlatenedNBT };
            //anotherAuction.FlatenedNBT["exp"] = "50000";
            service.TestNewAuction(anotherAuction);
            Assert.AreEqual(1000, found.Last().TargetPrice);
        }


        [Test]
        public void UsesMedianCorrectly()
        {
            service.AddSoldItem(highestValAuction);
            var anotherAuction = new SaveAuction(highestValAuction)
            { UId = 563, HighestBidAmount = 5000, AuctioneerId = "00000", FlatenedNBT = highestValAuction.FlatenedNBT };
            service.AddSoldItem(anotherAuction);
            service.AddSoldItem(secondAuction);
            // prices: 5000,1000,700
            service.TestNewAuction(firstAuction);
            Assert.AreEqual(1000, found.Last().TargetPrice);
        }

        [Test]
        public void FallbackToSecondLbin()
        {

        }


        [Test]
        public void RandomEnchantLbin()
        {
            var a = Dupplicate(highestValAuction);
            var targetEnchant = new hypixel.Enchantment(hypixel.Enchantment.EnchantmentType.ultimate_chimera, 1);
            a.Enchantments = new List<hypixel.Enchantment>(){
                targetEnchant
            };
            service.AddSoldItem(Dupplicate(a));
            service.AddSoldItem(Dupplicate(a));
            service.AddSoldItem(Dupplicate(a));

            a.Enchantments = new List<hypixel.Enchantment>(){
                targetEnchant,
                new hypixel.Enchantment(hypixel.Enchantment.EnchantmentType.sharpness, 6)
            };
            a.StartingBid = 5;
            service.TestNewAuction(a);
            Assert.AreEqual(1000, found.First().TargetPrice);
        }


        [Test]
        public void LoadTest()
        {
            var start = Stopwatch.StartNew();
            for (int i = 0; i < 1000; i++)
            {
                service.TestNewAuction(firstAuction);
            }
            Assert.Less(start.ElapsedMilliseconds, 6);
        }
    }

    public class MockPersistenceManager : IPersitanceManager
    {
        public Task LoadLookups(SniperService service)
        {
            return Task.CompletedTask;
        }

        public Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            return Task.CompletedTask;
        }
    }
}