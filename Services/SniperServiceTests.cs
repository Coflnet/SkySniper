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
        [SetUp]
        public void Setup()
        {
            firstAuction = new hypixel.SaveAuction()
            {
                Tag = "1",
                FlatenedNBT = new System.Collections.Generic.Dictionary<string, string>() { { "skin", "bear" } },
                StartingBid = 900,
                UId = 4,
                AuctioneerId = "12aaa"
            };
            secondAuction = new hypixel.SaveAuction()
            {
                Tag = "1",
                FlatenedNBT = new System.Collections.Generic.Dictionary<string, string>() { { "skin", "bear" } },
                StartingBid = 700,
                UId = 3,
                AuctioneerId = "12bbb"
            };


            highestValAuction = new hypixel.SaveAuction()
            {
                Tag = "1",
                FlatenedNBT = new System.Collections.Generic.Dictionary<string, string>() { { "skin", "bear" } },
                StartingBid = 1000,
                UId = 5,
                AuctioneerId = "12c144"

            };
        }
        [Test]
        public void UsesLbinFirst()
        {
            var service = new SniperService();
            var found = new List<LowPricedAuction>();
            service.FoundSnipe += found.Add;

            service.AddSoldItem(highestValAuction);


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
        [Test]
        public void FallbackOnNomatch()
        {
            var service = new SniperService();
            var found = new List<LowPricedAuction>();
            service.FoundSnipe += found.Add;
            service.TestNewAuction(highestValAuction);
            var anotherAuction = new SaveAuction(highestValAuction)
            { UId = 563, StartingBid = 500, AuctioneerId = "00000", FlatenedNBT = highestValAuction.FlatenedNBT };
            anotherAuction.FlatenedNBT["exp"] = "50000";
            service.TestNewAuction(anotherAuction);
            Assert.AreEqual(1000, found.Last().TargetPrice);
        }


        [Test]
        public void UsesMedianCorrectly()
        {
            var service = new SniperService();
            var found = new List<LowPricedAuction>();
            service.FoundSnipe += found.Add;
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
        public void LoadTest()
        {
            var service = new SniperService();
            var start = Stopwatch.StartNew();
            for (int i = 0; i < 1000; i++)
            {
                service.TestNewAuction(firstAuction);
            }
            Assert.Less(start.ElapsedMilliseconds, 5);
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