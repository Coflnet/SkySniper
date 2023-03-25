using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using dev;
using Microsoft.VisualBasic;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services
{
    public class SniperServiceTests
    {
        SaveAuction firstAuction;
        SaveAuction secondAuction;
        SaveAuction highestValAuction;
        SniperService service;
        List<LowPricedAuction> found = new();
        [SetUp]
        public void Setup()
        {
            firstAuction = new SaveAuction()
            {
                Tag = "1",
                FlatenedNBT = new System.Collections.Generic.Dictionary<string, string>() { { "skin", "bear" } },
                StartingBid = 900,
                HighestBidAmount = 900,
                UId = 4,
                AuctioneerId = "12aaa"
            };
            secondAuction = new SaveAuction()
            {
                Tag = "1",
                FlatenedNBT = new System.Collections.Generic.Dictionary<string, string>() { { "skin", "bear" } },
                StartingBid = 700,
                HighestBidAmount = 700,
                UId = 3,
                AuctioneerId = "12bbb"
            };


            highestValAuction = new SaveAuction()
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
            service.State = SniperState.Ready;

            AddVolume(highestValAuction);
            service.AddSoldItem(Dupplicate(firstAuction));


            service.TestNewAuction(firstAuction);
            Assert.AreEqual(1000, found.First().TargetPrice);
            Assert.AreEqual(LowPricedAuction.FinderType.SNIPER_MEDIAN, found.First().Finder);
            service.FinishedUpdate();
            service.TestNewAuction(secondAuction);
            var flip = found.Skip(2).First();
            Assert.AreEqual(900, flip.TargetPrice);
            Assert.AreEqual(LowPricedAuction.FinderType.SNIPER, flip.Finder);
            // first is sold
            service.AddSoldItem(firstAuction);
            service.TestNewAuction(secondAuction);
            Assert.AreEqual(LowPricedAuction.FinderType.STONKS, found.Last().Finder);
            Assert.AreEqual(LowPricedAuction.FinderType.SNIPER_MEDIAN, found.AsEnumerable().Reverse().Skip(1).First().Finder);
            Assert.AreEqual(810, found.Last().TargetPrice, JsonConvert.SerializeObject(found, Formatting.Indented));
        }

        public static SaveAuction Dupplicate(SaveAuction origin)
        {
            return new SaveAuction(origin)
            {
                Uuid = new System.Random().Next().ToString(),
                UId = new System.Random().Next(),
                AuctioneerId = new System.Random().Next().ToString(),
                FlatenedNBT = new Dictionary<string, string>(origin.FlatenedNBT),
                Enchantments = origin.Enchantments == null ? null : new(origin.Enchantments)
            };
        }

        [Test]
        [TestCase("400001", "0")]
        [TestCase("4225539", "1")]
        [TestCase("9700001", "2")]
        [TestCase("25353220", "5")]
        [TestCase("25353230", "6")]
        [TestCase("25770000000", "6")]
        public void Grouping(string input, string expected)
        {
            var a = SniperService.NormalizeNumberTo(new KeyValuePair<string, string>("a", input), 4_225_538, 6);
            Assert.AreEqual(expected, a.Value);
        }

        [Test]
        public void FallbackOnNoEnchmatch()
        {
            highestValAuction.FlatenedNBT = new Dictionary<string, string>();
            highestValAuction.Enchantments = new List<Core.Enchantment>();
            AddVolume(highestValAuction);
            service.TestNewAuction(highestValAuction);
            var anotherAuction = new SaveAuction(highestValAuction)
            { UId = 563, StartingBid = 500, AuctioneerId = "00000", FlatenedNBT = highestValAuction.FlatenedNBT };

            anotherAuction.Enchantments = new(){
                new Core.Enchantment(Core.Enchantment.EnchantmentType.sharpness,7),
                new Core.Enchantment(Core.Enchantment.EnchantmentType.critical,6)
            };
            service.TestNewAuction(anotherAuction);
            Assert.AreEqual(1000, found.Last().TargetPrice);
        }
        [Test]
        public void FallbackOnNomatchLevel2()
        {
            AddVolume(highestValAuction);
            service.TestNewAuction(highestValAuction);
            var anotherAuction = new SaveAuction(highestValAuction)
            { UId = 563, StartingBid = 500, AuctioneerId = "00000", FlatenedNBT = highestValAuction.FlatenedNBT };
            //anotherAuction.FlatenedNBT["exp"] = "50000";
            service.TestNewAuction(anotherAuction);
            Assert.AreEqual(1000, found.Last().TargetPrice);
        }
        [Test]
        public void CheckBelowHigherTier()
        {
            highestValAuction.Tier = Tier.MYTHIC;
            highestValAuction.HighestBidAmount = 1000000;
            AddVolume(highestValAuction);
            highestValAuction.Tier = Tier.LEGENDARY;
            highestValAuction.HighestBidAmount = 50000000;
            AddVolume(highestValAuction);
            highestValAuction.HighestBidAmount = 5000;
            service.TestNewAuction(highestValAuction);
            Assert.AreEqual(1000000, found.Last().TargetPrice);
        }
        /// <summary>
        /// Checks if references with more valuable things are cheaper
        /// </summary>
        [Test]
        public void CheckBelowMoreEnchants()
        {
            highestValAuction.Enchantments = new List<Core.Enchantment>() {
                new Core.Enchantment(Core.Enchantment.EnchantmentType.sharpness,7),
                new Core.Enchantment(Core.Enchantment.EnchantmentType.ultimate_legion,5),
                new Core.Enchantment(Core.Enchantment.EnchantmentType.critical,7),
            };
            highestValAuction.HighestBidAmount = 1000000;
            AddVolume(highestValAuction);
            highestValAuction.Enchantments.RemoveAt(1);
            highestValAuction.HighestBidAmount = 50000000;
            AddVolume(highestValAuction);
            highestValAuction.HighestBidAmount = 5000;
            service.TestNewAuction(highestValAuction);
            Assert.AreEqual(1000000, found.First().TargetPrice);
        }

        private void AddVolume(SaveAuction toAdd)
        {
            service.AddSoldItem(Dupplicate(toAdd));
            service.AddSoldItem(Dupplicate(toAdd));
            service.AddSoldItem(Dupplicate(toAdd));
            service.AddSoldItem(Dupplicate(toAdd));
        }

        [Test]
        public void UsesMedianCorrectly()
        {
            service.AddSoldItem(highestValAuction);
            var anotherAuction = new SaveAuction(highestValAuction)
            { UId = 563, HighestBidAmount = 5000, AuctioneerId = "00000", FlatenedNBT = highestValAuction.FlatenedNBT };
            service.AddSoldItem(anotherAuction);
            service.AddSoldItem(secondAuction);
            service.AddSoldItem(Dupplicate(highestValAuction));
            // prices: 5000,5000,1000,700
            service.TestNewAuction(firstAuction);
            Assert.AreEqual(1000, found.Last().TargetPrice);
        }

        [Test]
        public void TakesClosestCake()
        {
            AuctionKey key = CreateKey(252, 4);

            Assert.Greater(key.Similarity(CreateKey(250, 0)), key.Similarity(CreateKey(2, 0)));

            AuctionKey CreateKey(int year, int drop)
            {
                var auction = new SaveAuction()
                {
                    Tag = "1",
                    FlatenedNBT = new() { { "new_years_cake", year.ToString() } },
                    StartingBid = 900,
                    HighestBidAmount = 900,
                    UId = System.Random.Shared.NextInt64(),
                    AuctioneerId = "12aaa"
                };
                var key = service.KeyFromSaveAuction(auction, drop);
                return key;
            }
        }


        [Test]
        public void RandomEnchantLbin()
        {
            var a = Dupplicate(highestValAuction);
            var targetEnchant = new Core.Enchantment(Core.Enchantment.EnchantmentType.ultimate_chimera, 1);
            a.Enchantments = new List<Core.Enchantment>(){
                targetEnchant
            };
            service.AddSoldItem(Dupplicate(a));
            service.AddSoldItem(Dupplicate(a));
            service.AddSoldItem(Dupplicate(a));
            service.AddSoldItem(Dupplicate(a));

            a.Enchantments = new List<Core.Enchantment>(){
                targetEnchant,
                new Core.Enchantment(Core.Enchantment.EnchantmentType.sharpness, 6)
            };
            a.StartingBid = 5;
            service.TestNewAuction(a);
            Assert.AreEqual(1000, found.First().TargetPrice);
        }

        [Test]
        public void LbinUpdateTest()
        {
            highestValAuction.StartingBid = 5;
            var a = Dupplicate(highestValAuction);
            a.HighestBidAmount = 500;
            var b = Dupplicate(highestValAuction);
            b.HighestBidAmount = 1000;
            var c = Dupplicate(highestValAuction);
            c.HighestBidAmount = 700;
            var d = Dupplicate(highestValAuction);
            d.HighestBidAmount = 900;
            service.TestNewAuction(a);
            service.TestNewAuction(b);
            service.TestNewAuction(c);
            service.TestNewAuction(d);
            service.FinishedUpdate();

            service.AddSoldItem(a);
            service.AddSoldItem(b);
            service.AddSoldItem(c);

            var price = service.GetPrice(a);

            Assert.AreEqual(900, price.Lbin.Price);
        }
        [Test]
        public void AdjustsMedian()
        {
            highestValAuction.FlatenedNBT = new();
            var part = Dupplicate(highestValAuction);
            part.Tag = "COMPONENT";
            part.HighestBidAmount = 100;
            AddVolume(part);

            var drill = Dupplicate(highestValAuction);
            drill.Tag = "DRILL";
            drill.FlatenedNBT["drill_part_engine"] = "component";
            AddVolume(drill);
            service.FinishedUpdate();
            drill.FlatenedNBT = new();
            var estimate = service.GetPrice(drill);
            Assert.AreEqual(900, estimate.Median, "1000 base - 100 component");
            Assert.AreEqual(" Any [drill_part_engine, component] UNKNOWN 0- component", estimate.MedianKey);

        }

        [Test]
        public void SubstractsEnchants()
        {
            highestValAuction.FlatenedNBT = new();
            var moreEnchants = Dupplicate(highestValAuction);
            moreEnchants.HighestBidAmount = 100_000_000;
            moreEnchants.Enchantments = new List<Core.Enchantment>(){
                new Core.Enchantment(Core.Enchantment.EnchantmentType.sharpness,7)
            };
            AddVolume(moreEnchants);
            service.UpdateBazaar(new()
            {
                Products = new(){
                new (){
                    ProductId = "ENCHANTMENT_SHARPNESS_7",
                    SellSummary = new(){
                        new (){
                            PricePerUnit = 49_000_000
                        }
                    }
                }
            }
            });

            var toTest = Dupplicate(highestValAuction);
            service.FinishedUpdate();
            service.State = SniperState.Ready;
            service.TestNewAuction(toTest);
            service.FinishedUpdate();
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(45900000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("sharpness_7 (49000000)", estimate.AdditionalProps["missingEnchants"]);
        }

        [Test]
        public void StonksReforgeDifference()
        {
            highestValAuction.FlatenedNBT = new();
            var reforge = Dupplicate(highestValAuction);
            reforge.HighestBidAmount = 10_000_000;
            reforge.Reforge = ItemReferences.Reforge.aote_stone;
            AddVolume(reforge);
            service.UpdateBazaar(new()
            {
                Products = new(){
                new (){
                    ProductId = "AOTE_STONE",
                    SellSummary = new(){
                        new (){
                            PricePerUnit = 4_000_000
                        }
                    }
                }
            }
            });

            var toTest = Dupplicate(highestValAuction);
            service.FinishedUpdate();
            service.State = SniperState.Ready;
            service.TestNewAuction(toTest);
            service.FinishedUpdate();
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(2500000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("aote_stone -> None (6500000)", estimate.AdditionalProps["reforge"]);
        }

        [Test]
        public void SubstractsStarCost()
        {
            highestValAuction.FlatenedNBT = new();
            var upgradeLvl9 = Dupplicate(highestValAuction);
            upgradeLvl9.HighestBidAmount = 100_000_000;
            upgradeLvl9.FlatenedNBT["upgrade_level"] = "9";
            AddVolume(upgradeLvl9);
            service.UpdateBazaar(new()
            {
                Products = new(){
                new (){
                    ProductId = "FOURTH_MASTER_STAR",
                    SellSummary = new(){
                        new (){
                            PricePerUnit = 49_000_000
                        }
                    }
                },
                new (){
                    ProductId = "THIRD_MASTER_STAR",
                    SellSummary = new(){
                        new (){
                            PricePerUnit = 19_000_000
                        }
                    }
                }
            }
            });
            var toTest = Dupplicate(highestValAuction);
            service.FinishedUpdate();
            service.State = SniperState.Ready;
            toTest.FlatenedNBT["upgrade_level"] = "7";
            service.TestNewAuction(toTest);
            service.FinishedUpdate();
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(28800000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("upgrade_level:9 (68000000)", estimate.AdditionalProps["missingModifiers"], "Third and fourth master star combned cost 68000000");
        }

        [Test]
        public void AdjustDueToCount()
        {
            highestValAuction.FlatenedNBT = new();
            var biggerStack = Dupplicate(highestValAuction);
            biggerStack.Count = 3;
            biggerStack.HighestBidAmount = 100_000_000;
            AddVolume(biggerStack);
            
            var toTest = Dupplicate(highestValAuction);
            toTest.Count = 1;
            service.FinishedUpdate();
            service.State = SniperState.Ready;
            service.TestNewAuction(toTest);
            service.FinishedUpdate();
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(30000000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("2 (60000000)", estimate.AdditionalProps["countDiff"]);
        }

        [Test]
        public void LbinSimilarity()
        {
            highestValAuction.StartingBid = 5;
            var a = Dupplicate(highestValAuction);
            a.HighestBidAmount = 501;
            a.FlatenedNBT["exp"] = "0";
            a.FlatenedNBT["candyUsed"] = "2";
            var b = Dupplicate(highestValAuction);
            b.HighestBidAmount = 1000;
            b.FlatenedNBT["heldItem"] = "something";
            var c = Dupplicate(highestValAuction);
            c.HighestBidAmount = 700;
            c.Enchantments = new List<Core.Enchantment>(){
                new Core.Enchantment(Core.Enchantment.EnchantmentType.sharpness,6)
            };
            var d = Dupplicate(highestValAuction);
            d.HighestBidAmount = 900;
            service.TestNewAuction(a);
            service.TestNewAuction(b);
            service.TestNewAuction(c);
            service.FinishedUpdate();
            service.TestNewAuction(d);

            highestValAuction.FlatenedNBT["skin"] = "something";
            highestValAuction.FlatenedNBT["heldItem"] = "something";
            var price = service.GetPrice(highestValAuction);

            Assert.AreEqual(1000, price.Lbin.Price);
        }

        [Test]
        public void HigherLvlPetLbinTest()
        {
            highestValAuction.FlatenedNBT["exp"] = "10000";
            highestValAuction.StartingBid = 5;
            var badActiveLbin = Dupplicate(highestValAuction);

            badActiveLbin.HighestBidAmount = 1500;
            var cheaperHigherLevel = Dupplicate(highestValAuction);
            cheaperHigherLevel.FlatenedNBT["exp"] = "10000000";
            cheaperHigherLevel.HighestBidAmount = 500;
            var volumeHelp = Dupplicate(highestValAuction);
            volumeHelp.HighestBidAmount = 1900;
            service.AddSoldItem(Dupplicate(volumeHelp));
            service.AddSoldItem(Dupplicate(volumeHelp));
            service.AddSoldItem(Dupplicate(volumeHelp));
            service.AddSoldItem(Dupplicate(volumeHelp));
            service.TestNewAuction(badActiveLbin);
            service.TestNewAuction(cheaperHigherLevel);
            service.FinishedUpdate();
            LowPricedAuction found = null;
            var lowAssert = (LowPricedAuction s) =>
            {
                if (s.Finder == LowPricedAuction.FinderType.SNIPER)
                    found = s;
                System.Console.WriteLine(JsonConvert.SerializeObject(s, Formatting.Indented));
            };
            service.FoundSnipe += lowAssert;
            var testFlip = Dupplicate(highestValAuction);
            testFlip.StartingBid = 700;
            service.TestNewAuction(testFlip);
            Assert.IsNull(found, "low priced should not be triggered because higer level lower price exists");


            testFlip.StartingBid = 200;
            service.TestNewAuction(testFlip);
            Assert.IsNotNull(found, "flip should have been found as its lower than higher level");
            Assert.AreEqual(500, found.TargetPrice, "lowest bin price should be used");

        }

        [Test]
        public void ComponetExtraValue()
        {
            SaveAuction drill = SetupDrill();
            LowPricedAuction found = null;
            var lowAssert = (LowPricedAuction s) =>
            {
                found = s;
                Assert.AreEqual(2000, s.TargetPrice, "extra value should be added to price");
                System.Console.WriteLine(JsonConvert.SerializeObject(s));
            };
            service.FoundSnipe += lowAssert;
            service.TestNewAuction(Dupplicate(drill));
            service.FinishedUpdate();
            service.PrintLogQueue();
            Assert.IsNotNull(found, "flip with extra value should pop up");
        }

        private SaveAuction SetupDrill()
        {
            var part = Dupplicate(highestValAuction);
            part.Tag = "COMPONENT";
            AddVolume(part);

            var drill = Dupplicate(highestValAuction);
            drill.Tag = "DRILL";
            AddVolume(drill);
            service.FinishedUpdate();

            drill.FlatenedNBT["drill_part_engine"] = "component";
            return drill;
        }

        [Test]
        public void GemExtraValue()
        {
            highestValAuction.FlatenedNBT = new();
            service.AddSoldItem(Dupplicate(highestValAuction));
            service.AddSoldItem(Dupplicate(highestValAuction));
            service.AddSoldItem(Dupplicate(highestValAuction));
            service.AddSoldItem(Dupplicate(highestValAuction));
            service.FinishedUpdate();
            highestValAuction.FlatenedNBT = new Dictionary<string, string>()
            {
                {"rarity_upgrades","1"},
                {"JADE_0","PERFECT"},
                {"AMBER_0","PERFECT"},
                {"SAPPHIRE_0","PERFECT"},
                {"TOPAZ_0","PERFECT"},
                {"AMETHYST_0","PERFECT"},
                {"uid","7c2447a6ad9d"}
            };

            service.UpdateBazaar(new()
            {
                Timestamp = System.DateTime.UtcNow,
                Products = new() { CreateGemPrice("JADE"), CreateGemPrice("AMBER"), CreateGemPrice("SAPPHIRE"), CreateGemPrice("TOPAZ"), CreateGemPrice("AMETHYST") }
            });
            service.TestNewAuction(highestValAuction);
            Assert.AreEqual(7501000, found.First().TargetPrice);
        }

        private static ProductInfo CreateGemPrice(string gemName)
        {
            return new()
            {
                ProductId = $"PERFECT_{gemName}_GEM",
                QuickStatus = new()
                {
                    BuyPrice = 1000,
                    SellPrice = 1000,
                    SellVolume = 1000,
                    BuyVolume = 1000
                },
                SellSummary = new()
                {
                    new()
                    {
                        PricePerUnit = 2_000_000,
                        Amount = 1000
                    }
                }
            };
        }

        //[Test]
        public void LoadTest()
        {
            var start = Stopwatch.StartNew();
            for (int i = 0; i < 1000; i++)
            {
                service.TestNewAuction(firstAuction);
            }
            Assert.Less(start.ElapsedMilliseconds, 40);
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