using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using dev;
using FluentAssertions;
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
            service = new SniperService(new(null, null));

            found = new List<LowPricedAuction>();
            service.FoundSnipe += found.Add;
        }
        //[Test] disable for closest to clean test
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


        /// <summary>
        /// Checks that sold auction is removed from lbin list
        /// </summary>
        [Test]
        public void RemoveSoldFromLbin()
        {
            var sell = Dupplicate(highestValAuction);
            sell.End = DateTime.UtcNow + TimeSpan.FromDays(10);
            service.TestNewAuction(sell);
            service.FinishedUpdate();
            Assert.AreEqual(1, service.Lookups[sell.Tag].Lookup.First().Value.Lbins.Count);
            sell.End = DateTime.UtcNow;
            service.AddSoldItem(sell);
            Assert.AreEqual(0, service.Lookups[sell.Tag].Lookup.First().Value.Lbins.Count);
        }
        /// <summary>
        /// Least valuable entries are removed first until only 5 are left
        /// </summary>
        [Test]
        public void CapKeySize()
        {
            SetBazaarPrice("ENCHANTMENT_MANA_VAMPIRE_6", 2_100_000);
            SetBazaarPrice("ENCHANTMENT_GROWTH_6", 0, 6_200_000);
            SetBazaarPrice("ENCHANTMENT_SNIPE_4", 86_000_000);
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_LEGION_1", 1_800_000);
            SetBazaarPrice("HOT_POTATO_BOOK", 80_000);
            SetBazaarPrice("FUMING_POTATO_BOOK", 1_100_000);
            SetBazaarPrice("RECOMBOBULATOR_3000", 6_600_000);
            var key = service.KeyFromSaveAuction(new SaveAuction()
            {
                Enchantments = new List<Core.Enchantment>(){
                    new (Enchantment.EnchantmentType.ultimate_legion, 1),
                    new (Enchantment.EnchantmentType.snipe, 4),
                    new (Enchantment.EnchantmentType.growth, 6),
                    new (Enchantment.EnchantmentType.mana_vampire, 6),
                },
                FlatenedNBT = new(){
                    {"hpc", "15"},
                    {"rarity_upgrades", "1"},
                    {"upgrade_level", "5"}
                }
            });
            Assert.IsTrue(key.Enchants.Any(e => e.Type == Enchantment.EnchantmentType.growth), "Growth should be in key even if there are no buy orders on bazaar");
            Assert.IsTrue(!key.Enchants.Any(e => e.Type == Enchantment.EnchantmentType.ultimate_legion));
        }

        [Test]
        public async Task UnlockedAllOnlyForUnlockable()
        {
            await service.Init();
            var a = new SaveAuction()
            {
                Tag = "SLUG_BOOTS",
                FlatenedNBT = new(){
                    {"jasper", "FINE"}
                },
                ItemCreatedAt = new DateTime(2020, 1, 1)
            };
            var key = service.KeyFromSaveAuction(a);
            Assert.AreEqual(0, key.Modifiers.Count);
            a.Tag = "DIVAN_CHESTPLATE";
            key = service.KeyFromSaveAuction(a);
            Assert.AreEqual(1, key.Modifiers.Count, JsonConvert.SerializeObject(key.Modifiers, Formatting.Indented));
            Assert.AreEqual("AMBER_0,AMBER_1,JADE_0,JADE_1,TOPAZ_0", key.Modifiers.First().Value);
        }
        [Test]
        public async Task DropUnlockedSlotsIfBelow500k()
        {
            await service.Init();
            var a = new SaveAuction()
            {
                Tag = "ATOMSPLIT_KATANA",
                FlatenedNBT = new(){
                    {"unlocked_slots", "JASPER_0,SAPPHIRE_0"}
                },
                ItemCreatedAt = new DateTime(2022, 1, 1)
            };
            var key = service.KeyFromSaveAuction(a);
            Assert.AreEqual(0, key.Modifiers.Count, JsonConvert.SerializeObject(key.Modifiers, Formatting.Indented));
        }

        /// <summary>
        /// Gems are excluded from keys because they don't need comparison
        /// </summary>
        [Test]
        public void RemoveGemsFromKey()
        {
            SetBazaarPrice("PERFECT_AMBER_GEM", 8_000_000);
            SetBazaarPrice("PERFECT_JASPER_GEM", 8_000_000);
            var key = service.KeyFromSaveAuction(new SaveAuction()
            {
                Enchantments = new List<Core.Enchantment>()
                {
                },
                FlatenedNBT = new(){
                    {"AMBER_0", "PERFECT"},
                    {"AMBER_1", "PERFECT"},
                    {"AMBER_2", "PERFECT"},
                    {"JASPER_1", "PERFECT"}
                }
            });
            Assert.IsTrue(!key.Modifiers.Any(e => e.Value == "PERFECT"));
            Assert.AreEqual(32_000_000, key.ValueSubstract);
        }

        [Test]
        public void RemovedValueIsSubstractedInReference()
        {
            SetBazaarPrice("PERFECT_AMBER_GEM", 8_000_000);
            AddVolume(new SaveAuction()
            {
                Tag = "test",
                FlatenedNBT = new() { { "AMBER_0", "PERFECT" } },
                HighestBidAmount = 10_000_000
            });
            var actualPrice = service.Lookups["test"].Lookup.First().Value.References.First().Price;
            Assert.AreEqual(2_000_000, actualPrice);
        }

        /// <summary>
        /// Same uuid with very high profit percent is probably a bait, so we ignore it after the first listing
        /// </summary>
        [Test]
        public void PreventRefindShort()
        {
            highestValAuction.HighestBidAmount = 1_000_000;
            AddVolume(highestValAuction);
            service.State = SniperState.Ready;
            firstAuction.FlatenedNBT.Add("uid", "123456789");
            service.TestNewAuction(firstAuction);
            service.TestNewAuction(firstAuction);
            Assert.AreEqual(1, found.Count);
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
                new Core.Enchantment(Enchantment.EnchantmentType.sharpness,7),
                new Core.Enchantment(Enchantment.EnchantmentType.critical,6)
            };
            service.TestNewAuction(anotherAuction);
            Assert.AreEqual(1000, found.Last().TargetPrice);
        }
        /// <summary>
        /// szenario: overvaluing of " Any Rare 1"
        /// used as fallback from "Scavenger 5 Any Rare 1"
        /// but is default for Dreadlord sword 
        /// higher value https://sky.coflnet.com/auction/d78a9edcc29f4df3a7606340e20e9a85
        /// false flip https://sky.coflnet.com/auction/2591040d79434d5698b8c5d3ae971061
        /// </summary>
        [Test]
        public void HigherValueCheckOnFallback()
        {
            highestValAuction.FlatenedNBT = new();
            highestValAuction.Enchantments = new List<Core.Enchantment>();
            highestValAuction.Tag = "CRYPT_DREADLORD_SWORD";
            highestValAuction.HighestBidAmount = 4000000;
            AddVolume(highestValAuction);
            highestValAuction.Enchantments.Add(new Core.Enchantment(Enchantment.EnchantmentType.scavenger, 5));
            var toTest = Dupplicate(highestValAuction);
            highestValAuction.FlatenedNBT.Add("rarity_upgrades", "1");
            highestValAuction.HighestBidAmount = 1000000;
            AddVolume(highestValAuction);
            // set to production amount
            SniperService.MIN_TARGET = 200_000;
            service.TestNewAuction(toTest);
            Assert.AreEqual(1000000, found.Last().TargetPrice);
        }
        [Test]
        public void CapValueAtHigheEnchantPrice()
        {
            highestValAuction.Enchantments = new(){
                new Enchantment(Enchantment.EnchantmentType.sharpness, 7),
            };
            highestValAuction.HighestBidAmount = 1000000;
            AddVolume(highestValAuction);
            highestValAuction.Enchantments.Add(new Enchantment(Enchantment.EnchantmentType.critical, 6));
            highestValAuction.HighestBidAmount = 50000000;
            AddVolume(highestValAuction);
            highestValAuction.HighestBidAmount = 5000;
            service.TestNewAuction(highestValAuction);
            Assert.AreEqual(1000000, found.Last().TargetPrice);
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
            SetBazaarPrice("ENCHANTMENT_SHARPNESS_7", 49_000_000);
            highestValAuction.Enchantments = new List<Core.Enchantment>() {
                new Core.Enchantment(Enchantment.EnchantmentType.sharpness,7),
                new Core.Enchantment(Enchantment.EnchantmentType.ultimate_legion,5),
                new Core.Enchantment(Enchantment.EnchantmentType.critical,7),
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
        public void RemovesDupplicatesFromLookup()
        {
            var dict = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>();
            var refPrice = new ReferencePrice[] { new() { AuctionId = 1, Price = 1000, Day = SniperService.GetDay() } };
            var keyWithEnch = new AuctionKey()
            {
                Enchants = new(new List<Models.Enchant>() {
                new() { Type = Enchantment.EnchantmentType.ultimate_legion, Lvl = 5 } })
            };
            dict.TryAdd(new AuctionKey(), new() { References = new(refPrice) });
            dict.TryAdd(keyWithEnch, new() { References = new(refPrice) });
            service.AddLookupData("test", new()
            {
                Lookup = dict
            });
            Assert.AreEqual(1, service.Lookups["test"].Lookup.Count);
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
                    UId = Random.Shared.NextInt64(),
                    AuctioneerId = "12aaa"
                };
                var key = service.KeyFromSaveAuction(auction, drop);
                return key;
            }
        }

        [Test]
        public void UpdatesOldestRefWithMedian()
        {
            var bucket = new ReferenceAuctions();
            var end = DateTime.UtcNow;
            var auction = new SaveAuction
            {
                Tag = "1",
                FlatenedNBT = new() { { "new_years_cake", "252" } },
                StartingBid = 900,
                HighestBidAmount = 900,
                End = end
            };
            var lookup = new PriceLookup();
            lookup.Lookup.TryAdd(new(), bucket);
            service.Lookups.TryAdd("1", lookup);
            for (int i = 0; i < 11; i++)
            {
                Console.WriteLine($"Day: {SniperService.GetDay(auction.End)}");
                service.AddAuctionToBucket(Dupplicate(auction), false, bucket);
                auction.End = auction.End.AddDays(1);
            }
            var day = SniperService.GetDay(auction.End) - 3;
            Assert.AreEqual(day, bucket.OldestRef);
        }

        [Test]
        public void UpdateMedianWithShortTermOnDrop()
        {
            var bucket = new ReferenceAuctions();
            var end = DateTime.UtcNow;
            var auction = new SaveAuction
            {
                Tag = "1",
                FlatenedNBT = new() { { "new_years_cake", "252" } },
                StartingBid = 2900,
                HighestBidAmount = 2900,
                End = end
            };
            var lookup = new PriceLookup();
            lookup.Lookup.TryAdd(new(), bucket);
            service.Lookups.TryAdd("1", lookup);
            for (int i = 0; i < 11; i++)
            {
                service.AddAuctionToBucket(Dupplicate(auction), false, bucket);
                auction.HighestBidAmount -= 100;
            }
            Assert.AreEqual(2000, bucket.Price);
        }

        [TestCase(1, 0)]
        [TestCase(200, 1)]
        [TestCase(499, 1)]
        [TestCase(999, 2)]
        [TestCase(999999, 3)]
        public void NormalizeGroupNumber(int val, int expectedGroup)
        {
            var simAttr = new KeyValuePair<string, string>("new_years_cake", val.ToString());
            var comb = SniperService.NormalizeGroupNumber(simAttr, 200, 500, 1000, 2000);
            Assert.AreEqual(expectedGroup.ToString(), comb.Value);
        }

        [Test]
        public void DropOldkey()
        {
            service.AddLookupData("PET_TEST", new PriceLookup()
            {
                Lookup = new(new Dictionary<AuctionKey, ReferenceAuctions>()
                {
                    {new(){
                        Modifiers = new(new List<KeyValuePair<string,string>>(){
                            new("exp","6"),
                            new("candyUsed","0"),
                        })
                    }, new ReferenceAuctions(){
                        Price = 1000,
                        OldestRef = 1
                    } }
                })
            });
            // not added because can't be reached anymore
            service.Lookups["PET_TEST"].Lookup.Count.Should().Be(0);
        }


        [Test]
        public void RandomEnchantLbin()
        {
            var a = Dupplicate(highestValAuction);
            var targetEnchant = new Core.Enchantment(Enchantment.EnchantmentType.ultimate_chimera, 1);
            a.Enchantments = new List<Core.Enchantment>(){
                targetEnchant
            };
            service.AddSoldItem(Dupplicate(a));
            service.AddSoldItem(Dupplicate(a));
            service.AddSoldItem(Dupplicate(a));
            service.AddSoldItem(Dupplicate(a));

            a.Enchantments = new List<Core.Enchantment>(){
                targetEnchant,
                new Core.Enchantment(Enchantment.EnchantmentType.sharpness, 6)
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
            highestValAuction.HighestBidAmount = 10_000_000;
            var part = Dupplicate(highestValAuction);
            part.Tag = "COMPONENT";
            part.HighestBidAmount = 1_000_000;
            AddVolume(part);

            var drill = Dupplicate(highestValAuction);
            drill.Tag = "DRILL";
            drill.FlatenedNBT["drill_part_engine"] = "component";
            AddVolume(drill);
            service.FinishedUpdate();
            drill.FlatenedNBT = new();
            var estimate = service.GetPrice(drill);
            Assert.AreEqual(9_000_000, estimate.Median, "10m base - 1m component");
            Assert.AreEqual(" Any [drill_part_engine, component] UNKNOWN 0- component", estimate.MedianKey);

        }

        [Test]
        public void SubstractsEnchants()
        {
            highestValAuction.FlatenedNBT = new();
            var moreEnchants = Dupplicate(highestValAuction);
            moreEnchants.HighestBidAmount = 100_000_000;
            moreEnchants.Enchantments = new List<Core.Enchantment>(){
                new Core.Enchantment(Enchantment.EnchantmentType.sharpness,7)
            };
            AddVolume(moreEnchants);
            SetBazaarPrice("ENCHANTMENT_SHARPNESS_7", 49_000_000);

            SimulateNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(45900000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("sharpness_7 (49000000)", estimate.AdditionalProps["missingEnchants"]);
        }
        [Test]
        public void PartiallyAddsEnchants()
        {
            highestValAuction.FlatenedNBT = new();
            highestValAuction.Enchantments = new List<Enchantment>(){
                new Core.Enchantment(Enchantment.EnchantmentType.sharpness,7),
                new Core.Enchantment(Enchantment.EnchantmentType.critical,7)
            };
            var lessEnchants = Dupplicate(highestValAuction);
            lessEnchants.HighestBidAmount = 100_000_000;
            lessEnchants.Enchantments = new();
            AddVolume(lessEnchants);
            SetBazaarPrice("ENCHANTMENT_SHARPNESS_7", 49_000_000);
            SetBazaarPrice("ENCHANTMENT_CRITICAL_7", 60_000_000);
            SimulateNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(135000000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("critical_7,sharpness_7 (50000000)", estimate.AdditionalProps["additionalEnchants"]);
        }
        [Test]
        public void SubstractsEnchantsCrafted()
        {
            highestValAuction.FlatenedNBT = new();
            var moreEnchants = Dupplicate(highestValAuction);
            moreEnchants.HighestBidAmount = 1_600_000_000;
            moreEnchants.Enchantments = new List<Core.Enchantment>(){
                new Core.Enchantment(Enchantment.EnchantmentType.ultimate_chimera,5)
            };
            AddVolume(moreEnchants);
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_CHIMERA_1", 95_000_000);
            SimulateNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(72000000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            // substracted 2^lvldifference * price
            Assert.AreEqual("ultimate_chimera_5 (1520000000)", estimate.AdditionalProps["missingEnchants"]);
        }
        [Test]
        public void SubstractsSkinValue()
        {
            highestValAuction.FlatenedNBT = new() { { "candyUsed", "1" }, { "exp", "0" } };
            var withSkin = Dupplicate(highestValAuction);
            withSkin.HighestBidAmount = 60_000_000;
            withSkin.FlatenedNBT.Add("skin", "SKELETON");
            AddVolume(withSkin);
            SetBazaarPrice("SKELETON", 25_000_000);
            SimulateNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual((withSkin.HighestBidAmount - 25_000_000) * 0.9, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("skin:SKELETON (25000000)", estimate.AdditionalProps["missingModifiers"]);
        }

        private void SimulateNewAuction(SaveAuction x)
        {
            var toTest = Dupplicate(x);
            service.FinishedUpdate();
            service.State = SniperState.Ready;
            service.TestNewAuction(toTest);
            service.FinishedUpdate();
        }

        [Test]
        public void StonksSubstractsLeveledEnchant()
        {
            // {"enchantments":[{"color":"§9","type":"efficiency","level":5},{"color":"§9","type":"smelting_touch","level":1},{"color":"§5","type":"harvesting","level":6},{"color":"§5","type":"cultivating","level":9},{"color":"§5","type":"dedication","level":3},{"color":"§9","type":"turbo_cactus","level":5}],"uuid":"fb4d9ec40a834f808147bb6dff74dfb5","count":1,"startingBid":60000000,"tag":"CACTUS_KNIFE","itemName":"Blessed Cactus Knife","start":"2023-09-13T09:37:41","end":"2023-09-15T19:47:03","auctioneerId":"90f20a02e67146659f44ae54abb6aecc","profileId":"1280e7de2f5e4d2086a3a57766556660","coop":null,"coopMembers":null,"highestBidAmount":60000000,"bids":[{"bidder":"98730e6ba68b403c84f756ccbfd136cb","profileId":"unknown","amount":60000000,"timestamp":"2023-09-15T19:47:03"}],"anvilUses":0,"nbtData":{"data":{"rarity_upgrades":1,"farmed_cultivating":33602119,"uid":"a3f0b16cac7e","farming_for_dummies_count":5,"uuid":"5789afb2-80f7-45ce-bf6d-a3f0b16cac7e"}},"itemCreatedAt":"2023-02-14T22:09:00","reforge":"blessed","category":"MISC","tier":"LEGENDARY","bin":true,"flatNbt":{"rarity_upgrades":"1","farmed_cultivating":"33602119","uid":"a3f0b16cac7e","farming_for_dummies_count":"5","uuid":"5789afb2-80f7-45ce-bf6d-a3f0b16cac7e"}}
            highestValAuction.FlatenedNBT = new();
            var moreEnchants = Dupplicate(highestValAuction);
            moreEnchants.HighestBidAmount = 60_000_000;
            moreEnchants.Enchantments = new List<Core.Enchantment>(){
                new Core.Enchantment(Enchantment.EnchantmentType.cultivating,9),
            };
            AddVolume(moreEnchants);
            SetBazaarPrice("ENCHANTMENT_CULTIVATING_1", 3_000_000);

            SimulateNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            var expectedValue = (moreEnchants.HighestBidAmount - 3_000_000 - 1_000_000 * 9) * 9 / 10;
            Assert.AreEqual(expectedValue, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
        }

        [Test]
        public void PutsIntoCorrectBucket()
        {
            highestValAuction.Enchantments = new List<Core.Enchantment>(){
                new Core.Enchantment(Enchantment.EnchantmentType.cultivating,9),
            };
            highestValAuction.Tag = "test";
            AddVolume(highestValAuction);
            var val = service.Lookups["test"].Lookup;
            Assert.AreEqual(1, val.Count, JsonConvert.SerializeObject(val));
            Assert.AreEqual(1, val.First().Key.Enchants.Count);
        }

        [Test]
        public void CorrectsDuplexToReiterate()
        {
            var mapper = new PropertyMapper();
            var result = mapper.EnchantValue(new Core.Enchantment(Enchantment.EnchantmentType.ultimate_duplex, 1), null, new(){
                {"ENCHANTMENT_ULTIMATE_REITERATE_1", 3000}
            });
            Assert.AreEqual(3000, result);
        }

        [Test]
        public void StonksReforgeDifference()
        {
            highestValAuction.FlatenedNBT = new();
            var reforge = Dupplicate(highestValAuction);
            reforge.HighestBidAmount = 10_000_000;
            reforge.Reforge = ItemReferences.Reforge.Gilded;
            AddVolume(reforge);
            SetBazaarPrice("MIDAS_JEWEL", 4_000_000);

            TestNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(2500000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("Gilded -> None (6500000)", estimate.AdditionalProps["reforge"]);
        }

        /// <summary>
        /// The difference between godrolls and non godrolls is ~96%
        /// changed for https://github.com/Coflnet/SkySniper/issues/62
        /// </summary>
        [Test]
        public void StonksGodRollAttributeDifference()
        {
            highestValAuction.FlatenedNBT = new() { { "mana_pool", "1" } };
            var withRegen = Dupplicate(highestValAuction);
            withRegen.HighestBidAmount = 100_000_000;
            withRegen.FlatenedNBT.Add("mana_regeneration", "1");
            AddVolume(withRegen);

            TestNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            // 96m for missing mana_regeneration (godroll), 10% for stonks
            Assert.AreEqual(3600000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("mana_regeneration:1 (96000000)", estimate.AdditionalProps["missingModifiers"]);
        }
        [Test]
        public void StonksBigAttributeDifference()
        {
            highestValAuction.FlatenedNBT = new() { { "mana_pool", "1" } };
            var withRegen = Dupplicate(highestValAuction);
            withRegen.HighestBidAmount = 10_000_000;
            withRegen.FlatenedNBT.Add("veteran", "5");
            AddVolume(withRegen);

            TestNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(92160, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("veteran:5 (9897600)", estimate.AdditionalProps["missingModifiers"]);
        }
        [Test]
        public void StonksPetCandyReduction()
        {
            highestValAuction.FlatenedNBT = new() { { "candyUsed", "1" } };
            var withoutCandy = Dupplicate(highestValAuction);
            withoutCandy.HighestBidAmount = 10_000_000;
            withoutCandy.FlatenedNBT["candyUsed"] = "0";
            AddVolume(withoutCandy);

            TestNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(8100000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("candyUsed:0 (1000000)", estimate.AdditionalProps["missingModifiers"]);
        }

        private void TestNewAuction(SaveAuction a)
        {
            var toTest = Dupplicate(a);
            service.FinishedUpdate();
            service.State = SniperState.Ready;
            service.TestNewAuction(toTest);
            service.FinishedUpdate();
        }

        [Test]
        public void HigherEnchantTierAreWorthMoreThanLowerBazaar()
        {
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_FATAL_TEMPO_1", 80_000_000, 78_000_000);
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_FATAL_TEMPO_2", 40, 3);
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_FATAL_TEMPO_3", 50, 3);
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_FATAL_TEMPO_4", 60, 3);
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_FATAL_TEMPO_5", 0, 3);
            Assert.AreEqual(1_042_568_000, service.Lookups["ENCHANTMENT_ULTIMATE_FATAL_TEMPO_5"].Lookup.First().Value.Price);
        }

        [Test]
        public void StonksExpDifference()
        {
            firstAuction.FlatenedNBT = new() { { "candyUsed", "0" }, { "exp", "5000000" } };
            firstAuction.Count = 1;
            var higherExp = Dupplicate(firstAuction);
            higherExp.FlatenedNBT["exp"] = "9000000";
            higherExp.HighestBidAmount = 10_000_000;
            AddVolume(higherExp);
            AddVolume(higherExp);
            var maxExp = Dupplicate(higherExp);
            maxExp.FlatenedNBT["exp"] = "100000000";
            maxExp.HighestBidAmount = 20_000_000;
            AddVolume(maxExp);
            var noExp = Dupplicate(firstAuction);
            noExp.FlatenedNBT["exp"] = "0";
            noExp.HighestBidAmount = 5_000_000;
            AddVolume(noExp);

            TestNewAuction(firstAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.Greater(7200000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
        }
        [Test]
        public void TalismanEnrichmentCorrection()
        {
            highestValAuction.FlatenedNBT = new() { { "talisman_enrichment", "attack_speed" } };
            highestValAuction.HighestBidAmount = 2_000_000;
            AddVolume(Dupplicate(highestValAuction));
            var withoutEnrichment = Dupplicate(highestValAuction);
            withoutEnrichment.HighestBidAmount = 10_000_000;
            withoutEnrichment.FlatenedNBT = new();
            AddVolume(withoutEnrichment);
            var toTest = Dupplicate(withoutEnrichment);
            service.FinishedUpdate();
            service.State = SniperState.Ready;
            service.TestNewAuction(toTest);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(2_000_000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
        }
        [Test]
        public void StonksIncreaseForKills()
        {
            highestValAuction.FlatenedNBT = new() { { "zombie_kills", "15000" } };
            var withoutKills = Dupplicate(highestValAuction);
            withoutKills.HighestBidAmount = 10_000_000;
            withoutKills.FlatenedNBT["zombie_kills"] = "0";
            AddVolume(withoutKills);

            TestNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(9450000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("zombie_kills:0 (-500000)", estimate.AdditionalProps["missingModifiers"]);
        }
        [Test]
        public void StonksDecreaseForKills()
        {
            highestValAuction.FlatenedNBT = new() { { "zombie_kills", "0" } };
            var withoutKills = Dupplicate(highestValAuction);
            withoutKills.HighestBidAmount = 10_000_000;
            withoutKills.FlatenedNBT["zombie_kills"] = "25000";
            AddVolume(withoutKills);

            TestNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(7200000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual("zombie_kills:2 (2000000)", estimate.AdditionalProps["missingModifiers"]);
        }
        [TestCase("MINOS_RELIC", "petItem:MINOS_RELIC (4000000)")]
        [TestCase("PET_ITEM_QUICK_CLAW", "petItem:QUICK_CLAW (4000000)")]
        public void StonksDecreaseForPetItem(string itemId, string textNote)
        {
            highestValAuction.FlatenedNBT = new() { { "heldItem", "YELLOW_BANDANA" } };
            var withoutKills = Dupplicate(highestValAuction);
            withoutKills.HighestBidAmount = 10_000_000;
            withoutKills.FlatenedNBT["heldItem"] = itemId;
            AddVolume(withoutKills);
            SetBazaarPrice(itemId, 4_000_000);
            TestNewAuction(highestValAuction);
            var estimate = found.Where(f => f.Finder == LowPricedAuction.FinderType.STONKS).FirstOrDefault();
            Assert.NotNull(estimate, JsonConvert.SerializeObject(found));
            Assert.AreEqual(5400000, estimate.TargetPrice, JsonConvert.SerializeObject(estimate.AdditionalProps));
            Assert.AreEqual(textNote, estimate.AdditionalProps["missingModifiers"]);
        }

        [Test]
        public void SubstractsStarCost()
        {
            highestValAuction.FlatenedNBT = new();
            var upgradeLvl9 = Dupplicate(highestValAuction);
            upgradeLvl9.HighestBidAmount = 100_000_000;
            upgradeLvl9.FlatenedNBT["upgrade_level"] = "9";
            AddVolume(upgradeLvl9);
            SetBazaarPrice("FOURTH_MASTER_STAR", 49_000_000);
            SetBazaarPrice("THIRD_MASTER_STAR", 19_000_000);
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
                new Core.Enchantment(Enchantment.EnchantmentType.sharpness,6)
            };
            var d = Dupplicate(highestValAuction);
            d.HighestBidAmount = 900;
            service.TestNewAuction(a);
            service.TestNewAuction(b);
            // lbins are only seceted with median volume
            AddVolume(b);
            service.TestNewAuction(c);
            service.FinishedUpdate();
            service.TestNewAuction(d);

            highestValAuction.FlatenedNBT["skin"] = "something";
            highestValAuction.FlatenedNBT["heldItem"] = "something";
            var price = service.GetPrice(highestValAuction);

            Assert.AreEqual(1000, price.Lbin.Price);
        }
        [Test]
        public void AdjustsForMissingEnchant()
        {
            highestValAuction.FlatenedNBT = new();
            var moreEnchants = Dupplicate(highestValAuction);
            moreEnchants.HighestBidAmount = 100_000_000;
            moreEnchants.Enchantments = new List<Core.Enchantment>(){
                new Core.Enchantment(Enchantment.EnchantmentType.sharpness,7)
            };
            AddVolume(moreEnchants);
            SetBazaarPrice("ENCHANTMENT_SHARPNESS_7", 49_000_000);
            var price = service.GetPrice(highestValAuction);
            Assert.AreEqual(51000000, price.Median);
            Assert.AreEqual("sharpness=7 Any  UNKNOWN 0-sharpness7", price.MedianKey);
        }
        [Test]
        public void AdjustForDifferentPetItemInLbin()
        {
            highestValAuction.FlatenedNBT = new();
            var withSkin = Dupplicate(highestValAuction);
            withSkin.HighestBidAmount = 150_000_000;
            withSkin.FlatenedNBT.Add("heldItem", "PET_ITEM_QUICK_CLAW");
            AddVolume(withSkin);
            var lbin = Dupplicate(withSkin);
            // random value to make sure its not a sideeffect
            var random = Random.Shared.Next(1, 10000);
            lbin.StartingBid = 140_000_000 + random;
            lbin.HighestBidAmount = 0;
            service.TestNewAuction(lbin);
            service.FinishedUpdate();
            SetBazaarPrice("PET_ITEM_QUICK_CLAW", 99_000_000);
            var price = service.GetPrice(highestValAuction);
            Assert.AreEqual(41_000_000 + random, price.Lbin.Price, JsonConvert.SerializeObject(price));
        }

        private void SetBazaarPrice(string tag, int value, int buyValue = 0)
        {
            var sellOrder = new List<SellOrder>();
            if (value > 0)
                sellOrder.Add(new SellOrder() { PricePerUnit = value });
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

        [Test]
        public void NotAdjustsForNonMissingEnchant()
        {
            highestValAuction.FlatenedNBT = new();
            var medianRef = Dupplicate(highestValAuction);
            medianRef.HighestBidAmount = 100_000_000;
            medianRef.Enchantments = new List<Core.Enchantment>(){
                new (Enchantment.EnchantmentType.growth,6),
                new (Enchantment.EnchantmentType.ultimate_legion,5),
            };
            AddVolume(medianRef);
            highestValAuction.Enchantments = new List<Core.Enchantment>(){
                new (Enchantment.EnchantmentType.growth,7),
                new (Enchantment.EnchantmentType.ultimate_legion,5),
            };
            SetBazaarPrice("ENCHANTMENT_GROWTH_6", 8_000_000);
            SetBazaarPrice("ENCHANTMENT_GROWTH_7", 22_000_000);
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_LEGION_5", 80_000_000);
            highestValAuction.Tier = Tier.VERY_SPECIAL;
            var price = service.GetPrice(highestValAuction);
            Assert.AreEqual(100_000_000, price.Median);
            Assert.AreEqual("growth=6,ultimate_legion=5 Any  UNKNOWN 0", price.MedianKey);
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
                Console.WriteLine(JsonConvert.SerializeObject(s, Formatting.Indented));
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


        [TestCase(4300000, 4821272)]
        [TestCase(8400000, 5468132)]
        [TestCase(20_400000, 5_000_000)]
        public void MedianAdjustForBucketExpDiff(int exp, int expectedPrice)
        {
            highestValAuction.Count = 1;
            highestValAuction.FlatenedNBT = new(){
                {"exp",SniperService.PetExpMaxlevel.ToString()}
            };
            highestValAuction.HighestBidAmount = 5_000_000;
            AddVolume(highestValAuction);
            highestValAuction.FlatenedNBT["candyUsed"] = "0";
            var lowerExp = Dupplicate(highestValAuction);
            lowerExp.FlatenedNBT["exp"] = "0";
            lowerExp.HighestBidAmount = 1_000_000;
            AddVolume(lowerExp);
            var sample = Dupplicate(highestValAuction);
            sample.FlatenedNBT["exp"] = exp.ToString();
            AddVolume(sample);
            service.TestNewAuction(sample);
            var flip = found.Where(a => a.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN).FirstOrDefault();
            Assert.IsNotNull(flip, "flip should have been found");
            Assert.AreEqual(expectedPrice, flip.TargetPrice, "median should be adjusted for exp diff");
        }

        [TestCase(31023190, 800000000)]
        public void MedianAdjustForBucketExpDiffGoldenDrag(int exp, int expectedPrice)
        {
            highestValAuction.Count = 1;
            highestValAuction.Tag = "PET_GOLDEN_DRAGON";
            highestValAuction.FlatenedNBT = new(){
                {"exp",(SniperService.PetExpMaxlevel * 50).ToString()}
            };
            highestValAuction.HighestBidAmount = 1_100_000_000;
            AddVolume(highestValAuction);
            highestValAuction.FlatenedNBT["candyUsed"] = "0";
            var lowerExp = Dupplicate(highestValAuction);
            lowerExp.FlatenedNBT["exp"] = "0";
            lowerExp.HighestBidAmount = 600_000_000;
            AddVolume(lowerExp);
            highestValAuction.HighestBidAmount = 800_000_000;
            var sample = Dupplicate(highestValAuction);
            sample.FlatenedNBT["exp"] = exp.ToString();
            AddVolume(sample);
            AddVolume(sample);
            sample.HighestBidAmount = 5;
            service.TestNewAuction(sample);
            var flip = found.Where(a => a.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN).FirstOrDefault();
            Assert.IsNotNull(flip, "flip should have been found");
            Assert.AreEqual(expectedPrice, flip.TargetPrice, "median should be adjusted for exp diff");
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
                Console.WriteLine(JsonConvert.SerializeObject(s));
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
        public void DoNotUseHigherLevelRune()
        {
            highestValAuction.FlatenedNBT = new() { { "MUSIC", "1" } };
            highestValAuction.Tag = "RUNE_MUSIC";
            var higherLevel = Dupplicate(highestValAuction);
            higherLevel.FlatenedNBT["MUSIC"] = "3";
            higherLevel.HighestBidAmount = 100_000_000;
            AddVolume(higherLevel);
            service.State = SniperState.Ready;
            service.FinishedUpdate();
            service.TestNewAuction(highestValAuction);
            Assert.AreEqual(0, found.Count, "should not use raw rune");
        }

        [Test]
        public void AttributeCombination()
        {
            highestValAuction.FlatenedNBT = new() { { "mana_pool", "7" } };
            var onlyAttrib = Dupplicate(highestValAuction);
            onlyAttrib.HighestBidAmount = 1_000_000;
            AddVolume(onlyAttrib);
            AddVolume(onlyAttrib);

            var highAttrib = Dupplicate(highestValAuction);
            highAttrib.FlatenedNBT["mana_pool"] = "6";
            service.State = SniperState.Ready;
            service.FinishedUpdate();
            var price = service.GetPrice(highAttrib);
            Assert.AreEqual(400000, price.Median, price.MedianKey);
        }

        [Test]
        public void WitherBladeCombination()
        {
            highestValAuction.Tag = "HYPERION";
            highestValAuction.FlatenedNBT = new();
            highestValAuction.HighestBidAmount = 100_000_000;
            AddVolume(highestValAuction);
            SetBazaarPrice("GIANT_FRAGMENT_LASER", 20_000);
            var scylla = Dupplicate(highestValAuction);
            scylla.StartingBid = 5;
            scylla.Tag = "SCYLLA";
            service.TestNewAuction(scylla);
            Assert.AreEqual(100_000_000 - 8 * 20_000, found.First().TargetPrice);
        }

        [Test]
        public void CombineFragged()
        {
            highestValAuction.Tag = "SHADOW_FURY";
            highestValAuction.FlatenedNBT = new();
            highestValAuction.HighestBidAmount = 40_000_000;
            AddVolume(highestValAuction);
            SetBazaarPrice("LIVID_FRAGMENT", 20_000);
            var starred = Dupplicate(highestValAuction);
            starred.StartingBid = 5;
            starred.Tag = "STARRED_SHADOW_FURY";
            var price = service.GetPrice(starred);
            Assert.AreEqual(40_000_000 - 8 * 20_000, price.Median);
        }

        [Test]
        public void AttributeHigherThanRef()
        {
            highestValAuction.FlatenedNBT = new() { { "mana_pool", "8" } };
            var onlyAttrib = Dupplicate(highestValAuction);
            onlyAttrib.HighestBidAmount = 1_000_000;
            AddVolume(onlyAttrib);
            AddVolume(onlyAttrib);

            var highAttrib = Dupplicate(highestValAuction);
            highAttrib.FlatenedNBT["mana_pool"] = "10";
            service.State = SniperState.Ready;
            service.FinishedUpdate();
            var price = service.GetPrice(highAttrib);
            Assert.AreEqual(1210000, price.Median, price.MedianKey);
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
                Timestamp = DateTime.UtcNow,
                Products = new() { CreateGemPrice("JADE"), CreateGemPrice("AMBER"), CreateGemPrice("SAPPHIRE"), CreateGemPrice("TOPAZ"), CreateGemPrice("AMETHYST") }
            });
            service.TestNewAuction(highestValAuction);
            Assert.AreEqual(7501000, found.First().TargetPrice);
        }

        [Test]
        public void MarkifAll5Gems()
        {
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
            var key = service.KeyFromSaveAuction(highestValAuction);
            Assert.AreEqual("5", key.Modifiers.Where(x => x.Key == "pgems").First().Value);
        }

        [Test]
        public void CleanAndEnchantLower()
        {
            highestValAuction.Enchantments = new List<Core.Enchantment>(){
                new Core.Enchantment(Enchantment.EnchantmentType.sharpness,7)
            };

            var clean = Dupplicate(highestValAuction);
            clean.Enchantments = new List<Core.Enchantment>();
            clean.HighestBidAmount = 500_000;
            AddVolume(clean);
            highestValAuction.HighestBidAmount = 10_000_000;
            SetBazaarPrice("ENCHANTMENT_SHARPNESS_7", 3_000_000);
            AddVolume(highestValAuction);
            service.FinishedUpdate();
            highestValAuction.StartingBid = 5;
            service.TestNewAuction(Dupplicate(highestValAuction));
            Assert.AreEqual(3500000, found.Last().TargetPrice);
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
        public Task<ConcurrentDictionary<string, AttributeLookup>> GetWeigths()
        {
            return Task.FromResult(new ConcurrentDictionary<string, AttributeLookup>());
        }

        public Task LoadLookups(SniperService service)
        {
            return Task.CompletedTask;
        }

        public Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            return Task.CompletedTask;
        }

        public Task SaveWeigths(ConcurrentDictionary<string, AttributeLookup> lookups)
        {
            return Task.CompletedTask;
        }
    }
}