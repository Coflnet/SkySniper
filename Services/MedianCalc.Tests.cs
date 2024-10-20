using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;
public class MedianCalcTests
{
    private SniperService service;
    [SetUp]
    public void Setup()
    {
        service = new SniperService(null, null, NullLogger<SniperService>.Instance, null);
        SniperService.StartTime = new DateTime(2021, 9, 25);
    }
    [Test]
    public void LargeData()
    {
        ReferenceAuctions bucket = LoadJsonReferences(SampleJson);
        service.UpdateMedian(bucket);
        Assert.That(54900000, Is.EqualTo(bucket.Price));
    }

    private static ReferenceAuctions LoadJsonReferences(string json)
    {
        var sample = JsonConvert.DeserializeObject<ReferencePrice[]>(json);
        ReferenceAuctions bucket = PutReferencesInBucket(sample);

        return bucket;
    }

    private static ReferenceAuctions PutReferencesInBucket(ReferencePrice[] sample)
    {
        var bucket = new ReferenceAuctions();
        bucket.References = new ConcurrentQueue<ReferencePrice>();
        var dayDiff = SniperService.GetDay() - sample.Last().Day;
        foreach (var item in sample)
        {
            ReferencePrice adopted = AdjustDay(dayDiff, item);
            bucket.References.Enqueue(adopted);
        }

        return bucket;
    }

    private static ReferencePrice AdjustDay(int dayDiff, ReferencePrice item)
    {
        return new ReferencePrice()
        {
            AuctionId = item.AuctionId,
            Day = (short)(item.Day + dayDiff),
            Price = item.Price,
            Seller = item.Seller,
            Buyer = item.Buyer
        };
    }

    [Test]
    public void DedupsBuyer()
    {
        var random = new Random(1);
        var auction = new SaveAuction()
        {
            AuctioneerId = random.Next().ToString(),
            Tag = "test",
            Uuid = random.Next().ToString(),
            End = DateTime.Now - TimeSpan.FromDays(1),
        };
        for (int i = 1; i < 6; i++)
        {
            var copy = auction.Dupplicate();
            copy.Bids = new() { new SaveBids() { Bidder = random.Next().ToString(), Amount = i * 1000 } };
            copy.HighestBidAmount = i * 1000;
            service.AddSoldItem(copy);
        }
        for (int i = 0; i < 10; i++)
        {
            var copy = auction.Dupplicate();
            copy.Bids = new() { new SaveBids() { Bidder = "abcdef", Amount = 5000000 } };
            copy.HighestBidAmount = 5000000;
            service.AddSoldItem(copy);
        }
        Assert.That(3000, Is.EqualTo(service.Lookups.First().Value.Lookup.First().Value.Price));
    }

    [Test]
    public void IgnoreFlips()
    {
        ReferenceAuctions bucket = LoadJsonReferences(FlipSample);
        service.UpdateMedian(bucket);
        Assert.That(bucket.Price, Is.EqualTo(30000000), "Both flips should be ignored for median");
    }

    [Test]
    public void TerrorChestplateMedian()
    {
        ReferenceAuctions bucket = LoadJsonReferences(TerroChestplateSample);
        service.UpdateMedian(bucket);
        Assert.That(bucket.Price, Is.EqualTo(52628738));
    }

    [Test]
    public void LowDropMedianLimit()
    {
        ReferenceAuctions bucket = LoadJsonReferences(LowDropMedian);
        service.UpdateMedian(bucket);
        Assert.That(bucket.Price, Is.EqualTo(50000000));
    }
    [Test]
    public void BraceletLimit()
    {
        SaveAuction bare;
        AuctionKeyWithValue key;
        List<LowPricedAuction> flips;
        SetupPlain(out bare, out key, out flips);
        var sample = bare.Dupplicate();
        sample.StartingBid = 10_000_000;
        service.TestNewAuction(sample);
        Assert.That(flips.Count, Is.EqualTo(1));
        // median is 0 because anti manipulation, reference price is devided by 5
        Assert.That(flips.First().TargetPrice, Is.EqualTo(80_000_000));

        // craft cost can cap it lower
        SetupPlain(out bare, out key, out flips);
        sample.FlatenedNBT = new() { { "life_regeneration", "1" } };
        sample.HighestBidAmount = 2_000_000;
        service.AddSoldItem(sample.Dupplicate());
        service.AddSoldItem(sample.Dupplicate());
        service.AddSoldItem(sample.Dupplicate());
        service.AddSoldItem(sample.Dupplicate()); // build median
        service.FinishedUpdate();
        sample = bare.Dupplicate();
        sample.StartingBid = 1_000_000;
        service.TestNewAuction(sample);
        Assert.That(flips.First().TargetPrice, Is.EqualTo(7280000));

        void SetupPlain(out SaveAuction bare, out AuctionKeyWithValue key, out List<LowPricedAuction> flips)
        {
            ReferenceAuctions bucket = LoadJsonReferences(NacklaceSample);
            bare = new SaveAuction()
            {
                Tag = "THUNDERBOLT_NECKLACE",
                StartingBid = 180_000_000,
                End = DateTime.Now + TimeSpan.FromDays(-1),
                AuctioneerId = "000100",
                Uuid = "000100",
                FlatenedNBT = new() { { "life_regeneration", "2" } }
            };
            key = service.KeyFromSaveAuction(bare);
            service.AddLookupData("THUNDERBOLT_NECKLACE", new() { Lookup = new(new Dictionary<AuctionKey, ReferenceAuctions>() { { key, bucket } }) });
            service.TestNewAuction(bare);
            service.FinishedUpdate();
            flips = new List<LowPricedAuction>();
            service.FoundSnipe += flips.Add;
            service.UpdateMedian(bucket);
            service.State = SniperState.Ready;
        }
    }

    /// <summary>
    /// real world example of manipulated portal
    /// back and forth selling should be ignored
    /// </summary>
    [Test]
    public void PortalSampleIsNotOvervalued()
    {
        ReferenceAuctions bucket = LoadJsonReferences(PortalSample);
        service.UpdateMedian(bucket);
        Assert.That(bucket.Price, Is.EqualTo(0));
    }

    /// <summary>
    /// Irl trader moves money through the same item and shold be ignored because he is most of the sells above the median
    /// Using 0.5x of his lowest sale as reference
    /// </summary>
    [Test]
    public void GlacialScytheAntiManipulation()
    {
        ReferenceAuctions bucket = LoadJsonReferences(GlacialScytheSample);
        service.UpdateMedian(bucket);
        Assert.That(bucket.Price, Is.EqualTo(99558488));
    }

    [Test]
    public void MedianLimitedByMedianLbin()
    {
        var bucket = JsonConvert.DeserializeObject<ReferenceAuctions>(LbinDropSample);
        var adjusted = PutReferencesInBucket(bucket.References.ToArray());
        var dayDiff = SniperService.GetDay() - bucket.References.Last().Day;
        foreach (var item in bucket.Lbins)
        {
            ReferencePrice adopted = AdjustDay(dayDiff, item);
            adjusted.Lbins.Add(adopted);
        }
        service.UpdateMedian(adjusted);
        var bare = new SaveAuction()
        {
            Tag = "DYE_NYANZA",
            StartingBid = 180_000_000,
            End = DateTime.Now + TimeSpan.FromDays(1),
            AuctioneerId = "000100",
            Uuid = "000100",
            FlatenedNBT = []
        };
        var key = service.KeyFromSaveAuction(bare);
        service.Lookups["DYE_NYANZA"] = new() { Lookup = new(new Dictionary<AuctionKey, ReferenceAuctions>() { { key, adjusted } }) };
        List<LowPricedAuction> flips = new();
        service.FoundSnipe += flips.Add;
        service.TestNewAuction(bare);
        service.FinishedUpdate();
        Assert.That(flips.First(f => f.Finder == LowPricedAuction.FinderType.SNIPER_MEDIAN).TargetPrice, Is.EqualTo(1350_000_000));
    }
    [Test]
    public void InflationCanChangeMedianUpwardsOverTime()
    {
        ReferenceAuctions bucket = LoadJsonReferences(InflationExample);
        service.UpdateMedian(bucket);
        Assert.That(bucket.Price, Is.EqualTo(133_900_000));
    }
    [Test]
    public void EstimateRiskyHigher()
    {
        ReferenceAuctions bucket = LoadJsonReferences(EstimateRisky);
        var allOrdered = bucket.References.Where(r => r.Day > SniperService.GetDay() - 10).OrderBy(r => r.Price).Select(r => r.Price).ToArray();
        service.UpdateMedian(bucket, ("test", new KeyWithValueBreakdown() { Key = new AuctionKey() { }, ValueBreakdown = new(), SubstractedValue = 10 }));
        List<LowPricedAuction> flips = new();
        service.FoundSnipe += flips.Add;
        var bare = new SaveAuction()
        {
            Tag = "test",
            StartingBid = 22_000_000,
            End = DateTime.Now + TimeSpan.FromDays(1),
            AuctioneerId = "000100",
            Uuid = "000100",
            FlatenedNBT = []
        };
        var key = service.KeyFromSaveAuction(bare);
        service.Lookups["test"] = new() { Lookup = new(new Dictionary<AuctionKey, ReferenceAuctions>() { { key, bucket } }) };
        service.FoundSnipe += flips.Add;
        service.TestNewAuction(bare);
        service.FinishedUpdate();
        Assert.That(flips.First(f => f.Finder == LowPricedAuction.FinderType.STONKS).TargetPrice, Is.EqualTo(27700000));
        Assert.That(bucket.RiskyEstimate, Is.EqualTo(27700000));
        var dupplicate = bare.Dupplicate();
        flips.Clear();
        service.TestNewAuction(dupplicate);
        service.FinishedUpdate();
        // max 5% over lbin
        Assert.That(flips.First(f => f.Finder == LowPricedAuction.FinderType.STONKS).TargetPrice, Is.EqualTo(23100000));
    }

    [Test]
    public void IgnoredBackAndWorthSellingExcludedFromVolume()
    {
        ReferenceAuctions bucket = LoadJsonReferences(PortalSample);
        bucket.References.Enqueue(new() { AuctionId = 1, Day = SniperService.GetDay(), Price = 2_000, Seller = 1, Buyer = 2 });
        bucket.References.Enqueue(new() { AuctionId = 2, Day = SniperService.GetDay(), Price = 2_200, Seller = 2, Buyer = 3 });
        bucket.References.Enqueue(new() { AuctionId = 3, Day = SniperService.GetDay(), Price = 2_000, Seller = 3, Buyer = 1 });

        service.UpdateMedian(bucket);
        Assert.That(bucket.Price, Is.EqualTo(2_200));
        Assert.That(bucket.Volume, Is.EqualTo(1));
    }
    private const string PortalSample =
    """
    [
        {
            "auctionId": -7278140679223593702,
            "price": 200000000,
            "day": 949,
            "seller": 15975,
            "buyer": -1461
        },
        {
            "auctionId": 6836884872096665689,
            "price": 200000000,
            "day": 951,
            "seller": -25965,
            "buyer": 2103
        },
        {
            "auctionId": 8092881010753573897,
            "price": 200000000,
            "day": 951,
            "seller": 2103,
            "buyer": -25965
        },
        {
            "auctionId": 264493352913775978,
            "price": 200000000,
            "day": 951,
            "seller": 28104,
            "buyer": 2103
        },
        {
            "auctionId": 8960060504682202984,
            "price": 200000000,
            "day": 951,
            "seller": -25965,
            "buyer": 28104
        },
        {
            "auctionId": 6252479138994737017,
            "price": 130000000,
            "day": 952,
            "seller": 2103,
            "buyer": 16780
        }
        ]
    """;

    private const string EstimateRisky = """
    [
        {
            "auctionId": 7254234092519418792,
            "price": 17999999,
            "day": 1063,
            "seller": 21928,
            "buyer": 27479
        },
        {
            "auctionId": -8038662720899688519,
            "price": 10800000,
            "day": 1063,
            "seller": -25629,
            "buyer": 25176
        },
        {
            "auctionId": -4367876076329092805,
            "price": 16500000,
            "day": 1064,
            "seller": 25176,
            "buyer": 808
        },
        {
            "auctionId": -6245639738059498423,
            "price": 22000000,
            "day": 1064,
            "seller": -23222,
            "buyer": 5230
        },
        {
            "auctionId": -1003165974223945639,
            "price": 22000000,
            "day": 1064,
            "seller": 3126,
            "buyer": 5230
        },
        {
            "auctionId": 6580729828753937098,
            "price": 20000000,
            "day": 1064,
            "seller": 14943,
            "buyer": 20740
        },
        {
            "auctionId": 6327018430150432011,
            "price": 20000000,
            "day": 1064,
            "seller": 20740,
            "buyer": 26959
        },
        {
            "auctionId": 882054134640534152,
            "price": 16969696,
            "day": 1065,
            "seller": -12959,
            "buyer": -5070
        },
        {
            "auctionId": 8076464048198441289,
            "price": 13000000,
            "day": 1065,
            "seller": 10446,
            "buyer": 13703
        },
        {
            "auctionId": -5999127142946077543,
            "price": 28400000,
            "day": 1065,
            "seller": 10551,
            "buyer": -529
        },
        {
            "auctionId": -7088397846779175014,
            "price": 28500000,
            "day": 1065,
            "seller": 13703,
            "buyer": -11115
        },
        {
            "auctionId": -7574668465663236472,
            "price": 21000000,
            "day": 1065,
            "seller": 5573,
            "buyer": -10105
        },
        {
            "auctionId": -513550495886215559,
            "price": 18000000,
            "day": 1066,
            "seller": -18676,
            "buyer": -17566
        },
        {
            "auctionId": -7246338352849458903,
            "price": 30000000,
            "day": 1066,
            "seller": -24390,
            "buyer": -23497
        },
        {
            "auctionId": 2431427818692381801,
            "price": 34990000,
            "day": 1066,
            "seller": 26959,
            "buyer": -20275
        },
        {
            "auctionId": -5782756744280901576,
            "price": 19999999,
            "day": 1071,
            "seller": -11412,
            "buyer": -15708
        },
        {
            "auctionId": 2695350292326742504,
            "price": 24000000,
            "day": 1072,
            "seller": -31441,
            "buyer": 21192
        },
        {
            "auctionId": -1470755108905246744,
            "price": 20000000,
            "day": 1073,
            "seller": -248,
            "buyer": 15870
        },
        {
            "auctionId": 3253105640461724697,
            "price": 18000000,
            "day": 1073,
            "seller": -21215,
            "buyer": -14579
        },
        {
            "auctionId": -3415675231492193527,
            "price": 26900000,
            "day": 1074,
            "seller": 8251,
            "buyer": -24026
        },
        {
            "auctionId": 1016498545246685946,
            "price": 27000000,
            "day": 1075,
            "seller": 8651,
            "buyer": -20792
        },
        {
            "auctionId": 1600162728333628363,
            "price": 20000000,
            "day": 1075,
            "seller": -15209,
            "buyer": -12402
        },
        {
            "auctionId": 8723756055860828905,
            "price": 25000000,
            "day": 1075,
            "seller": 12683,
            "buyer": 31396
        },
        {
            "auctionId": -6690422422630602422,
            "price": 30000000,
            "day": 1075,
            "seller": -1971,
            "buyer": 13109
        },
        {
            "auctionId": -8272892623669801287,
            "price": 19999000,
            "day": 1076,
            "seller": -18765,
            "buyer": 18255
        },
        {
            "auctionId": -1847207349785059831,
            "price": 4800000,
            "day": 1076,
            "seller": -23344,
            "buyer": 10012
        },
        {
            "auctionId": -1460329492135256854,
            "price": 15100000,
            "day": 1076,
            "seller": 10012,
            "buyer": 16184
        },
        {
            "auctionId": 5899335358870713257,
            "price": 24200000,
            "day": 1076,
            "seller": 16184,
            "buyer": -23036
        },
        {
            "auctionId": 8870183180032961355,
            "price": 24200000,
            "day": 1076,
            "seller": 18255,
            "buyer": -30320
        },
        {
            "auctionId": -2444847048960763221,
            "price": 5000000,
            "day": 1076,
            "seller": -28325,
            "buyer": 10411
        },
        {
            "auctionId": -297222486433925031,
            "price": 19000000,
            "day": 1077,
            "seller": -16172,
            "buyer": 29495
        },
        {
            "auctionId": -3045232868878028456,
            "price": 21900000,
            "day": 1077,
            "seller": 29495,
            "buyer": -3616
        },
        {
            "auctionId": 6637511862527160490,
            "price": 26000000,
            "day": 1077,
            "seller": -11608,
            "buyer": 2879
        },
        {
            "auctionId": 3335224956108045931,
            "price": 25000000,
            "day": 1077,
            "seller": 4951,
            "buyer": 20144
        },
        {
            "auctionId": -223046668533279447,
            "price": 21000000,
            "day": 1078,
            "seller": 17662,
            "buyer": 3220
        },
        {
            "auctionId": -86932548095094502,
            "price": 15500000,
            "day": 1078,
            "seller": 20222,
            "buyer": -4905
        },
        {
            "auctionId": -7412985086698813062,
            "price": 19400000,
            "day": 1078,
            "seller": -4905,
            "buyer": 13083
        },
        {
            "auctionId": 4701196181573153451,
            "price": 5500000,
            "day": 1078,
            "seller": 9113,
            "buyer": -23282
        },
        {
            "auctionId": 4116365642928416072,
            "price": 21800000,
            "day": 1078,
            "seller": 13083,
            "buyer": 7109
        },
        {
            "auctionId": 2796575566987713450,
            "price": 18000000,
            "day": 1078,
            "seller": -16013,
            "buyer": -12842
        },
        {
            "auctionId": 3223273706456324713,
            "price": 25000000,
            "day": 1078,
            "seller": 5000,
            "buyer": -12842
        },
        {
            "auctionId": -6280027814138139768,
            "price": 27500000,
            "day": 1078,
            "seller": -23282,
            "buyer": -28919
        },
        {
            "auctionId": 482137037084733192,
            "price": 34000000,
            "day": 1079,
            "seller": -13630,
            "buyer": 19032
        },
        {
            "auctionId": 8080020411301283691,
            "price": 22000000,
            "day": 1080,
            "seller": 24304,
            "buyer": 24503
        },
        {
            "auctionId": 354306183352009705,
            "price": 26000000,
            "day": 1080,
            "seller": 24503,
            "buyer": 28783
        },
        {
            "auctionId": -8234921450611878406,
            "price": 27499998,
            "day": 1080,
            "seller": -15559,
            "buyer": 31999
        },
        {
            "auctionId": 2288947888660404346,
            "price": 27500000,
            "day": 1081,
            "seller": -248,
            "buyer": 25880
        },
        {
            "auctionId": -2605898621370005415,
            "price": 25000000,
            "day": 1081,
            "seller": 20859,
            "buyer": 11950
        },
        {
            "auctionId": -2291957499371645557,
            "price": 29000000,
            "day": 1081,
            "seller": 18424,
            "buyer": 9927
        },
        {
            "auctionId": -7981269941346885973,
            "price": 6000000,
            "day": 1082,
            "seller": -27551,
            "buyer": 2594
        },
        {
            "auctionId": -4135081511301996312,
            "price": 25200000,
            "day": 1082,
            "seller": 2594,
            "buyer": 20476
        },
        {
            "auctionId": 5572246967193610027,
            "price": 35000000,
            "day": 1083,
            "seller": 2419,
            "buyer": 4995
        },
        {
            "auctionId": 3381504786117282634,
            "price": 21999999,
            "day": 1083,
            "seller": -30278,
            "buyer": -3433
        },
        {
            "auctionId": -2015060006680909302,
            "price": 27500000,
            "day": 1084,
            "seller": -13568,
            "buyer": 978
        },
        {
            "auctionId": 2128922496163711865,
            "price": 28000000,
            "day": 1084,
            "seller": 8651,
            "buyer": -19845
        },
        {
            "auctionId": 5892492801287536073,
            "price": 28000000,
            "day": 1084,
            "seller": -17960,
            "buyer": -27830
        },
        {
            "auctionId": 6454708718972884249,
            "price": 28400000,
            "day": 1084,
            "seller": -3433,
            "buyer": -8401
        },
        {
            "auctionId": -527745267492881959,
            "price": 28000000,
            "day": 1084,
            "seller": 21047,
            "buyer": 26310
        },
        {
            "auctionId": 1324180952782660392,
            "price": 29999000,
            "day": 1085,
            "seller": 9984,
            "buyer": 9824
        },
        {
            "auctionId": -50569598724946773,
            "price": 39900000,
            "day": 1086,
            "seller": 2626,
            "buyer": -12985
        },
        {
            "auctionId": 1841635527606576953,
            "price": 39000000,
            "day": 1086,
            "seller": -16392,
            "buyer": -31753
        },
        {
            "auctionId": 1003427088842968105,
            "price": 44999999,
            "day": 1086,
            "seller": 5942,
            "buyer": -30481
        },
        {
            "auctionId": -8983660641598894854,
            "price": 45000000,
            "day": 1086,
            "seller": -31753,
            "buyer": -15040
        },
        {
            "auctionId": -1745269596749245622,
            "price": 18000000,
            "day": 1087,
            "seller": -31735,
            "buyer": 14421
        },
        {
            "auctionId": 5943859445999571097,
            "price": 22000000,
            "day": 1087,
            "seller": 14240,
            "buyer": -24409
        },
        {
            "auctionId": -8122557197815968229,
            "price": 30310000,
            "day": 1087,
            "seller": 14421,
            "buyer": -30728
        },
        {
            "auctionId": -3177485040222959192,
            "price": 27700000,
            "day": 1087,
            "seller": -24409,
            "buyer": 15604
        },
        {
            "auctionId": 2153039474809529499,
            "price": 5000000,
            "day": 1087,
            "seller": -4315,
            "buyer": 9274
        },
        {
            "auctionId": -6023916458771272344,
            "price": 29300000,
            "day": 1087,
            "seller": 9274,
            "buyer": 27300
        },
        {
            "auctionId": -8019664403266678086,
            "price": 45000000,
            "day": 1087,
            "seller": -31753,
            "buyer": 12967
        },
        {
            "auctionId": 2084035366666070873,
            "price": 22994400,
            "day": 1088,
            "seller": -10935,
            "buyer": 22912
        },
        {
            "auctionId": 6890722597062886377,
            "price": 23999999,
            "day": 1089,
            "seller": 26493,
            "buyer": 12957
        },
        {
            "auctionId": 2493706985656177547,
            "price": 5000000,
            "day": 1089,
            "seller": 12739,
            "buyer": -31193
        },
        {
            "auctionId": -6805826831154542135,
            "price": 23700000,
            "day": 1089,
            "seller": 10873,
            "buyer": 26496
        },
        {
            "auctionId": 5759468522930336970,
            "price": 25400000,
            "day": 1089,
            "seller": -31193,
            "buyer": -23089
        },
        {
            "auctionId": 8960096996585549467,
            "price": 21000000,
            "day": 1089,
            "seller": 3983,
            "buyer": 12692
        }
    ]
    """;

    /// <summary>
    /// Taken from terror boots
    /// </summary>
    private const string InflationExample =
    """
    [
        {
            "auctionId": 518123438723975451,
            "price": 74999999,
            "day": 1047,
            "seller": 7410,
            "buyer": 17414
        },
        {
            "auctionId": -8035730067847406085,
            "price": 71000000,
            "day": 1047,
            "seller": 11845,
            "buyer": 15422
        },
        {
            "auctionId": 3126536004740535545,
            "price": 88888888,
            "day": 1047,
            "seller": -12149,
            "buyer": 22671
        },
        {
            "auctionId": -7227451059622661463,
            "price": 111111111,
            "day": 1047,
            "seller": -30264,
            "buyer": 26784
        },
        {
            "auctionId": -4237598102266207141,
            "price": 105000000,
            "day": 1048,
            "seller": -6669,
            "buyer": 31347
        },
        {
            "auctionId": -1644773753825752805,
            "price": 82000000,
            "day": 1048,
            "seller": -26049,
            "buyer": 30332
        },
        {
            "auctionId": -870498092126017400,
            "price": 83000000,
            "day": 1049,
            "seller": -11505,
            "buyer": -22334
        },
        {
            "auctionId": -7348502473071684536,
            "price": 95000000,
            "day": 1049,
            "seller": -2252,
            "buyer": 29260
        },
        {
            "auctionId": -359256412094768917,
            "price": 100000000,
            "day": 1049,
            "seller": 19995,
            "buyer": 25619
        },
        {
            "auctionId": -8634012289838782742,
            "price": 86999999,
            "day": 1049,
            "seller": -15795,
            "buyer": -19423
        },
        {
            "auctionId": 3608048053612840106,
            "price": 82000000,
            "day": 1049,
            "seller": 18502,
            "buyer": 16003
        },
        {
            "auctionId": 2063593661831173242,
            "price": 68696969,
            "day": 1050,
            "seller": -4407,
            "buyer": 10961
        },
        {
            "auctionId": -1375443632874158680,
            "price": 67000000,
            "day": 1050,
            "seller": 31852,
            "buyer": 8604
        },
        {
            "auctionId": -3020568833750711765,
            "price": 69990000,
            "day": 1051,
            "seller": -25942,
            "buyer": 27721
        },
        {
            "auctionId": -6750653980762842040,
            "price": 69999000,
            "day": 1051,
            "seller": -31968,
            "buyer": -4447
        },
        {
            "auctionId": -5625132751979660791,
            "price": 61000000,
            "day": 1052,
            "seller": 3976,
            "buyer": -7266
        },
        {
            "auctionId": -8886153930752444294,
            "price": 70000000,
            "day": 1052,
            "seller": -19555,
            "buyer": -11489
        },
        {
            "auctionId": 5597666832170976603,
            "price": 100000000,
            "day": 1052,
            "seller": -10281,
            "buyer": 5392
        },
        {
            "auctionId": 9138549377262385019,
            "price": 74999000,
            "day": 1053,
            "seller": -3739,
            "buyer": 16815
        },
        {
            "auctionId": 4910848911159194139,
            "price": 59000000,
            "day": 1053,
            "seller": -25719,
            "buyer": 7016
        },
        {
            "auctionId": 4485545432293442170,
            "price": 73400000,
            "day": 1053,
            "seller": 7016,
            "buyer": 3881
        },
        {
            "auctionId": 1398598197084097595,
            "price": 75000000,
            "day": 1053,
            "seller": -26843,
            "buyer": 11987
        },
        {
            "auctionId": -5282102473868191318,
            "price": 60000000,
            "day": 1054,
            "seller": -11166,
            "buyer": -2885
        },
        {
            "auctionId": -8322309046128235239,
            "price": 69300000,
            "day": 1054,
            "seller": -2885,
            "buyer": -22792
        },
        {
            "auctionId": -1959464807825516917,
            "price": 58000000,
            "day": 1054,
            "seller": -13454,
            "buyer": -22629
        },
        {
            "auctionId": 5496535110696879176,
            "price": 61000000,
            "day": 1054,
            "seller": -22629,
            "buyer": 24531
        },
        {
            "auctionId": -8087649363232903480,
            "price": 68000000,
            "day": 1054,
            "seller": 24531,
            "buyer": 27517
        },
        {
            "auctionId": -4179366429599409656,
            "price": 65999900,
            "day": 1054,
            "seller": -29932,
            "buyer": -11705
        },
        {
            "auctionId": 2361039031420623978,
            "price": 66000000,
            "day": 1054,
            "seller": -10450,
            "buyer": 998
        },
        {
            "auctionId": 8944235862229574411,
            "price": 61969696,
            "day": 1055,
            "seller": -12959,
            "buyer": -22485
        },
        {
            "auctionId": 4805787813806197561,
            "price": 73900000,
            "day": 1055,
            "seller": -7266,
            "buyer": -18997
        },
        {
            "auctionId": 8023910729130169113,
            "price": 63000000,
            "day": 1055,
            "seller": -26637,
            "buyer": -4050
        },
        {
            "auctionId": 3595353372889112,
            "price": 76900000,
            "day": 1055,
            "seller": -28277,
            "buyer": -17530
        },
        {
            "auctionId": -6053236472448802584,
            "price": 70000000,
            "day": 1056,
            "seller": 13408,
            "buyer": -23174
        },
        {
            "auctionId": 7138072777177988282,
            "price": 69000000,
            "day": 1056,
            "seller": -1791,
            "buyer": 980
        },
        {
            "auctionId": -3473764253927346120,
            "price": 67900000,
            "day": 1056,
            "seller": 18191,
            "buyer": 2657
        },
        {
            "auctionId": -9101818875999450200,
            "price": 67000000,
            "day": 1056,
            "seller": -7351,
            "buyer": -26488
        },
        {
            "auctionId": -759842589988688967,
            "price": 70000000,
            "day": 1057,
            "seller": 1578,
            "buyer": 4746
        },
        {
            "auctionId": 5001894804720516842,
            "price": 78000000,
            "day": 1059,
            "seller": -3925,
            "buyer": -31274
        },
        {
            "auctionId": -4351156733514953191,
            "price": 82000000,
            "day": 1059,
            "seller": 32495,
            "buyer": 28821
        },
        {
            "auctionId": -3842623353092893285,
            "price": 83000000,
            "day": 1059,
            "seller": -17267,
            "buyer": 11657
        },
        {
            "auctionId": 7083687501523573096,
            "price": 76500000,
            "day": 1059,
            "seller": -16342,
            "buyer": 7262
        },
        {
            "auctionId": -3447781499670308533,
            "price": 87000000,
            "day": 1059,
            "seller": 20663,
            "buyer": 13684
        },
        {
            "auctionId": -1120612316428530983,
            "price": 91000000,
            "day": 1059,
            "seller": 7262,
            "buyer": 10455
        },
        {
            "auctionId": 8846181317417642617,
            "price": 84444444,
            "day": 1060,
            "seller": 24486,
            "buyer": 22580
        },
        {
            "auctionId": -2663994807240052933,
            "price": 73000000,
            "day": 1060,
            "seller": -23853,
            "buyer": 17532
        },
        {
            "auctionId": 6289725292694948315,
            "price": 73000000,
            "day": 1060,
            "seller": -27853,
            "buyer": -14428
        },
        {
            "auctionId": 2549281749344935992,
            "price": 76000000,
            "day": 1061,
            "seller": -14428,
            "buyer": -31092
        },
        {
            "auctionId": -9070547458816868248,
            "price": 80000000,
            "day": 1062,
            "seller": 20199,
            "buyer": 29692
        },
        {
            "auctionId": -5464156019106227160,
            "price": 69999000,
            "day": 1062,
            "seller": 14534,
            "buyer": 20046
        },
        {
            "auctionId": 5031687995072036584,
            "price": 70000000,
            "day": 1063,
            "seller": 6837,
            "buyer": -19941
        },
        {
            "auctionId": 8332464203024102426,
            "price": 72000000,
            "day": 1063,
            "seller": 25741,
            "buyer": 22007
        },
        {
            "auctionId": -2793547398167719015,
            "price": 70000000,
            "day": 1063,
            "seller": -13311,
            "buyer": 16441
        },
        {
            "auctionId": 3892781089186368696,
            "price": 85000000,
            "day": 1064,
            "seller": -2252,
            "buyer": -4307
        },
        {
            "auctionId": -3516737537149558280,
            "price": 80000000,
            "day": 1065,
            "seller": 8184,
            "buyer": 8686
        },
        {
            "auctionId": 8660259422120401336,
            "price": 88000000,
            "day": 1067,
            "seller": 20447,
            "buyer": -10224
        },
        {
            "auctionId": -5568926787376117944,
            "price": 85000000,
            "day": 1068,
            "seller": 18123,
            "buyer": -21283
        },
        {
            "auctionId": 4130857919379834635,
            "price": 85900000,
            "day": 1070,
            "seller": -19934,
            "buyer": 727
        },
        {
            "auctionId": 5505764498829444825,
            "price": 110000000,
            "day": 1072,
            "seller": -2369,
            "buyer": 2910
        },
        {
            "auctionId": 6905107282004789242,
            "price": 145000000,
            "day": 1072,
            "seller": -27120,
            "buyer": -1667
        },
        {
            "auctionId": -33668552054569653,
            "price": 145000000,
            "day": 1073,
            "seller": -8115,
            "buyer": -15599
        },
        {
            "auctionId": 6431098038151656139,
            "price": 142999000,
            "day": 1073,
            "seller": -17594,
            "buyer": -3267
        },
        {
            "auctionId": -8631933638542529829,
            "price": 120000000,
            "day": 1073,
            "seller": 17294,
            "buyer": 7735
        },
        {
            "auctionId": 6053979410808003979,
            "price": 133000000,
            "day": 1073,
            "seller": 22293,
            "buyer": 27156
        },
        {
            "auctionId": -497881893390831845,
            "price": 145000000,
            "day": 1074,
            "seller": 17909,
            "buyer": -9869
        },
        {
            "auctionId": -7651292190390522902,
            "price": 159999999,
            "day": 1075,
            "seller": -25012,
            "buyer": -18277
        },
        {
            "auctionId": -3347391556618468965,
            "price": 130000000,
            "day": 1075,
            "seller": -22186,
            "buyer": -31496
        },
        {
            "auctionId": -6740488299579855141,
            "price": 130000000,
            "day": 1075,
            "seller": 1274,
            "buyer": -25317
        },
        {
            "auctionId": -8770031943576406166,
            "price": 122222000,
            "day": 1076,
            "seller": 14151,
            "buyer": 20271
        },
        {
            "auctionId": -7648831391496683303,
            "price": 110000000,
            "day": 1076,
            "seller": -17917,
            "buyer": 29495
        },
        {
            "auctionId": 2397646645616956906,
            "price": 125000000,
            "day": 1076,
            "seller": 20271,
            "buyer": 21632
        },
        {
            "auctionId": 2197834729016149785,
            "price": 129999999,
            "day": 1076,
            "seller": -1407,
            "buyer": 27803
        },
        {
            "auctionId": -8625907750029863767,
            "price": 158000000,
            "day": 1077,
            "seller": -411,
            "buyer": -10787
        },
        {
            "auctionId": -7488335364002575253,
            "price": 175000000,
            "day": 1078,
            "seller": -20317,
            "buyer": 5573
        },
        {
            "auctionId": 7381181370262024136,
            "price": 180000000,
            "day": 1078,
            "seller": -10678,
            "buyer": -20852
        },
        {
            "auctionId": -5444039105043879752,
            "price": 160000000,
            "day": 1079,
            "seller": -1791,
            "buyer": -32626
        },
        {
            "auctionId": -1310996187893927365,
            "price": 115599000,
            "day": 1079,
            "seller": 29495,
            "buyer": -31193
        },
        {
            "auctionId": -6836073598062357685,
            "price": 133900000,
            "day": 1079,
            "seller": -31193,
            "buyer": 20724
        },
        {
            "auctionId": 1602443803216371497,
            "price": 152000000,
            "day": 1079,
            "seller": 21264,
            "buyer": -29225
        },
        {
            "auctionId": 4292354270240248361,
            "price": 134000000,
            "day": 1080,
            "seller": -20638,
            "buyer": -22186
        }
    ]
    """;

    private const string GlacialScytheSample =
    """
    [
        {
            "auctionId": 4020836841702215851,
            "price": 220240442,
            "day": 994,
            "seller": -20267,
            "buyer": 0
        },
        {
            "auctionId": -2270585272559252613,
            "price": 211734696,
            "day": 1024,
            "seller": -20267,
            "buyer": 11344
        },
        {
            "auctionId": -4787987846722082038,
            "price": 193463985,
            "day": 1028,
            "seller": -20267,
            "buyer": -25726
        },
        {
            "auctionId": -8209096686535755701,
            "price": 225523770,
            "day": 1036,
            "seller": -366,
            "buyer": -12571
        },
        {
            "auctionId": 5469785141746872779,
            "price": 173856124,
            "day": 1039,
            "seller": -20267,
            "buyer": 5875
        },
        {
            "auctionId": -3793096646597804501,
            "price": 180009764,
            "day": 1043,
            "seller": -20267,
            "buyer": -30221
        },
        {
            "auctionId": -4734344319201572776,
            "price": 179678755,
            "day": 1048,
            "seller": -20267,
            "buyer": -17595
        },
        {
            "auctionId": 7675932848496993704,
            "price": 66638813,
            "day": 1049,
            "seller": -17595,
            "buyer": 2103
        },
        {
            "auctionId": 6144904731850084216,
            "price": 75367558,
            "day": 1049,
            "seller": 2103,
            "buyer": -31838
        },
        {
            "auctionId": 4733466244300888522,
            "price": 177033833,
            "day": 1051,
            "seller": -19322,
            "buyer": 26607
        },
        {
            "auctionId": -739731391836618360,
            "price": 99558485,
            "day": 1053,
            "seller": -31838,
            "buyer": 19884
        }
    ]
    """;

    private const string NacklaceSample =
    """
    [{
            "auctionId": 7626175173905406105,
            "price": 1800000,
            "day": 969,
            "seller": -30444,
            "buyer": 27755
        },
        {
            "auctionId": 8709509529764403195,
            "price": 400000000,
            "day": 971,
            "seller": -30734,
            "buyer": 5654
        },
        {
            "auctionId": -1251946910355872664,
            "price": 400000000,
            "day": 971,
            "seller": 5654,
            "buyer": -30734
        },
        {
            "auctionId": -2876664507248205639,
            "price": 400000000,
            "day": 971,
            "seller": -30734,
            "buyer": 5654
        },
        {
            "auctionId": -2200987455195264277,
            "price": 400000000,
            "day": 971,
            "seller": 5654,
            "buyer": -30734
        }]
    """;

    private const string LowDropMedian =
    """
        [{
            "auctionId": 2270766506477480217,
            "price": 64000000,
            "day": 891,
            "seller": 26983,
            "buyer": 0
        },
        {
            "auctionId": -3902876673741236581,
            "price": 46500000,
            "day": 917,
            "seller": 17673,
            "buyer": 0
        },
        {
            "auctionId": -4870748277545419096,
            "price": 58178747,
            "day": 930,
            "seller": -22547,
            "buyer": 0
        },
        {
            "auctionId": -82296909508691448,
            "price": 61000000,
            "day": 930,
            "seller": -32312,
            "buyer": 0
        },
        {
            "auctionId": -3625130011186531175,
            "price": 54000000,
            "day": 933,
            "seller": -16422,
            "buyer": 0
        },
        {
            "auctionId": 3454623913310012187,
            "price": 59000000,
            "day": 938,
            "seller": -16748,
            "buyer": 0
        },
        {
            "auctionId": -7189316192931376501,
            "price": 50000000,
            "day": 938,
            "seller": 26405,
            "buyer": 0
        },
        {
            "auctionId": 4409215473139842457,
            "price": 57,
            "day": 953,
            "seller": -26648,
            "buyer": 0
        }]
    """;

    private const string TerroChestplateSample =
    """
        [{
            "auctionId": -7527501475029030085,
            "price": 110000000,
            "day": 932,
            "seller": -4267,
            "buyer": 0
        },
        {
            "auctionId": -5970723489250354072,
            "price": 128000000,
            "day": 933,
            "seller": 24244,
            "buyer": 0
        },
        {
            "auctionId": -8043377983277264503,
            "price": 99000000,
            "day": 934,
            "seller": 31195,
            "buyer": 0
        },
        {
            "auctionId": -7564931173395299960,
            "price": 105000000,
            "day": 938,
            "seller": 25217,
            "buyer": 29594
        },
        {
            "auctionId": -5534639488087985413,
            "price": 112000000,
            "day": 939,
            "seller": 1675,
            "buyer": -12895
        },
        {
            "auctionId": 4834515643280139978,
            "price": 94000000,
            "day": 940,
            "seller": 312,
            "buyer": 2118
        },
        {
            "auctionId": -8331607608609323589,
            "price": 113000000,
            "day": 940,
            "seller": -10525,
            "buyer": -18586
        },
        {
            "auctionId": 9067452782427709083,
            "price": 80000000,
            "day": 948,
            "seller": -4554,
            "buyer": -19896
        },
        {
            "auctionId": -1936579432630648007,
            "price": 85555555,
            "day": 949,
            "seller": 10811,
            "buyer": -22895
        },
        {
            "auctionId": -5770107610556793077,
            "price": 65000000,
            "day": 951,
            "seller": 26446,
            "buyer": 23443
        },
        {
            "auctionId": 3455361164650038537,
            "price": 69230000,
            "day": 952,
            "seller": 23443,
            "buyer": -19024
        },
        {
            "auctionId": 9149645891195398155,
            "price": 69000000,
            "day": 952,
            "seller": -19481,
            "buyer": 21217
        },
        {
            "auctionId": -6555651574664982230,
            "price": 65000000,
            "day": 955,
            "seller": 11577,
            "buyer": -7735
        },
        {
            "auctionId": -1448169863757344694,
            "price": 498000,
            "day": 956,
            "seller": 9629,
            "buyer": 24484
        },
        {
            "auctionId": 29269359614874457,
            "price": 57500000,
            "day": 956,
            "seller": -7569,
            "buyer": -32017
        },
        {
            "auctionId": -6564201068353299014,
            "price": 59340000,
            "day": 957,
            "seller": -32017,
            "buyer": 0
        },


        {
            "auctionId": -3149981014479431397,
            "price": 1500000,
            "day": 957,
            "seller": 7522,
            "buyer": 0
        }]
        """;

    private const string LbinDropSample =
        """
            {"references":[{
                "auctionId": -1315403493071137448,
                "price": 1880000000,
                    "day": 1040,
                    "seller": -31001,
                    "buyer": -832
                },
                {
                    "auctionId": 5887142684324954953,
                    "price": 2059999000,
                    "day": 1040,
                    "seller": -14209,
                    "buyer": 7803
                },
                {
                    "auctionId": -1220969875094463031,
                    "price": 1705712656,
                    "day": 1041,
                    "seller": 28015,
                    "buyer": 21688
                },
                {
                    "auctionId": 5844703361014534667,
                    "price": 1500000000,
                    "day": 1041,
                    "seller": 7803,
                    "buyer": -11173
                },
                {
                    "auctionId": 1161483913175227803,
                    "price": 1475000000,
                    "day": 1041,
                    "seller": 21688,
                    "buyer": 17146
                },
                {
                    "auctionId": -6001875131109209783,
                    "price": 1200000000,
                    "day": 1042,
                    "seller": -11173,
                    "buyer": -10662
                }
            ],
            "oldestRef": 1042,
            "lbins": [
                {
                    "auctionId": 2845389508689271178,
                    "price": 1160000000,
                    "day": 1056,
                    "seller": 1522,
                    "buyer": 0
                },
                {
                    "auctionId": -9097057862076068245,
                    "price": 1340000000,
                    "day": 1056,
                    "seller": -28657,
                    "buyer": 0
                },
                {
                    "auctionId": 7305505674336939352,
                    "price": 1350000000,
                    "day": 1056,
                    "seller": 21688,
                    "buyer": 0
                },
                {
                    "auctionId": -4796842577665116744,
                    "price": 1460199000,
                    "day": 1046,
                    "seller": -11173,
                    "buyer": 0
                },
                {
                    "auctionId": -2635734077317869013,
                    "price": 1600000000,
                    "day": 1055,
                    "seller": -7210,
                    "buyer": 0
                }
                ]}
        """;

    private const string FlipSample =
"""
    [
        {
            "auctionId": 6256124353103244712,
            "price": 79500000,
            "day": 941,
            "seller": 3229,
            "buyer": 3966
        },
        {
            "auctionId": 5416676501549248587,
            "price": 50000000,
            "day": 944,
            "seller": -18023,
            "buyer": -14356
        },
        {
            "auctionId": -2245860291070239942,
            "price": 1950000,
            "day": 948,
            "seller": 1672,
            "buyer": 17385
        },
        {
            "auctionId": 2378801729071542763,
            "price": 1800000,
            "day": 948,
            "seller": 20682,
            "buyer": -13029
        },
        {
            "auctionId": -7040391235981127270,
            "price": 30000000,
            "day": 948,
            "seller": 17385,
            "buyer": 10048
        },
        {
            "auctionId": 4544445791807361193,
            "price": 29999000,
            "day": 948,
            "seller": -13029,
            "buyer": 1
        }
        ]
    """;
    private const string SampleJson =
    """
        [{
            "auctionId": -2732142930133056936,
            "price": 87000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -2752980860056834230,
            "price": 87000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": 5157413408858517017,
            "price": 85000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -6248523692441969303,
            "price": 83999999,
            "day": 740,
            "seller": 21753
        },
        {
            "auctionId": -4669724471806533975,
            "price": 87000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -2535979681380659686,
            "price": 85000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -5735291441509210773,
            "price": 85000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -7925995716324691528,
            "price": 85000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -2109575094840585542,
            "price": 85000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": 6194941366429649146,
            "price": 84000000,
            "day": 740,
            "seller": 21753
        },
        {
            "auctionId": -1055547798713158968,
            "price": 85000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -2853228903831685127,
            "price": 84000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -1265194111805962279,
            "price": 84000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -1567558984268466406,
            "price": 84000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -8417986652914667845,
            "price": 84000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": 5832276233042638763,
            "price": 84000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": 1398606653832273147,
            "price": 84000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -8162860575261880455,
            "price": 84000000,
            "day": 740,
            "seller": 21753
        },
        {
            "auctionId": -3812782279345376533,
            "price": 84000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -2245724075268271990,
            "price": 84000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -3256387947106061621,
            "price": 84000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": 308160824667747658,
            "price": 84990000,
            "day": 740,
            "seller": 15726
        },
        {
            "auctionId": 3643061862025901144,
            "price": 84990000,
            "day": 740,
            "seller": 15726
        },
        {
            "auctionId": 5614073037287391576,
            "price": 100000000,
            "day": 740,
            "seller": 2035
        },
        {
            "auctionId": -8401093005351728072,
            "price": 86000000,
            "day": 740,
            "seller": -20922
        },
        {
            "auctionId": -817467878497928357,
            "price": 77000000,
            "day": 740,
            "seller": -5827
        },
        {
            "auctionId": -8492982444423622101,
            "price": 77000000,
            "day": 740,
            "seller": 21753
        },
        {
            "auctionId": -7146355288575957861,
            "price": 87000000,
            "day": 740,
            "seller": 17919
        },
        {
            "auctionId": 5355803883513510328,
            "price": 84000000,
            "day": 740,
            "seller": -20072
        },
        {
            "auctionId": -217620008983301128,
            "price": 85000000,
            "day": 740,
            "seller": 2035
        },
        {
            "auctionId": -7934245860363916725,
            "price": 77000000,
            "day": 740,
            "seller": 7234
        },
        {
            "auctionId": -6121338238757290247,
            "price": 77000000,
            "day": 740,
            "seller": -30527
        },
        {
            "auctionId": -5684609079020203415,
            "price": 79999000,
            "day": 740,
            "seller": -29271
        },
        {
            "auctionId": 5985122077077142315,
            "price": 78000000,
            "day": 740,
            "seller": 690
        },
        {
            "auctionId": 4283357880454459849,
            "price": 78000000,
            "day": 740,
            "seller": 690
        },
        {
            "auctionId": -3790505612661485398,
            "price": 78000000,
            "day": 740,
            "seller": 690
        },
        {
            "auctionId": -3841283849167857976,
            "price": 76000000,
            "day": 740,
            "seller": 690
        },
        {
            "auctionId": 1948910241809469177,
            "price": 76000000,
            "day": 740,
            "seller": 690
        },
        {
            "auctionId": 5216960445902272619,
            "price": 75000000,
            "day": 740,
            "seller": 690
        },
        {
            "auctionId": -6602296037794514709,
            "price": 75000000,
            "day": 740,
            "seller": 690
        },
        {
            "auctionId": 6973062852207011930,
            "price": 75000000,
            "day": 740,
            "seller": -10933
        },
        {
            "auctionId": -5725029977886497544,
            "price": 74000000,
            "day": 740,
            "seller": 690
        },
        {
            "auctionId": -1740401114162613190,
            "price": 73000000,
            "day": 740,
            "seller": 690
        },
        {
            "auctionId": -927513081314928901,
            "price": 73000000,
            "day": 740,
            "seller": 11522
        },
        {
            "auctionId": -8078881803091887864,
            "price": 73000000,
            "day": 740,
            "seller": -8631
        },
        {
            "auctionId": -4788896231667045640,
            "price": 72999999,
            "day": 740,
            "seller": -8631
        },
        {
            "auctionId": -5520750451490669239,
            "price": 72800000,
            "day": 740,
            "seller": 2521
        },
        {
            "auctionId": 838849458588187579,
            "price": 70000000,
            "day": 741,
            "seller": 3076
        },
        {
            "auctionId": -2269662169490550920,
            "price": 72500000,
            "day": 741,
            "seller": -27996
        },
        {
            "auctionId": -2105272554209874421,
            "price": 72500000,
            "day": 741,
            "seller": 1836
        },
        {
            "auctionId": 4790338466129804328,
            "price": 72000000,
            "day": 741,
            "seller": 27858
        },
        {
            "auctionId": -2191951592574956184,
            "price": 71000000,
            "day": 741,
            "seller": 2103
        },
        {
            "auctionId": -5605455235241583429,
            "price": 71000000,
            "day": 741,
            "seller": 10059
        },
        {
            "auctionId": -6123845304522725669,
            "price": 62000000,
            "day": 741,
            "seller": 1259
        },
        {
            "auctionId": -7899604181580651895,
            "price": 62000000,
            "day": 741,
            "seller": 1259
        },
        {
            "auctionId": 4978043068622267656,
            "price": 62000000,
            "day": 741,
            "seller": -13124
        },
        {
            "auctionId": 7058143770274460203,
            "price": 61000000,
            "day": 741,
            "seller": -13124
        },
        {
            "auctionId": 2720957252630700377,
            "price": 67989990,
            "day": 741,
            "seller": 4783
        },
        {
            "auctionId": -5893356594800879077,
            "price": 60000000,
            "day": 741,
            "seller": -21518
        },
        {
            "auctionId": -7256269096306202983,
            "price": 60000000,
            "day": 741,
            "seller": -21518
        },
        {
            "auctionId": -1591183609152722631,
            "price": 68000000,
            "day": 741,
            "seller": -20983
        },
        {
            "auctionId": -8625444472254371797,
            "price": 69000000,
            "day": 741,
            "seller": -20983
        },
        {
            "auctionId": -7269145719223798967,
            "price": 68400000,
            "day": 741,
            "seller": 6419
        },
        {
            "auctionId": -450953611544247047,
            "price": 67000000,
            "day": 741,
            "seller": -1873
        },
        {
            "auctionId": 6975328423389521195,
            "price": 68400000,
            "day": 741,
            "seller": -17704
        },
        {
            "auctionId": 3559873552283618392,
            "price": 66999999,
            "day": 741,
            "seller": 15451
        },
        {
            "auctionId": -6251605260218425543,
            "price": 65999999,
            "day": 741,
            "seller": 15451
        },
        {
            "auctionId": 8136231316976600187,
            "price": 66400000,
            "day": 741,
            "seller": 29171
        },
        {
            "auctionId": -5897897769717177863,
            "price": 66400000,
            "day": 741,
            "seller": 6419
        },
        {
            "auctionId": 1797745995664858154,
            "price": 60000000,
            "day": 741,
            "seller": 12643
        },
        {
            "auctionId": 2758266720624140795,
            "price": 58000000,
            "day": 741,
            "seller": 15606
        },
        {
            "auctionId": 1921571230065061096,
            "price": 55000000,
            "day": 741,
            "seller": 5706
        },
        {
            "auctionId": 1664953430166786440,
            "price": 51000000,
            "day": 741,
            "seller": 853
        },
        {
            "auctionId": 9220642916581399050,
            "price": 47000000,
            "day": 741,
            "seller": -10566
        },
        {
            "auctionId": -2888743355973588790,
            "price": 47000000,
            "day": 741,
            "seller": -10566
        },
        {
            "auctionId": -8624155317494033286,
            "price": 50000000,
            "day": 741,
            "seller": -28582
        },
        {
            "auctionId": -6097448743503261669,
            "price": 54900000,
            "day": 741,
            "seller": -233
        },
        {
            "auctionId": -992311499414468006,
            "price": 50000000,
            "day": 741,
            "seller": -28582
        },
        {
            "auctionId": -1661857102310322327,
            "price": 55000000,
            "day": 741,
            "seller": 9823
        },
        {
            "auctionId": 512462678682031385,
            "price": 55000000,
            "day": 741,
            "seller": 9823
        }
    ]
    """;
}