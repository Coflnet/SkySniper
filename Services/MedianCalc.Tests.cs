using System;
using System.Collections.Concurrent;
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
        var bucket = new ReferenceAuctions();
        bucket.References = new ConcurrentQueue<ReferencePrice>();
        var sample = JsonConvert.DeserializeObject<ReferencePrice[]>(json);
        var dayDiff = SniperService.GetDay() - sample.Last().Day;
        foreach (var item in sample)
        {
            var adopted = new ReferencePrice()
            {
                AuctionId = item.AuctionId,
                Day = (short)(item.Day + dayDiff),
                Price = item.Price,
                Seller = item.Seller,
                Buyer = item.Buyer
            };
            bucket.References.Enqueue(adopted);
        }

        return bucket;
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
        for (int i = 0; i < 5; i++)
        {
            var copy = auction.Dupplicate();
            copy.Bids = new (){new SaveBids() { Bidder = random.Next().ToString(), Amount = i * 1000 }};
            copy.HighestBidAmount = i * 1000;
            service.AddSoldItem(copy);
        }
        for (int i = 0; i < 10; i++)
        {
            var copy = auction.Dupplicate();
            copy.Bids = new (){new SaveBids() { Bidder = "abcdef", Amount = 5000000 }};
            copy.HighestBidAmount = 5000000;
            service.AddSoldItem(copy);
        }
        Assert.That(2000,Is.EqualTo(service.Lookups.First().Value.Lookup.First().Value.Price));
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
        Assert.That(bucket.Price, Is.EqualTo(57500000));
    }

    [Test]
    public void LowDropMedianLimit()
    {
        ReferenceAuctions bucket = LoadJsonReferences(LowDropMedian);
        service.UpdateMedian(bucket);
        Assert.That(bucket.Price, Is.EqualTo(50000000));
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