using System.Collections.Concurrent;
using Coflnet.Sky.Sniper.Models;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;
public class MedianCalcTests
{
    [Test]
    public void LargeData()
    {
        var service = new SniperService(null);
        var bucket = new ReferenceAuctions();
        bucket.References = new ConcurrentQueue<ReferencePrice>();
        var sample = JsonConvert.DeserializeObject<ReferencePrice[]>(SampleJson);
        var dayDiff = SniperService.GetDay() - sample[0].Day;
        foreach (var item in sample)
        {
            var adopted = new ReferencePrice()
            {
                AuctionId = item.AuctionId,
                Day = (short)(item.Day + dayDiff),
                Price = item.Price,
                Seller = item.Seller
            };
            bucket.References.Enqueue(adopted);
        }
        service.UpdateMedian(bucket);
        Assert.AreEqual(54900000, bucket.Price);
    }

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