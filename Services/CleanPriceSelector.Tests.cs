using System;
using System.Collections.Generic;
using System.Linq;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using MessagePack;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>
/// The clean prices endpoint picked from buckets with an in-memory <see cref="ReferenceAuctions.TimeToSell"/> above 5.
/// That field is not persisted, so after a restart only the buckets recomputed since then were considered: pods
/// disagreed and exotic variants (GLACITE_LEGGINGS 990k instead of 12k) or 1 coin buckets became the clean price.
/// </summary>
public class CleanPriceSelectorTests
{
    private const string Tag = "GLACITE_LEGGINGS";

    [Test]
    public void SamePersistedStateGivesSamePriceAfterRestart()
    {
        var lookup = new PriceLookup();
        lookup.Lookup[CleanKey()] = Bucket(12_000, references: 40, overDays: 10);
        // only the exotic variant was recomputed since the last restart
        lookup.Lookup[EnchantedKey()] = Bucket(990_000, references: 40, overDays: 10, timeToSell: 60);

        var beforeRestart = Select(lookup);
        var afterRestart = Select(Restart(lookup));

        afterRestart.Should().BeEquivalentTo(beforeRestart);
        afterRestart[Tag].Should().Be(12_000);
    }

    [Test]
    public void BaseBucketIsPreferredOverExoticVariant()
    {
        var lookup = new PriceLookup();
        lookup.Lookup[CleanKey()] = Bucket(12_000, references: 6, overDays: 30, timeToSell: 60);
        lookup.Lookup[EnchantedKey()] = Bucket(50_000, references: 300, overDays: 30, timeToSell: 60);

        Select(lookup)[Tag].Should().Be(12_000);
    }

    [Test]
    public void OneCoinBucketIsIgnored()
    {
        var lookup = new PriceLookup();
        lookup.Lookup[CleanKey()] = Bucket(1, references: 40, overDays: 10, timeToSell: 60);
        lookup.Lookup[ReforgedKey()] = Bucket(300_000, references: 40, overDays: 10, timeToSell: 60);

        Select(lookup)[Tag].Should().Be(300_000);
    }

    [Test]
    public void ItemWithOnlyOneCoinBucketsIsLeftOut()
    {
        var lookup = new PriceLookup();
        lookup.Lookup[CleanKey()] = Bucket(1, references: 40, overDays: 10, timeToSell: 60);

        Select(lookup).Should().NotContainKey(Tag);
    }

    [Test]
    public void LowVolumeCleanOutlierIsIgnored()
    {
        var lookup = new PriceLookup();
        // four sales in two months, recomputed after the restart
        lookup.Lookup[CleanKey()] = Bucket(990_000, references: 4, overDays: 60, timeToSell: 10);
        lookup.Lookup[ReforgedKey()] = Bucket(12_000, references: 40, overDays: 10);

        Select(lookup)[Tag].Should().Be(12_000);
    }

    [Test]
    public void FewRecentSalesDoNotUndercutTheCleanPrice()
    {
        var lookup = new PriceLookup();
        lookup.Lookup[CleanKey()] = Bucket(12_000, references: 40, overDays: 20, timeToSell: 60);
        lookup.Lookup[CleanKey(Tier.EPIC)] = Bucket(500, references: 3, overDays: 1, timeToSell: 60);

        Select(lookup)[Tag].Should().Be(12_000);
    }

    [Test]
    public void ThinBucketsStillPriceARareItem()
    {
        var lookup = new PriceLookup();
        lookup.Lookup[CleanKey()] = Bucket(5_000_000, references: 3, overDays: 90);

        Select(lookup)[Tag].Should().Be(5_000_000);
    }

    [Test]
    public void ActiveLowestBinCapsThePrice()
    {
        var lookup = new PriceLookup();
        var bucket = Bucket(12_000, references: 40, overDays: 10);
        // the cheaper one already ended
        bucket.Lbins.Add(new ReferencePrice { AuctionId = 1, Price = 5_000, Day = (short)(SniperService.GetDay() - 2) });
        bucket.Lbins.Add(new ReferencePrice { AuctionId = 2, Price = 9_000, Day = SniperService.GetDay() });
        lookup.Lookup[CleanKey()] = bucket;

        Select(lookup)[Tag].Should().Be(9_000);
    }

    [Test]
    public void EndedOrPlaceholderBinDoesNotCapThePrice()
    {
        var lookup = new PriceLookup();
        var ended = Bucket(12_000, references: 40, overDays: 10);
        ended.Lbins.Add(new ReferencePrice { AuctionId = 1, Price = 9_000, Day = (short)(SniperService.GetDay() - 2) });
        var placeholder = Bucket(15_000, references: 40, overDays: 10);
        placeholder.Lbins.Add(new ReferencePrice { AuctionId = 2, Price = 1, Day = SniperService.GetDay() });
        lookup.Lookup[CleanKey()] = ended;
        var other = new PriceLookup();
        other.Lookup[CleanKey()] = placeholder;

        var prices = CleanPriceSelector.Select(new Dictionary<string, PriceLookup> { [Tag] = lookup, ["OTHER"] = other });

        prices[Tag].Should().Be(12_000);
        prices["OTHER"].Should().Be(15_000);
    }

    [Test]
    public void PetsArePricedPerLevelGroupAndHyperionIsExpanded()
    {
        var pet = new PriceLookup();
        pet.Lookup[PetKey("7")] = Bucket(40_000_000, references: 40, overDays: 10);
        pet.Lookup[PetKey("0")] = Bucket(10_000_000, references: 40, overDays: 10);
        var hyperion = new PriceLookup();
        hyperion.Lookup[CleanKey(Tier.MYTHIC)] = Bucket(900_000_000, references: 40, overDays: 10);

        var prices = CleanPriceSelector.Select(new Dictionary<string, PriceLookup> { ["PET_ENDER_DRAGON"] = pet, ["HYPERION"] = hyperion });

        prices["PET_ENDER_DRAGON_LEGENDARY_100"].Should().Be(40_000_000);
        prices["PET_ENDER_DRAGON_LEGENDARY_0"].Should().Be(10_000_000);
        prices["SCYLLA"].Should().Be(900_000_000);
    }

    /// <summary>The HYPERION lookup also holds NECRON_BLADE listings, stored without the cost to convert them</summary>
    [Test]
    public void GroupMemberBinDoesNotCapHyperion()
    {
        var hyperion = new PriceLookup();
        var bucket = Bucket(900_000_000, references: 40, overDays: 10);
        bucket.Lbins.Add(new ReferencePrice { AuctionId = 1, Price = 600_000_000, Day = SniperService.GetDay() });
        hyperion.Lookup[CleanKey(Tier.MYTHIC)] = bucket;

        var prices = CleanPriceSelector.Select(new Dictionary<string, PriceLookup> { ["HYPERION"] = hyperion });

        prices["HYPERION"].Should().Be(900_000_000);
        prices["SCYLLA"].Should().Be(900_000_000);
    }

    [Test]
    public void CheapStackKeepsItsOneCoinPerItemPrice()
    {
        var lookup = new PriceLookup();
        lookup.Lookup[new AuctionKey { Tier = Tier.COMMON, Count = 64 }] = Bucket(100, references: 40, overDays: 10);

        Select(lookup)[Tag].Should().Be(1);
    }

    [Test]
    public void MissingHyperionDoesNotFail()
    {
        var lookup = new PriceLookup();
        lookup.Lookup[CleanKey()] = Bucket(12_000, references: 40, overDays: 10);

        Select(lookup).Should().NotContainKey("SCYLLA");
    }

    private static Dictionary<string, long> Select(PriceLookup lookup)
        => CleanPriceSelector.Select(new Dictionary<string, PriceLookup> { [Tag] = lookup });

    /// <summary>Persists and loads the lookup like a pod restart does, dropping the in-memory bucket fields</summary>
    private static PriceLookup Restart(PriceLookup lookup)
    {
        lookup.CleanKey ??= CleanKey();
        var persisted = MessagePackSerializer.Serialize(new List<KeyValuePair<string, PriceLookup>> { new(Tag, lookup) },
            S3PersistanceManager.GroupOptions());
        var loaded = MessagePackSerializer.Deserialize<List<KeyValuePair<string, PriceLookup>>>(persisted,
            S3PersistanceManager.GroupOptions()).Single().Value;
        loaded.Lookup.Values.Should().OnlyContain(b => b.TimeToSell == 0);
        return loaded;
    }

    private static ReferenceAuctions Bucket(long price, int references, int overDays, long timeToSell = 0)
    {
        var today = SniperService.GetDay();
        var bucket = new ReferenceAuctions { Price = price, TimeToSell = timeToSell };
        var spacing = references > 1 ? (overDays - 1) / (double)(references - 1) : 0;
        // oldest first, like the queue fills up
        bucket.SetReferences(Enumerable.Range(0, references).Select(i => new ReferencePrice
        {
            AuctionId = i + 1,
            Price = price,
            Day = (short)(today - overDays + 1 + (int)Math.Round(i * spacing)),
        }).ToList());
        return bucket;
    }

    private static AuctionKey CleanKey(Tier tier = Tier.RARE) => new() { Tier = tier };

    private static AuctionKey ReforgedKey() => new() { Tier = Tier.RARE, Reforge = ItemReferences.Reforge.Fabled };

    private static AuctionKey EnchantedKey() => new()
    {
        Tier = Tier.RARE,
        Enchants = new List<Enchant> { new() { Type = Enchantment.EnchantmentType.growth, Lvl = 6 } }.AsReadOnly(),
    };

    private static AuctionKey PetKey(string exp) => new()
    {
        Tier = Tier.LEGENDARY,
        Modifiers = new List<KeyValuePair<string, string>> { new("exp", exp) }.AsReadOnly(),
    };
}
