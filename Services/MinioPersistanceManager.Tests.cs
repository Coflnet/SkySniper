using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using MessagePack;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class S3PersistanceManagerTests
{
    [Test]
    public void IsGroupStaleReturnsTrueWhenNoRecentReferencesExist()
    {
        var group = new List<KeyValuePair<string, PriceLookup>>
        {
            new("PET_BABY_YETI", new PriceLookup
            {
                Lookup = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>(new[]
                {
                    new KeyValuePair<AuctionKey, ReferenceAuctions>(CreateKey("6"), CreateBucket(95, 26_925_000)),
                    new KeyValuePair<AuctionKey, ReferenceAuctions>(CreateKey("5"), CreateBucket(96, 6_783_332))
                })
            })
        };

        var isStale = S3PersistanceManager.IsGroupStale(group, 100, 2);

        isStale.Should().BeTrue();
    }

    [Test]
    public void IsGroupStaleReturnsFalseWhenAnyBucketIsRecent()
    {
        var group = new List<KeyValuePair<string, PriceLookup>>
        {
            new("PET_BABY_YETI", new PriceLookup
            {
                Lookup = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>(new[]
                {
                    new KeyValuePair<AuctionKey, ReferenceAuctions>(CreateKey("6"), CreateBucket(95, 26_925_000)),
                    new KeyValuePair<AuctionKey, ReferenceAuctions>(CreateKey("5"), CreateBucket(99, 6_783_332))
                })
            })
        };

        var isStale = S3PersistanceManager.IsGroupStale(group, 100, 2);

        isStale.Should().BeFalse();
    }

    [Test]
    public void CreateSerializableGroupCopyNormalizesMalformedAuctionKeysWithoutMutatingSource()
    {
        var bucketKey = new AuctionKey(null, ItemReferences.Reforge.Any, null, Tier.LEGENDARY, 1);
        var lookup = new PriceLookup
        {
            Lookup = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>(new[]
            {
                new KeyValuePair<AuctionKey, ReferenceAuctions>(bucketKey, CreateBucket(100, 26_925_000))
            })
        };
        var group = new List<KeyValuePair<string, PriceLookup>>
        {
            new("PET_BABY_YETI", lookup)
        };

        var serializable = S3PersistanceManager.CreateSerializableGroupCopy(group);

        serializable.Should().HaveCount(1);
        serializable[0].Value.CleanKey.Should().NotBeNull();
        serializable[0].Value.CleanKey.Enchants.Should().NotBeNull();
        serializable[0].Value.CleanKey.Modifiers.Should().NotBeNull();
        serializable[0].Value.Lookup.Keys.Single().Enchants.Should().NotBeNull();
        serializable[0].Value.Lookup.Keys.Single().Modifiers.Should().NotBeNull();
        lookup.CleanKey.Should().BeNull();
        Action serialize = () => MessagePackSerializer.Serialize(
            serializable,
            MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray));
        serialize.Should().NotThrow();
    }

    [TestCase(null, true)]
    [TestCase("2026-04-18T18:59:59Z", true)]
    [TestCase("2026-04-18T19:00:01Z", false)]
    public void ShouldFlushGroupUsesObjectAge(string lastModifiedIso, bool shouldFlush)
    {
        DateTime? lastModified = lastModifiedIso == null
            ? null
            : DateTime.Parse(lastModifiedIso, null, System.Globalization.DateTimeStyles.AdjustToUniversal);

        var result = S3PersistanceManager.ShouldFlushGroup(lastModified, new DateTime(2026, 4, 18, 20, 0, 0, DateTimeKind.Utc), TimeSpan.FromHours(1));

        result.Should().Be(shouldFlush);
    }

    private static AuctionKey CreateKey(string exp)
    {
        return new AuctionKey(
            new List<Enchant>(),
            ItemReferences.Reforge.Any,
            new List<KeyValuePair<string, string>> { new("exp", exp) },
            Tier.LEGENDARY,
            1);
    }

    private static ReferenceAuctions CreateBucket(short day, long price)
    {
        return new ReferenceAuctions
        {
            Price = price,
            Volume = 1,
            References = new ConcurrentQueue<ReferencePrice>(new[]
            {
                new ReferencePrice
                {
                    Day = day,
                    Price = price,
                    AuctionId = day,
                    Seller = 1,
                    Buyer = 2,
                    SellTime = 60
                }
            })
        };
    }
}