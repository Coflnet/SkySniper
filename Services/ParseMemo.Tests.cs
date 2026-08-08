using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using NUnit.Framework;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R7/WS-A bit-exactness gate for the (contentHash, pricingEpoch) parse memo + the content-hash machinery.
    ///
    /// <para><b>Hash correctness</b> is the #1 risk (a wrong NonContentKeys exclusion → under-miss → wrong serve, and a
    /// weak commutative combine → a content collision → wrong serve). These tests prove the static
    /// <see cref="SniperService.BatchContentHash"/>: (a) DISTINGUISHES every distinct fixture content (no collision over
    /// all priced buckets) — the regression test for the commutative-sum collision the parity guard caught (two distinct
    /// {scroll_count,upgrade_level} contents summed equal before the splitmix finalizer); (b) is ORDER-INDEPENDENT (the
    /// flatNbt dict iteration order must not change the hash); (c) is SENSITIVE to each hashed field (tier/reforge/count/
    /// HighestBidAmount/an extra modifier all change the hash); (d) IGNORES per-instance uid keys (excluded from content).</para>
    ///
    /// <para><b>Memo bit-exactness</b>: over every priced bucket of the real fixtures, a memo store-then-serve roundtrip
    /// at a stable epoch returns a value byte-equal (by the parse contract) to an independent fresh re-parse. This is the
    /// in-process equivalent of the SNIPER_VERIFY_PARSE_MEMO replay guard.</para>
    /// </summary>
    public class ParseMemoTests
    {
        private SniperService service = null!;

        [SetUp]
        public void Setup()
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            SniperService.MIN_TARGET = 0;
            SniperService.StartTime = new DateTime(2021, 9, 25) + TimeSpan.FromDays(10_000);
            var itemService = new HypixelItemService(null, NullLogger<HypixelItemService>.Instance);
            service = new SniperService(itemService, null, NullLogger<SniperService>.Instance, null);
        }

        [TearDown]
        public void TearDown() => SniperService.MIN_TARGET = 200_000;

        private static readonly (string file, string tag)[] Fixtures =
        {
            ("boots.json", "WISE_WITHER_BOOTS"), ("HYPERION.json", "HYPERION"),
            ("juju.json", "JUJU_SHORTBOW"), ("midas_sword.json", "MIDAS_SWORD"),
        };

        [Test]
        public void BatchContentHash_NoCollisionOverAllFixtureBuckets()
        {
            var byHash = new Dictionary<ulong, string>();
            int chec_ = 0, collisions = 0; string first = "";
            foreach (var (file, tag) in Fixtures)
            {
                if (!File.Exists($"Mock/{file}")) continue;
                var lookup = LoadLookupMock(file);
                foreach (var key in lookup.Lookup.Keys)
                {
                    var a = AuctionFromKey(tag, key);
                    var canon = Canonical(a);
                    var h = SniperService.BatchContentHash(a);
                    if (byHash.TryGetValue(h, out var existing) && existing != canon)
                    {
                        if (collisions == 0) first = $"hash {h}: [{existing}] vs [{canon}]";
                        collisions++;
                    }
                    else byHash[h] = canon;
                    chec_++;
                }
            }
            chec_.Should().BeGreaterThan(0, "fixtures should yield buckets");
            collisions.Should().Be(0, $"distinct contents must hash distinctly. First collision: {first}");
        }

        [Test]
        public void BatchContentHash_IsOrderIndependent()
        {
            var nbtA = new Dictionary<string, string> { { "scroll_count", "3" }, { "upgrade_level", "7" }, { "rarity_upgrades", "1" } };
            // a different insertion order -> the Dictionary may enumerate differently; the hash must be identical.
            var nbtB = new Dictionary<string, string> { { "rarity_upgrades", "1" }, { "upgrade_level", "7" }, { "scroll_count", "3" } };
            var a = Mk("HYPERION", Tier.LEGENDARY, nbtA);
            var b = Mk("HYPERION", Tier.LEGENDARY, nbtB);
            SniperService.BatchContentHash(a).Should().Be(SniperService.BatchContentHash(b));
        }

        [Test]
        public void BatchContentHash_DistinguishesEveryField()
        {
            var baseNbt = new Dictionary<string, string> { { "upgrade_level", "5" } };
            var b0 = Mk("HYPERION", Tier.LEGENDARY, baseNbt, hb: 1000, count: 1, reforge: ItemReferences.Reforge.None);
            var h0 = SniperService.BatchContentHash(b0);

            SniperService.BatchContentHash(Mk("NECRON_BLADE", Tier.LEGENDARY, baseNbt)).Should().NotBe(h0, "tag");
            SniperService.BatchContentHash(Mk("HYPERION", Tier.EPIC, baseNbt)).Should().NotBe(h0, "tier");
            SniperService.BatchContentHash(Mk("HYPERION", Tier.LEGENDARY, baseNbt, count: 2)).Should().NotBe(h0, "count");
            SniperService.BatchContentHash(Mk("HYPERION", Tier.LEGENDARY, baseNbt, reforge: ItemReferences.Reforge.Spicy)).Should().NotBe(h0, "reforge");
            SniperService.BatchContentHash(Mk("HYPERION", Tier.LEGENDARY, baseNbt, hb: 2000)).Should().NotBe(h0, "HighestBidAmount");
            var withVal = new Dictionary<string, string> { { "upgrade_level", "6" } };
            SniperService.BatchContentHash(Mk("HYPERION", Tier.LEGENDARY, withVal)).Should().NotBe(h0, "modifier value");
            var withExtra = new Dictionary<string, string> { { "upgrade_level", "5" }, { "scroll_count", "3" } };
            SniperService.BatchContentHash(Mk("HYPERION", Tier.LEGENDARY, withExtra)).Should().NotBe(h0, "extra modifier");
            var withEnch = Mk("HYPERION", Tier.LEGENDARY, baseNbt);
            withEnch.Enchantments = new List<Enchantment> { new(EnchantmentType.sharpness, 6) };
            SniperService.BatchContentHash(withEnch).Should().NotBe(h0, "enchant");
        }

        [Test]
        public void BatchContentHash_IgnoresPerInstanceUidKeys()
        {
            var nbt = new Dictionary<string, string> { { "upgrade_level", "5" } };
            var clean = Mk("HYPERION", Tier.LEGENDARY, new Dictionary<string, string>(nbt));
            var withUid = Mk("HYPERION", Tier.LEGENDARY, new Dictionary<string, string>(nbt) { { "uid", "abc123" }, { "uuid", "deadbeef" } });
            SniperService.BatchContentHash(withUid).Should().Be(SniperService.BatchContentHash(clean),
                "per-instance uid/uuid keys are excluded from the content hash");
        }

        [Test]
        public void ParseMemo_BitExact_OverFixtures()
        {
            int chec_ = 0, mismatches = 0; string firstTag = "";
            foreach (var (file, tag) in Fixtures)
            {
                if (!File.Exists($"Mock/{file}")) continue;
                service.AddLookupData(tag, LoadLookupMock(file));
                var live = service.Lookups[tag];
                foreach (var key in live.Lookup.Keys.ToList())
                {
                    var a = AuctionFromKey(tag, key);
                    if (!service.ParseMemoRoundtripEqualsForTest(a))
                    {
                        if (mismatches == 0) firstTag = $"{tag} {key}";
                        mismatches++;
                    }
                    chec_++;
                }
            }
            chec_.Should().BeGreaterThan(0, "fixtures should yield buckets");
            mismatches.Should().Be(0, $"memo store-then-serve must be bit-exact vs a fresh parse. First: {firstTag}");
        }

        private static SaveAuction Mk(string tag, Tier tier, Dictionary<string, string> nbt,
            long hb = 1000, int count = 1, ItemReferences.Reforge reforge = ItemReferences.Reforge.None)
            => new()
            {
                Tag = tag, Tier = tier, Reforge = reforge, Count = count, HighestBidAmount = hb,
                FlatenedNBT = nbt, Enchantments = new List<Enchantment>(), StartingBid = hb,
            };

        private static string Canonical(SaveAuction a)
        {
            var nbt = string.Join(",", (a.FlatenedNBT ?? new()).Where(m => !m.Key.Contains("uid")).Select(m => m.Key + "=" + m.Value).OrderBy(x => x));
            var ench = string.Join(",", (a.Enchantments ?? new()).Select(e => e.Type + ":" + e.Level).OrderBy(x => x));
            return $"{a.Tag}|{a.Tier}|{a.Reforge}|{a.Count}|{a.HighestBidAmount}|N[{nbt}]|E[{ench}]";
        }

        private static SaveAuction AuctionFromKey(string tag, AuctionKey key)
        {
            var nbt = new Dictionary<string, string>();
            foreach (var m in key.Modifiers) nbt[m.Key] = m.Value;
            return new SaveAuction
            {
                Tag = tag, Tier = key.Tier, Reforge = key.Reforge, Count = Math.Max((int)key.Count, 1),
                Category = Category.UNKNOWN, FlatenedNBT = nbt,
                Enchantments = key.Enchants.Select(e => new Enchantment(e.Type, e.Lvl)).ToList(),
                StartingBid = 1000, HighestBidAmount = 1000,
            };
        }

        private static PriceLookup LoadLookupMock(string mockFileName)
        {
            var text = File.ReadAllText($"Mock/{mockFileName}");
            var parsed = JsonConvert.DeserializeObject<LookupLoad>(text);
            var dict = new Dictionary<AuctionKey, ReferenceAuctions>();
            foreach (var entry in parsed.Lookup)
            {
                AuctionKey key;
                try { key = ParseKey(entry); } catch { continue; }
                dict[key] = entry.Value;
            }
            return new PriceLookup
            {
                Lookup = new System.Collections.Concurrent.ConcurrentDictionary<AuctionKey, ReferenceAuctions>(dict),
                CleanPricePerDay = parsed.CleanPricePerDay,
                CleanKey = parsed.CleanKey
            };
        }

        private static AuctionKey ParseKey(KeyValuePair<string, ReferenceAuctions> e)
        {
            var parts = e.Key.Replace(", ", ",").Split(' ');
            return new AuctionKey
            {
                Enchants = new(parts[0].Split(',').Where(s => !string.IsNullOrWhiteSpace(s)).Select(s =>
                {
                    var eparts = s.Split('=');
                    return new Enchant { Type = Enum.Parse<EnchantmentType>(eparts.First()), Lvl = byte.Parse(eparts.Last()) };
                }).ToArray()),
                Reforge = Enum.Parse<ItemReferences.Reforge>(parts[1]),
                Tier = Enum.Parse<Tier>(parts.Reverse().Skip(1).First()),
                Count = byte.Parse(parts.Last()),
                Modifiers = new(parts.Skip(2).First().Split("],[").Select(s =>
                {
                    var mparts = s.Split(',', 2);
                    return new KeyValuePair<string, string>(mparts.First().TrimStart('['), mparts.Last().TrimEnd(']'));
                }).Where(m => !string.IsNullOrEmpty(m.Key)).ToList())
            };
        }

        public class LookupLoad
        {
            public Dictionary<string, ReferenceAuctions> Lookup { get; set; }
            public Dictionary<short, long> CleanPricePerDay { get; set; } = new();
            public AuctionKey CleanKey { get; set; }
        }
    }
}
