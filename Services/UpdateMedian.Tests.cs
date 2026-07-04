using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using AwesomeAssertions;
using dev;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R5 P2 bit-exactness oracle for the WRITE / repricing path
    /// (<c>benchmarks/COMPUTE_FLOOR_SPEC_R5.md</c> §3-P2 + §4). This is the gate a future write-path optimization
    /// (de-LINQ of <c>UpdateMedian</c>, pooled <c>ReferencePrice[]</c>, removing the incidental
    /// <c>KeyValuePair&lt;AuctionKey,ReferenceAuctions&gt;[]</c> materializations, etc.) must not break.
    ///
    /// <para>
    /// <b>Why.</b> The WARM alloc profile blames <c>UpdateMedian</c> for the single largest warm allocator (the
    /// <c>KeyValuePair&lt;AuctionKey,ReferenceAuctions&gt;[]</c>, 93.5% of which is from UpdateMedian) plus the
    /// <c>ReferencePrice[]</c> from <c>ApplyAntiMarketManipulation</c>/<c>DropUnderlistings</c> (both reached only
    /// through UpdateMedian) and the <c>Slot[ReferencePrice][]</c> from <c>SetReferences</c>. P2's job is to shrink
    /// that allocation without changing a single repriced output. This oracle pins the OBSERVABLE outputs of the
    /// repricing of a fixed, representative set of buckets so any allocation-shaving rewrite is provably
    /// behavior-neutral.
    /// </para>
    ///
    /// <para>
    /// <b>What it drives.</b> The production repricing entry point <see cref="SniperService.RefreshLookup"/>, which is
    /// the bulk-reprice that loops every bucket of a tag and calls the SAME
    /// <c>CapBucketSize</c> + <c>UpdateMedian(bucket, (tag, GetBreakdownKey(key, tag)))</c> (+ <c>GetLbinCap</c>) that
    /// the per-sale ingest path (<c>AddSoldItem → AddAuctionToBucket → UpdateMedian</c>) funnels through. Buckets are
    /// populated through the public <c>AddSoldItem</c> API (mirroring <c>GetPrice.Tests.cs</c> /
    /// <c>ValuationFinders.Tests.cs</c>) so the references/lbins are production-shaped; <c>RefreshLookup</c> then
    /// reprices, and we snapshot each bucket's observable fields.
    /// </para>
    ///
    /// <para>
    /// <b>Pinned outputs</b> (per bucket): <c>Price</c>, <c>OldestRef</c>, <c>Volatility</c>,
    /// <c>DeduplicatedReferenceCount</c>, <c>RiskyEstimate</c>, <c>Volume</c>, <c>TimeToSell</c>, <c>Lbin.Price</c>,
    /// and the full FIFO <c>References</c> sequence (id+day+price). These are exactly the fields a write-path rewrite
    /// could perturb. Captures the CURRENT behavior as the golden (assert current == captured).
    /// </para>
    ///
    /// <para>
    /// Construction mirrors <c>GetPrice.Tests.cs</c> Setup (NullLogger to dodge the known flaky-logger NRE,
    /// MIN_TARGET=0, fixed StartTime, FullyLoaded, AddSoldItem to populate buckets). <see cref="DISCOVER"/> is the
    /// regeneration switch: flip it to <c>true</c>, run, and paste the printed lines back into the golden constants —
    /// never hand-edit a golden to silence a real diff (that would defeat the gate).
    /// </para>
    /// </summary>
    public class UpdateMedianGoldenTests
    {
        /// <summary>
        /// Regeneration switch. Leave <c>false</c> in committed code: the test then *asserts* the live repriced bucket
        /// equals the embedded golden. Flip to <c>true</c> only to (re)derive the goldens after an *intended* behavior
        /// change — the test prints each fixture's serialized bucket and trivially passes so the new lines can be copied
        /// into the constants. A green CI run with DISCOVER=true proves nothing; it must be committed false.
        /// </summary>
        private static readonly bool DISCOVER = false;

        private SniperService service = null!;
        private List<LowPricedAuction> found = null!;
        private CraftCostMock craftCost = null!;
        private HypixelItemService itemService = null!;
        private static long idCounter;

        private class CraftCostMock : ICraftCostService
        {
            public Dictionary<string, double> Costs { get; } = new();
            public System.Collections.Concurrent.ConcurrentDictionary<string, Category> ItemCategories { get; } = new();
            public void AddCostForSpecialItems() { }
            public bool TryGetCost(string itemId, out double cost) => Costs.TryGetValue(itemId, out cost);
        }

        [SetUp]
        public void Setup()
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            SniperService.MIN_TARGET = 0;
            // Anchor the epoch to the wall clock so GetDay() is ALWAYS 45, no matter when the test runs. With a fixed
            // absolute StartTime the serialized Volume/TimeToSell drift as UtcNow advances (they divide by
            // "days since oldest ref", which grows daily) and the goldens rot within weeks.
            SniperService.StartTime = DateTime.UtcNow.Date.AddDays(-45);
            craftCost = new CraftCostMock();
            itemService = new HypixelItemService(null, NullLogger<HypixelItemService>.Instance);
            service = new SniperService(itemService, null, NullLogger<SniperService>.Instance, craftCost);
            idCounter = 100;
            found = new List<LowPricedAuction>();
            service.FoundSnipe += found.Add;
        }

        [TearDown]
        public void TearDown() => SniperService.MIN_TARGET = 200_000;

        // ============================================================================================================
        // Fixture 1 — plain priced bucket with a healthy, multi-day, multi-seller reference set. Exercises the main
        // UpdateMedian path: ApplyAntiMarketManipulation + DropUnderlistings keep all refs, short/long-term median,
        // volatility, risky estimate, and an active lbin. This is the steady-state warm reprice the profile blames.
        // ============================================================================================================
        [Test]
        public void UpdateMedian_PlainPricedBucket_BitExact()
        {
            const string tag = "UM_SWORD";
            // 10 sales spread over recent days from distinct sellers/buyers (so anti-manip/underlisting keep them),
            // rising-then-stable so the median is well defined.
            for (int i = 0; i < 10; i++)
                AddSale(tag, dayAgo: i, price: 10_000_000 + i * 250_000, seller: 1000 + i, buyer: 5000 + i);
            // an active (unsold) listing so the bucket carries an Lbin too
            AddLbin(tag, price: 13_500_000);

            service.RefreshLookup(tag);
            AssertGoldenBucket("PlainPricedBucket", tag, BucketFor(tag), GOLD_PLAIN);
        }

        // ============================================================================================================
        // Fixture 2 — pet bucket (tier-bearing, exp modifier). Exercises the pet-key breakdown + the clean-key /
        // clean-price-per-tier branches inside UpdateMedian on a tier-bearing item.
        // ============================================================================================================
        [Test]
        public void UpdateMedian_PetBucket_BitExact()
        {
            const string tag = "UM_PET_TIGER";
            for (int i = 0; i < 9; i++)
                AddPetSale(tag, dayAgo: i, price: 40_000_000 + i * 1_000_000, seller: 2000 + i, buyer: 6000 + i);
            AddPetLbin(tag, price: 46_000_000);

            service.RefreshLookup(tag);
            AssertGoldenBucket("PetBucket", tag, BucketFor(tag), GOLD_PET);
        }

        // ============================================================================================================
        // Fixture 3 — declining-market bucket: older sales high, recent sales lower, so the dropping-price /
        // short-term-vs-long-term protection branches and volatility computation engage. Pins that the trend handling
        // is preserved by a rewrite.
        // ============================================================================================================
        [Test]
        public void UpdateMedian_DecliningMarket_BitExact()
        {
            const string tag = "UM_DECLINING";
            // older = expensive, newer = cheaper (a falling market)
            for (int i = 0; i < 12; i++)
                AddSale(tag, dayAgo: i, price: 30_000_000 - i * 1_500_000, seller: 3000 + i, buyer: 7000 + i);

            service.RefreshLookup(tag);
            AssertGoldenBucket("DecliningMarket", tag, BucketFor(tag), GOLD_DECLINING);
        }

        // ============================================================================================================
        // Fixture 4 — low-volume bucket (exactly the boundary where UpdateMedian sets Price=0 for insufficient volume).
        // Pins the "too low vol" early-out so a rewrite can't accidentally start pricing thin buckets.
        // ============================================================================================================
        [Test]
        public void UpdateMedian_LowVolume_BitExact()
        {
            const string tag = "UM_THIN";
            // only 3 references (< 4) -> Price forced to 0
            for (int i = 0; i < 3; i++)
                AddSale(tag, dayAgo: i, price: 5_000_000, seller: 4000 + i, buyer: 8000 + i);

            service.RefreshLookup(tag);
            AssertGoldenBucket("LowVolume", tag, BucketFor(tag), GOLD_THIN);
        }

        // ============================================================================================================
        // Golden snapshots — serialized bucket, see SerializeBucket. Regenerate with DISCOVER=true; never hand-edit.
        // ============================================================================================================

        private const string GOLD_PLAIN =
            "Price=10500000;OldestRef=37;Volatility=2;DedupCount=9;RiskyEstimate=0;Volume=1.6667;TimeToSell=1729;Lbin.Price=13500000;Refs=100:40:10000000|101:39:10250000|102:38:10500000|103:37:10750000|104:36:11000000|105:35:11250000|106:34:11500000|107:33:11750000|108:32:12000000|109:31:12250000";

        private const string GOLD_PET =
            "Price=42000000;OldestRef=37;Volatility=2;DedupCount=9;RiskyEstimate=0;Volume=1.5;TimeToSell=1921;Lbin.Price=46000000;Refs=100:40:40000000|101:39:41000000|102:38:42000000|103:37:43000000|104:36:44000000|105:35:45000000|106:34:46000000|107:33:47000000|108:32:48000000";

        private const string GOLD_DECLINING =
            "Price=21000000;OldestRef=37;Volatility=10;DedupCount=12;RiskyEstimate=10500000;Volume=2;TimeToSell=1441;Lbin.Price=0;Refs=100:40:30000000|101:39:28500000|102:38:27000000|103:37:25500000|104:36:24000000|105:35:22500000|106:34:21000000|107:33:19500000|108:32:18000000|109:31:16500000|110:30:15000000|111:29:13500000";

        private const string GOLD_THIN =
            "Price=0;OldestRef=0;Volatility=0;DedupCount=0;RiskyEstimate=0;Volume=0.5;TimeToSell=0;Lbin.Price=0;Refs=100:40:5000000|101:39:5000000|102:38:5000000";

        // ============================================================================================================
        // Serialization + assertion: pin every observable repriced field a write-path rewrite could perturb.
        // ============================================================================================================

        /// <summary>
        /// Stable, order-fixed serialization of every observable <see cref="ReferenceAuctions"/> field UpdateMedian
        /// determines, plus the FIFO References sequence (id+day+price) and the active Lbin price. AuctionIds are
        /// deterministic here (idCounter is reset per test and sales are added in a fixed order) so they are pinned.
        /// </summary>
        private static string SerializeBucket(ReferenceAuctions b)
        {
            var refs = string.Join("|", b.References.Select(r => $"{r.AuctionId}:{r.Day}:{r.Price}"));
            return string.Join(";", new[]
            {
                $"Price={b.Price}",
                $"OldestRef={b.OldestRef}",
                $"Volatility={b.Volatility}",
                $"DedupCount={b.DeduplicatedReferenceCount}",
                $"RiskyEstimate={b.RiskyEstimate}",
                $"Volume={b.Volume.ToString("0.####", CultureInfo.InvariantCulture)}",
                $"TimeToSell={b.TimeToSell}",
                $"Lbin.Price={b.Lbin.Price}",
                $"Refs={refs}",
            });
        }

        private void AssertGoldenBucket(string name, string tag, ReferenceAuctions live, string golden)
        {
            live.Should().NotBeNull($"RefreshLookup must produce a repriced bucket for fixture {name}");
            var actual = SerializeBucket(live);
            // Always print so a DISCOVER=true run can harvest the line; the assert below is what actually gates.
            TestContext.Out.WriteLine($"[GOLD {name}] {actual}");
            if (DISCOVER)
                return;
            actual.Should().Be(golden,
                $"UpdateMedian/RefreshLookup result for fixture '{name}' must stay bit-identical (gates the P2 " +
                "write-path optimization); if this is an intended change, regenerate with DISCOVER=true and paste the " +
                "printed line into the golden constant");
        }

        // ---------- helpers (mirror GetPrice.Tests.cs / SniperService.Tests.cs) ----------

        /// <summary>The single bucket of <paramref name="tag"/>'s lookup (the fixtures put exactly one priced key per tag).</summary>
        private ReferenceAuctions BucketFor(string tag)
        {
            service.Lookups.TryGetValue(tag, out var lookup).Should().BeTrue($"lookup for {tag} must exist after populating");
            // Pick the bucket with the most references (the one the fixture filled), deterministic across runs.
            return lookup!.Lookup.OrderByDescending(kv => kv.Value.References.Count).First().Value;
        }

        /// <summary>Adds a single completed sale (a priced reference) to <paramref name="tag"/>'s clean bucket.</summary>
        private void AddSale(string tag, int dayAgo, long price, int seller, int buyer)
        {
            var a = SaleBase(tag, dayAgo, price, seller, buyer);
            a.FlatenedNBT = new();
            a.Enchantments = new List<Enchantment>();
            service.AddSoldItem(a);
            service.FinishedUpdate();
        }

        /// <summary>Adds a pet sale (tier + exp modifier) to <paramref name="tag"/>'s pet bucket.</summary>
        private void AddPetSale(string tag, int dayAgo, long price, int seller, int buyer)
        {
            var a = SaleBase(tag, dayAgo, price, seller, buyer);
            a.Tier = Tier.LEGENDARY;
            a.FlatenedNBT = new() { { "exp", "10000000" }, { "candyUsed", "0" } };
            a.Enchantments = new List<Enchantment>();
            service.AddSoldItem(a);
            service.FinishedUpdate();
        }

        /// <summary>Adds an active (unsold) BIN listing so the bucket carries an Lbin.</summary>
        private void AddLbin(string tag, long price)
        {
            var a = SaleBase(tag, dayAgo: -5, price: price, seller: 1500, buyer: 0);
            a.FlatenedNBT = new();
            a.Enchantments = new List<Enchantment>();
            a.HighestBidAmount = 0; // active listing
            a.StartingBid = price;
            a.Bin = true;
            DrivePublic(a);
        }

        private void AddPetLbin(string tag, long price)
        {
            var a = SaleBase(tag, dayAgo: -5, price: price, seller: 2500, buyer: 0);
            a.Tier = Tier.LEGENDARY;
            a.FlatenedNBT = new() { { "exp", "10000000" }, { "candyUsed", "0" } };
            a.Enchantments = new List<Enchantment>();
            a.HighestBidAmount = 0;
            a.StartingBid = price;
            a.Bin = true;
            DrivePublic(a);
        }

        private void DrivePublic(SaveAuction a)
        {
            service.FinishedUpdate();
            service.State = SniperState.FullyLoaded;
            service.TestNewAuction(a);
            service.FinishedUpdate();
        }

        /// <summary>A completed-sale auction <paramref name="dayAgo"/> days before StartTime+offset, with a deterministic
        /// seller/buyer so anti-manipulation / underlisting filters behave identically across runs.</summary>
        private SaveAuction SaleBase(string tag, int dayAgo, long price, int seller, int buyer)
        {
            var id = idCounter++;
            // StartTime is the day-0 epoch; place sales a few days in so GetDay(End) is a small positive day number.
            var end = SniperService.StartTime + TimeSpan.FromDays(40 - dayAgo);
            return new SaveAuction
            {
                Tag = tag,
                FlatenedNBT = new Dictionary<string, string>(),
                Enchantments = new List<Enchantment>(),
                StartingBid = price,
                HighestBidAmount = price,
                Category = Category.WEAPON,
                Count = 1,
                UId = id,
                Uuid = id.ToString("x32").PadLeft(32, '0'),
                // GetSellerId parses the first 4 hex chars; keep <= 0x7FFF and deterministic per seller.
                AuctioneerId = (seller & 0x7FFF).ToString("x4") + "umtest",
                Bids = buyer == 0 ? new List<SaveBids>() : new List<SaveBids>
                {
                    new() { Amount = price, Bidder = (buyer & 0x7FFF).ToString("x4") + "buyerum" }
                },
                Start = SniperService.StartTime + TimeSpan.FromDays(39 - dayAgo),
                End = end,
                Bin = true,
            };
        }
    }
}
