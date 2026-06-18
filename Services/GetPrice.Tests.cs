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
    /// R3 Phase 0 / F1 bit-exactness oracle for the read path
    /// (<c>benchmarks/COMPUTE_FLOOR_SPEC_R3.md</c> §4-F1 + §5 R3-READ).
    ///
    /// <para>
    /// <b>Why.</b> R3-READ rewrites the brute-force read search (<c>GetClosestLbins</c>'s
    /// <c>Where().OrderByDescending(Similarity)</c> and <c>GetEstimatedMedian</c>) onto the WS-A contiguous index +
    /// WS-C BnB + columnar kernel. That rewrite must reproduce the *current* public <see cref="SniperService.GetPrice"/>
    /// result <b>byte-for-byte</b> on every fixture — same <c>Median</c>, <c>Lbin</c>, <c>SLbin</c>, <c>MedianKey</c>,
    /// <c>LbinKey</c>, <c>ItemKey</c>, <c>Volume</c>, <c>Volatility</c>, <c>AvgSellTime</c>, <c>LastSale</c>. This is the
    /// gate: the harness drives <c>GetPrice(SaveAuction)</c> over representative auctions and pins every returned
    /// <see cref="PriceEstimate"/> field to an embedded golden snapshot.
    /// </para>
    ///
    /// <para>
    /// <b>Coverage.</b> The fixtures deliberately span both read-path shapes the rewrite touches:
    /// <list type="bullet">
    ///   <item><b>Exact-match</b> — the requested <c>itemKey</c> resolves to a populated bucket directly
    ///     (the <c>l.TryGetValue(itemKey, out bucket)</c> fast path: <c>AssignMedian</c> + the bucket's own Lbin).</item>
    ///   <item><b>No-exact-match (brute force)</b> — the requested key has no bucket, forcing the
    ///     <c>GetEstimatedMedian</c> closest-median scan AND the <c>ClosestLbin</c>/<c>GetClosestLbins</c>
    ///     <c>OrderByDescending(Similarity + Volume)</c> brute force. This is exactly the search R3-READ replaces, so
    ///     these cases are the load-bearing half of the gate.</item>
    ///   <item><b>Bazaar</b> — the <c>BazaarPrices</c> short-circuit (Median = bazaar * count).</item>
    ///   <item><b>Higher-value cap / lower-key</b> — the <c>GetLbinCap</c> + lower-value-key adjustment branches.</item>
    /// </list>
    /// </para>
    ///
    /// <para>
    /// Construction mirrors <c>ValuationFinders.Tests.cs</c> / <c>SniperService.Tests.cs</c> Setup (NullLogger to
    /// dodge the known flaky-logger NRE, MIN_TARGET=0, fixed StartTime, FullyLoaded, AddSoldItem/AddVolume to populate
    /// buckets). <see cref="DISCOVER"/> is the regeneration switch: flip it to <c>true</c>, run, and paste the printed
    /// <c>field=value</c> lines back into the golden constants below — never edit a golden by hand to make a failing
    /// run pass without first confirming the new value is intended (that would defeat the gate).
    /// </para>
    /// </summary>
    public class GetPriceGoldenTests
    {
        /// <summary>
        /// Regeneration switch. Leave <c>false</c> in committed code: the test then *asserts* the live GetPrice result
        /// equals the embedded golden. Flip to <c>true</c> only to (re)derive the goldens after an *intended* behavior
        /// change — the test prints each fixture's serialized estimate and trivially passes so the new lines can be
        /// copied into the constants. A green CI run with DISCOVER=true proves nothing; it must be committed false.
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
            public Dictionary<string, Category> ItemCategories { get; } = new();
            public void AddCostForSpecialItems() { }
            public bool TryGetCost(string itemId, out double cost) => Costs.TryGetValue(itemId, out cost);
        }

        [SetUp]
        public void Setup()
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            SniperService.MIN_TARGET = 0;
            SniperService.StartTime = new DateTime(2021, 9, 25);
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
        // EXACT-MATCH fixtures: the requested key resolves directly to a populated bucket.
        // ============================================================================================================

        /// <summary>
        /// Plain priced bucket with volume: the requested auction hits its own bucket (the
        /// <c>l.TryGetValue(itemKey, out bucket)</c> fast path) for both Median (AssignMedian) and Lbin.
        /// </summary>
        [Test]
        public void GetPrice_ExactMatch_PricedBucket_BitExact()
        {
            var sample = Dupplicate(Base());
            sample.FlatenedNBT = new();
            sample.Enchantments = new List<Enchantment>();
            sample.Tag = "EXACT_SWORD";
            sample.HighestBidAmount = 12_000_000;
            AddVolume(sample, 10);
            // an active (unsold) auction so the bucket has an Lbin too
            var lbin = Dupplicate(sample);
            lbin.HighestBidAmount = 0;
            lbin.StartingBid = 15_000_000;
            DrivePublic(lbin);

            var probe = Dupplicate(sample);
            AssertGoldenEstimate("ExactMatch_PricedBucket", service.GetPrice(probe), GOLD_EXACT_PRICED);
        }

        /// <summary>
        /// Pet exact-match across tiers: the requested pet key resolves to its own populated bucket; exercises
        /// AssignMedian on a pet bucket plus the pet-key Lbin path.
        /// </summary>
        [Test]
        public void GetPrice_ExactMatch_Pet_BitExact()
        {
            var pet = Dupplicate(Base());
            pet.Tag = "PET_LION";
            pet.FlatenedNBT = new() { { "exp", "10000000" }, { "candyUsed", "0" } };
            pet.Enchantments = new List<Enchantment>();
            pet.Tier = Tier.LEGENDARY;
            pet.HighestBidAmount = 40_000_000;
            AddVolume(pet, 9);
            var lbin = Dupplicate(pet);
            lbin.HighestBidAmount = 0;
            lbin.StartingBid = 45_000_000;
            DrivePublic(lbin);

            var probe = Dupplicate(pet);
            AssertGoldenEstimate("ExactMatch_Pet", service.GetPrice(probe), GOLD_EXACT_PET);
        }

        // ============================================================================================================
        // NO-EXACT-MATCH fixtures: forces GetEstimatedMedian (closest-median brute force) + GetClosestLbins.
        // ============================================================================================================

        /// <summary>
        /// No bucket exists for the probed key (it carries an extra modifier none of the populated buckets has), so
        /// GetPrice falls through to <c>GetEstimatedMedian</c> (the closest-median brute scan) and
        /// <c>ClosestLbin</c>/<c>GetClosestLbins</c> (the <c>OrderByDescending(Similarity + Volume)</c> brute force) —
        /// the exact search R3-READ replaces. Pins the resulting estimated Median/MedianKey + closest Lbin.
        /// </summary>
        [Test]
        public void GetPrice_NoExactMatch_ClosestMedianAndLbin_BitExact()
        {
            // populate a couple of neighbour buckets with volume + an lbin, none matching the probe key exactly
            var neighbourA = Dupplicate(Base());
            neighbourA.Tag = "CRIMSON_CHESTPLATE";
            neighbourA.FlatenedNBT = new() { { "magic_find", "5" }, { "veteran", "4" } };
            neighbourA.Enchantments = new List<Enchantment>();
            neighbourA.HighestBidAmount = 10_000_000;
            AddVolume(neighbourA, 8);
            var neighbourAlbin = Dupplicate(neighbourA);
            neighbourAlbin.HighestBidAmount = 0;
            neighbourAlbin.StartingBid = 11_000_000;
            DrivePublic(neighbourAlbin);

            var neighbourB = Dupplicate(neighbourA);
            neighbourB.FlatenedNBT = new() { { "magic_find", "3" }, { "veteran", "4" } };
            neighbourB.HighestBidAmount = 15_000_000;
            AddVolume(neighbourB, 8);
            var neighbourBlbin = Dupplicate(neighbourB);
            neighbourBlbin.HighestBidAmount = 0;
            neighbourBlbin.StartingBid = 16_000_000;
            DrivePublic(neighbourBlbin);

            // probe a key with a modifier set NONE of the buckets has -> no exact bucket -> brute force
            var probe = Dupplicate(neighbourA);
            probe.FlatenedNBT = new() { { "magic_find", "9" }, { "veteran", "4" }, { "mending", "5" } };
            probe.HighestBidAmount = 0;
            probe.StartingBid = 1000;
            AssertGoldenEstimate("NoExactMatch_ClosestMedianAndLbin", service.GetPrice(probe), GOLD_NOEXACT_CLOSEST);
        }

        /// <summary>
        /// Lower-value-key path: the probed (lower-value) auction has no own priced bucket, so the estimated-median
        /// scan picks up a higher-value bucket and adds back a fraction of the missing value. Mirrors
        /// <c>ChecksLowerKeysForHigherPrice</c>; gates the lower-key + missing-enchant adjustment in the read path.
        /// </summary>
        [Test]
        public void GetPrice_NoExactMatch_LowerKeyHigherValue_BitExact()
        {
            SetBazaarPrice("ENCHANTMENT_PROTECTION_6", 6_200_000);
            SetBazaarPrice("ENCHANTMENT_GROWTH_6", 6_200_000);
            SetBazaarPrice("FUMING_POTATO_BOOK", 1_200_000);
            SetBazaarPrice("FIRST_MASTER_STAR", 14_200_000);
            SetBazaarPrice("RECOMBOBULATOR_3000", 8_200_000);

            var seed = Dupplicate(Base());
            seed.Tag = "LOWERKEY_HELM";
            seed.FlatenedNBT = new() { { "rarity_ugprades", "1" }, { "upgrade_level", "6" } };
            seed.Enchantments = new List<Enchantment> { new(EnchantmentType.growth, 6) };
            var noVolume = Dupplicate(seed);
            var target = Dupplicate(seed);
            seed.Enchantments.Add(new(EnchantmentType.protection, 6));
            seed.FlatenedNBT.Add("hpc", "15");
            seed.HighestBidAmount = 32_000_000;

            target.Enchantments.Clear();
            target.HighestBidAmount = 35_000_000;
            AddVolume(target);
            AddVolume(seed);

            AssertGoldenEstimate("NoExactMatch_LowerKeyHigherValue", service.GetPrice(noVolume), GOLD_NOEXACT_LOWERKEY);
        }

        // ============================================================================================================
        // BAZAAR + EMPTY fixtures: short-circuit and no-lookup paths.
        // ============================================================================================================

        /// <summary>Bazaar short-circuit: Median = bazaar price * count, no bucket search at all.</summary>
        [Test]
        public void GetPrice_Bazaar_BitExact()
        {
            SetBazaarPrice("ENCHANTMENT_GROWTH_6", 6_200_000);
            var probe = Dupplicate(Base());
            probe.Tag = "ENCHANTMENT_GROWTH_6";
            probe.Count = 3;
            probe.FlatenedNBT = new();
            probe.Enchantments = new List<Enchantment>();
            AssertGoldenEstimate("Bazaar", service.GetPrice(probe), GOLD_BAZAAR);
        }

        /// <summary>Unknown tag with no lookup at all returns an empty estimate (the early <c>return result</c>).</summary>
        [Test]
        public void GetPrice_UnknownTag_Empty_BitExact()
        {
            var probe = Dupplicate(Base());
            probe.Tag = "TOTALLY_UNKNOWN_TAG_XYZ";
            probe.FlatenedNBT = new();
            probe.Enchantments = new List<Enchantment>();
            AssertGoldenEstimate("UnknownTag", service.GetPrice(probe), GOLD_UNKNOWN);
        }

        // ============================================================================================================
        // Golden snapshots. (string) = serialized PriceEstimate, see SerializeEstimate.
        // Regenerate with DISCOVER=true; never hand-edit to silence a real diff.
        // ============================================================================================================

        private const string GOLD_EXACT_PRICED =
            "Median=12000000;Volume=10;Volatility=0;AvgSellTime=0;MedianKey= Any  UNKNOWN 1;LbinKey= Any  UNKNOWN 1;ItemKey= Any  UNKNOWN 1;Lbin.Price=15000000;Lbin.AuctionId!=0;SLbin.Price=0;LastSale.Price=12000000";

        private const string GOLD_EXACT_PET =
            "Median=40000000;Volume=9;Volatility=0;AvgSellTime=0;MedianKey= Any [exp, 2],[candyUsed, 0] LEGENDARY 1;LbinKey= Any [exp, 2],[candyUsed, 0] LEGENDARY 1;ItemKey= Any [exp, 2],[candyUsed, 0] LEGENDARY 1;Lbin.Price=45000000;Lbin.AuctionId!=0;SLbin.Price=0;LastSale.Price=40000000";

        private const string GOLD_NOEXACT_CLOSEST =
            "Median=113906250;Volume=8;Volatility=57;AvgSellTime=0;MedianKey= Any [magic_find, 3],[veteran, 4] UNKNOWN 1- 3;LbinKey= Any [magic_find, 5],[veteran, 4] UNKNOWN 1- 5;ItemKey= Any [magic_find, 9],[veteran, 4],[mending, 5] UNKNOWN 1;Lbin.Price=473744140;Lbin.AuctionId!=0;SLbin.Price=0;LastSale.Price=15000000";

        private const string GOLD_NOEXACT_LOWERKEY =
            "Median=35688888;Volume=4;Volatility=57;AvgSellTime=0;MedianKey=growth=6,protection=6 Any [upgrade_level, 6],[hotpc, 1] UNKNOWN 1- 1-protection6+HV- Any [upgrade_level, 6] UNKNOWN 1;LbinKey=;ItemKey=growth=6 Any [upgrade_level, 6] UNKNOWN 1;Lbin.Price=0;Lbin.AuctionId=0;SLbin.Price=0;LastSale.Price=32000000";

        private const string GOLD_BAZAAR =
            "Median=18600000;Volume=0;Volatility=0;AvgSellTime=0;MedianKey=;LbinKey=;ItemKey=;Lbin.Price=0;Lbin.AuctionId=0;SLbin.Price=0;LastSale.Price=0";

        private const string GOLD_UNKNOWN =
            "Median=0;Volume=0;Volatility=0;AvgSellTime=0;MedianKey=;LbinKey=;ItemKey=;Lbin.Price=0;Lbin.AuctionId=0;SLbin.Price=0;LastSale.Price=0";

        // ============================================================================================================
        // Serialization + assertion: pin every PriceEstimate field a read-path rewrite could perturb.
        // ============================================================================================================

        /// <summary>
        /// Stable, order-fixed serialization of every <see cref="PriceEstimate"/> field R3-READ could perturb. The
        /// Lbin/SLbin/LastSale <c>AuctionId</c> values are randomized by <see cref="Dupplicate"/> (idCounter), so for
        /// those structs we pin the deterministic <c>Price</c> and only the *presence* of an AuctionId (=0 vs !=0),
        /// not its exact id — every price/median/key field that the search determines is pinned exactly.
        /// </summary>
        private static string SerializeEstimate(PriceEstimate e)
        {
            string AuctionIdPresence(long id) => id == 0 ? "=0" : "!=0";
            return string.Join(";", new[]
            {
                $"Median={e.Median}",
                $"Volume={e.Volume.ToString("0.####", CultureInfo.InvariantCulture)}",
                $"Volatility={e.Volatility}",
                $"AvgSellTime={e.AvgSellTime}",
                $"MedianKey={e.MedianKey}",
                $"LbinKey={e.LbinKey}",
                $"ItemKey={e.ItemKey}",
                $"Lbin.Price={e.Lbin.Price}",
                $"Lbin.AuctionId{AuctionIdPresence(e.Lbin.AuctionId)}",
                $"SLbin.Price={e.SLbin.Price}",
                $"LastSale.Price={e.LastSale.Price}",
            });
        }

        private void AssertGoldenEstimate(string name, PriceEstimate live, string golden)
        {
            live.Should().NotBeNull($"GetPrice must return an estimate for fixture {name}");
            var actual = SerializeEstimate(live);
            // Always print so a DISCOVER=true run can harvest the line; the assert below is what actually gates.
            TestContext.Out.WriteLine($"[GOLD {name}] {actual}");
            if (DISCOVER)
                return;
            actual.Should().Be(golden,
                $"GetPrice result for fixture '{name}' must stay bit-identical (gates R3-READ); " +
                "if this is an intended change, regenerate with DISCOVER=true and paste the printed line");
        }

        // ---------- helpers (mirror ValuationFinders.Tests.cs / SniperService.Tests.cs) ----------

        private void DrivePublic(SaveAuction a)
        {
            var toTest = Dupplicate(a);
            service.FinishedUpdate();
            service.State = SniperState.FullyLoaded;
            service.TestNewAuction(toTest);
            service.FinishedUpdate();
        }

        private void AddVolume(SaveAuction toAdd, int count = 4)
        {
            for (int i = 0; i < count; i++)
                service.AddSoldItem(Dupplicate(toAdd));
            service.FinishedUpdate();
        }

        private static SaveAuction Base() => new SaveAuction
        {
            Tag = "PRICE_TEST",
            FlatenedNBT = new Dictionary<string, string> { { "skin", "bear" } },
            StartingBid = 1000,
            HighestBidAmount = 1000,
            Category = Category.ARMOR,
            UId = 5,
            AuctioneerId = "12c144",
            Count = 1,
        };

        private static SaveAuction Dupplicate(SaveAuction origin) => new SaveAuction(origin)
        {
            Uuid = (idCounter++).ToString().PadRight(8, '0'),
            UId = idCounter++,
            AuctioneerId = ((short)(idCounter++ % 20000)).ToString().PadRight(8, '0'),
            FlatenedNBT = new Dictionary<string, string>(origin.FlatenedNBT),
            Enchantments = origin.Enchantments == null ? null : new List<Enchantment>(origin.Enchantments),
        };

        private void SetBazaarPrice(string tag, int value, int buyValue = 0)
        {
            var sellOrder = new List<SellOrder>();
            if (value > 0)
                sellOrder.Add(new SellOrder { PricePerUnit = value });
            var buyOrder = new List<BuyOrder>();
            if (buyValue > 0)
                buyOrder.Add(new BuyOrder { PricePerUnit = buyValue });
            service.UpdateBazaar(new()
            {
                Products = new()
                {
                    new() { ProductId = tag, SellSummary = sellOrder, BuySummery = buyOrder }
                }
            });
        }
    }
}
