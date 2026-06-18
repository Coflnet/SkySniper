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
    /// R2 Phase 0 / F3 bit-exactness oracles for the valuation finders
    /// (<c>benchmarks/COMPUTE_FLOOR_SPEC_R2.md</c> §4-F3 + §5 R2-A / R2-B / R2-D). Two complementary gates:
    ///
    /// <list type="number">
    ///   <item><b>Snipe-set oracle</b> (gates <c>CheckCombined</c> = R2-A and <c>PotentialSnipe</c> = R2-B).
    ///     Those finders are private + stateful and emit through the <c>FoundSnipe</c> event, not a return value, so the
    ///     contract is the resulting <b>snipe set</b>: the ordered list of <c>(finder, targetPrice)</c> emitted while a
    ///     populated service processes a fixed scenario through the public <c>TestNewAuction</c>. The scenarios drive the
    ///     combined-attribute path (<c>CheckCombined</c>, the "combined" prop), the snipe path (<c>PotentialSnipe</c>),
    ///     and the lower-full-key path. The captured set is asserted against an embedded golden snapshot — a rewrite of
    ///     either finder is proven correct iff it reproduces the identical snipe set.</item>
    ///   <item><b>Golden-output oracle</b> (gates <c>GetCleanItemPrice</c> = R2-D). The clean-price recompute is a pure
    ///     read; the harness drives many (tag, reduced-tier, force) inputs over populated lookups and asserts the live
    ///     production result is byte-identical to <see cref="SniperService.GetCleanItemPriceForTest"/>'s verbatim
    ///     reference (<c>ValuationFinders.Reference.cs</c>). Covers pet vs non-pet tier selection, the
    ///     TIER_BOOST/rarity_upgrades tier-reduction branch, the cached-per-tier fast path, and the gem deviders.</item>
    /// </list>
    ///
    /// Construction mirrors <c>SniperService.Tests.cs</c> Setup (NullLogger to avoid the known flaky-logger NRE,
    /// MIN_TARGET=0, FullyLoaded state, AddSoldItem/AddVolume to populate buckets) and the
    /// <c>ChecksLowerValueAllAttributes</c> / <c>UseLbinFromCombinedLowerKeys</c> scenario shapes.
    /// </summary>
    public class ValuationFindersTests
    {
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
        // 1) Snipe-set oracle for CheckCombined (R2-A) + PotentialSnipe (R2-B): combined-attribute / lower-key paths.
        // ============================================================================================================

        /// <summary>
        /// Combined-key path: a high-value multi-component item whose exact bucket is thin, with a cheaper lower-value
        /// bucket priced; <c>CheckCombined</c> assembles a virtual bucket from the lower keys and (via FindFlip ->
        /// PotentialSnipe / SNIPER_MEDIAN) emits. The full emitted snipe set is the gate.
        /// </summary>
        [Test]
        public void Combined_SnipeSet_BitExact()
        {
            SetBazaarPrice("ENCHANTMENT_GROWTH_6", 6_200_000);
            SetBazaarPrice("ENCHANTMENT_PROTECTION_6", 6_200_000);
            SetBazaarPrice("ENCHANTMENT_MANA_VAMPIRE_4", 2_100_000);
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_LEGION_3", 1_800_000);
            SetBazaarPrice("FUMING_POTATO_BOOK", 1_100_000);
            SetBazaarPrice("HOT_POTATO_BOOK", 80_000);
            SetBazaarPrice("FIRST_MASTER_STAR", 12_000_000);
            SetBazaarPrice("FARMING_FOR_DUMMIES", 50_000_000);

            var sample = Dupplicate(Base());
            sample.Reforge = ItemReferences.Reforge.Magnetic;
            sample.FlatenedNBT = new() { { "farming_for_dummies_count", "1" }, { "hpc", "9" } };
            sample.Enchantments = new List<Enchantment>
            {
                new(EnchantmentType.growth, 6),
                new(EnchantmentType.protection, 6),
                new(EnchantmentType.mana_vampire, 4),
                new(EnchantmentType.ultimate_legion, 3),
            };

            var lowerValue = Dupplicate(sample);
            lowerValue.Enchantments = new List<Enchantment> { new(EnchantmentType.mana_vampire, 4) };
            lowerValue.HighestBidAmount = 14_000_000;
            AddVolume(lowerValue, 6);

            DrivePublic(sample);

            // gate: the combined finder must have contributed (the "combined" prop only comes from CheckCombined)
            found.Should().Contain(f => f.AdditionalProps != null && f.AdditionalProps.ContainsKey("combined"),
                "CheckCombined must emit at least one snipe carrying the 'combined' prop");
            // Golden snapshot of the FULL ordered snipe set this scenario emits. CheckCombined assembles the virtual
            // bucket that drives the first SNIPER_MEDIAN (the one carrying the "combined" prop); the remaining emits
            // (SNIPER_MEDIAN/STONKS/CraftCost) are part of the same TestNewAuction pass and must all stay identical
            // when R2-A/R2-B rewrite the finders.
            CaptureSnipeSet().Should().Be(
                "SNIPER_MEDIAN@13166986;SNIPER_MEDIAN@13435701;STONKS@15113913;CraftCost@69790701",
                "the combined/snipe finders' emitted snipe set must stay bit-identical (gates R2-A/R2-B rewrites)");
        }

        /// <summary>
        /// Pure snipe path (<c>PotentialSnipe</c>): a priced bucket with volume and a much higher LBIN wall, an incoming
        /// auction far below — the SNIPER finder fires. The (finder, targetPrice) set is the gate for R2-B.
        /// </summary>
        [Test]
        public void PotentialSnipe_SnipeSet_BitExact()
        {
            var sample = Dupplicate(Base());
            sample.FlatenedNBT = new();
            sample.Enchantments = new List<Enchantment>();
            sample.HighestBidAmount = 10_000_000;
            AddVolume(sample, 12);

            // a higher-priced LBIN wall so PotentialSnipe has room
            var wall = Dupplicate(sample);
            wall.HighestBidAmount = 0;
            wall.StartingBid = 30_000_000;
            DrivePublic(wall); // adds the lbin

            var snipe = Dupplicate(sample);
            snipe.HighestBidAmount = 0;
            snipe.StartingBid = 5;
            service.FinishedUpdate();
            service.State = SniperState.FullyLoaded;
            found.Clear();
            service.TestNewAuction(Dupplicate(snipe));

            found.Should().Contain(f => f.Finder == LowPricedAuction.FinderType.SNIPER,
                "an undercut below a priced volume bucket must trigger PotentialSnipe");
            // PotentialSnipe emits the leading SNIPER; the trailing SNIPER_MEDIAN is the same FindFlip pass. Both pinned.
            CaptureSnipeSet().Should().Be(
                "SNIPER@10000000;SNIPER_MEDIAN@10000000",
                "PotentialSnipe's emitted snipe (finder + target price) must stay bit-identical (gates R2-B)");
        }

        /// <summary>
        /// Lower-full-key path (<c>CheckLowerKeyFull</c>) + combined LBIN: the complicated SHADOW_ASSASSIN scenario from
        /// the suite. Gates the lower-key + combined finders by their emitted SNIPER target.
        /// </summary>
        [Test]
        public void LowerKeyAndCombined_SnipeSet_BitExact()
        {
            SetBazaarPrice("ENCHANTMENT_ULTIMATE_LEGION_5", 10_000_000);
            SetBazaarPrice("RECOMBOBULATOR_3000", 10_000_000);
            SetBazaarPrice("FINE_JASPER_GEM", 85_000);
            SetBazaarPrice("ESSENCE_WITHER", 2_600);
            SetBazaarPrice("HOT_POTATO_BOOK", 80_000);
            SetBazaarPrice("FUMING_POTATO_BOOK", 1_200_000);

            var first = Dupplicate(Base());
            first.FlatenedNBT = new() { { "rarity_upgrades", "1" }, { "upgrade_level", "5" }, { "unlocked_slots", "COMBAT_0,JASPER_0" }, { "hpc", "10" } };
            first.Tag = "SHADOW_ASSASSIN_HELMET";
            first.StartingBid = 19_000_000;
            first.HighestBidAmount = 13_000_000;
            AddVolume(first, 10);
            first.HighestBidAmount = 0;
            DrivePublic(first); // add lbin

            var higherValue = Dupplicate(first);
            higherValue.FlatenedNBT["hpc"] = "15";
            higherValue.Enchantments = new List<Enchantment> { new(EnchantmentType.ultimate_legion, 5) };
            higherValue.StartingBid = 14_000_000;
            higherValue.HighestBidAmount = 14_500_000;
            AddVolume(higherValue);
            higherValue.HighestBidAmount = 14_000_000;
            DrivePublic(higherValue);

            var sniper = found.Where(f => f.Finder == LowPricedAuction.FinderType.SNIPER).ToList();
            sniper.Should().NotBeEmpty("the combined-lower-key scenario must emit a SNIPER snipe");
            sniper.Select(f => f.TargetPrice).Should().Contain(18760000L,
                "the combined-lower-keys SNIPER target must stay bit-identical (gates R2-A + CheckLowerKeyFull)");
            // full ordered snipe set pinned too, so a finder rewrite that drops/adds an emit is caught.
            CaptureSnipeSet().Should().Be(
                "SNIPER@18760000;CraftCost@31834853",
                "the combined-lower-keys snipe set must stay bit-identical (gates R2-A + CheckLowerKeyFull)");
        }

        // ============================================================================================================
        // 1b) R6 WS-CMB2 — snipe-set A/B fuzz: the de-LINQ'd CheckCombined must emit the IDENTICAL snipe set (incl. the
        //     "combined" prop string) as the verbatim pre-R6 LINQ reference, over randomized HEAVY-TIE lookups. Heavy
        //     ties stress every stable-sort tiebreak the de-LINQ reproduces: equal value (relevant order), equal
        //     relevancy (combined order), equal Day (newestRef / props prefix), equal Lbin price (MinBy first-wins).
        // ============================================================================================================

        /// <summary>
        /// For many randomized scenarios, drives the SAME populated lookup + incoming auction through both the de-LINQ'd
        /// CheckCombined (production default) and the verbatim LINQ <c>CheckCombinedReference</c> (via the
        /// <c>UseCombinedReference</c> seam), asserting the full emitted snipe set — finder, target price, and the
        /// CheckCombined-only "combined" prop — is byte-identical. Each scenario builds two fresh services from one
        /// deterministic template list so both see bit-identical state.
        /// </summary>
        [Test]
        public void Combined_DeLinq_BitExact_Fuzz()
        {
            int scenarios = 400;
            int mismatches = 0;
            int firstMismatch = -1;
            string detail = null;
            int withSnipes = 0;
            int withCombinedProp = 0;
            for (int seed = 0; seed < scenarios; seed++)
            {
                var rnd = new Random(seed * 7919 + 13);
                var templates = BuildCombinedTemplates(rnd);
                var incoming = templates.incoming;

                var newSet = RunCombinedScenario(templates.sold, incoming, useReference: false);
                var refSet = RunCombinedScenario(templates.sold, incoming, useReference: true);

                if (newSet.Length > 0)
                    withSnipes++;
                if (newSet.Contains("|") && newSet.Split(';').Any(s => s.Contains('|') && s.Split('|', 2)[1].Length > 0))
                    withCombinedProp++;

                if (newSet != refSet)
                {
                    if (firstMismatch < 0)
                    {
                        firstMismatch = seed;
                        detail = $"seed={seed}\n  new={newSet}\n  ref={refSet}";
                    }
                    mismatches++;
                }
            }
            mismatches.Should().Be(0,
                $"de-LINQ'd CheckCombined must emit the identical snipe set as the LINQ reference across {scenarios} heavy-tie scenarios. First mismatch:\n{detail}");
            // Coverage guard: the fuzz must actually drive the combined path (else it passes vacuously).
            withCombinedProp.Should().BeGreaterThan(0,
                $"the fuzz must exercise CheckCombined's combined-emit path (scenarios with snipes: {withSnipes}/{scenarios}, with a non-empty 'combined' prop: {withCombinedProp})");
        }

        /// <summary>Builds one randomized scenario: a set of "sold" templates (heavy ties in value/relevancy/Day/lbin) + an incoming auction.</summary>
        private (List<(SaveAuction tmpl, int volume)> sold, SaveAuction incoming) BuildCombinedTemplates(Random rnd)
        {
            // A small pool of enchant types + levels so distinct keys collide on the `similar`/dominance predicates and
            // value/relevancy frequently TIE (the whole point of the fuzz).
            EnchantmentType[] enchPool = { EnchantmentType.growth, EnchantmentType.protection, EnchantmentType.mana_vampire, EnchantmentType.ultimate_legion, EnchantmentType.sharpness };
            string tag = "FUZZ_ITEM";
            var sold = new List<(SaveAuction, int)>();
            int buckets = 2 + rnd.Next(6); // 2..7 distinct buckets
            // Quantized values so many buckets share a value → forces value/relevancy ties.
            for (int b = 0; b < buckets; b++)
            {
                int enchCount = rnd.Next(3); // 0..2 enchants
                var enchants = new List<Enchantment>();
                for (int e = 0; e < enchCount; e++)
                    enchants.Add(new Enchantment(enchPool[rnd.Next(enchPool.Length)], (byte)(1 + rnd.Next(3))));
                var a = new SaveAuction
                {
                    Tag = tag,
                    Tier = Tier.LEGENDARY,
                    Category = Category.ARMOR,
                    Reforge = rnd.Next(2) == 0 ? ItemReferences.Reforge.Any : ItemReferences.Reforge.Magnetic,
                    FlatenedNBT = new Dictionary<string, string>(),
                    Enchantments = enchants,
                    Count = 1,
                    // quantized bid → repeated Price/Day across buckets → ties
                    StartingBid = 1_000_000 * (1 + rnd.Next(4)),
                    HighestBidAmount = 1_000_000 * (1 + rnd.Next(4)),
                };
                int volume = 3 + rnd.Next(6); // 3..8 references
                sold.Add((a, volume));
            }
            // incoming: a higher-value multi-component auction whose exact bucket is thin → exercises the combined path
            var incoming = new SaveAuction
            {
                Tag = tag,
                Tier = Tier.LEGENDARY,
                Category = Category.ARMOR,
                Reforge = rnd.Next(2) == 0 ? ItemReferences.Reforge.Any : ItemReferences.Reforge.Magnetic,
                FlatenedNBT = new Dictionary<string, string>(),
                Enchantments = new List<Enchantment>
                {
                    new(enchPool[rnd.Next(enchPool.Length)], (byte)(1 + rnd.Next(3))),
                    new(enchPool[rnd.Next(enchPool.Length)], (byte)(1 + rnd.Next(3))),
                },
                Count = 1,
                StartingBid = 5,
                HighestBidAmount = 0,
            };
            return (sold, incoming);
        }

        /// <summary>Builds a fresh service, populates it from the templates, drives the incoming auction, returns the captured snipe set string.</summary>
        private string RunCombinedScenario(List<(SaveAuction tmpl, int volume)> sold, SaveAuction incoming, bool useReference)
        {
            var localCraft = new CraftCostMock();
            var localItem = new HypixelItemService(null, NullLogger<HypixelItemService>.Instance);
            var localService = new SniperService(localItem, null, NullLogger<SniperService>.Instance, localCraft);
            localService.UseCombinedReference = useReference;
            var localFound = new List<LowPricedAuction>();
            localService.FoundSnipe += localFound.Add;

            long localId = 1000;
            SaveAuction Dup(SaveAuction o) => new SaveAuction(o)
            {
                Uuid = (localId++).ToString().PadRight(8, '0'),
                UId = localId++,
                AuctioneerId = ((short)(localId++ % 20000)).ToString().PadRight(8, '0'),
                FlatenedNBT = new Dictionary<string, string>(o.FlatenedNBT),
                Enchantments = o.Enchantments == null ? null : new List<Enchantment>(o.Enchantments),
            };

            localService.State = SniperState.FullyLoaded;
            foreach (var (tmpl, volume) in sold)
                for (int i = 0; i < volume; i++)
                    localService.AddSoldItem(Dup(tmpl));
            localService.FinishedUpdate();
            localService.State = SniperState.FullyLoaded;
            localFound.Clear();
            localService.TestNewAuction(Dup(incoming));

            // Snipe set incl. the CheckCombined-only "combined" prop (the de-LINQ'd prefix/TakeWhile output).
            return string.Join(";", localFound.Select(f =>
            {
                string combinedProp = (f.AdditionalProps != null && f.AdditionalProps.TryGetValue("combined", out var c)) ? c : "";
                return $"{f.Finder}@{f.TargetPrice}|{combinedProp}";
            }));
        }

        // ============================================================================================================
        // 2) Golden-output oracle for GetCleanItemPrice (R2-D): live == verbatim reference, on populated lookups.
        // ============================================================================================================

        [Test]
        public void GetCleanItemPrice_BitExact_VsReference_NonPet()
        {
            // build a non-pet item with several priced buckets across tiers
            string tag = "SOME_SWORD";
            for (int t = (int)Tier.UNCOMMON; t <= (int)Tier.MYTHIC; t++)
            {
                var a = Dupplicate(Base());
                a.Tag = tag;
                a.Tier = (Tier)t;
                a.FlatenedNBT = new();
                a.Enchantments = new List<Enchantment>();
                a.HighestBidAmount = 1_000_000L * (t + 1);
                AddVolume(a, 8);
            }
            // a bucket with an attribute modifier (excluded from non-pet clean selection)
            var attr = Dupplicate(Base());
            attr.Tag = tag;
            attr.Tier = Tier.LEGENDARY;
            attr.FlatenedNBT = new() { { "mending", "5" } };
            attr.HighestBidAmount = 50_000_000;
            AddVolume(attr, 8);
            service.FinishedUpdate();

            AssertCleanPriceParityForAllBuckets(tag);
        }

        [Test]
        public void GetCleanItemPrice_BitExact_VsReference_Pet()
        {
            string tag = "PET_LION";
            for (int t = (int)Tier.COMMON; t <= (int)Tier.LEGENDARY; t++)
            {
                var a = Dupplicate(Base());
                a.Tag = tag;
                a.Tier = (Tier)t;
                a.FlatenedNBT = new() { { "exp", "10000000" }, { "candyUsed", "0" } };
                a.Enchantments = new List<Enchantment>();
                a.HighestBidAmount = 2_000_000L * (t + 1);
                AddVolume(a, 8);
            }
            // a TIER_BOOST bucket (the pet selection excludes TIER_BOOST keys; clean-price tier-reduction branch)
            var boosted = Dupplicate(Base());
            boosted.Tag = tag;
            boosted.Tier = Tier.LEGENDARY;
            boosted.FlatenedNBT = new() { { "exp", "10000000" }, { "candyUsed", "0" }, { "heldItem", "PET_ITEM_TIER_BOOST" } };
            boosted.HighestBidAmount = 99_000_000;
            AddVolume(boosted, 8);
            service.FinishedUpdate();

            AssertCleanPriceParityForAllBuckets(tag);
        }

        /// <summary>
        /// Drives every live bucket key through both the production GetCleanItemPrice and the verbatim reference, with
        /// and without <c>force</c> (force bypasses the cached-per-tier fast path, exercising the full recompute), and
        /// asserts byte-equality. Also exercises the TIER_BOOST/rarity_upgrades reduced-tier key branch explicitly.
        /// </summary>
        private void AssertCleanPriceParityForAllBuckets(string tag)
        {
            service.Lookups.TryGetValue(tag, out var lookup).Should().BeTrue($"lookup for {tag} must exist");
            int checkedKeys = 0;
            foreach (var kv in lookup.Lookup.Keys.ToList())
            {
                var auction = AuctionFromKey(tag, kv);
                var detailed = service.ValueKeyForTest(auction);
                foreach (var force in new[] { false, true })
                {
                    var (live, reference) = service.GetCleanItemPriceForTest(tag, detailed, lookup, force);
                    live.Should().Be(reference,
                        $"GetCleanItemPrice (force={force}) must equal the verbatim reference for key {kv} (gates R2-D)");
                }
                checkedKeys++;
            }
            checkedKeys.Should().BeGreaterThan(0, "must have buckets to check");

            // explicit reduced-tier branch: a key carrying TIER_BOOST must read the reduced-tier clean price identically
            var boostKey = service.ValueKeyForTest(AuctionWith(tag, Tier.LEGENDARY,
                new() { { SniperService.PetItemKey, SniperService.TierBoostShorthand } }));
            var (liveB, refB) = service.GetCleanItemPriceForTest(tag, boostKey, lookup, false);
            liveB.Should().Be(refB, "TIER_BOOST reduced-tier clean price must equal the reference (gates R2-D)");

            var recombKey = service.ValueKeyForTest(AuctionWith(tag, Tier.LEGENDARY,
                new() { { "rarity_upgrades", "1" } }));
            var (liveR, refR) = service.GetCleanItemPriceForTest(tag, recombKey, lookup, false);
            liveR.Should().Be(refR, "rarity_upgrades reduced-tier clean price must equal the reference (gates R2-D)");
        }

        // ---------- snipe-set capture ----------

        /// <summary>Normalizes the emitted snipe set into a stable, order-preserving string the golden snapshot pins.</summary>
        private string CaptureSnipeSet()
            => string.Join(";", found.Select(f => $"{f.Finder}@{f.TargetPrice}"));

        // ---------- helpers (mirror SniperService.Tests.cs) ----------

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
        }

        private static SaveAuction Base() => new SaveAuction
        {
            Tag = "VAL_TEST",
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

        private static SaveAuction AuctionWith(string tag, Tier tier, Dictionary<string, string> nbt) => new SaveAuction
        {
            Tag = tag,
            Tier = tier,
            FlatenedNBT = nbt,
            Enchantments = new List<Enchantment>(),
            Count = 1,
            StartingBid = 1000,
            HighestBidAmount = 1000,
        };

        private static SaveAuction AuctionFromKey(string tag, AuctionKey key)
        {
            var nbt = new Dictionary<string, string>();
            foreach (var m in key.Modifiers)
                nbt[m.Key] = m.Value;
            return new SaveAuction
            {
                Tag = tag,
                Tier = key.Tier,
                Reforge = key.Reforge,
                Count = Math.Max((int)key.Count, 1),
                Category = Category.UNKNOWN,
                FlatenedNBT = nbt,
                Enchantments = key.Enchants.Select(e => new Enchantment(e.Type, e.Lvl)).ToList(),
                StartingBid = 1000,
                HighestBidAmount = 1000,
            };
        }

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
