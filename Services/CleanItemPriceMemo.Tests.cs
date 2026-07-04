using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R6/MEMO2 bit-exactness soak for the entry-epoch-stamped <see cref="CleanItemPriceMemo"/> (the version-invalidated
    /// memo of the <c>GetCleanItemPrice</c> recompute). Mirrors the <c>ReferenceSnapshot</c> soak / <c>VerifyClosestIndex</c>
    /// parity pattern: drives MANY <c>GetCleanItemPrice</c> calls across tiers/tags/forces under simulated lookup
    /// mutation and asserts the memoized result (live production, which now routes through the memo) EQUALS a fresh,
    /// non-memoized recompute (<see cref="SniperService.GetCleanItemPriceForTest"/>'s verbatim reference) EVERY time —
    /// 0 divergences.
    ///
    /// <para>The reference (<c>GetCleanItemPriceReference</c>) re-checks <c>CleanPricePerTier</c> and re-runs the full
    /// flatten/sort/select itself, touching NONE of the memo, so equality proves the memo never returns a value a fresh
    /// recompute (with identical force/non-force semantics) wouldn't — including across invalidation boundaries and for
    /// the <c>force:true</c> callers (force is exercised on every key).</para>
    ///
    /// <para>The warm fix (vs the deferred clean3) is per-ENTRY stamping: ONE memo dict per lookup for its lifetime (the
    /// dict reference never changes after first publication), each entry carrying its own (epoch, Count, Volume, builtAt)
    /// stamp. <see cref="Memo_IsActuallyHit_AndInvalidatedByEntryStamp"/> asserts exactly that — the dict is reused
    /// across an epoch bump (no realloc) while the stale entry is re-stamped on the next read.</para>
    /// </summary>
    public class CleanItemPriceMemoTests
    {
        private SniperService service = null!;
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
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            SniperService.MIN_TARGET = 0;
            SniperService.StartTime = new DateTime(2021, 9, 25);
            craftCost = new CraftCostMock();
            itemService = new HypixelItemService(null, NullLogger<HypixelItemService>.Instance);
            service = new SniperService(itemService, null, NullLogger<SniperService>.Instance, craftCost);
            idCounter = 100;
        }

        [TearDown]
        public void TearDown() => SniperService.MIN_TARGET = 200_000;

        // ============================================================================================================
        // The soak: live (memo) == fresh recompute, across tiers/tags/forces, REPEATED (hits) and under MUTATION.
        // ============================================================================================================

        [Test]
        public void Memo_BitExact_VsFreshRecompute_UnderMutation()
        {
            // A non-pet tag, a pet tag, and a gem-capable tag — exercises isPet, the gem-devider, and tier filtering.
            string[] tags = { "SOME_SOAK_SWORD", "PET_SOAK_LION", "DIVAN_DRILL_4" };
            foreach (var tag in tags)
                SeedTag(tag);
            service.FinishedUpdate();

            int comparisons = 0;
            // Outer loop simulates lookup mutation between waves: each AddSoldItem -> UpdateMedian bumps the pricing
            // epoch (and may change references / dict Count / Volume), which is exactly what invalidates the memo
            // entries. We assert equality BEFORE and AFTER each mutation, and also repeatedly within a wave (to force
            // memo HITS off the freshly-stamped entries).
            for (int wave = 0; wave < 12; wave++)
            {
                foreach (var tag in tags)
                {
                    service.Lookups.TryGetValue(tag, out var lookup).Should().BeTrue();
                    var keys = lookup.Lookup.Keys.ToList();
                    // Repeat the inner sweep so the 2nd+ pass within a stable epoch is served by the memo (hits), not builds.
                    for (int pass = 0; pass < 3; pass++)
                    {
                        foreach (var k in keys)
                            comparisons += AssertParity(tag, lookup, k);
                        // also drive synthetic reduced-tier keys (TIER_BOOST / rarity_upgrades) per pass
                        comparisons += AssertParity(tag, lookup, ReducedTierKey(tag, Tier.LEGENDARY, SniperService.PetItemKey, SniperService.TierBoostShorthand));
                        comparisons += AssertParity(tag, lookup, ReducedTierKey(tag, Tier.LEGENDARY, "rarity_upgrades", "1"));
                        // and a few tiers that may not have a bucket at all (empty-flatten path)
                        foreach (var t in new[] { Tier.UNKNOWN, Tier.COMMON, Tier.MYTHIC, Tier.DIVINE })
                            comparisons += AssertParity(tag, lookup, PlainTierKey(tag, t));
                    }
                }

                // mutate: add a sold item to one tag (bumps epoch + grows references/volume; sometimes a new dict key)
                var mutTag = tags[wave % tags.Length];
                var mut = AuctionFor(mutTag, (Tier)(Tier.UNCOMMON + wave % 4));
                if (wave % 2 == 0)
                    mut.FlatenedNBT["soakmod" + wave] = "1"; // occasionally a novel bucket key -> dict Count change
                AddVolume(mut, 5);
                service.FinishedUpdate();
            }

            comparisons.Should().BeGreaterThan(500, "the soak must exercise many (tag, tier, force) recomputes + hits");
        }

        [Test]
        public void Memo_IsActuallyHit_AndInvalidatedByEntryStamp()
        {
            string tag = "MEMO_HIT_SWORD";
            SeedTag(tag);
            service.FinishedUpdate();
            service.Lookups.TryGetValue(tag, out var lookup).Should().BeTrue();

            var key = lookup.Lookup.Keys.First();
            var detailed = service.ValueKeyForTest(AuctionFromKey(tag, key));

            // first force call builds+stamps the memo entry; subsequent identical force calls must hit it (same epoch)
            var first = service.GetCleanItemPrice(tag, detailed, lookup, force: true);
            var memo = lookup.CleanPriceMemo;
            memo.Should().NotBeNull("a memo dict must be published after the recompute");
            memo!.Values.TryGetValue((tag, key.Tier), out var entry1).Should().BeTrue("the recompute result must be memoized");
            entry1.Value.Should().Be(first);

            var second = service.GetCleanItemPrice(tag, detailed, lookup, force: true);
            second.Should().Be(first, "a force call within the same epoch must return the memoized recompute (bit-exact)");
            // The warm fix: the memo DICT is reused for the lookup's lifetime — never re-allocated on a hit.
            ReferenceEquals(lookup.CleanPriceMemo, memo).Should().BeTrue("a hit must not rebuild the memo dict");

            // a mutation bumps the pricing epoch -> the existing entry's stamp goes stale (a MISS), the next call must
            // recompute and RE-STAMP the entry in place. The dict reference itself must NOT change (no per-window realloc).
            AddVolume(AuctionFor(tag, Tier.RARE), 5);
            service.FinishedUpdate();
            var third = service.GetCleanItemPrice(tag, detailed, lookup, force: true);
            ReferenceEquals(lookup.CleanPriceMemo, memo).Should().BeTrue("an epoch bump must NOT reallocate the memo dict (the warm fix)");
            lookup.CleanPriceMemo!.Values.TryGetValue((tag, key.Tier), out var entry2).Should().BeTrue();
            entry2.Epoch.Should().NotBe(entry1.Epoch, "the stale entry must have been re-stamped at the new epoch");

            // and it stays bit-exact with a fresh recompute after invalidation
            var (live, reference) = service.GetCleanItemPriceForTest(tag, detailed, lookup, force: true);
            live.Should().Be(reference, "the post-invalidation memo must equal a fresh recompute");
        }

        [Test]
        public void Memo_BitExact_UnderConcurrency()
        {
            // The risky finder runs on a background Task; assert the memo is tear-safe and bit-exact under parallel
            // GetCleanItemPrice calls for the same lookup (concurrent build + hit races store identical deterministic
            // values, so a concurrent reader can never observe a value a fresh recompute wouldn't).
            string tag = "MEMO_CONC_SWORD";
            SeedTag(tag);
            service.FinishedUpdate();
            service.Lookups.TryGetValue(tag, out var lookup).Should().BeTrue();
            var keys = lookup.Lookup.Keys.ToList();
            var detaileds = keys.Select(k => service.ValueKeyForTest(AuctionFromKey(tag, k))).ToList();

            long divergences = 0;
            Parallel.For(0, 2000, i =>
            {
                int idx = i % detaileds.Count;
                bool force = (i & 1) == 0;
                var (live, reference) = service.GetCleanItemPriceForTest(tag, detaileds[idx], lookup, force);
                if (live != reference)
                    Interlocked.Increment(ref divergences);
            });
            divergences.Should().Be(0, "the memo must be bit-exact with a fresh recompute under concurrent access");
            // The memo dict is created exactly once even under a concurrent first-touch race (atomic CAS publish).
            lookup.CleanPriceMemo.Should().NotBeNull();
        }

        // ---------- helpers ----------

        /// <summary>Asserts live (memo) == fresh recompute for both force values; returns the number of comparisons made.</summary>
        private int AssertParity(string tag, PriceLookup lookup, AuctionKey key)
        {
            var detailed = service.ValueKeyForTest(AuctionFromKey(tag, key));
            int made = 0;
            foreach (var force in new[] { false, true })
            {
                var (live, reference) = service.GetCleanItemPriceForTest(tag, detailed, lookup, force);
                live.Should().Be(reference,
                    $"memo (force={force}) must equal a fresh recompute for {tag} tier={key.Tier} mods=[{string.Join(",", key.Modifiers.Select(m => m.Key + "=" + m.Value))}]");
                made++;
            }
            return made;
        }

        private AuctionKey ReducedTierKey(string tag, Tier tier, string modKey, string modVal)
        {
            var a = AuctionWith(tag, tier, new() { { modKey, modVal } });
            return service.ValueKeyForTest(a).Key;
        }

        private AuctionKey PlainTierKey(string tag, Tier tier)
            => service.ValueKeyForTest(AuctionWith(tag, tier, new())).Key;

        private void SeedTag(string tag)
        {
            bool isPet = NBT.IsPet(tag);
            for (int t = (int)Tier.COMMON; t <= (int)Tier.LEGENDARY; t++)
            {
                var a = AuctionFor(tag, (Tier)t);
                a.HighestBidAmount = 1_000_000L * (t + 1);
                AddVolume(a, 8);
            }
            // a bucket carrying an excluded modifier (attribute for non-pet, TIER_BOOST for pet) for filter coverage
            var extra = AuctionFor(tag, Tier.LEGENDARY);
            if (isPet)
                extra.FlatenedNBT["heldItem"] = "PET_ITEM_TIER_BOOST";
            else
                extra.FlatenedNBT["mending"] = "5";
            extra.HighestBidAmount = 77_000_000;
            AddVolume(extra, 8);
        }

        private SaveAuction AuctionFor(string tag, Tier tier)
        {
            var nbt = new Dictionary<string, string>();
            if (NBT.IsPet(tag))
            {
                nbt["exp"] = "10000000";
                nbt["candyUsed"] = "0";
            }
            else
            {
                nbt["skin"] = "soak";
            }
            return new SaveAuction
            {
                Tag = tag,
                Tier = tier,
                FlatenedNBT = nbt,
                Enchantments = new List<Enchantment>(),
                Category = Category.UNKNOWN,
                Count = 1,
                StartingBid = 1000,
                HighestBidAmount = 1000,
                UId = idCounter++,
                Uuid = (idCounter++).ToString().PadRight(8, '0'),
                AuctioneerId = ((short)(idCounter++ % 20000)).ToString().PadRight(8, '0'),
            };
        }

        private static SaveAuction AuctionWith(string tag, Tier tier, Dictionary<string, string> nbt)
        {
            if (NBT.IsPet(tag) && !nbt.ContainsKey("exp"))
            {
                nbt["exp"] = "10000000";
                nbt["candyUsed"] = "0";
            }
            return new SaveAuction
            {
                Tag = tag,
                Tier = tier,
                FlatenedNBT = nbt,
                Enchantments = new List<Enchantment>(),
                Count = 1,
                StartingBid = 1000,
                HighestBidAmount = 1000,
            };
        }

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

        private void AddVolume(SaveAuction toAdd, int count)
        {
            for (int i = 0; i < count; i++)
            {
                var dup = new SaveAuction(toAdd)
                {
                    Uuid = (idCounter++).ToString().PadRight(8, '0'),
                    UId = idCounter++,
                    AuctioneerId = ((short)(idCounter++ % 20000)).ToString().PadRight(8, '0'),
                    FlatenedNBT = new Dictionary<string, string>(toAdd.FlatenedNBT),
                    Enchantments = toAdd.Enchantments == null ? null : new List<Enchantment>(toAdd.Enchantments),
                };
                service.AddSoldItem(dup);
            }
        }
    }
}
