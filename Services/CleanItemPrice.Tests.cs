using System;
using AwesomeAssertions;
using Coflnet.Sky.Sniper.Models;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R5B (partial selection) bit-exactness gate for the clean-price order-statistic selection.
    ///
    /// <para>
    /// <b>What is under test.</b> <see cref="SniperService.GetCleanItemPrice"/>'s hottest internal (≈55–65% of cold/rare
    /// Sniper CPU, per <c>COMPUTE_FLOOR_SPEC_R5.md</c> P4) is the two-full-stable-sort selection
    /// <see cref="SniperService.CleanItemPriceSelectTargetReference"/>: it sorts ALL n references by
    /// (Day DESC, Price ASC, idx ASC), takes the first <c>size</c>, stable-sorts that prefix by (Price ASC,
    /// prefix-position ASC), and returns the element at position <c>skip</c>. R5B replaces it with
    /// <see cref="SniperService.CleanItemPriceSelectTarget"/> — two bounded quickselect partitions (no full sort) —
    /// which must return the <b>identical</b> <see cref="ReferencePrice"/> for every input.
    /// </para>
    ///
    /// <para>
    /// <b>Why this fuzz is heavier than competitor A's.</b> Partial/selection methods diverge from a stable full sort
    /// precisely at TIE boundaries (equal Day and/or equal Price). A naive <c>nth_element</c> is unstable and would
    /// pick the wrong element of a tie group. This fuzz therefore generates <b>40,000</b> seeds of randomized
    /// <see cref="ReferencePrice"/>[] (length 0..~400) with deliberately <b>narrow Day AND Price ranges</b> so ties are
    /// dense, plus random valid (size, skip), and asserts the partial selection equals the reference selection on
    /// <b>all of Price, Day, AND AuctionId</b> (AuctionId is the discriminator that catches a same-Price/same-Day tie
    /// being resolved to the wrong original element). 0 mismatches is the ship gate.
    /// </para>
    /// </summary>
    public class CleanItemPriceTests
    {
        [Test]
        public void Fuzz_PartialSelection_BitExact_VsReference_HeavyTies()
        {
            int mismatches = 0;
            string first = "";
            long totalRefs = 0;
            int maxN = 0;
            int tiePathExercised = 0; // count of cases where ties actually existed in the consumed prefix

            for (int seed = 1; seed <= 40_000; seed++)
            {
                var rng = new Random(seed);
                int n = rng.Next(0, 401); // 0..400 references

                // Heavy ties: keep Day and Price ranges SMALL relative to n so equal-key collisions are dense.
                // Range width is itself randomized (1..8 for Day, 1..6 for Price) so we span "almost all equal"
                // through "moderately distinct"; AuctionId is unique per element (the tie discriminator).
                int dayRange = rng.Next(1, 9);
                int priceRange = rng.Next(1, 7);
                short dayBase = (short)rng.Next(-30, 30);

                var refs = new ReferencePrice[n];
                bool sawDup = false;
                for (int i = 0; i < n; i++)
                {
                    refs[i] = new ReferencePrice
                    {
                        AuctionId = ((long)seed << 20) | (uint)i, // globally unique within the run
                        Day = (short)(dayBase + rng.Next(0, dayRange)),
                        Price = rng.Next(0, priceRange),
                        Seller = (short)rng.Next(0, 4),
                        Buyer = (short)rng.Next(0, 4),
                    };
                }
                _ = sawDup;

                // Mirror the GetCleanItemPrice sizing so (size, skip) span realistic and edge shapes.
                // size = min(max(Volume*10, 50), n) with a randomized Volume; plus the devider-derived skip.
                float volume = (float)(rng.NextDouble() * rng.Next(0, 60));
                int size = (int)Math.Min(Math.Max(volume * 10, 50), n);
                // also throw in fully-random valid (size, skip) on half the seeds to stress arbitrary ranks
                if ((seed & 1) == 0 && n > 0)
                    size = rng.Next(1, n + 1);
                int devider = new[] { 10, 14, 30 }[rng.Next(3)];
                int skip = size / devider + 1;
                if ((seed & 3) == 0 && size > 0)
                    skip = rng.Next(0, size + 2); // sometimes push skip past the guard boundary

                var live = SniperService.CleanItemPriceSelectTarget(refs, size, skip);
                var reference = SniperService.CleanItemPriceSelectTargetReference(refs, size, skip);

                if (live.Price != reference.Price || live.Day != reference.Day || live.AuctionId != reference.AuctionId)
                {
                    if (mismatches == 0)
                        first = $"seed={seed} n={n} size={size} skip={skip} dayRange={dayRange} priceRange={priceRange}\n"
                              + $"  live=(Price={live.Price},Day={live.Day},Auc={live.AuctionId})\n"
                              + $"  ref =(Price={reference.Price},Day={reference.Day},Auc={reference.AuctionId})";
                    mismatches++;
                }

                totalRefs += n;
                if (n > maxN) maxN = n;
                // crude tie-density signal: dense ranges over many refs => the prefix has ties
                if (n >= 20 && (dayRange <= 4 || priceRange <= 3)) tiePathExercised++;
            }

            // Sanity: the fuzz actually exercised dense-tie inputs at non-trivial sizes.
            tiePathExercised.Should().BeGreaterThan(1000,
                "the heavy-tie generator must have produced many dense-tie cases");
            maxN.Should().BeGreaterThan(300, "the fuzz must cover large reference counts");

            mismatches.Should().Be(0,
                $"partial selection must be bit-exact with the two-full-sort reference over 40000 heavy-tie seeds "
              + $"({totalRefs} refs total, maxN={maxN}). First mismatch: {first}");
        }

        [Test]
        public void EdgeCases_Match_Reference()
        {
            // Empty / guard / single-element / all-equal / size==n / skip at boundaries.
            var cases = new (ReferencePrice[] refs, int size, int skip)[]
            {
                (Array.Empty<ReferencePrice>(), 0, 0),                       // empty -> guard -> default
                (Refs((1, 0, 5)), 1, 0),                                     // single element, skip 0
                (Refs((1, 0, 5)), 1, 1),                                     // skip >= size -> guard -> default
                (Refs((1, 0, 5)), 0, 0),                                     // size 0 -> guard -> default
                (Refs((1,1,5),(2,1,5),(3,1,5),(4,1,5)), 4, 2),              // all Day & Price equal -> idx tiebreak only
                (Refs((1,1,5),(2,2,5),(3,1,5),(4,2,5)), 4, 0),              // Day ties, Price ties interleaved
                (Refs((1,1,10),(2,1,20),(3,1,30)), 3, 0),                    // same Day, distinct Price, skip first
                (Refs((1,1,10),(2,1,20),(3,1,30)), 2, 1),                    // size<n prefix + skip last in prefix
                (Refs((1,3,5),(2,2,5),(3,1,5)), 3, 1),                       // distinct Day same Price (Day desc)
                (Refs((1,1,5),(2,1,5),(3,1,5),(4,1,5),(5,1,5)), 5, 4),      // all equal, skip last
            };

            foreach (var (refs, size, skip) in cases)
            {
                var live = SniperService.CleanItemPriceSelectTarget(refs, size, skip);
                var reference = SniperService.CleanItemPriceSelectTargetReference(refs, size, skip);
                live.Price.Should().Be(reference.Price);
                live.Day.Should().Be(reference.Day);
                live.AuctionId.Should().Be(reference.AuctionId);
            }
        }

        // helper: build a ReferencePrice[] from (auctionId, day, price) tuples.
        private static ReferencePrice[] Refs(params (long auc, short day, long price)[] xs)
        {
            var r = new ReferencePrice[xs.Length];
            for (int i = 0; i < xs.Length; i++)
                r[i] = new ReferencePrice { AuctionId = xs[i].auc, Day = xs[i].day, Price = xs[i].price };
            return r;
        }
    }
}
