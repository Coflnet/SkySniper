using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Coflnet.Sky.Sniper.Models;
using AwesomeAssertions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// WS-AMM (R6) bit-exactness oracle for <see cref="SniperService.ApplyAntiMarketManipulation"/>.
    ///
    /// <para>
    /// <b>Why.</b> AMM is the #1 residual write-path allocator (~93% of write-path alloc after R5). De-LINQ'ing it is
    /// HIGH RISK: it builds the deduped reference list via multi-level <c>GroupBy</c>/<c>OrderBy</c> with stable
    /// tie-breaks AND a side-effecting counter (<c>buyerCounter++</c>) inside a <i>deferred</i> <c>GroupBy</c> key
    /// selector. LINQ's deferred + lazy execution means that counter fires in a specific order and a specific number
    /// of times that a naive hand-rewrite silently changes. AMM's output (the returned <c>List&lt;ReferencePrice&gt;</c>)
    /// feeds order-sensitive downstream code (UpdateMedian's <c>OrderBy(Price).Take</c>, <c>Take(29)</c>,
    /// <c>OrderByDescending(Day).Take(9)</c>, ...), so the exact sequence — not just the set — is observable.
    /// </para>
    ///
    /// <para>
    /// <b>What it gates.</b> A candidate de-LINQ must produce a <c>List&lt;ReferencePrice&gt;</c> element-for-element
    /// identical (every field, in order) to the verbatim LINQ reference
    /// (<see cref="SniperService.ApplyAntiMarketManipulationReference"/>) over ≥40k randomized buckets with HEAVY ties:
    /// dense Day/Price/Seller/Buyer ranges (incl. many <c>Buyer==0</c> to stress the deferred-counter cardinality, and
    /// buyer/seller collisions to stress the back-and-forth and "is-manipulating" detectors). This file's
    /// <see cref="ParityFuzz_DeLinq_Matches_Reference"/> is the prove-or-defer acceptance bar; the
    /// <c>UpdateMedian</c> golden is the second gate.
    /// </para>
    ///
    /// <para>
    /// AMM does not mutate the bucket (it reads <c>bucket.ReferenceSnapshot()</c> and returns a fresh list), so the
    /// returned list IS the complete observable output and is what we compare.
    /// </para>
    /// </summary>
    public class ApplyAntiMarketManipulationFuzzTests
    {
        // ≥40k as mandated; bump for a heavier soak.
        private const int FuzzIterations = 60_000;

        private static ReferenceAuctions BucketFromRefs(IEnumerable<ReferencePrice> refs)
        {
            var b = new ReferenceAuctions();
            b.SetReferences(refs);
            return b;
        }

        /// <summary>
        /// Build one randomized reference set with HEAVY ties across SEVERAL adversarial profiles. The goal is to
        /// stress every GroupBy bucketing, every OrderBy stable tie-break, the back-and-forth ToLookup, the
        /// "is-manipulating" majority detector, and — critically — the number/order of <c>buyerCounter++</c> increments
        /// AND the case where a counter value COLLIDES with a real non-zero Buyer key (proven to merge groups in LINQ).
        /// WorkingSize/References-length boundaries are also exercised.
        /// <para>Production note: Seller/Buyer come from <c>Convert.ToInt16(hex4,16)</c> so they span the FULL signed
        /// short range (including NEGATIVES, e.g. "FFFF"=-1, "8000"=-32768) and <c>Buyer</c> can be set from
        /// <c>(short)Ticks</c>; the shift key <c>Buyer&lt;&lt;(15+Seller)</c> therefore exercises negative/large shift
        /// counts (int shift masks count to &amp;31). Profile B covers this.</para>
        /// </summary>
        private static List<ReferencePrice> RandomRefs(Random rnd)
        {
            // Mix of sizes: tiny (boundary), around references.Length/2 and /3 thresholds, and > WorkingSize(60).
            int n = rnd.Next(0, 4) switch
            {
                0 => rnd.Next(0, 6),    // tiny: empty / near-empty boundaries
                1 => rnd.Next(6, 20),   // small
                2 => rnd.Next(20, 65),  // around WorkingSize
                _ => rnd.Next(65, 130), // larger than WorkingSize (Take(60) truncation)
            };
            int profile = rnd.Next(0, 4);
            var list = new List<ReferencePrice>(n);
            for (int i = 0; i < n; i++)
            {
                short seller, buyer;
                long price;
                short day;
                switch (profile)
                {
                    case 0: // dense small non-negative: heavy ties + counter cardinality
                        seller = (short)rnd.Next(0, 6);
                        buyer = rnd.Next(0, 3) == 0 ? (short)0 : (short)rnd.Next(0, 6);
                        price = rnd.Next(0, 8);
                        day = (short)rnd.Next(0, 5);
                        break;
                    case 1: // full signed-short range incl. negatives: shift-mask + negative-key stress
                        seller = (short)rnd.Next(short.MinValue, short.MaxValue + 1);
                        buyer = rnd.Next(0, 4) == 0 ? (short)0 : (short)rnd.Next(short.MinValue, short.MaxValue + 1);
                        price = (long)rnd.Next(0, 1_000_000) * (rnd.Next(0, 5) == 0 ? 1000 : 1);
                        day = (short)rnd.Next(-3, 40);
                        break;
                    case 2: // COUNTER-vs-BUYER COLLISION construction: lots of Buyer==0 so the counter climbs through
                            // the small positive buyer keys 1..8 that also appear -> forces group merges.
                        seller = (short)rnd.Next(0, 4);
                        buyer = rnd.Next(0, 2) == 0 ? (short)0 : (short)rnd.Next(0, 10);
                        price = rnd.Next(0, 6);
                        day = (short)rnd.Next(0, 4);
                        break;
                    default: // wide price/day, moderate id space: sort paths with large values + many distinct groups
                        seller = (short)rnd.Next(-20, 20);
                        buyer = rnd.Next(0, 5) == 0 ? (short)0 : (short)rnd.Next(-20, 20);
                        price = (long)rnd.Next() * (rnd.Next(0, 2) == 0 ? -1 : 1) + rnd.Next(0, 50);
                        day = (short)rnd.Next(-5, 50);
                        break;
                }
                list.Add(new ReferencePrice
                {
                    // AuctionId unique per element so the comparison can detect any element identity swap.
                    AuctionId = (i + 1) + ((long)rnd.Next(0, 3) << 40),
                    Price = price,
                    Day = day,
                    Seller = seller,
                    Buyer = buyer,
                    SellTime = (short)rnd.Next(0, 3),
                });
            }
            return list;
        }

        private static string Serialize(List<ReferencePrice> list)
        {
            var sb = new StringBuilder();
            sb.Append('[').Append(list.Count).Append(']');
            foreach (var r in list)
                sb.Append(r.AuctionId).Append(':').Append(r.Price).Append(':').Append(r.Day)
                  .Append(':').Append(r.Seller).Append(':').Append(r.Buyer).Append(':').Append(r.SellTime).Append('|');
            return sb.ToString();
        }

        /// <summary>
        /// Sanity guard: the reference compared against itself must be identical for every fuzz case. If THIS ever
        /// fails, the fuzz harness (RNG / bucket construction / serialization) is non-deterministic, not the SUT —
        /// fix the harness before trusting <see cref="ParityFuzz_DeLinq_Matches_Reference"/>.
        /// </summary>
        [Test]
        public void ParityFuzz_Reference_Is_Deterministic()
        {
            var rnd = new Random(0xA11C0DE);
            for (int iter = 0; iter < FuzzIterations; iter++)
            {
                var refs = RandomRefs(rnd);
                var a = SniperService.ApplyAntiMarketManipulationReference(BucketFromRefs(refs));
                var b = SniperService.ApplyAntiMarketManipulationReference(BucketFromRefs(refs));
                if (Serialize(a) != Serialize(b))
                    Assert.Fail($"Reference non-deterministic at iter {iter}\nInput={Serialize(refs)}\nA={Serialize(a)}\nB={Serialize(b)}");
            }
            Assert.Pass($"reference deterministic over {FuzzIterations} cases");
        }

        /// <summary>
        /// THE prove-or-defer gate. Runs the production AMM entry point (the de-LINQ once shipped; the reference until
        /// then) against the verbatim LINQ reference over ≥40k heavy-tie buckets and asserts the returned list is
        /// element-for-element identical (every field, in order). 0 mismatches is the bar to ship; any mismatch ⇒ defer.
        /// </summary>
        [Test]
        public void ParityFuzz_DeLinq_Matches_Reference()
        {
            var rnd = new Random(unchecked((int)0x5C0FFEE5));
            int mismatches = 0;
            string firstFail = null;
            for (int iter = 0; iter < FuzzIterations; iter++)
            {
                var refs = RandomRefs(rnd);
                // Two independent buckets so the cached snapshot can't be shared / accidentally aliased.
                var expected = SniperService.ApplyAntiMarketManipulationReference(BucketFromRefs(refs));
                var actual = SniperService.ApplyAntiMarketManipulation(BucketFromRefs(refs));
                var se = Serialize(expected);
                var sa = Serialize(actual);
                if (se != sa)
                {
                    mismatches++;
                    firstFail ??= $"iter {iter}\nInput   ={Serialize(refs)}\nExpected={se}\nActual  ={sa}";
                }
            }
            mismatches.Should().Be(0,
                $"de-LINQ ApplyAntiMarketManipulation must be bit-exact vs the LINQ reference over {FuzzIterations} " +
                $"heavy-tie buckets (WS-AMM prove-or-defer gate). First mismatch:\n{firstFail}");
            TestContext.Out.WriteLine($"[AMM-FUZZ] 0 mismatches over {FuzzIterations} heavy-tie buckets");
        }
    }
}
