using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using Coflnet.Sky.Sniper.Models;
using AwesomeAssertions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R3 Phase 0 / F2 bit-exactness soak for the <see cref="ReferenceAuctions.References"/> cached snapshot
    /// (<c>benchmarks/COMPUTE_FLOOR_SPEC_R3.md</c> §4-F2 + §5 R3-REFS).
    ///
    /// <para>
    /// <b>Why.</b> R3-REFS is the headline lever: it adds an <c>[IgnoreMember]</c> cached <c>ReferencePrice[]</c>
    /// snapshot to <see cref="ReferenceAuctions"/>, atomic-ref published and invalidated by a version bumped at every
    /// <c>References</c> mutation, and converts the per-candidate hot reads (<c>CheckLowerKeyFull</c>,
    /// <c>GetCleanItemPrice</c>, <c>PotentialSnipe*</c>, <c>AddMedianSample</c>, <c>UpdateMedian</c>) from
    /// <c>foreach (var r in bucket.References)</c> (which allocates a <c>ConcurrentQueue&lt;T&gt;</c> enumerator each
    /// call) to a zero-alloc array/span read. The <b>invariant that change must preserve</b> is:
    /// </para>
    ///
    /// <para><b>the snapshot iterates element-for-element, FIFO-identical to a fresh
    /// <c>ConcurrentQueue&lt;ReferencePrice&gt;</c> enumeration, after every mutation.</b></para>
    ///
    /// <para>
    /// A missed mutation site = a stale snapshot = a silent price/tier/snipe drift. This test encodes that invariant
    /// directly: it builds a <see cref="ReferenceAuctions"/>, performs many interleaved Enqueue / TryDequeue /
    /// whole-queue-reassignment operations, and after EACH asserts the snapshot accessor matches a fresh queue
    /// enumeration. Since the snapshot accessor does not exist yet, <see cref="SnapshotForTest"/> currently returns the
    /// live queue contents (so the test is GREEN against current code and pins the reference semantics); when R3-REFS
    /// lands, swap that ONE helper to call the new accessor and this same soak directly validates it.
    /// </para>
    ///
    /// <para>
    /// See <see cref="ReferenceMutationSiteMap"/> for the exhaustive, grep-verified list of every codebase site that
    /// mutates a <c>ReferenceAuctions.References</c> — R3-REFS must bump the snapshot version at each of those.
    /// </para>
    /// </summary>
    public class ReferenceSnapshotTests
    {
        // ============================================================================================================
        // The snapshot accessor seam. TODO(R3-REFS): swap the body to the new zero-alloc snapshot accessor
        // (e.g. `bucket.ReferenceSnapshot()` returning ReferencePrice[] / ReadOnlySpan<ReferencePrice> in FIFO order).
        // Everything below this line then validates the real accessor unchanged. Until then it returns the live queue
        // contents, which IS the reference semantics the snapshot must reproduce.
        // ============================================================================================================
        private static IEnumerable<ReferencePrice> SnapshotForTest(ReferenceAuctions bucket)
        {
            // R3-REFS: now validates the real zero-alloc cached snapshot accessor.
            return bucket.ReferenceSnapshot();
        }

        /// <summary>A fresh ConcurrentQueue enumeration — the FIFO reference the snapshot must match element-for-element.</summary>
        private static List<ReferencePrice> FreshQueueEnumeration(ReferenceAuctions bucket)
        {
            var list = new List<ReferencePrice>();
            foreach (var r in bucket.References) // ConcurrentQueue enumerates in FIFO (enqueue) order
                list.Add(r);
            return list;
        }

        /// <summary>
        /// Asserts the snapshot accessor yields exactly the fresh-queue enumeration: same length, same elements, same
        /// order. This is the R3-REFS gate predicate, checked after every mutation in the soak.
        /// </summary>
        private static void AssertSnapshotMatchesQueue(ReferenceAuctions bucket, string after)
        {
            var snapshot = SnapshotForTest(bucket).ToList();
            var fresh = FreshQueueEnumeration(bucket);
            snapshot.Should().Equal(fresh,
                $"the cached References snapshot must iterate FIFO-identical to a fresh ConcurrentQueue enumeration "
                + $"after [{after}] (gates R3-REFS; a mismatch means a mutation site did not invalidate the snapshot)");
        }

        // ============================================================================================================
        // The soak: many interleaved Enqueue / TryDequeue / whole-queue reassignment ops, assert after each.
        // ============================================================================================================

        [Test]
        public void ReferencesSnapshot_FifoIdentical_UnderInterleavedMutations()
        {
            var bucket = new ReferenceAuctions();
            var rng = new Random(12345); // deterministic
            short day = 100;
            long auctionId = 1;

            AssertSnapshotMatchesQueue(bucket, "init (empty)");

            for (int step = 0; step < 5000; step++)
            {
                int op = rng.Next(0, 100);
                if (op < 55)
                {
                    // Enqueue (the AddAuctionToBucket / UpdateBazaar / Move-target shape) — via the encapsulated method.
                    bucket.EnqueueReference(new ReferencePrice
                    {
                        AuctionId = auctionId++,
                        Price = rng.Next(1, 1_000_000),
                        Day = day,
                        Seller = (short)rng.Next(0, 20000),
                        Buyer = (short)rng.Next(0, 20000),
                    });
                    AssertSnapshotMatchesQueue(bucket, $"Enqueue step={step}");
                }
                else if (op < 80)
                {
                    // TryDequeue (the CapBucketSize / CombineBuckets-trim shape) — via the encapsulated method.
                    bucket.TryDequeueReference(out _);
                    AssertSnapshotMatchesQueue(bucket, $"TryDequeue step={step}");
                }
                else if (op < 92)
                {
                    // Whole-queue reassignment (the AddLookupData / Move-source / Reassign / CombineBuckets shape):
                    // a filtered+reordered new ConcurrentQueue replaces the field entirely — via the encapsulated method.
                    var filtered = bucket.References
                        .Where(r => r.Price > 0)
                        .OrderBy(r => r.Day)
                        .ThenBy(r => r.AuctionId)
                        .ToList();
                    bucket.SetReferences(filtered);
                    AssertSnapshotMatchesQueue(bucket, $"Reassign(filter+order) step={step}");
                }
                else
                {
                    // Reassign to an empty queue (the new-bucket / cleared-bucket shape) — via the encapsulated method.
                    bucket.SetReferences(Array.Empty<ReferencePrice>());
                    AssertSnapshotMatchesQueue(bucket, $"Reassign(empty) step={step}");
                }

                if (step % 500 == 0)
                    day++;
            }

            // a final burst of enqueues then a full drain, asserting throughout
            for (int i = 0; i < 50; i++)
            {
                bucket.EnqueueReference(new ReferencePrice { AuctionId = auctionId++, Price = i + 1, Day = day });
                AssertSnapshotMatchesQueue(bucket, $"final-burst enqueue {i}");
            }
            while (bucket.TryDequeueReference(out _))
                AssertSnapshotMatchesQueue(bucket, "final-drain dequeue");
            AssertSnapshotMatchesQueue(bucket, "fully drained");
        }

        /// <summary>
        /// Reassignment-with-Concat shape from <c>CombineBuckets</c> (existing refs concatenated with incoming,
        /// de-duplicated by AuctionId, ordered by Day): the snapshot must follow the new queue's FIFO order exactly.
        /// </summary>
        [Test]
        public void ReferencesSnapshot_FifoIdentical_AfterConcatDistinctReassign()
        {
            var bucket = new ReferenceAuctions();
            for (short d = 1; d <= 12; d++)
                bucket.EnqueueReference(new ReferencePrice { AuctionId = d, Price = d * 1000, Day = d });
            AssertSnapshotMatchesQueue(bucket, "seeded");

            var incoming = Enumerable.Range(8, 10)
                .Select(i => new ReferencePrice { AuctionId = i, Price = i * 1000, Day = (short)i })
                .ToList();

            bucket.SetReferences(
                bucket.References.Concat(incoming).ToList()
                    .DistinctBy(r => r.AuctionId)
                    .OrderBy(r => r.Day));
            AssertSnapshotMatchesQueue(bucket, "concat+distinct+order reassign");

            // trim from the front (the CombineBuckets / CapBucketSize trailing dequeue loop)
            while (bucket.References.Count > 7 && bucket.TryDequeueReference(out _))
                AssertSnapshotMatchesQueue(bucket, "trim dequeue");
            AssertSnapshotMatchesQueue(bucket, "trimmed");
        }

        /// <summary>
        /// Empty-bucket and single-element edge cases — common FIFO snapshot off-by-ones live here.
        /// </summary>
        [Test]
        public void ReferencesSnapshot_FifoIdentical_EdgeCases()
        {
            var bucket = new ReferenceAuctions();
            AssertSnapshotMatchesQueue(bucket, "empty");

            bucket.EnqueueReference(new ReferencePrice { AuctionId = 1, Price = 1, Day = 1 });
            AssertSnapshotMatchesQueue(bucket, "single");

            bucket.TryDequeueReference(out _);
            AssertSnapshotMatchesQueue(bucket, "emptied-by-dequeue");

            bucket.TryDequeueReference(out _); // dequeue on empty (no-op)
            AssertSnapshotMatchesQueue(bucket, "dequeue-on-empty");

            bucket.SetReferences(new[]
            {
                new ReferencePrice { AuctionId = 7, Price = 7, Day = 7 },
            });
            AssertSnapshotMatchesQueue(bucket, "reassign-single");
        }

        // ============================================================================================================
        // ReferenceMutationSiteMap — EXHAUSTIVE list of every site that mutates a ReferenceAuctions.References.
        //
        // R3-REFS MUST bump the cached-snapshot version at every one of these (Enqueue / TryDequeue / whole-queue
        // `new ConcurrentQueue` reassignment). Derived from `grep -rn "\.References" --include="*.cs"` then filtered to
        // mutating operations (assignment, .Enqueue, .TryDequeue). Read-only iterations (foreach / LINQ / .Count /
        // .TryPeek / .First / .Reverse etc.) are NOT version-bump sites and are deliberately excluded.
        //
        // ---- PRODUCTION (these are the version-bump sites R3-REFS owns) ----
        //
        //   Services/SniperService.cs
        //     L1270  Move()                 oldBucket.References = new ConcurrentQueue<ReferencePrice>(newList);   (reassign, source bucket)
        //     L1273  Move()                 newBucket.References.Enqueue(toChange);                                (enqueue, target bucket)
        //     L1320  AddLookupData()        item.Value.References = new ConcurrentQueue<ReferencePrice>(...);      (reassign, filter+order)
        //     L1336  AddLookupData()        item.Value.References = new ConcurrentQueue<ReferencePrice>(...);      (reassign, filter+order)
        //     L1361  AddLookupData()        tocheck.References = new(...);                                         (reassign, dedup by id)
        //     L1378  CombineBuckets()       existingBucket.References = item.Value.References;                     (reassign, adopt incoming queue ref)
        //     L1381  CombineBuckets()       existingBucket.References = new(existingRef.Concat(...)...);           (reassign, concat+distinct+order)
        //     L1403  CombineBuckets()       existingBucket.References.TryDequeue(out _);                           (dequeue, front-trim loop)
        //     L1490  CapBucketSize()        bucket.References.TryDequeue(out _);                                   (dequeue, size-cap loop)
        //     L1573  AddAuctionToBucket()   bucket.References.Enqueue(reference);                                  (enqueue, the primary ingest path)
        //     L4591  UpdateBazaar()         bucket.References.Enqueue(new() {...});                                (enqueue, bazaar reference)
        //
        //   Controllers/ApiController.cs
        //     L376   Migrate()             newBucket.References.Enqueue(reference);                                (enqueue, migration)
        //     L487   DeleteReferencesBetween()  lookup.Value.References = new(... .Where(day filter));            (reassign, day-range delete)
        //
        //   Services/MinioPersistanceManager.cs
        //     L213   CreateSerializableReferenceAuctionsCopy()  new ReferenceAuctions { References = new ConcurrentQueue<...>(bucket.References) ... }
        //            NOTE: this is an object-INITIALIZER on a freshly-constructed *copy* ReferenceAuctions, not an
        //            in-place mutation of a live bucket. A snapshot is per-instance and this new instance starts with
        //            no cached snapshot, so it is benign for invalidation — but it IS a place the queue is populated,
        //            so R3-REFS should confirm the copy either has no live snapshot or builds a fresh one. Listed for
        //            completeness so the audit is exhaustive.
        //
        // ---- TESTS (mutate References directly; not production version-bump sites, but R3-REFS must keep them
        //            compiling/green and they exercise the same operations) ----
        //
        //   Services/MedianCalc.Tests.cs   L40 (reassign new), L45/L259/L260/L261 (Enqueue)
        //   Services/DropOff.Tests.cs      L1433 (reassign new ConcurrentQueue)
        //   Services/ReferenceSnapshot.Tests.cs (this file)  — the F2 soak's own Enqueue/TryDequeue/reassign ops.
        //
        // If you add/remove a `.References` mutation, update this map AND the soak above.
        // ============================================================================================================
        private const string ReferenceMutationSiteMap = "see comment above";
    }
}
