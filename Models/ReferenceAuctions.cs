using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Coflnet.Sky.Sniper.Services;
using MessagePack;
using Newtonsoft.Json;

namespace Coflnet.Sky.Sniper.Models
{
    [MessagePackObject]
    public class ReferenceAuctions
    {
        [Key(0)]
        public long Price;
        [Key(1)]
        public ConcurrentQueue<ReferencePrice> References = new ConcurrentQueue<ReferencePrice>();
        [Key(2)]
        [Obsolete("replaed by Lbins ")]
        [JsonIgnore]
        public ReferencePrice LastLbin;
        /// <summary>
        /// Second lowest bin, used if the lowest bin got sold
        /// </summary>
        [IgnoreMember]
        [Obsolete("replaed by Lbins ", true)]
        [JsonIgnore]
        public ReferencePrice SecondLbin;
        /// <summary>
        /// The day of the oldest used reference for <see cref="Price"/>
        /// </summary>
        [Key(4)]
        public short OldestRef;
        [Key(5)]
        public List<ReferencePrice> Lbins = new();
        [Key(6)]
        public short HitsSinceCalculating = 0;
        [IgnoreMember]
        public short StonksHits = 0;
        [Key(7)]
        public byte Volatility = 0;
        [IgnoreMember]
        [JsonIgnore]
        public ReferencePrice Lbin => Lbins?.FirstOrDefault() ?? default;

        [IgnoreMember]
        [JsonIgnore]
        public short DeduplicatedReferenceCount;
        [IgnoreMember]
        public long RiskyEstimate;
        [IgnoreMember]
        public long TimeToSell;
        /// <summary>
        /// In-memory cache of this bucket's interned columnar score vector for the closest-match kernel, co-located so
        /// the closest search reads it without a per-candidate dictionary lookup. Held as an immutable reference so
        /// concurrent searches read a consistent vector (the risky finder runs on a background Task). The vector is a
        /// pure function of (tag, key, cross-item prices) — independent of this bucket's own price/references — so it is
        /// rebuilt on the same TTL cadence as the value caches it derives from (see SniperService.GetBucketScoreVec).
        /// Never persisted.
        /// </summary>
        [IgnoreMember]
        [JsonIgnore]
        public ClosestScoreKernel.ScoreVecCache ScoreVecCache;

        /// <summary>
        /// R4 — in-memory cache of this bucket's interned columnar <see cref="DomKey"/> for the dominance kernel.
        /// Co-located so DominatorIndex rebuilds reuse it instead of re-deriving ~11 arrays per key. The DomKey is a pure
        /// function of this bucket's immutable key, so (unlike <see cref="ScoreVecCache"/>) it needs no TTL — built once,
        /// valid for the bucket's lifetime. Never persisted.
        /// </summary>
        [IgnoreMember]
        [JsonIgnore]
        public DomKeyBox DomKeyCache;

        /// <summary>
        /// R5c (idx-grow) — the bucket's membership stamp = the GENERATION of the <see cref="DominatorIndexStore"/>
        /// snapshot this bucket currently belongs to. Lets the append-amortized rebuild classify each bucket as a prior
        /// member (stamp == the store's current generation) vs newly added by OBJECT identity — a single field read
        /// instead of an allocating <see cref="AuctionKey.GetHashCode"/> dictionary probe per bucket per auction.
        /// Generations are drawn from a PROCESS-GLOBAL monotonic counter, so a stamp from one store can never collide with
        /// another store's generation — robust even if a bucket object is adopted between lookups by the load path.
        /// Default 0 matches no generation (the counter starts at 1). Read/written ONLY under the owning store's lock
        /// (never by the lock-free finders / the published view), so it needs no atomics. Never persisted.
        /// </summary>
        [IgnoreMember]
        [JsonIgnore]
        public long DomRowStamp;

        private float _volume;
        [IgnoreMember]
        public float Volume
        {
            get
            {
                if (_volume != 0)
                    return _volume;
                return (float)(References.TryPeek(out ReferencePrice price)
                    ? (float)ReferenceCount / (SniperService.GetDay() - price.Day + 1)
                    : 0);
            }
            set => _volume = value;
        }

        // ===== R3-REFS: zero-alloc cached snapshot of References =====
        // The hot snipe/valuation path reads References (FIFO) many times per auction, but ingest mutates it rarely.
        // Iterating a ConcurrentQueue allocates a heap enumerator per scan (the largest residual warm allocator). This
        // is the WS-A cached-snapshot / version-invalidation pattern applied to References: every mutation goes through
        // the Enqueue/TryDequeue/Set methods below and bumps _refVersion (Interlocked); ReferenceSnapshot() rebuilds a
        // ReferencePrice[] (FIFO order via ConcurrentQueue.ToArray) only when stale and atomic-publishes it. Readers
        // iterate the array (zero-alloc, FIFO-identical to a fresh queue enumeration). Never persisted: private fields
        // are not MessagePack-serialized (no AllowPrivate), and [JsonIgnore] keeps them out of JSON too.
        // Snapshot + its version are published as ONE immutable holder through a single reference store. Publishing
        // them as two independent fields had a lost-update interleaving: rebuilder A (old version) could overwrite
        // rebuilder B's newer array and then B's version stamp lands on top — a stale snapshot certified as current
        // until the NEXT mutation (potentially hours for a quiet bucket).
        private sealed class RefSnapshot
        {
            public readonly long Version;
            public readonly ReferencePrice[] Items;
            public RefSnapshot(long version, ReferencePrice[] items) { Version = version; Items = items; }
        }
        [JsonIgnore]
        private RefSnapshot _refSnapshot;
        [JsonIgnore]
        private long _refVersion;

        // Version-keyed count cache, same pattern as the snapshot below. ConcurrentQueue.Count takes the queue's
        // cross-segment lock and walks segments — the profiler shows it (plus its Monitor.Enter) as one of the top
        // remaining hot-path costs, because the snipe path reads References.Count many times per auction while the
        // queue mutates rarely. Keyed off _refVersion, so it is exact whenever all mutations go through the
        // Enqueue/TryDequeue/Set methods (the same contract the snapshot already relies on), and it self-heals on
        // first read after deserialization (version 0 != -1 sentinel forces a real Count).
        [JsonIgnore]
        private int _refCountCache;
        [JsonIgnore]
        private long _refCountVersion = -1;

        /// <summary>Cached <c>References.Count</c> (see field comment). Use instead of References.Count on hot paths.</summary>
        [IgnoreMember]
        [JsonIgnore]
        public int ReferenceCount
        {
            get
            {
                var currentVersion = Volatile.Read(ref _refVersion);
                if (Volatile.Read(ref _refCountVersion) == currentVersion)
                    return _refCountCache;
                var queue = References;
                if (queue == null)
                    return 0; // defensive vs deserialized null; don't cache so a later SetReferences is seen
                var count = queue.Count;
                _refCountCache = count;
                Volatile.Write(ref _refCountVersion, currentVersion);
                return count;
            }
        }

        /// <summary>Enqueue a reference and invalidate the cached snapshot. The primary ingest path.</summary>
        public void EnqueueReference(ReferencePrice r)
        {
            References.Enqueue(r);
            Interlocked.Increment(ref _refVersion);
        }

        /// <summary>TryDequeue a reference and invalidate the cached snapshot. The size-cap / front-trim path.</summary>
        public bool TryDequeueReference(out ReferencePrice r)
        {
            var ok = References.TryDequeue(out r);
            Interlocked.Increment(ref _refVersion);
            return ok;
        }

        /// <summary>Replace the whole queue (filter / reorder / dedup / adopt) and invalidate the cached snapshot.</summary>
        public void SetReferences(IEnumerable<ReferencePrice> items)
        {
            References = new ConcurrentQueue<ReferencePrice>(items);
            Interlocked.Increment(ref _refVersion);
        }

        /// <summary>
        /// Zero-alloc, FIFO-ordered view of <see cref="References"/>. Returns a cached <c>ReferencePrice[]</c> that is
        /// rebuilt (and atomic-published) only when a mutation has bumped the version since the last build. Tear-safe:
        /// the snapshot reference is read once. FIFO order is preserved (ConcurrentQueue + ToArray enumerate in
        /// enqueue order), so iterating the result is element-for-element identical to <c>foreach (var r in References)</c>.
        /// </summary>
        public ReferencePrice[] ReferenceSnapshot()
        {
            var currentVersion = Volatile.Read(ref _refVersion);
            var snapshot = Volatile.Read(ref _refSnapshot); // one holder read: version+array can never desynchronize
            if (snapshot != null && snapshot.Version == currentVersion)
                return snapshot.Items;
            var rebuilt = References.ToArray(); // FIFO order, matches a fresh ConcurrentQueue enumeration
            var holder = new RefSnapshot(currentVersion, rebuilt);
            // Only install if nobody published a NEWER snapshot meanwhile; losing the race is fine — we still
            // return our own consistent rebuild for this call.
            var seen = Volatile.Read(ref _refSnapshot);
            if (seen == null || seen.Version <= currentVersion)
                Interlocked.CompareExchange(ref _refSnapshot, holder, seen);
            return rebuilt;
        }
    }

}