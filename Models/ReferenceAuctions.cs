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
                    ? (float)References.Count / (SniperService.GetDay() - price.Day + 1)
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
        [JsonIgnore]
        private ReferencePrice[] _refSnapshot;
        [JsonIgnore]
        private long _refSnapshotVersion = -1; // != initial _refVersion(0) so the first ReferenceSnapshot() builds
        [JsonIgnore]
        private long _refVersion;

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
            if (Volatile.Read(ref _refSnapshotVersion) != currentVersion)
            {
                var rebuilt = References.ToArray(); // FIFO order, matches a fresh ConcurrentQueue enumeration
                // Publish the ARRAY before stamping the version so a concurrent reader that observes the matching
                // version is guaranteed to see the corresponding array (no tear): array write happens-before the
                // version write, and the version write is what gates the fast path below.
                Volatile.Write(ref _refSnapshot, rebuilt);
                Volatile.Write(ref _refSnapshotVersion, currentVersion);
                return rebuilt;
            }
            var snapshot = Volatile.Read(ref _refSnapshot); // read the published reference once (tear-safe)
            return snapshot ?? Array.Empty<ReferencePrice>();
        }
    }

}