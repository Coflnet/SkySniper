using System;
using System.Collections.Concurrent;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R6/MEMO2 (entry-epoch-stamped recompute memoization) — a per-<see cref="PriceLookup"/>, lifetime-stable,
    /// in-memory memo of the <c>GetCleanItemPrice</c> RECOMPUTE result (the value returned on a persistent-cache MISS
    /// or a <c>force</c> call), keyed by the inputs that determine it and validated by a PER-ENTRY freshness stamp.
    ///
    /// <para><b>Why an entry stamp (the warm fix).</b> R5's clean3 (branch <c>r5-deferred-cleanprice-memo</c>,
    /// <c>dbf920a</c>) memoized the same recompute but with a per-WINDOW invalidation: a new <see cref="CleanItemPriceMemo"/>
    /// (and its whole <see cref="ConcurrentDictionary{TKey,TValue}"/>) was allocated whenever the pricing epoch / dict
    /// Count / Volume / TTL changed. On the WARM path the pricing epoch is bumped on EVERY auction (ingest /
    /// <c>UpdateMedian</c>), so that design re-allocated the entire memo dict every auction — pure overhead where warm
    /// never repeat-recomputes — and regressed warm p50 ~17 µs (the reason clean3 was deferred). This version keeps ONE
    /// dict per lookup for its lifetime (NO per-window realloc) and stamps each ENTRY with the epoch/Count/Volume it was
    /// computed at. A read whose entry stamp != the current epoch/Count/Volume (or is past the TTL) is a MISS — recompute
    /// via the clean2 partial-select and OVERWRITE the entry with the fresh value+stamp. So warm becomes a no-op:
    /// entries are always stale (epoch changed) → recompute the fast clean2 path, but ZERO dict realloc / alloc churn.
    /// Cold/rare keep the intra-window hits: within one stable epoch the 5 per-auction call sites that touch the same
    /// (tag, tier) hit the freshly-stamped entry instead of re-sorting.</para>
    ///
    /// <para><b>Key (what determines the recompute output).</b> For a fixed <see cref="PriceLookup"/> state the
    /// recompute body is a pure function of <c>(tag, key.Key.Tier)</c>: <c>tag</c> drives <c>isPet</c> /
    /// <c>matchRarity</c> / gem-devider / Midas; <c>key.Key.Tier</c> (the UNREDUCED query tier) drives the flatten
    /// filter (<c>minRarity</c> / <c>ownTier</c>); everything else it reads — the matching buckets' references (via
    /// <see cref="ReferenceAuctions.ReferenceSnapshot"/>) and <see cref="PriceLookup.Volume"/> — is a property of the
    /// lookup, not the query key. The query key's Modifiers/Reforge/Enchants do NOT enter the recompute body (they
    /// only pick the REDUCED tier used for the persistent-cache lookup, which is handled by the caller, never here).
    /// So <c>(tag, unreduced-tier)</c> is the exact memo key.</para>
    ///
    /// <para><b>Per-entry freshness (identical triggers to <c>GetOrBuildCandidateIndex</c>).</b> An entry is valid while
    /// its stamp's epoch == the service pricing epoch (bumped on every price transition — <c>UpdateMedian</c> plus every
    /// out-of-band price write), its Count == the live dict Count (bucket add/remove), its Volume == the live
    /// <see cref="PriceLookup.Volume"/>, and it is within the TTL. Reference and Volume mutations flow through
    /// <c>UpdateMedian</c> (which bumps the epoch) on the hot path; the rare bulk-load paths that enqueue without an
    /// immediate bump are bounded by the same TTL the candidate/dominator indexes already accept. These are the proven,
    /// signed-off campaign invalidation triggers — only now applied per-entry instead of per-window.</para>
    ///
    /// <para><b>Force callers.</b> The memo caches the recompute OUTPUT, which is deterministic for fixed inputs within
    /// a window; <c>force</c> currently means "ignore the persistent per-tier cache and recompute", and two recomputes
    /// with a matching stamp are identical, so serving force callers from the memo is bit-exact (verified by the memo
    /// soak, which drives forces under mutation and asserts equality with a fresh recompute every time).</para>
    ///
    /// <para><b>Concurrency.</b> The risky finder runs on a background Task and many auctions for one tag may evaluate
    /// concurrently. The memo dict itself is published to <see cref="PriceLookup.CleanPriceMemo"/> exactly ONCE per
    /// lookup lifetime by a single atomic reference assignment (tear-safe); after that the reference never changes, so
    /// readers never observe a torn or swapped dict. Each entry value is a struct stored/read atomically through the
    /// <see cref="ConcurrentDictionary{TKey,TValue}"/> (its own striping lock makes the struct write/read atomic). A
    /// stale-stamp miss recomputes and OVERWRITES the entry (last-writer-wins); the recompute is deterministic for the
    /// current stamp's inputs, so any concurrent writer for the same (key, stamp) stores the IDENTICAL value and the
    /// race is harmless. Never serialized — a derived in-memory acceleration structure.</para>
    /// </summary>
    public sealed class CleanItemPriceMemo
    {
        /// <summary>
        /// One memoized recompute result plus the freshness stamp it was computed at. A value-type so the
        /// <see cref="ConcurrentDictionary{TKey,TValue}"/> stores it inline (no per-entry boxing/alloc) and a slot
        /// overwrite is a single locked struct copy.
        /// </summary>
        public readonly struct Entry
        {
            public readonly long Value;
            public readonly long Epoch;
            public readonly int Count;
            public readonly float Volume;
            public readonly DateTime BuiltAt;

            public Entry(long value, long epoch, int count, float volume, DateTime builtAt)
            {
                Value = value;
                Epoch = epoch;
                Count = count;
                Volume = volume;
                BuiltAt = builtAt;
            }

            /// <summary>
            /// True while this entry still reflects the live lookup state. Mirrors <c>GetOrBuildCandidateIndex</c>'s
            /// triggers (pricing epoch + live dict Count + TTL) PLUS <see cref="PriceLookup.Volume"/>: the recompute's
            /// <c>size</c> reads <c>lookup.Volume</c>, which <c>PreCalculateVolume</c> sets INSIDE <c>UpdateMedian</c>
            /// AFTER the start-of-method epoch bump, so an entry stamped between that bump and the Volume write would
            /// otherwise serve a value computed against a stale Volume. Bit-comparing the float is exact (Volume is only
            /// ever assigned a freshly computed value, never arithmetically derived from the cached one).
            /// </summary>
            public bool IsFresh(long epoch, int liveCount, float volume, DateTime now, double maxAgeMinutes)
                => Epoch == epoch && Count == liveCount && Volume == volume
                   && BuiltAt > now.AddMinutes(-maxAgeMinutes);
        }

        /// <summary>Memoized recompute results, keyed by (tag, UNREDUCED query tier), each carrying its freshness stamp.
        /// ONE dict per lookup for the lookup's whole lifetime — never re-allocated on an epoch bump (the warm fix). A
        /// stale-stamp read overwrites the slot in place. Sized small with low concurrency: a given lookup only ever
        /// holds the few distinct (tag, tier) its call sites touch (one tag per lookup; a handful of tiers). The default
        /// ConcurrentDictionary ctor over-allocates its bucket table + a per-core lock array (4×ProcessorCount locks);
        /// concurrencyLevel=1 keeps the lock array to a single entry. The value is a small struct and concurrent writers
        /// for a matching stamp store the identical deterministic value, so one striping lock is sufficient for
        /// correctness.</summary>
        public readonly ConcurrentDictionary<(string tag, Tier tier), Entry> Values
            = new(concurrencyLevel: 1, capacity: 8);
    }
}
