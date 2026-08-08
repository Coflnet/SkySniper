using System.Collections.Concurrent;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R7/WS-A — the entry-epoch-stamped <c>(contentHash, pricingEpoch)</c> memo for
    /// <c>SniperService.DetailedKeyFromSaveAuction</c>. A service-wide, lifetime-stable, in-memory memo of the WHOLE
    /// parse output (<see cref="KeyWithValueBreakdown"/> = the capped <see cref="AuctionKey"/> + the value breakdown),
    /// keyed by an FNV-1a content hash of the immutable item content and validated by a per-ENTRY pricing-epoch stamp.
    ///
    /// <para><b>Why an entry stamp (the warm fix / clean3 lesson).</b> The parse key SHAPE is market-dependent —
    /// <c>CapKeyLength</c> drops low-value modifiers/enchants by a live market threshold — so the memo MUST key on
    /// <c>(contentHash, pricingEpoch)</c>, not content alone (a pure-content hit would be wrong after a price move).
    /// The naive realization (realloc the dict whenever the epoch changes) regresses warm, where <c>pricingEpoch</c>
    /// is bumped on ~every auction (ingest / <c>UpdateMedian</c>): it would re-allocate the whole dict every auction.
    /// This memo keeps ONE dict for the service lifetime and stamps each ENTRY with the epoch it was computed at. A read
    /// whose entry epoch != the current pricing epoch is a MISS — re-parse and OVERWRITE the entry with the fresh
    /// value+stamp. So warm degrades to a no-op (entries always stale → re-parse) with ZERO dict realloc; cold/rare keep
    /// the intra-epoch cross-auction hits.</para>
    ///
    /// <para><b>Bit-exactness (the pricingEpoch rail).</b> A hit only occurs within one epoch, where <c>CapKeyLength</c>
    /// would compute the identical threshold/drops, so the cached <see cref="KeyWithValueBreakdown"/> is byte-for-byte
    /// what a fresh parse would produce — proven by the <c>SNIPER_VERIFY_PARSE_MEMO</c> guard (every call recomputes
    /// fresh and asserts equality). The cached value is READ-ONLY downstream: the dispatch loop only calls
    /// <see cref="KeyWithValueBreakdown.GetReduced"/> (which allocates fresh reduced keys and never mutates its source)
    /// and reads <c>ValueBreakdown</c>; <c>CapKeyLength</c>'s in-place modifier mutation happens INSIDE the memoized
    /// parse body, before <c>Constructkey</c> snapshots the lists — so the stored entry is immutable post-store.</para>
    ///
    /// <para><b>Content hash (what enters it).</b> The immutable item content only: every <c>FlatenedNBT</c> entry minus
    /// a CONSERVATIVE per-instance exclusion set (only keys containing <c>"uid"</c>/<c>"uuid"</c> — the codebase's own
    /// proven per-instance marker), the filtered <c>Enchantments</c> (Type+Lvl), <c>Tier</c>, <c>Reforge</c>,
    /// <c>Count</c>, <c>Tag</c>, AND <c>HighestBidAmount</c> (which feeds <c>CapKeyLength.percentDiff</c> and so the
    /// substracted value — a per-instance value that genuinely affects the output, so included for correctness:
    /// over-miss is safe, under-miss is a correctness bug). When in doubt a key is INCLUDED in the hash → over-miss.</para>
    ///
    /// <para><b>Concurrency.</b> Auctions for different tags evaluate concurrently on the shard workers. The dict is a
    /// <see cref="ConcurrentDictionary{TKey,TValue}"/>; each entry is a small struct stored/read atomically through its
    /// striping lock. A stale-stamp miss re-parses and OVERWRITES the entry (last-writer-wins); within one epoch the
    /// parse is deterministic for fixed content, so a concurrent writer for the same (hash, epoch) stores an equivalent
    /// value and the race is harmless. Never serialized — a derived in-memory acceleration structure.</para>
    /// </summary>
    public sealed class ParseMemo
    {
        /// <summary>One memoized parse output plus the pricing epoch it was computed at. A value-type so the
        /// <see cref="ConcurrentDictionary{TKey,TValue}"/> stores it inline (no per-entry boxing) and a slot overwrite
        /// is a single locked struct copy. <see cref="Value"/> is the (reference-typed) <see cref="KeyWithValueBreakdown"/>
        /// parse output, read-only downstream.</summary>
        public readonly struct Entry
        {
            public readonly KeyWithValueBreakdown Value;
            public readonly long Epoch;

            public Entry(KeyWithValueBreakdown value, long epoch)
            {
                Value = value;
                Epoch = epoch;
            }
        }

        /// <summary>Memoized parse outputs, keyed by the FNV-1a content hash, each carrying its pricing-epoch stamp.
        /// ONE dict for the service's whole lifetime — never re-allocated on an epoch bump (the warm fix). A stale-stamp
        /// read overwrites the slot in place.</summary>
        public readonly ConcurrentDictionary<ulong, Entry> Values = new();
    }
}
