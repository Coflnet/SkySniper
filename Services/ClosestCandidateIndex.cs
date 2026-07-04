using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// WS-A — contiguous candidate store for the closest-match arg-max scan (<c>FindClosestTo</c>).
    ///
    /// The deployed scan iterated a <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}"/> per
    /// auction, paying ~50–60 ns/bucket of pointer-chasing (dict-node chase + per-bucket heap deref) to set up ~few-ns
    /// of integer scoring. The <c>SoaScanBenchmarks</c> spike proved that walking a flat <see cref="ScoreVec"/>[]
    /// instead is a clean ~2.2× at every bucket count — pure memory-access-pattern, both allocate ~0.
    ///
    /// This holds that flat layout: the priced, non-virtual candidate buckets of one <see cref="PriceLookup"/>, packed
    /// into parallel columns the scan walks sequentially. It is built once per (lookup, pricing-epoch) and reused across
    /// the many <c>FindClosestTo</c> calls between candidate-set changes — the snipe-evaluation hot path never reprices,
    /// so the index is stable across calls and the amortized per-auction cost approaches the pure array scan.
    ///
    /// <b>R6 WS-LOH — chunked columns (off the LOH).</b> Each column used to be ONE flat array. On a hot tag
    /// (N≈3000 priced buckets) the widest column, the <see cref="ScoreVec"/>[] (the struct is 112 B — eleven array
    /// references plus scalars, see <see cref="ClosestScoreKernel.ScoreVec"/>), is ~3000·112 ≈ 336 KB — far over the
    /// ~85 KB Large-Object-Heap threshold — so a fresh index per novel-key (price-dependent membership: the new-bucket
    /// trap forces a real rebuild, the idx-grow append is unsound here) allocated on the LOH and promoted to gen2,
    /// driving the rare gen2 <c>AllocLarge</c> stop-the-world pause tail (Round 10). The fix is to split every column
    /// into <see cref="BlockSize"/>-element blocks so each block stays under the LOH threshold and is born in the SOH
    /// (gen0), dying young instead of promoting. <see cref="BlockSize"/> = 512 keeps the dominant ScoreVec[] block at
    /// 512·112 ≈ 57 KB — comfortably under 85 KB with margin — and every narrower column (8-byte refs/longs → ≤4 KB,
    /// 2-byte shorts → 1 KB per block) far smaller. Only the per-column STORAGE layout changes: membership, ordering,
    /// the epoch/Count/TTL invalidation, the ScoreVec reuse, and the branch-and-bound scan are all unchanged, and the
    /// scan still reads element <c>i</c> as <c>X[i]</c> (an indexer hides the block math; <c>VecRef(i)</c> gives a
    /// <c>ref readonly</c> ScoreVec so <c>in index.VecRef(i)</c> stays zero-copy).
    ///
    /// <b>Immutability + publication.</b> Every field is readonly and the blocks are never mutated after construction;
    /// a fresh instance is published to <see cref="PriceLookup.CandidateIndex"/> by a single atomic reference
    /// assignment (tear-safe), mirroring <see cref="ClosestScoreKernel.ScoreVecCache"/>. Concurrent rebuilds (the risky
    /// finder runs on a background Task) are harmless: both build identical content and one wins the publish. Each
    /// rebuild allocates BRAND-NEW private blocks owned solely by the index it returns (the blocks are never pooled,
    /// shared, or reused across indexes), so there is structurally no use-after-return — a reader holding an older
    /// index keeps walking that index's own immutable blocks, which no later rebuild ever touches.
    ///
    /// <b>Staleness / correctness.</b> The index is rebuilt when any of these no longer hold (see
    /// <c>SniperService.FindClosestTo</c>):
    ///   • <see cref="BuiltEpoch"/> != the service pricing epoch — bumped on every price transition (the new-bucket
    ///     trap: a bucket flips <c>Price</c> 0↔positive without a dict add/remove, so Count alone is insufficient);
    ///   • <see cref="BuiltLookupCount"/> != the live dict Count — catches bucket add/remove;
    ///   • <see cref="BuiltAt"/> older than the vec TTL — matches the per-bucket ScoreVec refresh window so scores stay
    ///     bit-exact with the deployed lazy-rebuild.
    /// The arg-max it produces is bit-exact with the dict scan (same candidate set, same per-candidate score); a
    /// soak parity assert (<c>SniperService.VerifyClosestIndex</c>) cross-checks index vs a fresh filter under churn.
    /// </summary>
    public sealed class ClosestCandidateIndex
    {
        /// <summary>
        /// Per-block element count. Chosen so the widest column — the <see cref="ClosestScoreKernel.ScoreVec"/>[] at
        /// 112 B/element — stays well under the 85,000-byte LOH threshold: 512·112 + 24 ≈ 57 KB/block. All narrower
        /// columns (8-byte refs/longs, 2-byte shorts) are far smaller per block. A power of two so the block/offset
        /// split is a shift/mask. Must not exceed ~759 (the point where ScoreVec[] crosses 85 KB).
        /// </summary>
        public const int BlockSize = 512;
        private const int BlockShift = 9;          // log2(BlockSize)
        private const int BlockMask = BlockSize - 1;

        /// <summary>Columnar score vectors, one per candidate, in build-time enumeration order (chunked).</summary>
        public readonly Chunked<ClosestScoreKernel.ScoreVec> Vecs;
        /// <summary>Candidate keys, parallel to <see cref="Vecs"/> (needed to return the winning entry).</summary>
        public readonly Chunked<AuctionKey> Keys;
        /// <summary>Candidate buckets, parallel to <see cref="Vecs"/> (needed to return the winning entry).</summary>
        public readonly Chunked<ReferenceAuctions> Buckets;
        /// <summary>Per-candidate <see cref="ReferenceAuctions.OldestRef"/>, captured at build time; the call-time age
        /// adjustment (<c>OldestRef &gt; minDay ? 0 : -10</c>) is applied during the scan so the index stays
        /// independent of the per-call <c>minDay</c>.</summary>
        public readonly Chunked<short> OldestRef;
        /// <summary>WS-C — per-candidate <see cref="ClosestScoreKernel.PosCap"/>, query-independent, precomputed once here
        /// so the closest scan's branch-and-bound prefilter (a sound upper bound) costs only a few int ops per candidate
        /// and skips the exact kernel on the majority that can't beat the running best.</summary>
        public readonly Chunked<long> Cap;
        /// <summary>Number of populated entries.</summary>
        public readonly int Count;

        public readonly long BuiltEpoch;
        public readonly int BuiltLookupCount;
        public readonly DateTime BuiltAt;

        public ClosestCandidateIndex(in Chunked<ClosestScoreKernel.ScoreVec> vecs, in Chunked<AuctionKey> keys,
            in Chunked<ReferenceAuctions> buckets, in Chunked<short> oldestRef, in Chunked<long> cap, int count,
            long builtEpoch, int builtLookupCount, DateTime builtAt)
        {
            Vecs = vecs; Keys = keys; Buckets = buckets; OldestRef = oldestRef; Cap = cap; Count = count;
            BuiltEpoch = builtEpoch; BuiltLookupCount = builtLookupCount; BuiltAt = builtAt;
        }

        /// <summary>
        /// Zero-copy <c>ref readonly</c> access to score vector <paramref name="i"/> so the scan can pass
        /// <c>in index.VecRef(i)</c> to the kernel without copying the 112-byte struct. The <c>ref</c> aliases the
        /// element inside this index's own immutable block (never mutated after construction).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref readonly ClosestScoreKernel.ScoreVec VecRef(int i) => ref Vecs.Ref(i);

        /// <summary>
        /// A column split into <see cref="BlockSize"/>-element blocks so no single backing array reaches the ~85 KB LOH
        /// threshold. Element <c>i</c> lives at <c>blocks[i &gt;&gt; BlockShift][i &amp; BlockMask]</c>. A readonly
        /// value-type wrapper over a private jagged array; the blocks are allocated fresh per index and never mutated
        /// after the owning index is constructed, so concurrent readers are safe.
        /// </summary>
        public readonly struct Chunked<T>
        {
            private readonly T[][] _blocks;
            public readonly int Count;

            internal Chunked(T[][] blocks, int count) { _blocks = blocks; Count = count; }

            /// <summary>Indexed element read; keeps the <c>X[i]</c> call-site syntax of the old flat arrays.</summary>
            public T this[int i]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _blocks[i >> BlockShift][i & BlockMask];
            }

            /// <summary><c>ref readonly</c> element access (used by <see cref="VecRef"/> for the zero-copy ScoreVec read).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ref readonly T Ref(int i) => ref _blocks[i >> BlockShift][i & BlockMask];
        }

        /// <summary>
        /// Append-then-freeze builder for one <see cref="Chunked{T}"/> column. <see cref="Add"/> writes into the current
        /// block, allocating a brand-new private block (in the SOH — each block is under the LOH threshold) whenever the
        /// current one fills, so the column grows without a single large allocation and without copying. <see cref="Build"/>
        /// freezes the accumulated blocks into the immutable <see cref="Chunked{T}"/> (trimming the final partial block
        /// so the scan never reads past <see cref="Count"/>). Used only during construction; the produced blocks are
        /// owned solely by the resulting index.
        /// </summary>
        public struct Builder<T>
        {
            private readonly List<T[]> _blocks;
            private T[] _current;
            private int _inBlock;   // elements used in _current
            private int _count;     // total elements appended

            public Builder(int expectedCapacity)
            {
                int blockCount = expectedCapacity <= 0 ? 1 : (expectedCapacity + BlockSize - 1) / BlockSize;
                _blocks = new List<T[]>(blockCount);
                _current = new T[BlockSize];
                _blocks.Add(_current);
                _inBlock = 0;
                _count = 0;
            }

            public readonly int Count => _count;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Add(in T value)
            {
                if (_inBlock == BlockSize)
                {
                    _current = new T[BlockSize]; // fresh SOH block; never copies the prior block's contents
                    _blocks.Add(_current);
                    _inBlock = 0;
                }
                _current[_inBlock++] = value;
                _count++;
            }

            public readonly Chunked<T> Build()
            {
                var arr = _blocks.ToArray();
                // Trim the final partial block to exactly its populated length so a reader can never observe a
                // default-valued tail slot. Full blocks (and the block-pointer array itself) are kept as-is.
                int lastUsed = _count == 0 ? 0 : _count - ((arr.Length - 1) << BlockShift);
                if (lastUsed != BlockSize)
                {
                    var last = arr[arr.Length - 1];
                    if (lastUsed != last.Length)
                        Array.Resize(ref last, lastUsed);
                    arr[arr.Length - 1] = last;
                }
                return new Chunked<T>(arr, _count);
            }
        }
    }
}
