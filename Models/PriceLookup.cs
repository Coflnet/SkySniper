using System.Collections.Concurrent;
using System.Collections.Generic;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;
using MessagePack;

namespace Coflnet.Sky.Sniper.Models
{
    [MessagePackObject]
    public class PriceLookup
    {
        [Key(0)]
        public ConcurrentDictionary<AuctionKey, ReferenceAuctions> Lookup = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>(1, 3);
        /// <summary>
        /// What category this item would be in the AH
        /// </summary>
        [Key(1)]
        public Category Category;
        [Key(2)]
        public float Volume;

        [Key(3)]
        public Dictionary<short, long> CleanPricePerDay = new();
        /// <summary>
        /// Clean key (like dropped, usually with highest volume)
        /// </summary>
        [Key(4)]
        public AuctionKey CleanKey;
        [Key(5)]
        public Dictionary<Tier, long> CleanPricePerTier = new();
        [Key(7)]
        public bool HasMultipleRarities;

        /// <summary>
        /// WS-A contiguous candidate store for the closest-match arg-max scan, lazily (re)built by
        /// <c>SniperService.FindClosestTo</c> and published by atomic reference assignment. Not serialized — it is a
        /// derived, in-memory acceleration structure rebuilt on demand from <see cref="Lookup"/>.
        /// </summary>
        [IgnoreMember]
        public ClosestCandidateIndex CandidateIndex;

        /// <summary>
        /// R4 contiguous candidate store for the dominance finders (the snipe higher/lower-value scans), built by
        /// <see cref="DominatorIndex.Build"/> and published by atomic reference assignment. Not serialized — it is a
        /// derived, in-memory acceleration structure rebuilt on demand from <see cref="Lookup"/>.
        /// </summary>
        [IgnoreMember]
        public DominatorIndex DominatorIndex;

        /// <summary>
        /// R5c (idx-grow) — the append-amortized growable backing store for <see cref="DominatorIndex"/>. Owns the
        /// doubling-capacity column arrays + key->row map and serves the cheapest correct path (cache hit / amortized
        /// append of newly-added buckets / full rebuild). Lazily created on first use. Not serialized — a derived,
        /// in-memory acceleration structure rebuilt on demand from <see cref="Lookup"/>.
        /// </summary>
        [IgnoreMember]
        public DominatorIndexStore DominatorIndexStore;

        /// <summary>
        /// R6/MEMO2 — per-lookup, lifetime-stable, entry-epoch-stamped in-memory memo of the <c>GetCleanItemPrice</c>
        /// recompute result, keyed by (tag, unreduced query tier). Cuts the cold/rare RECOMPUTE frequency (the #1
        /// cold+rare CPU cost): the 5 per-auction call sites that miss the persistent <see cref="CleanPricePerTier"/>
        /// cache (or force) re-sort the same (tag, tier) repeatedly; within a stable pricing epoch they hit the
        /// freshly-stamped entry instead. ONE dict per lookup — never re-allocated on an epoch bump (so warm, which
        /// bumps the epoch every auction, is a zero-alloc no-op: entries are simply stale → recompute the fast clean2
        /// path). Created+published exactly once by atomic reference assignment. Not serialized — a derived, in-memory
        /// acceleration structure (see <see cref="CleanItemPriceMemo"/>).
        /// </summary>
        [IgnoreMember]
        public CleanItemPriceMemo CleanPriceMemo;
    }

}