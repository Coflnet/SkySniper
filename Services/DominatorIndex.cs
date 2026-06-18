using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>One interned modifier of a <see cref="DomKey"/>. Packed into a single struct array (instead of ~9
    /// parallel arrays) so building one DomKey allocates one array, not nine — the per-auction query build is on the
    /// snipe hot path — and the kernel scans contiguous structs (better locality).</summary>
    public readonly struct DomMod
    {
        public readonly int KeyId;          // interner.Intern(key)
        public readonly int ValId;          // interner.Intern(value)  (== compare via id; interner is 1:1)
        public readonly float Parsed;       // float.TryParse(value, out parsed) — default culture (subtlety A)
        public readonly bool IsNumeric;     // float.TryParse(value, out _) — default culture (subtlety A)
        public readonly bool IsInvKey;      // SniperService.InvertedValueKey.Contains(key)
        public readonly bool IsImpCakeYear; // SniperService.ImportantCakeYears.Contains(value)
        public readonly bool HasSpaceOrComma; // value.Contains(' ') || value.Contains(',')
        public readonly bool IsExpKey;      // key == "exp" (MatchesTierBoost gate, interner-free)
        public readonly string ValRaw;      // raw value string (mode-c substring compare)
        public DomMod(int keyId, int valId, float parsed, bool isNumeric, bool isInvKey, bool isImpCakeYear,
            bool hasSpaceOrComma, bool isExpKey, string valRaw)
        {
            KeyId = keyId; ValId = valId; Parsed = parsed; IsNumeric = isNumeric; IsInvKey = isInvKey;
            IsImpCakeYear = isImpCakeYear; HasSpaceOrComma = hasSpaceOrComma; IsExpKey = isExpKey; ValRaw = valRaw;
        }
    }

    /// <summary>One enchant of a <see cref="DomKey"/> (type + level).</summary>
    public readonly struct DomEnch
    {
        public readonly int Type;  // (int)Enchant.Type
        public readonly byte Lvl;
        public DomEnch(int type, byte lvl) { Type = type; Lvl = lvl; }
    }

    /// <summary>
    /// Round-4 Phase-0 foundation — the interned, columnar <b>dominance kernel</b> + a contiguous candidate store
    /// (<see cref="DominatorIndex"/>) for the snipe-finding path's "does X dominate Y" predicate
    /// (<see cref="SniperService.IsHigherValue"/>).
    ///
    /// <para>
    /// <b>What "dominates" means.</b> <c>Dominates(b, c)</c> is true iff candidate <c>c</c> is same-or-higher on every
    /// axis of base <c>b</c>: tier, count, every base modifier covered with an equal-or-greater value, and every base
    /// enchant present at an equal-or-greater level. It is the de-LINQ'd production predicate
    /// <see cref="SniperService.IsHigherValue"/> (oracle: <see cref="SniperService.IsHigherValueReference"/>),
    /// re-expressed over interned integer ids so the hot loop is integer compares with no per-call string work.
    /// </para>
    ///
    /// <para>
    /// <b>BIT-EXACTNESS is the contract.</b> The kernel is fuzzed (40k pairs) against the LINQ oracle in
    /// <c>IsHigherValueTests.Fuzz_ColumnarKernel_VsReference</c> with zero tolerance. Two subtleties are reproduced by
    /// construction (the fuzz may not surface them): (A) numeric pre-parse uses <c>float.TryParse(value, out _)</c> — the
    /// <b>default-culture</b> overload, matching <see cref="SniperService.IsHigherValue"/> (NOT the InvariantCulture
    /// overload <see cref="ClosestScoreKernel.Build"/> uses); (B) the <c>other.Value == m.Value</c> equality is realized
    /// as an interned-id compare via the single shared <see cref="ClosestScoreKernel.Interner"/> (1:1, so id-equality
    /// ⟺ string-equality), while the rarer substring branch keeps the raw value strings.
    /// </para>
    ///
    /// <para>
    /// <b>Mask prefilter.</b> Each <see cref="DomKey"/> carries a <see cref="DomKey.RequiredMask"/> (the keys/enchants a
    /// base must have covered) and a <see cref="DomKey.ProvidedMask"/> (the keys/enchants a candidate supplies). For
    /// direction base=query, <c>(query.RequiredMask &amp; cand.ProvidedMask) == query.RequiredMask</c> is a SOUND
    /// necessary condition for <c>Dominates(query, cand)</c> (proven in the fuzz: when the oracle says true, the mask
    /// test holds), so it never false-rejects and lets the scan skip the exact kernel on the majority of candidates.
    /// </para>
    /// </summary>
    public readonly struct DomKey
    {
        public readonly DomMod[] Mods;   // interned modifiers, index i over key.Modifiers
        public readonly DomEnch[] Enchs; // enchants, index i over key.Enchants

        // ---- per-key scalars ----
        public readonly int Tier;                // (int)key.Tier
        public readonly int Count;
        public readonly bool FirstModIsNYC;      // Modifiers.Count > 0 && Modifiers[0].Key == "new_years_cake"
        public readonly bool HasPetTierBoost;    // any modifier key=="petItem" && value=="TIER_BOOST"

        /// <summary>OR of <c>1UL &lt;&lt; (id &amp; 63)</c> over (non-inverted-key modifiers) ∪ (enchant types) — what a base
        /// REQUIRES covered. Used when this DomKey is the BASE side of the mask prefilter.</summary>
        public readonly ulong RequiredMask;
        /// <summary>OR of <c>1UL &lt;&lt; (id &amp; 63)</c> over ALL modifiers ∪ enchant types — what a candidate PROVIDES. Used
        /// when this DomKey is the CHECK/coverage side of the mask prefilter.</summary>
        public readonly ulong ProvidedMask;

        public DomKey(DomMod[] mods, DomEnch[] enchs, int tier, int count, bool firstModIsNyc, bool hasPetTierBoost,
            ulong requiredMask, ulong providedMask)
        {
            Mods = mods; Enchs = enchs;
            Tier = tier; Count = count; FirstModIsNYC = firstModIsNyc; HasPetTierBoost = hasPetTierBoost;
            RequiredMask = requiredMask; ProvidedMask = providedMask;
        }
    }

    /// <summary>
    /// Immutable holder for a bucket's cached <see cref="DomKey"/>, co-located on the bucket so index rebuilds reuse it
    /// instead of re-deriving it. The DomKey is a pure function of the bucket's (immutable) <see cref="AuctionKey"/> +
    /// the process-stable interner/InvertedValueKey/ImportantCakeYears, so unlike <see cref="ClosestScoreKernel.ScoreVecCache"/>
    /// it never needs a TTL — built once, valid forever. A reference assignment is atomic (tear-safe); concurrent builds
    /// produce identical content and one wins the publish.
    /// </summary>
    public sealed class DomKeyBox
    {
        public readonly DomKey Value;
        public DomKeyBox(DomKey value) { Value = value; }
    }

    public sealed class DominatorIndex
    {
        /// <summary>Builds one key's interned columnar <see cref="DomKey"/>. All DomKeys compared together MUST be built
        /// with the same <paramref name="interner"/> instance (shared id space — see the interner contract).</summary>
        public static DomKey BuildDomKey(AuctionKey key, ClosestScoreKernel.Interner interner)
        {
            var mods = key.Modifiers;
            int mc = mods?.Count ?? 0;
            var domMods = new DomMod[mc];
            ulong required = 0, provided = 0;
            bool firstNyc = mc > 0 && mods[0].Key == "new_years_cake";
            bool hasPtb = false;
            for (int i = 0; i < mc; i++)
            {
                var k = mods[i].Key;
                var v = mods[i].Value;
                int keyId = interner.Intern(k);
                // SUBTLETY A: default-culture overload, matching IsHigherValue's float.TryParse(value, out var x).
                bool num = float.TryParse(v, out var parsed);
                bool inv = SniperService.InvertedValueKey.Contains(k);
                bool impCake = SniperService.ImportantCakeYears.Contains(v);
                bool hasSC = v != null && (v.Contains(' ') || v.Contains(','));
                bool isExp = k == "exp";
                domMods[i] = new DomMod(keyId, interner.Intern(v), num ? parsed : 0f, num, inv, impCake, hasSC, isExp, v);
                ulong bit = 1UL << (keyId & 63);
                provided |= bit;
                if (!inv) required |= bit; // RequiredMask: only keys whose absence cannot be excused by the inverted-key rule
                if (!hasPtb && k == SniperService.PetItemKey && v == SniperService.TierBoostShorthand) hasPtb = true;
            }

            var enchs = key.Enchants;
            int ec = enchs?.Count ?? 0;
            var domEnchs = new DomEnch[ec];
            for (int i = 0; i < ec; i++)
            {
                int t = (int)enchs[i].Type;
                domEnchs[i] = new DomEnch(t, enchs[i].Lvl);
                ulong bit = 1UL << (t & 63);
                required |= bit;
                provided |= bit;
            }

            return new DomKey(domMods, domEnchs, (int)key.Tier, key.Count, firstNyc, hasPtb, required, provided);
        }

        /// <summary>
        /// Bit-exact with <c>SniperService.IsHigherValue(tag, baseKey=b, checkKey=c)</c>:
        /// true iff <paramref name="c"/> dominates <paramref name="b"/>. <paramref name="tagIsPetSpirit"/> is
        /// <c>tag == "PET_SPIRIT"</c> (the only way the tag enters the predicate).
        /// </summary>
        public static bool Dominates(in DomKey b, in DomKey c, bool tagIsPetSpirit)
        {
            // 1. tier
            if (b.Tier > c.Tier)
                return false;
            // 2. PET_SPIRIT legendary rule
            if (tagIsPetSpirit && c.Tier == (int)Coflnet.Sky.Core.Tier.LEGENDARY)
                return false;
            // 3. count
            if (b.Count > c.Count)
                return false;

            // 4. modifiers — every base modifier covered, or excused by the inverted-key-absent exception.
            var bMods = b.Mods;
            var cMods = c.Mods;
            for (int i = 0; i < bMods.Length; i++)
            {
                ref readonly var bm = ref bMods[i];
                bool tierBoostOk = MatchesTierBoost(in b, in c, in bm); // constant per i (mirrors IsHigherValue folding it inside the j-loop)
                bool covered = false;
                if (tierBoostOk)
                {
                    int keyId = bm.KeyId;
                    for (int j = 0; j < cMods.Length; j++)
                    {
                        if (cMods[j].KeyId != keyId)
                            continue;
                        if (ValueOk(in bm, in b, in cMods[j]))
                        {
                            covered = true;
                            break;
                        }
                    }
                }
                // original: covered || (InvertedValueKey.Contains(m.Key) && !check has any mod with key m.Key)
                if (!covered && !(bm.IsInvKey && !CheckHasKey(in c, bm.KeyId)))
                    return false;
            }

            // 5. enchants — every base enchant present in check at >= level.
            var bEnchs = b.Enchs;
            var cEnchs = c.Enchs;
            for (int i = 0; i < bEnchs.Length; i++)
            {
                int type = bEnchs[i].Type;
                byte lvl = bEnchs[i].Lvl;
                bool found = false;
                for (int j = 0; j < cEnchs.Length; j++)
                {
                    if (cEnchs[j].Type == type && cEnchs[j].Lvl >= lvl) { found = true; break; }
                }
                if (!found)
                    return false;
            }
            return true;
        }

        /// <summary>
        /// valueOk(base mod, check mod) — OR of three modes, with base/check sides exactly as in IsHigherValue
        /// (there: other=check, m=base):
        ///  (a) other.Value == m.Value                       -> cm.ValId == bm.ValId (interned 1:1)
        ///  (b) numeric (check parses) && cake guard && (base parses) && inverted-aware compare on the CHECK side
        ///  (c) other.Value.Contains(m.Value) && !checkNumeric && check value has space/comma
        /// </summary>
        private static bool ValueOk(in DomMod bm, in DomKey b, in DomMod cm)
        {
            // (a) equality via interned ids
            if (cm.ValId == bm.ValId)
                return true;
            // (b) numeric compare. NOTE the sides: numeric guard + cake guard read the CHECK value; inverted-ness reads
            // the CHECK key (InvertedValueKey.Contains(other.Key)); the cake first-mod guard reads the BASE first mod.
            if (cm.IsNumeric
                && (!b.FirstModIsNYC || !cm.IsImpCakeYear)
                && bm.IsNumeric
                && (cm.IsInvKey ? cm.Parsed < bm.Parsed : cm.Parsed > bm.Parsed))
                return true;
            // (c) substring containment on the raw CHECK value (rare branch, survivors only).
            if (cm.HasSpaceOrComma && !cm.IsNumeric && cm.ValRaw.Contains(bm.ValRaw))
                return true;
            return false;
        }

        /// <summary>
        /// MatchesTierBoostOrLowerTier(base, check, base mod) — only constrains the "exp" modifier:
        /// if the base mod is not "exp" -> true; else if check has no petItem=TIER_BOOST -> true; else base must itself
        /// carry petItem=TIER_BOOST OR be of strictly lower tier.
        /// </summary>
        private static bool MatchesTierBoost(in DomKey b, in DomKey c, in DomMod bm)
        {
            if (!bm.IsExpKey)
                return true;
            if (!c.HasPetTierBoost)
                return true;
            return b.HasPetTierBoost || b.Tier < c.Tier;
        }

        /// <summary>Does check have ANY modifier whose key id == <paramref name="keyId"/>? (the inverted-key-absent exception)</summary>
        private static bool CheckHasKey(in DomKey c, int keyId)
        {
            var m = c.Mods;
            for (int j = 0; j < m.Length; j++)
                if (m[j].KeyId == keyId) return true;
            return false;
        }

        // ============================== contiguous candidate store ==============================

        /// <summary>Candidate keys, parallel to the other arrays (returned to the caller on a hit).</summary>
        public readonly AuctionKey[] Keys;
        /// <summary>Candidate buckets, parallel to <see cref="Keys"/>. The finders read all MUTABLE bucket state
        /// (Price / Lbin / References) LIVE off these, so no price/ref columns are cached (they would be stale).</summary>
        public readonly ReferenceAuctions[] Buckets;
        /// <summary>Per-candidate interned columnar form, parallel to <see cref="Keys"/>.</summary>
        public readonly DomKey[] Doms;
        /// <summary>Mirror of <c>Doms[i].RequiredMask</c> into a flat array (cache-friendly prefilter — used when the
        /// candidate is the BASE side: <c>(RequiredMask[i] &amp; query.ProvidedMask) == RequiredMask[i]</c>).</summary>
        public readonly ulong[] RequiredMask;
        /// <summary>Mirror of <c>Doms[i].ProvidedMask</c> into a flat array (used when the candidate is the CHECK side:
        /// <c>(query.RequiredMask &amp; ProvidedMask[i]) == query.RequiredMask</c>).</summary>
        public readonly ulong[] ProvidedMask;
        /// <summary>Per-candidate (int)reforge (key-derived, immutable). The one mutable-looking value the finders read
        /// off the index rather than live, because reforge is part of the immutable key.</summary>
        public readonly int[] Reforge;
        /// <summary>Number of populated entries (arrays may be over-allocated; only the first Count are valid).</summary>
        public readonly int Count;

        public readonly long BuiltEpoch;
        public readonly int BuiltLookupCount;
        public readonly DateTime BuiltAt;

        public DominatorIndex(AuctionKey[] keys, ReferenceAuctions[] buckets, DomKey[] doms,
            ulong[] requiredMask, ulong[] providedMask, int[] reforge, int count,
            long builtEpoch, int builtLookupCount, DateTime builtAt)
        {
            Keys = keys; Buckets = buckets; Doms = doms;
            RequiredMask = requiredMask; ProvidedMask = providedMask;
            Reforge = reforge; Count = count;
            BuiltEpoch = builtEpoch; BuiltLookupCount = builtLookupCount; BuiltAt = builtAt;
        }

        /// <summary>
        /// Packs the qualifying candidate buckets of <paramref name="l"/> into parallel arrays. The qualifying set is
        /// the BROAD superset the four finders share — just the non-null guards
        /// <c>m.Key != null &amp;&amp; m.Value?.References != null</c>. Unlike the closest search, the dominance finders
        /// (GetLbinCap, PotentialSnipeHigherValueScan, CheckLowerKeyFull, the median lower-value scan) do NOT exclude
        /// <c>Price==0</c> or <c>virtual</c> buckets (e.g. PotentialSnipeHigherValueScan's keyCount/percentile/divided-ref
        /// outputs depend on those buckets being present), so the index is a superset and each finder applies its OWN
        /// secondary predicate (Price&gt;0 / Lbin!=0 / RefCount / recency / reforge) over the buckets during its scan.
        /// Only IMMUTABLE key-derived data is cached (DomKey/masks/Reforge/Keys/Buckets); mutable price/ref state is read
        /// live. Over-allocated to <c>l.Count</c> and grown if the dict grows mid-enumeration so a new candidate is never
        /// dropped.
        /// </summary>
        public static DominatorIndex Build(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l,
            ClosestScoreKernel.Interner interner, long epoch, DateTime builtAt)
        {
            int cap = l.Count;
            var keys = new AuctionKey[cap];
            var buckets = new ReferenceAuctions[cap];
            var doms = new DomKey[cap];
            var required = new ulong[cap];
            var provided = new ulong[cap];
            var reforge = new int[cap];
            int n = 0;
            foreach (var m in l)
            {
                // BROAD superset: non-null guards only. Each finder applies its own Price>0 / virtual / RefCount / recency
                // / reforge secondary predicate over the buckets during its scan (see the summary above).
                if (m.Key == null || m.Value?.References == null)
                    continue;
                if (n >= keys.Length) // dict grew during enumeration -> grow so a new candidate is never dropped
                {
                    int g = n + 8;
                    Array.Resize(ref keys, g); Array.Resize(ref buckets, g); Array.Resize(ref doms, g);
                    Array.Resize(ref required, g); Array.Resize(ref provided, g); Array.Resize(ref reforge, g);
                }
                // Reuse the bucket's cached DomKey (pure-key-derived, never stale) so a rebuild does not re-derive it;
                // only the flat column arrays below are re-allocated. Atomic-ref publish (tear-safe).
                var box = m.Value.DomKeyCache;
                DomKey dom;
                if (box != null)
                    dom = box.Value;
                else
                {
                    dom = BuildDomKey(m.Key, interner);
                    m.Value.DomKeyCache = new DomKeyBox(dom);
                }
                keys[n] = m.Key; buckets[n] = m.Value; doms[n] = dom;
                required[n] = dom.RequiredMask; provided[n] = dom.ProvidedMask;
                reforge[n] = (int)m.Key.Reforge;
                n++;
            }
            return new DominatorIndex(keys, buckets, doms, required, provided, reforge, n, epoch, l.Count, builtAt);
        }
    }

    /// <summary>
    /// R5c (idx-grow) — an <b>append-amortized growable backing store</b> for the <see cref="DominatorIndex"/> columns.
    ///
    /// <para><b>Why.</b> On the RARE path a novel key is ADDED every auction, so the lookup dict's <c>Count</c> changes
    /// every auction and the old <see cref="DominatorIndex.Build"/> re-allocated six fresh O(N) column arrays per call →
    /// LOH → gen2 <c>AllocLarge</c> stop-the-world pauses (the #1 rare-pause driver). This store replaces the per-auction
    /// full rebuild with an <b>amortized-O(1) APPEND</b> of only the newly-added buckets' rows into doubling-capacity
    /// backing arrays, and falls back to a full rebuild only when a bucket was REMOVED/REPLACED or the TTL elapsed.</para>
    ///
    /// <para><b>The crucial property this exploits.</b> The DominatorIndex's membership is <b>price-INDEPENDENT</b> — it is
    /// exactly the non-null buckets (<c>m.Key != null &amp;&amp; m.Value?.References != null</c>), and every finder reads all
    /// MUTABLE price/ref state LIVE off <c>Buckets[i]</c>. So membership changes ONLY when the dict ADDS or REMOVES a
    /// bucket — never on a price flip. Adds dominate the rare path, and an add is an append.</para>
    ///
    /// <para><b>Bit-exactness contract.</b> An appended snapshot must be byte-for-byte identical to what
    /// <see cref="DominatorIndex.Build"/> would produce at that instant. A <c>Build</c> at append-time captures, for each
    /// qualifying key, <c>(Key, Buckets[i]=current dict value, DomKey, masks, Reforge)</c>; the DomKey/masks/Reforge are
    /// pure functions of the IMMUTABLE key, so the ONLY value that depends on the live dict is <c>Buckets[i]</c> (the
    /// bucket object the finder reads live state off). The append therefore: (1) VERIFIES every prior member is still
    /// present (else it cannot be a pure add → full rebuild); (2) appends only the new buckets' rows.</para>
    ///
    /// <para><b>Membership by OBJECT identity (zero per-bucket hashing).</b> Each bucket carries a
    /// <see cref="ReferenceAuctions.DomRowStamp"/> = the GENERATION of the snapshot it currently belongs to, written when
    /// its row is added. The append scan classifies each qualifying bucket by a single FIELD read of that stamp — a prior
    /// member iff the stamp equals the store's current generation — instead of an allocating
    /// <see cref="AuctionKey.GetHashCode"/> dictionary probe (the <c>ReadOnlyCollection</c> enumerators that hashing
    /// allocates were the per-auction cost of a key-map design). Generations come from a PROCESS-GLOBAL monotonic counter,
    /// so a stamp from one store can never equal a different store's generation — robust even if the load path adopts a
    /// bucket object between lookups. The membership soundness gate is then exact: <c>seenPrior == priorCount</c> means
    /// every prior member is still in the dict. A bucket object lives in the dict under exactly one key, and distinct dict
    /// entries are distinct objects, so OBJECT-identity membership ≡ KEY-identity membership. A same-key bucket-OBJECT
    /// replacement removes the OLD object from the dict, so the old row's stamp is no longer seen ⇒
    /// <c>seenPrior &lt; priorCount</c> ⇒ rebuild (which captures the new object) — never a stale/duplicate row. A full
    /// rebuild takes a fresh generation, invalidating every old stamp at once.</para>
    ///
    /// <para><b>Tear-safety.</b> Readers are lock-free: each holds one published <see cref="DominatorIndex"/> reference,
    /// reads its <c>Count</c>, and indexes <c>[0, Count)</c> into its arrays. Append/grow + all stamp reads/writes are
    /// serialized under <see cref="_gate"/> (only the rebuild path locks; finders never do, and the stamp is never in the
    /// published view). A NO-GROW append writes the new rows into the shared backing arrays at <c>[priorCount, newCount)</c>
    /// BEFORE the atomic publish of the larger-Count view, so an old reader (Count=priorCount) never touches them. A GROW
    /// append allocates new doubled arrays, copies, writes the new rows, and publishes a view over the NEW arrays — old
    /// readers keep the old (smaller) arrays untouched.</para>
    /// </summary>
    public sealed class DominatorIndexStore
    {
        // Process-global monotonic generation source. Every full rebuild (of ANY store) takes a fresh, never-reused
        // generation, so a bucket's DomRowStamp from one store can NEVER equal a different store's current generation —
        // making the OBJECT-identity membership test robust even if the load path adopts a bucket between lookups. Starts
        // at 0, so the first generation handed out is 1; the default DomRowStamp(0) therefore matches no live generation.
        private static long _globalGeneration;
        private static long NextGeneration() => System.Threading.Interlocked.Increment(ref _globalGeneration);

        private readonly object _gate = new();

        // Growable backing columns (capacity may exceed Count; only [0,_count) are valid).
        private AuctionKey[] _keys = Array.Empty<AuctionKey>();
        private ReferenceAuctions[] _buckets = Array.Empty<ReferenceAuctions>();
        private DomKey[] _doms = Array.Empty<DomKey>();
        private ulong[] _required = Array.Empty<ulong>();
        private ulong[] _provided = Array.Empty<ulong>();
        private int[] _reforge = Array.Empty<int>();
        private int _count;

        // This store's current snapshot generation (the value its member buckets' DomRowStamp carry). Replaced by a fresh
        // global generation on every full rebuild, which invalidates all old stamps at once. 0 until the first rebuild.
        private long _generation;

        // Reused scratch for the append's new-bucket batch (the dict entry carries both the key and the bucket). Reused
        // across appends (cleared, not re-allocated) so the steady rare-path append (typically +1 bucket) is alloc-free
        // once warmed. Touched only under _gate.
        private readonly List<KeyValuePair<AuctionKey, ReferenceAuctions>> _scratchNew = new();

        // The last published immutable view (returned on a cache HIT). Published by atomic ref store; read lock-free.
        private DominatorIndex _published;

        /// <summary>
        /// Returns the up-to-date immutable <see cref="DominatorIndex"/> view of <paramref name="l"/>, taking the cheapest
        /// correct path: a cache HIT (Count unchanged + within TTL), an amortized APPEND (live Count grew and every prior
        /// member is still present), or a full REBUILD (a member was removed/de-qualified, the live Count shrank, or the
        /// TTL elapsed). Identical output to <see cref="DominatorIndex.Build"/> in all cases.
        /// </summary>
        public DominatorIndex GetOrBuild(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l,
            ClosestScoreKernel.Interner interner, long epoch, DateTime now, DateTime ttlCutoff)
        {
            // Lock-free cache-HIT fast path (mirrors the old GetOrBuildDominatorIndex hit guard: Count + TTL, epoch-free).
            int liveCount = l.Count;
            var pub = _published; // single atomic read
            if (pub != null && pub.BuiltLookupCount == liveCount && pub.BuiltAt > ttlCutoff)
                return pub;

            // Slow path: serialize all builders/appenders for this lookup so in-place appends never race each other.
            lock (_gate)
            {
                liveCount = l.Count; // re-read under the lock
                pub = _published;
                if (pub != null && pub.BuiltLookupCount == liveCount && pub.BuiltAt > ttlCutoff)
                    return pub; // another thread published while we waited

                // APPEND is admissible only when the dict grew (more entries) and we have a prior snapshot to extend.
                // (liveCount < prior dict count -> a remove happened -> rebuild; liveCount == prior -> only same-count
                //  replacements/price flips, which the Count-keyed HIT already covers exactly as the old code did.)
                if (pub != null && liveCount > pub.BuiltLookupCount && pub.BuiltAt > ttlCutoff && TryAppend(l, interner, epoch, now, liveCount))
                    return _published;

                // Fall back to a full rebuild (cold start, removal, TTL, or an append that could not prove soundness).
                return Rebuild(l, interner, epoch, now, liveCount);
            }
        }

        /// <summary>
        /// Full rebuild into the growable store (the <see cref="DominatorIndex.Build"/> fallback). Bumps the generation
        /// (invalidating all old stamps), repacks the current qualifying membership into freshly-sized arrays, stamps each
        /// row, and publishes a fresh view. Always correct. Caller holds <see cref="_gate"/>.
        /// </summary>
        private DominatorIndex Rebuild(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l,
            ClosestScoreKernel.Interner interner, long epoch, DateTime now, int liveCount)
        {
            long gen = _generation = NextGeneration(); // fresh globally-unique generation; invalidates all prior stamps
            int cap = liveCount > 0 ? liveCount : 0;
            var keys = new AuctionKey[cap];
            var buckets = new ReferenceAuctions[cap];
            var doms = new DomKey[cap];
            var required = new ulong[cap];
            var provided = new ulong[cap];
            var reforge = new int[cap];
            int n = 0;
            foreach (var m in l)
            {
                if (m.Key == null || m.Value?.References == null)
                    continue;
                if (n >= keys.Length) // dict grew during enumeration -> grow so a new candidate is never dropped
                {
                    int g = n + 8;
                    Array.Resize(ref keys, g); Array.Resize(ref buckets, g); Array.Resize(ref doms, g);
                    Array.Resize(ref required, g); Array.Resize(ref provided, g); Array.Resize(ref reforge, g);
                }
                var dom = GetDomKey(m.Value, m.Key, interner);
                keys[n] = m.Key; buckets[n] = m.Value; doms[n] = dom;
                required[n] = dom.RequiredMask; provided[n] = dom.ProvidedMask;
                reforge[n] = (int)m.Key.Reforge;
                m.Value.DomRowStamp = gen; // stamp this bucket OBJECT as a member of the new snapshot generation
                n++;
            }
            _keys = keys; _buckets = buckets; _doms = doms; _required = required; _provided = provided; _reforge = reforge;
            _count = n;
            var view = new DominatorIndex(keys, buckets, doms, required, provided, reforge, n, epoch, liveCount, now);
            _published = view; // atomic publish
            return view;
        }

        /// <summary>
        /// Amortized append: tries to extend the prior snapshot with ONLY the newly-added buckets' rows. Returns true and
        /// publishes the grown view iff append is provably bit-exact with a full rebuild (every prior member still present);
        /// returns false (no state changed) to signal the caller to rebuild. Caller holds <see cref="_gate"/>.
        /// </summary>
        private bool TryAppend(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l,
            ClosestScoreKernel.Interner interner, long epoch, DateTime now, int liveCount)
        {
            int priorCount = _count;
            long gen = _generation;
            // PASS 1 (read-only, NO store mutation so we can bail to rebuild cleanly): classify each qualifying bucket by a
            // single FIELD read of its stamp — prior member iff stamp's generation == this store's generation — and stage
            // the new ones in the reused scratch list. Zero per-bucket hashing (the key-map design's GetHashCode/enumerator
            // alloc is gone). One dict enumeration (unavoidable to discover the new keys).
            var newRows = _scratchNew;
            newRows.Clear();
            int seenPrior = 0;
            foreach (var m in l)
            {
                if (m.Key == null || m.Value?.References == null)
                    continue;
                if (m.Value.DomRowStamp == gen) // prior member of THIS store's current snapshot (object identity)
                    seenPrior++;
                else
                    newRows.Add(m);
            }

            // SOUNDNESS GATE: append is bit-exact with a rebuild iff every prior member is still present. If a prior member
            // vanished (seenPrior < priorCount) the change is NOT a pure add (a remove + an add can net to a larger Count)
            // — the prior columns would hold a stale/removed row — so rebuild. (seenPrior can't exceed priorCount: each of
            // the priorCount stamped objects appears once in the dict.)
            if (seenPrior != priorCount)
            {
                newRows.Clear();
                return false;
            }
            if (newRows.Count == 0)
            {
                // No genuinely-new qualifying key (the Count delta was all non-qualifying entries). Rebuild to produce the
                // canonical snapshot stamped with the new dict Count (appending nothing would mis-stamp BuiltLookupCount).
                return false;
            }

            // PASS 2 (commit): the change is a proven pure add. Grow once, then write + stamp the new rows.
            int newN = priorCount + newRows.Count;
            EnsureCapacity(newN); // grows the backing arrays (doubling + copy) only when needed; else writes in place.
            var keys = _keys; var buckets = _buckets; var doms = _doms;
            var required = _required; var provided = _provided; var reforge = _reforge;
            int n = priorCount;
            for (int i = 0; i < newRows.Count; i++)
            {
                var m = newRows[i];
                var dom = GetDomKey(m.Value, m.Key, interner);
                keys[n] = m.Key; buckets[n] = m.Value; doms[n] = dom;
                required[n] = dom.RequiredMask; provided[n] = dom.ProvidedMask;
                reforge[n] = (int)m.Key.Reforge;
                m.Value.DomRowStamp = gen; // stamp the appended bucket OBJECT as a member of the current generation
                n++;
            }
            newRows.Clear();
            _count = n;
            // Publish a view over the (possibly newly-grown) backing arrays with the larger Count. The new rows at
            // [priorCount,n) were written above (happens-before this atomic publish), so any reader that reads this new
            // reference sees them; readers holding the OLD view (Count=priorCount) never index past priorCount.
            var view = new DominatorIndex(keys, buckets, doms, required, provided, reforge, n, epoch, liveCount, now);
            _published = view; // atomic publish
            return true;
        }

        /// <summary>Reuse the bucket's cached pure-key-derived DomKey (never stale); derive+cache it on first sight.</summary>
        private static DomKey GetDomKey(ReferenceAuctions bucket, AuctionKey key, ClosestScoreKernel.Interner interner)
        {
            var box = bucket.DomKeyCache;
            if (box != null)
                return box.Value;
            var dom = DominatorIndex.BuildDomKey(key, interner);
            bucket.DomKeyCache = new DomKeyBox(dom);
            return dom;
        }

        /// <summary>
        /// Ensures the backing columns hold at least <paramref name="needed"/> rows, growing by DOUBLING (allocate new
        /// arrays + copy the live prefix) when the current capacity is insufficient — so a published view that aliases the
        /// OLD arrays is never mutated past its own Count, and the amortized per-append copy is O(1). When capacity already
        /// suffices, the existing arrays are kept and new rows are written in place at [Count, needed) (no allocation).
        /// </summary>
        private void EnsureCapacity(int needed)
        {
            int cap = _keys.Length;
            if (cap >= needed)
                return; // capacity suffices -> write new rows in place into the shared arrays (no realloc)
            int newCap = cap == 0 ? Math.Max(needed, 4) : cap;
            while (newCap < needed)
                newCap *= 2;
            var keys = new AuctionKey[newCap];
            var buckets = new ReferenceAuctions[newCap];
            var doms = new DomKey[newCap];
            var required = new ulong[newCap];
            var provided = new ulong[newCap];
            var reforge = new int[newCap];
            int copy = _count;
            Array.Copy(_keys, keys, copy);
            Array.Copy(_buckets, buckets, copy);
            Array.Copy(_doms, doms, copy);
            Array.Copy(_required, required, copy);
            Array.Copy(_provided, provided, copy);
            Array.Copy(_reforge, reforge, copy);
            _keys = keys; _buckets = buckets; _doms = doms; _required = required; _provided = provided; _reforge = reforge;
        }
    }
}
