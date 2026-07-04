using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// Data-oriented, allocation-free re-implementation of the closest-match similarity score
    /// (<see cref="AuctionKey"/>.SimilarityByMarketPrice / CompareValues), operating on interned, columnar
    /// "score vectors" instead of <c>List&lt;RankElem&gt;</c> + LINQ.
    ///
    /// Goal: <b>bit-exact</b> with the production scorer so the closest search can be swapped wholesale. The accumulator
    /// is <c>long</c> integer arithmetic; the single floating-point term (numeric-modifier difference) is reproduced with
    /// the identical sequence — parse to <c>float</c>, multiply by the (int) importance factor in <c>float</c>, widen to
    /// <c>double</c>, truncate to <c>long</c> — and only consulted when the matched element's value is 0, exactly as the
    /// original. Strings are interned 1:1, so integer-id matching yields the same matches as the original string compares.
    ///
    /// This is the spike kernel; the columnar bucket store + SIMD pruning build on top of <see cref="Score"/>.
    /// </summary>
    public static class ClosestScoreKernel
    {
        // ============================ CANONICAL ID-SPACE CONTRACT (Phase 0 / F1) ============================
        // This is THE single source of integer ids for the whole snipe path. Every ScoreVec compared against another
        // MUST be built with the same Interner instance, or matches silently break. Production holds exactly one
        // instance per SniperService (`scoreInterner`); tests use their own isolated instance.
        //
        // What is interned here (string -> int, 1:1, stable for the interner's lifetime):
        //   - modifier KEYS   (AuctionKey.Modifiers[*].Key)   -> ScoreVec.ModKeyId
        //   - modifier VALUES (AuctionKey.Modifiers[*].Value)  -> ScoreVec.ModValId
        // Both draw from ONE monotonic counter (`_next`), so a modKey id and a modVal id can collide numerically. That
        // is SAFE and intentional: the kernel only ever compares ModKeyId-vs-ModKeyId and ModValId-vs-ModValId (never
        // a key id against a value id), so a shared counter never causes a false match. A future SIMD/columnar packing
        // (WS-C) that wants width-segregated ids (e.g. modKey->u16, modVal->u32) must change this counter scheme AND
        // re-prove parity against the oracle — do not assume the spaces are independent today.
        //
        // What is NOT interned (already stable small ints, fed to ScoreVec raw — keep it that way):
        //   - enchant TYPE  -> raw `(int)EnchantmentType` enum value (dense, < 2^16, stable across the process)
        //   - tier / count  -> raw key scalars
        // The four well-known ids below (PetItemKey, TIER_BOOST, candyUsed, exp) are interned eagerly in the ctor so
        // hot-path branch checks are integer compares, not string compares.
        //
        // Thread-safety: the closest search runs on a background Task and can race vec builds at update time, so
        // Intern() must be safe under concurrency. GetOrAdd + Interlocked.Increment guarantee unique, stable ids; under
        // contention the GetOrAdd factory may run twice and discard a counter value (harmless gap — uniqueness and
        // stability are preserved). Ids are append-only and never reused, so a vec built earlier stays valid forever.
        // ====================================================================================================
        public sealed class Interner
        {
            private readonly ConcurrentDictionary<string, int> _ids = new(StringComparer.Ordinal);
            private int _next = -1;
            public readonly int PetItemKeyId;
            public readonly int TierBoostValId;
            public readonly int CandyUsedKeyId;
            public readonly int ExpKeyId;

            public Interner()
            {
                PetItemKeyId = Intern(SniperService.PetItemKey);
                TierBoostValId = Intern(SniperService.TierBoostShorthand);
                CandyUsedKeyId = Intern("candyUsed");
                ExpKeyId = Intern("exp");
            }

            public int Intern(string s)
            {
                if (s == null) return -1;
                return _ids.GetOrAdd(s, _ => Interlocked.Increment(ref _next));
            }
        }

        /// <summary>Columnar, interned form of one key's priced breakdown (the <c>List&lt;RankElem&gt;</c>) + key scalars.</summary>
        public readonly struct ScoreVec
        {
            // enchants
            public readonly int[] EnchType;   // enum value
            public readonly byte[] EnchLvl;
            public readonly long[] EnchValue;
            // modifiers
            public readonly int[] ModKeyId;
            public readonly int[] ModValId;
            public readonly long[] ModValue;
            public readonly bool[] ModIsNumeric;
            public readonly float[] ModParsed;
            public readonly int[] ModImportance;
            public readonly bool[] ModIsCandyUsed;
            public readonly bool[] ModIsPetTierBoost; // key==petItem && value==TIER_BOOST
            // reforge (first reforge-bearing RankElem, if any)
            public readonly bool HasReforge;
            public readonly int Reforge;       // enum value
            public readonly long ReforgeValue;
            // key scalars
            public readonly int Tier;
            public readonly int Count;
            public readonly bool HasExpModifier; // key.Modifiers.Any(m => m.Key == "exp")

            public ScoreVec(int[] enchType, byte[] enchLvl, long[] enchValue,
                int[] modKeyId, int[] modValId, long[] modValue, bool[] modIsNumeric, float[] modParsed,
                int[] modImportance, bool[] modIsCandyUsed, bool[] modIsPetTierBoost,
                bool hasReforge, int reforge, long reforgeValue, int tier, int count, bool hasExpModifier)
            {
                EnchType = enchType; EnchLvl = enchLvl; EnchValue = enchValue;
                ModKeyId = modKeyId; ModValId = modValId; ModValue = modValue;
                ModIsNumeric = modIsNumeric; ModParsed = modParsed; ModImportance = modImportance;
                ModIsCandyUsed = modIsCandyUsed; ModIsPetTierBoost = modIsPetTierBoost;
                HasReforge = hasReforge; Reforge = reforge; ReforgeValue = reforgeValue;
                Tier = tier; Count = count; HasExpModifier = hasExpModifier;
            }
        }

        /// <summary>
        /// Immutable holder for a bucket's cached <see cref="ScoreVec"/> + build time. Stored as a reference on the
        /// bucket so concurrent closest searches (the risky finder runs on a background Task) read a whole, consistent
        /// vector — a reference assignment is atomic, whereas writing the multi-field struct in place is not.
        /// </summary>
        public sealed class ScoreVecCache
        {
            public readonly ScoreVec Vec;
            public readonly DateTime At;
            /// <summary>The item tag the vec was built for. ComparisonValue is tag-dependent and one PriceLookup can
            /// serve several raw tags (HYPERION group, STARRED_*), so a tag-blind bucket cache would serve one tag's
            /// values to another tag's queries for the whole TTL.</summary>
            public readonly string Tag;
            public ScoreVecCache(ScoreVec vec, DateTime at, string tag) { Vec = vec; At = at; Tag = tag; }
        }

        private static int ImportanceFactor(string key)
        {
            if (SniperService.Increadable.Contains(key)) return 100 * 1_000_000;
            if (SniperService.VeryValuable.Contains(key)) return 10 * 1_000_000;
            return 1 * 1_000_000;
        }

        /// <summary>Builds a <see cref="ScoreVec"/> from a key and its priced breakdown (the <c>cv</c> list from ComparisonValue).</summary>
        public static ScoreVec Build(AuctionKey key, List<SniperService.RankElem> cv, Interner interner)
        {
            var enchType = new List<int>(); var enchLvl = new List<byte>(); var enchValue = new List<long>();
            var modKeyId = new List<int>(); var modValId = new List<int>(); var modValue = new List<long>();
            var modIsNumeric = new List<bool>(); var modParsed = new List<float>(); var modImportance = new List<int>();
            var modIsCandy = new List<bool>(); var modIsPtb = new List<bool>();
            bool hasReforge = false; int reforge = 0; long reforgeValue = 0;

            foreach (var e in cv)
            {
                if (e.Enchant.Lvl != default) // enchant RankElem
                {
                    enchType.Add((int)e.Enchant.Type); enchLvl.Add((byte)e.Enchant.Lvl); enchValue.Add(e.Value);
                }
                else if (e.Modifier.Key != default) // modifier RankElem
                {
                    var k = e.Modifier.Key; var v = e.Modifier.Value;
                    modKeyId.Add(interner.Intern(k));
                    modValId.Add(interner.Intern(v));
                    modValue.Add(e.Value);
                    // Identical overload to CompareValues' float.TryParse(value, InvariantCulture, out _) so the parsed
                    // float is bit-identical.
                    bool num = float.TryParse(v, CultureInfo.InvariantCulture, out var parsed);
                    modIsNumeric.Add(num); modParsed.Add(num ? parsed : 0f);
                    modImportance.Add(ImportanceFactor(k));
                    modIsCandy.Add(k == "candyUsed");
                    modIsPtb.Add(k == SniperService.PetItemKey && v == SniperService.TierBoostShorthand);
                }
                else if (e.Reforge != default && !hasReforge) // first reforge-bearing RankElem
                {
                    hasReforge = true; reforge = (int)e.Reforge; reforgeValue = e.Value;
                }
            }

            bool hasExp = false;
            if (key.Modifiers != null)
                foreach (var m in key.Modifiers)
                    if (m.Key == "exp") { hasExp = true; break; }

            return new ScoreVec(
                enchType.ToArray(), enchLvl.ToArray(), enchValue.ToArray(),
                modKeyId.ToArray(), modValId.ToArray(), modValue.ToArray(), modIsNumeric.ToArray(), modParsed.ToArray(),
                modImportance.ToArray(), modIsCandy.ToArray(), modIsPtb.ToArray(),
                hasReforge, reforge, reforgeValue, (int)key.Tier, key.Count, hasExp);
        }

        // long GetValueOrDefault(value, defaultVal) — mirrors RankElem.GetValueOrDefault(double).
        private static long Gvod(long value, double defaultVal) => value == 0 ? (long)defaultVal : value;

        /// <summary>
        /// WS-C — a SOUND positive cap on the additive contribution of <c>CompareValues(*, self=v)</c>: the sum of the
        /// largest positive value each of <paramref name="v"/>'s own elements could add. Every additive term in
        /// CompareValues reads <c>self</c>'s value and each self element matches at most once (match is by unique
        /// enchant-type / mod-key), so this bounds the positive part; all subtractive terms are dropped. Used to build a
        /// branch-and-bound upper bound for the closest scan — <c>UB(i) = PosCap(query) + PosCap(cand) − exact
        /// tier/exp/count penalties ≥ exact Score·100</c>, so pruning a candidate whose UB can't beat the running best is
        /// bit-exact (it could never have won). Query-independent per candidate → precompute once at index-build time.
        /// </summary>
        public static long PosCap(in ScoreVec v)
        {
            long cap = 0;
            for (int e = 0; e < v.EnchValue.Length; e++)
                if (v.EnchValue[e] > 0) cap += v.EnchValue[e];
            for (int m = 0; m < v.ModValue.Length; m++)
            {
                long g = Gvod(v.ModValue[m], v.ModImportance[m]);
                if (g > 0) cap += g;
            }
            if (v.ReforgeValue > 0) cap += v.ReforgeValue;
            return cap;
        }

        /// <summary>
        /// Bit-exact equivalent of <c>query.Similarity(candidateKey, service, candidateCv, queryCv)</c>.
        /// <paramref name="a"/> is the query vec (the <c>this</c>/self side), <paramref name="b"/> the candidate (the
        /// <c>key</c>/keyvalue side) — matching the production call where keyvalue=candidate, self=query.
        /// </summary>
        public static int Score(in ScoreVec a, in ScoreVec b)
        {
            long matchValue = 0;
            matchValue = CompareValues(in b, in a, matchValue); // CompareValues(keyvalue=candidate, self=query)
            matchValue = CompareValues(in a, in b, matchValue); // CompareValues(self=query, keyvalue=candidate)

            int tierDiff = Math.Abs(a.Tier - b.Tier);
            matchValue -= tierDiff * 11_000_000;
            if (tierDiff > 0 && b.HasExpModifier)
                matchValue -= 100_000_000;
            int countDiff = Math.Abs(a.Count - b.Count);
            matchValue -= countDiff * 1_000_000;
            return (int)(matchValue / 100);
        }

        private static long CompareValues(in ScoreVec keyvalue, in ScoreVec self, long matchValue)
        {
            // enchants
            for (int i = 0; i < keyvalue.EnchType.Length; i++)
            {
                int type = keyvalue.EnchType[i];
                int mi = IndexOfEnch(in self, type);
                if (mi < 0 || self.EnchLvl[mi] == default)
                {
                    matchValue -= keyvalue.EnchValue[i];
                    continue;
                }
                if (self.EnchLvl[mi] == keyvalue.EnchLvl[i])
                    matchValue += self.EnchValue[mi];
            }
            // modifiers
            for (int i = 0; i < keyvalue.ModKeyId.Length; i++)
            {
                int keyId = keyvalue.ModKeyId[i];
                int mi = IndexOfMod(in self, keyId);
                int importance = keyvalue.ModImportance[i];
                if (mi < 0)
                {
                    matchValue -= Gvod(keyvalue.ModValue[i], importance);
                    continue;
                }
                if (self.ModValId[mi] == keyvalue.ModValId[i] && !keyvalue.ModIsCandyUsed[i])
                    matchValue += Gvod(self.ModValue[mi], importance);
                else if (keyvalue.ModIsNumeric[i] && self.ModIsNumeric[mi])
                {
                    float fdef = Math.Abs(self.ModParsed[mi] - keyvalue.ModParsed[i]) * importance;
                    matchValue -= Gvod(self.ModValue[mi], fdef);
                }
                else
                    matchValue -= Gvod(self.ModValue[mi], importance);

                if (self.ModValId[mi] != keyvalue.ModValId[i] && keyvalue.ModIsPetTierBoost[i])
                    matchValue -= 108_000_000;
            }
            // reforge
            if (keyvalue.HasReforge && keyvalue.Reforge != default)
            {
                if (self.HasReforge && self.Reforge == keyvalue.Reforge)
                    matchValue += self.ReforgeValue;
                else
                    matchValue -= 1_000; // self has no matching reforge -> the original's `match?.Value ?? 1_000` is 1_000
            }
            return matchValue;
        }

        private static int IndexOfEnch(in ScoreVec v, int type)
        {
            for (int i = 0; i < v.EnchType.Length; i++)
                if (v.EnchType[i] == type) return i;
            return -1;
        }

        private static int IndexOfMod(in ScoreVec v, int keyId)
        {
            for (int i = 0; i < v.ModKeyId.Length; i++)
                if (v.ModKeyId[i] == keyId) return i;
            return -1;
        }
    }
}
