using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;
using MessagePack;

namespace Coflnet.Sky.Sniper.Models
{
    public class AuctionKeyWithValue : AuctionKey
    {
        public AuctionKeyWithValue(AuctionKey key) : base(key)
        {
        }

        public AuctionKeyWithValue()
        {
        }

        public long ValueSubstract { get; set; }
    }
    [MessagePackObject]
    public class AuctionKey
    {
        // Explicit backing fields: every init accessor resets the memoized hash. The copy ctor copies _hash (so
        // GetReduced(0)-style pure copies keep the memo), but the widespread `new AuctionKey(source) { Count = 1 }`
        // pattern mutates hashed fields AFTER the ctor ran — without the reset those derived keys would carry the
        // source's hash forever and silently miss every dictionary probe (stale-memo bug).
        private ReadOnlyCollection<Enchant> _enchants = new(new List<Enchant>());
        private ItemReferences.Reforge _reforge;
        private ReadOnlyCollection<KeyValuePair<string, string>> _modifiers = new(new List<KeyValuePair<string, string>>());
        private Tier _tier;
        private byte _count;

        [Key(0)]
        public ReadOnlyCollection<Enchant> Enchants { get => _enchants; init { _enchants = value; _hash = 0; } }
        [Key(1)]
        public ItemReferences.Reforge Reforge { get => _reforge; init { _reforge = value; _hash = 0; } }
        [Key(2)]
        public ReadOnlyCollection<KeyValuePair<string, string>> Modifiers { get => _modifiers; init { _modifiers = value; _hash = 0; } }
        [Key(3)]
        public Tier Tier { get => _tier; init { _tier = value; _hash = 0; } }
        [Key(4)]
        public byte Count { get => _count; init { _count = value; _hash = 0; } }

        public static ReadOnlyCollection<KeyValuePair<string, string>> EmptyModifiers = new(new List<KeyValuePair<string, string>>());

        public AuctionKey WithEnchants(IEnumerable<Enchant> enchants)
        {
            return new AuctionKey(this)
            {
                Enchants = enchants.ToList().AsReadOnly()
            };
        }

        public bool IsClean()
        {
            return Enchants?.Count == 0 && Modifiers?.Count == 0 && Reforge == ItemReferences.Reforge.Any && Count <= 1;
        }

        /// <summary>
        /// The higher the better
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public int Similarity(AuctionKey key, SniperService service = null, List<SniperService.RankElem> keyvalue = null, List<SniperService.RankElem> self = null)
        {
            if (key == null)
                return -100000;

            if (service != null)
            {
                return SimilarityByMarketPrice(key, keyvalue, self);
            }
            var sum = 0;
            if (this.Reforge == key.Reforge)
                sum++;
            else
                sum--;
            var petDifference = Math.Abs(this.Tier - key.Tier);
            sum -= petDifference * 11;
            // De-LINQ'd Any(m => m.Key == "exp") to a loop (no Func/enumerator alloc per closest-search candidate).
            if (petDifference > 0 && ModifiersContainKey(key.Modifiers, "exp"))
                sum -= 555; // higher tier is very bad
            if (this.Count == key.Count)
                sum += this.Count;
            if (this.Enchants != null && key.Enchants != null)
            {
                //sum += this.Enchants.Count(e => key.Enchants.Any(k => k.Lvl == e.Lvl && k.Type == e.Type)) * 3;
                sum -= EnchantSimilarity(this.Enchants, key);
                sum -= EnchantSimilarity(key.Enchants, this);
                // Filter the very-valuable enchant subset inline instead of .Where(...).ToList() (was 2 list allocs/call).
                sum -= EnchantSimilarityVeryValuable(this.Enchants, key) * 10;
                sum -= EnchantSimilarityVeryValuable(key.Enchants, this) * 10;
            }
            sum -= (this.Enchants?.Count ?? 0) + (key.Enchants?.Count ?? 0);

            if (this.Modifiers != null && key.Modifiers != null)
            {
                //sum += this.Modifiers.Count(m => key.Modifiers.Any(k => k.Key == m.Key && k.Value == m.Value)) * 3;
                sum -= (int)ModifierDifference(key, this.Modifiers);
                sum -= (int)ModifierDifference(this, key.Modifiers);

                // Filter the very-valuable / increadable subsets inline instead of .Where(...).ToList() (was 4 list allocs/call).
                sum -= (int)(ModifierDifferenceFiltered(this, key.Modifiers, SniperService.VeryValuable) * 10);
                sum -= (int)(ModifierDifferenceFiltered(key, this.Modifiers, SniperService.VeryValuable) * 10);

                sum -= (int)ModifierDifferenceFiltered(this, key.Modifiers, SniperService.Increadable) * 100;
                sum -= (int)ModifierDifferenceFiltered(key, this.Modifiers, SniperService.Increadable) * 100;
            }
            else
                sum -= this.Modifiers?.Count ?? 0 - key.Modifiers?.Count ?? 0;
            sum -= Math.Abs(key.Count - Count);
            return sum;
        }

        private int SimilarityByMarketPrice(AuctionKey key, List<SniperService.RankElem> keyvalue, List<SniperService.RankElem> self)
        {
            var matchValue = 0L;
            matchValue = CompareValues(keyvalue, self, matchValue);
            matchValue = CompareValues(self, keyvalue, matchValue);

            var tierDiffere = Math.Abs(this.Tier - key.Tier);
            matchValue -= tierDiffere * 11_000_000;
            if (tierDiffere > 0 && ModifiersContainKey(key.Modifiers, "exp"))
                matchValue -= 100_000_000; // higher tier is very bad
            var countDiff = Math.Abs(this.Count - key.Count);
            matchValue -= countDiff * 1_000_000;
            return (int)(matchValue / 100);
        }

        private static long CompareValues(List<SniperService.RankElem> keyvalue, List<SniperService.RankElem> self, long matchValue)
        {
            // De-LINQ'd: the old Where/FirstOrDefault chains allocated a Func + enumerator per element AND per call (two
            // calls per closest-search candidate, the single biggest read-path allocator under SimilarityByMarketPrice).
            // Rewritten to indexed loops with explicit first-match-by-key lookups. Bit-exact: same iteration order, same
            // first-match semantics (FirstOrDefault on a reference type -> null when absent), same arithmetic — gated by
            // the ClosestScoreKernel parity tests (kernel == Similarity) and the AuctionKey similarity oracle.
            for (int i = 0; i < keyvalue.Count; i++)
            {
                var item = keyvalue[i];
                if (item.Enchant.Lvl == default)
                    continue;
                SniperService.RankElem enchMatch = null;
                for (int j = 0; j < self.Count; j++)
                    if (self[j].Enchant.Type == item.Enchant.Type) { enchMatch = self[j]; break; }
                if (enchMatch?.Enchant.Lvl == default)
                {
                    matchValue -= item.Value;
                    continue;
                }
                if (enchMatch.Enchant.Lvl == item.Enchant.Lvl)
                    matchValue += enchMatch.Value;
            }
            for (int i = 0; i < keyvalue.Count; i++)
            {
                var item = keyvalue[i];
                if (item.Modifier.Key == default)
                    continue;
                SniperService.RankElem modMatch = null;
                for (int j = 0; j < self.Count; j++)
                    if (self[j].Modifier.Key == item.Modifier.Key) { modMatch = self[j]; break; }
                var m = item.Modifier;
                if (modMatch == default)
                {
                    matchValue -= item.GetValueOrDefault(ImportanceFactor(m.Key));
                    continue;
                }

                if (modMatch.Modifier.Value == m.Value && modMatch.Modifier.Key != "candyUsed") // don't add value for same candy
                    matchValue += modMatch.GetValueOrDefault(ImportanceFactor(m.Key));
                else if (modMatch.Modifier.Key == null && float.TryParse(m.Value, CultureInfo.InvariantCulture, out var parsed))
                    matchValue -= modMatch.GetValueOrDefault(Math.Abs(parsed) * ImportanceFactor(m.Key));
                else if (float.TryParse(modMatch.Modifier.Value, CultureInfo.InvariantCulture, out var mValue) && float.TryParse(m.Value, CultureInfo.InvariantCulture, out var value))
                    // numeric difference
                    matchValue -= modMatch.GetValueOrDefault(Math.Abs(mValue - value) * ImportanceFactor(m.Key));
                else
                    matchValue -= modMatch.GetValueOrDefault(ImportanceFactor(m.Key));

                if(modMatch.Modifier.Value != m.Value && m.Key == SniperService.PetItemKey && m.Value == SniperService.TierBoostShorthand)
                    matchValue -= 108_000_000; // tier boost is very valuable
            }
            SniperService.RankElem reforge = null;
            for (int i = 0; i < keyvalue.Count; i++)
                if (keyvalue[i].Reforge != default) { reforge = keyvalue[i]; break; }
            if (reforge != default && reforge.Reforge != default)
            {
                SniperService.RankElem match = null;
                for (int j = 0; j < self.Count; j++)
                    if (self[j].Reforge == reforge.Reforge) { match = self[j]; break; }
                if (match?.Reforge == reforge.Reforge)
                    matchValue += match.Value;
                else
                    matchValue -= match?.Value ?? 1_000;
            }

            return matchValue;
        }

        private static bool ModifiersContainKey(IReadOnlyList<KeyValuePair<string, string>> modifiers, string wantedKey)
        {
            if (modifiers == null)
                return false;
            for (int i = 0; i < modifiers.Count; i++)
                if (modifiers[i].Key == wantedKey)
                    return true;
            return false;
        }

        private static int ImportanceFactor(string key)
        {
            if (SniperService.Increadable.Contains(key))
                return 100 * 1_000_000;
            if (SniperService.VeryValuable.Contains(key))
                return 10 * 1_000_000;
            return 1 * 1_000_000;
        }

        private int EnchantSimilarity(IEnumerable<Enchant> enchantsToCompare, AuctionKey key)
        {
            // De-LINQ'd: the old .Sum(ench => key.Enchants.FirstOrDefault(...)) allocated a Func + sum/lookup
            // enumerator per call (this runs per closest-search candidate, a top read-path allocator). Same result —
            // per-enchant score summed with the identical FirstOrDefault-by-Type match — so bit-exact (gated by the
            // AuctionKey similarity oracle + ClosestScoreKernel parity).
            int sum = 0;
            foreach (var ench in enchantsToCompare)
                sum += ScoreEnchant(ench, key);
            return sum;
        }

        /// <summary>
        /// R3-READ helper: same as the old per-enchant <c>EnchantSimilarity</c> lambda but lets the caller filter the
        /// "very valuable" subset inline (skip enchants that are not very-valuable-at-or-above-their-threshold), so the
        /// veryValuable variants avoid the <c>.Where(...).ToList()</c> the original allocated. Bit-exact with
        /// <c>EnchantSimilarity(enchants.Where(veryValuablePredicate).ToList(), key)</c>.
        /// </summary>
        private int EnchantSimilarityVeryValuable(IReadOnlyList<Enchant> enchantsToCompare, AuctionKey key)
        {
            if (enchantsToCompare == null)
                return 0;
            int sum = 0;
            for (int i = 0; i < enchantsToCompare.Count; i++)
            {
                var ench = enchantsToCompare[i];
                if (Constants.VeryValuableEnchant.TryGetValue(ench.Type, out var lvl) && ench.Lvl >= lvl)
                    sum += ScoreEnchant(ench, key);
            }
            return sum;
        }

        private static int ScoreEnchant(Enchant ench, AuctionKey key)
        {
            // First enchant in key with the same Type (FirstOrDefault semantics: default Enchant has Lvl 0).
            Enchant match = default;
            var keyEnchants = key.Enchants;
            if (keyEnchants != null)
                for (int j = 0; j < keyEnchants.Count; j++)
                    if (keyEnchants[j].Type == ench.Type) { match = keyEnchants[j]; break; }
            if (match.Lvl == 0)
                return 6;
            if (match.Lvl == ench.Lvl)
                return -2;
            var multiplier = 1;
            if (match.Lvl > ench.Lvl)
                multiplier = 2;
            return Math.Abs(match.Lvl - ench.Lvl) * multiplier;
        }

        private static float ModifierDifference(AuctionKey key, IEnumerable<KeyValuePair<string, string>> leftMods)
        {
            // De-LINQ'd: the old .Sum(m => key.Modifiers.Where(...).FirstOrDefault()) allocated a Func + sum/Where
            // enumerators per call (per closest-search candidate -> a top read-path allocator). Same per-modifier score
            // summed with the identical first-match-by-Key lookup, so bit-exact (AuctionKey similarity oracle gate).
            if (leftMods == null)
                return 0;
            float sum = 0;
            foreach (var m in leftMods)
                sum += ScoreModifier(key, m);
            return sum;
        }

        /// <summary>
        /// R3-READ helper: <c>ModifierDifference(key, leftMods.Where(m => set.Contains(m.Key)).ToList())</c> without the
        /// intermediate filtered list (the original allocated one per very-valuable / increadable variant, four per
        /// Similarity call). Bit-exact: scores exactly the modifiers whose Key is in <paramref name="set"/>.
        /// </summary>
        private static float ModifierDifferenceFiltered(AuctionKey key, IReadOnlyList<KeyValuePair<string, string>> leftMods, HashSet<string> set)
        {
            if (leftMods == null)
                return 0;
            float sum = 0;
            for (int i = 0; i < leftMods.Count; i++)
            {
                var m = leftMods[i];
                if (set.Contains(m.Key))
                    sum += ScoreModifier(key, m);
            }
            return sum;
        }

        private static float ScoreModifier(AuctionKey key, KeyValuePair<string, string> m)
        {
            // First modifier in key with the same Key (Where(...).FirstOrDefault() semantics: default KVP has null Key).
            KeyValuePair<string, string> match = default;
            var keyMods = key.Modifiers;
            if (keyMods != null)
                for (int j = 0; j < keyMods.Count; j++)
                    if (keyMods[j].Key == m.Key) { match = keyMods[j]; break; }
            if (match.Key == null)
                if (float.TryParse(m.Value, CultureInfo.InvariantCulture, out var parsed))
                    return Math.Abs(parsed) * 8;
                else if (m.Value == SniperService.TierBoostShorthand)
                    return 58; // tier boost is very valuable
                else
                    return 5 + m.Value.Length;
            if (float.TryParse(match.Value, CultureInfo.InvariantCulture, out var matchValue) && float.TryParse(m.Value, CultureInfo.InvariantCulture, out var value))
                return Math.Abs(matchValue - value);
            if (match.Value == m.Value)
                if (m.Key == SniperService.PetItemKey && m.Value == SniperService.TierBoostShorthand)
                    return -28; // tier boost is very valuable
                else
                    return -2;
            return 3 + Math.Abs(match.Value.Length - m.Value.Length);
        }

        // R8-spike: lazily-memoized hash. AuctionKey is immutable (all hash inputs are { get; init; } over
        // ReadOnlyCollections; the only mutable member is AuctionKeyWithValue.ValueSubstract, which is NOT hashed), so
        // the hash is a pure function computed once. The dispatch loop probes the lookup up to 4× per auction and every
        // finder/index hashes keys, each previously recomputing this O(mods+enchants) sum. Single-field sentinel (0 =
        // not computed; a real 0 is bumped to 1) makes the lazy init torn-read-free and race-benign: concurrent first
        // calls all compute the identical value. Bit-exact: returns exactly what the original computed.
        private int _hash;
        public override int GetHashCode()
        {
            var h = _hash;
            if (h != 0)
                return h;
            // Order-independent (Equals compares the enchant/modifier sets regardless of order): sum well-mixed
            // per-element hashes instead of multiplying them. The old multiplicative mix collapsed toward poorly
            // distributed values (a single zero element hash annihilated it), clustering keys and inflating the number
            // of Equals comparisons on every dictionary lookup. Equal keys still produce equal hashes.
            var enchRes = 0;
            if (Enchants != null)
                foreach (var item in Enchants)
                    enchRes += item.GetHashCode();
            var modRes = 0;
            if (Modifiers != null)
                foreach (var item in Modifiers)
                    modRes += HashCode.Combine(item.Key, item.Value);
            h = HashCode.Combine(enchRes, Reforge, modRes, Tier, Count);
            _hash = h == 0 ? 1 : h; // cache; 0 stays the "uncomputed" sentinel
            return h;
        }

        public override string ToString()
        {
            return $"{(Enchants == null ? "ne" : string.Join(',', Enchants.Select(m => $"{m.Type}={m.Lvl}")))} {Reforge} {(Modifiers == null ? "nm" : string.Join(',', Modifiers.Select(m => m.ToString())))} {Tier} {Count}";
        }

        public override bool Equals(object obj)
        {
            // De-LINQ'd to explicit loops: the old All(e => Any(ke => ...)) over Enchants/Modifiers allocated Func +
            // enumerator closures on EVERY dictionary probe (a top per-auction allocator). Same boolean result —
            // scalars equal, and each collection both-null or both-non-null with equal count and every left element
            // matched in the right (the original All/Any containment) — so it is bit-exact (gated by
            // AuctionKeyPrimitives.Tests' 40k-fuzz golden output).
            if (obj is not AuctionKey key)
                return false;
            if (Reforge != key.Reforge || Tier != key.Tier || Count != key.Count)
                return false;
            if (!(key.Enchants == null && this.Enchants == null))
            {
                if (this.Enchants == null || key.Enchants == null || this.Enchants.Count != key.Enchants.Count)
                    return false;
                for (int i = 0; i < this.Enchants.Count; i++)
                {
                    var e = this.Enchants[i];
                    bool found = false;
                    for (int j = 0; j < key.Enchants.Count; j++)
                    {
                        var ke = key.Enchants[j];
                        if (ke.Type == e.Type && ke.Lvl == e.Lvl) { found = true; break; }
                    }
                    if (!found) return false;
                }
            }
            if (!(key.Modifiers == null && this.Modifiers == null))
            {
                if (this.Modifiers == null || key.Modifiers == null || this.Modifiers.Count != key.Modifiers.Count)
                    return false;
                for (int i = 0; i < this.Modifiers.Count; i++)
                {
                    var m = this.Modifiers[i];
                    bool found = false;
                    for (int j = 0; j < key.Modifiers.Count; j++)
                    {
                        var km = key.Modifiers[j];
                        if (km.Key == m.Key && km.Value == m.Value) { found = true; break; }
                    }
                    if (!found) return false;
                }
            }
            return true;
        }

        public AuctionKey(List<Enchant> enchants, ItemReferences.Reforge reforge, List<KeyValuePair<string, string>> modifiers, Tier tier, byte count)
        {
            Enchants = enchants?.AsReadOnly();
            Reforge = reforge;
            Modifiers = modifiers?.AsReadOnly();
            Tier = tier;
            Count = count;
        }

        public AuctionKey(AuctionKey key)
        {
            Enchants = key.Enchants;
            Reforge = key.Reforge;
            Modifiers = key.Modifiers;
            Tier = key.Tier;
            Count = key.Count;
            _hash = key._hash; // R8-spike: same (shared) content -> same hash; copy the memo so GetReduced(0) doesn't recompute
        }

        public AuctionKey()
        {
        }

        public static bool operator ==(AuctionKey a, AuctionKey b)
        {
            return a?.Equals(b) ?? false;
        }
        public static bool operator !=(AuctionKey a, AuctionKey b)
        {
            return !(a?.Equals(b) ?? true);
        }
    }
}