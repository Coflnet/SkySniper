using System;
using System.Collections.Generic;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// Cache key for <c>ModifierValueLookup</c>. A <b>readonly struct</b> (was a class): on the hot key-extraction path
    /// this is built once per modifier purely to probe the cache, so a heap allocation per modifier per auction was pure
    /// waste. As a struct the probe is allocation-free; it implements <see cref="IEquatable{T}"/> so the
    /// <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}"/> compares it without boxing.
    /// <para>Equality semantics are unchanged from the original class: tag + modifier KVP + the (order-independent)
    /// contents of <see cref="RelevantModifiers"/>. The struct holds a reference to the shared per-call
    /// <c>RelevantModifiers</c> dictionary, exactly as the class did, so a stored key keeps that dictionary alive the
    /// same way.</para>
    /// </summary>
    public readonly struct ModifierLookupKey : IEquatable<ModifierLookupKey>
    {
        public string ItemTag { get; init; }
        public KeyValuePair<string, string> Modifier { get; init; }
        public Dictionary<string, string> RelevantModifiers { get; init; }

        /// <summary>
        /// R10: pre-computed order-independent hash of <see cref="RelevantModifiers"/> (Σ HashCode.Combine(k,v)). The
        /// hot caller (<c>ComparisonValue</c>) builds the same <c>RelevantModifiers</c> for ALL M modifier probes in a
        /// call, so iterating it inside every probe's <see cref="GetHashCode"/> is O(M²) per auction even on cache hits.
        /// Computing the sum once and stamping it here makes each probe O(1). 0 = "not pre-computed" → fall back to
        /// iterating (other callers / the reference oracle). Bit-exact: the stamped value equals the iteration result,
        /// so the final hash — and thus every cache hit/miss placement — is identical.
        /// </summary>
        public int RelevantHash { get; init; }

        public bool Equals(ModifierLookupKey other)
        {
            // Compare ItemTag
            if (ItemTag != other.ItemTag)
                return false;

            // Compare Modifier
            if (!Modifier.Equals(other.Modifier))
                return false;

            // Compare RelevantModifiers
            if (RelevantModifiers == null && other.RelevantModifiers == null)
                return true;
            if (RelevantModifiers == null || other.RelevantModifiers == null)
                return false;
            if (RelevantModifiers.Count != other.RelevantModifiers.Count)
                return false;

            // Check if all modifiers in this instance are in the other instance
            foreach (var modifier in RelevantModifiers)
            {
                if (!other.RelevantModifiers.TryGetValue(modifier.Key, out string value) || value != modifier.Value)
                    return false;
            }

            return true;
        }

        public override bool Equals(object obj) => obj is ModifierLookupKey other && Equals(other);

        public override int GetHashCode()
        {
            // Order-independent: RelevantModifiers is a dictionary (equality compares contents regardless of order),
            // so the per-entry contributions are summed. Avoids the previous multiplicative mix, which collapsed to a
            // poorly-distributed value (any zero element hash annihilated it) and drove excessive Equals comparisons.
            // R10: use the caller-stamped RelevantHash when present (non-zero); fall back to iterating otherwise. The
            // stamped value IS this same sum, so the result is bit-identical either way.
            var entries = RelevantHash;
            if (entries == 0 && RelevantModifiers != null)
            {
                foreach (var modifier in RelevantModifiers)
                    entries += System.HashCode.Combine(modifier.Key, modifier.Value);
            }
            return System.HashCode.Combine(ItemTag, Modifier.Key, Modifier.Value, entries);
        }
    }
}
