using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R2 Phase 0 / F3 bit-exactness oracle for the <b>cross-cutting <see cref="AuctionKey"/> primitives</b>
    /// (<c>Equals</c> / <c>GetHashCode</c> / <c>ToString</c>) — the gate for workstream <b>R2-E</b>
    /// (zero-alloc primitives, see <c>benchmarks/COMPUTE_FLOOR_SPEC_R2.md</c> §2.1 + §5 R2-E).
    ///
    /// These static <c>*Reference</c> methods are <b>verbatim copies of the CURRENT (pre-de-LINQ)
    /// <see cref="AuctionKey"/> implementation</b> of <c>Equals(object)</c>, <c>GetHashCode()</c> and
    /// <c>ToString()</c> (Models/AuctionKey.cs as of this commit). R2-E will rewrite the production methods to
    /// explicit, zero-alloc loops; the only contract that rewrite must preserve is that, for every fuzzed input,
    /// the production result is byte-identical to the snapshot captured here. They are referenced <b>only</b> by
    /// <see cref="AuctionKeyPrimitivesTests"/>; they are never called from production.
    ///
    /// Because <c>GetHashCode</c>/<c>Equals</c> are exercised on every dictionary probe, the oracle also asserts the
    /// hash/equality contract invariant (<c>Equals(a,b) =&gt; GetHashCode(a)==GetHashCode(b)</c>) directly against the
    /// reference, so a refactor cannot silently break the dictionary-correctness property R2-E depends on.
    /// </summary>
    internal static class AuctionKeyPrimitivesReference
    {
        /// <summary>
        /// Verbatim copy of <see cref="AuctionKey.GetHashCode"/> as of the R2 Phase 0 snapshot
        /// (order-independent: sums well-mixed per-element hashes).
        /// </summary>
        public static int GetHashCodeReference(AuctionKey self)
        {
            var enchRes = 0;
            if (self.Enchants != null)
                foreach (var item in self.Enchants)
                    enchRes += item.GetHashCode();
            var modRes = 0;
            if (self.Modifiers != null)
                foreach (var item in self.Modifiers)
                    modRes += HashCode.Combine(item.Key, item.Value);
            return HashCode.Combine(enchRes, self.Reforge, modRes, self.Tier, self.Count);
        }

        /// <summary>
        /// Verbatim copy of <see cref="AuctionKey.ToString"/> as of the R2 Phase 0 snapshot.
        /// </summary>
        public static string ToStringReference(AuctionKey self)
        {
            return $"{(self.Enchants == null ? "ne" : string.Join(',', self.Enchants.Select(m => $"{m.Type}={m.Lvl}")))} {self.Reforge} {(self.Modifiers == null ? "nm" : string.Join(',', self.Modifiers.Select(m => m.ToString())))} {self.Tier} {self.Count}";
        }

        /// <summary>
        /// Verbatim copy of <see cref="AuctionKey.Equals(object)"/> as of the R2 Phase 0 snapshot
        /// (order-independent set comparison of enchants and modifiers, LINQ <c>All</c>/<c>Any</c>).
        /// </summary>
        public static bool EqualsReference(AuctionKey self, object obj)
        {
            return obj is AuctionKey key &&
                   self.Reforge == key.Reforge &&
                   self.Tier == key.Tier &&
                   self.Count == key.Count &&
                   (key.Enchants == null && self.Enchants == null ||
                    (self.Enchants != null && key.Enchants != null &&
                     self.Enchants.Count == key.Enchants.Count &&
                     self.Enchants.All(e => key.Enchants.Any(ke => ke.Type == e.Type && ke.Lvl == e.Lvl)))) &&
                   (key.Modifiers == null && self.Modifiers == null ||
                    (self.Modifiers != null && key.Modifiers != null &&
                     self.Modifiers.Count == key.Modifiers.Count &&
                     self.Modifiers.All(m => key.Modifiers.Any(km => km.Key == m.Key && km.Value == m.Value))));
        }
    }
}
