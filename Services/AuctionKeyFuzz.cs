using System;
using System.Collections.Generic;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// Phase 0 / F2 — the <b>single shared</b> random <see cref="AuctionKey"/> + priced breakdown generator the
    /// bit-exactness fuzz harnesses reuse (see <c>benchmarks/COMPUTE_FLOOR_SPEC.md</c> §2 non-negotiables and §4 F2).
    ///
    /// It consolidates the two formerly-private generators (the closest-score kernel fuzz and the IsHigherValue fuzz)
    /// into one pool/generator so every new scorer/extractor is fuzzed against the same broad input distribution. The
    /// pools deliberately cover <b>every</b> branch the production scorers/extractors hit:
    /// <list type="bullet">
    ///   <item>enchants at matching / differing / missing level, including very-valuable enchant types;</item>
    ///   <item>numeric modifiers (the float-difference path);</item>
    ///   <item><c>candyUsed</c> (the "don't add value for same candy" branch);</item>
    ///   <item><c>petItem</c> + <c>TIER_BOOST</c> (the tier-boost penalty);</item>
    ///   <item>reforge match / mismatch;</item>
    ///   <item>comma- and space-substring values (e.g. <c>unlocked_slots</c> "0,1,2") — the substring-containment rule;</item>
    ///   <item>cake-year values (<c>new_years_cake</c>) including the <c>ImportantCakeYears</c> (69/420/400) guard;</item>
    ///   <item>PET_SPIRIT-style pet values (the inverted/numeric pet modifiers) and non-numeric / "200k"-style values;</item>
    ///   <item>every ImportanceFactor tier: normal / VeryValuable / Increadable.</item>
    /// </list>
    ///
    /// The generator is <b>seed-deterministic</b>: a given <see cref="Random"/> sequence yields the same key/breakdown,
    /// so the harnesses' per-seed loops remain reproducible. Modifier keys and enchant types are kept unique within a
    /// single key (matching production key construction, where the inner key-match is at most one hit).
    /// </summary>
    internal static class AuctionKeyFuzz
    {
        /// <summary>
        /// Modifier keys spanning all three <c>ImportanceFactor</c> tiers (normal / VeryValuable / Increadable) and the
        /// special branches: <c>exp</c> (tier-boost interaction), <c>candyUsed</c> (same-candy / inverted), the
        /// <c>petItem</c> tier-boost key, <c>new_years_cake</c> (cake-year / inverted), <c>edition</c> (inverted),
        /// <c>unlocked_slots</c> (comma-substring), plus plain numeric and non-numeric attributes.
        /// </summary>
        public static readonly string[] ModKeyPool =
        {
            "exp", "candyUsed", "skin", "color", "ability_scroll", SniperService.PetItemKey,
            "kills", "logs_cut", "rarity_upgrades", "winning_bid",
            "edition", "new_years_cake", "unlocked_slots", "captured_player",
        };

        /// <summary>
        /// Modifier values covering: small/large integers and a decimal (numeric float path), <c>"200k"</c> and
        /// <c>"true"</c> / <c>"PERFECT"</c> (non-numeric else path), the <c>TIER_BOOST</c> shorthand, the
        /// <c>ImportantCakeYears</c> 69/420/400, ordinary cake years (1/9/120), and comma-/space-substring values
        /// (<c>"0,1,2"</c>, <c>"1,2,3,4"</c>, <c>"GEM RUBY"</c>) for the containment rule.
        /// </summary>
        public static readonly string[] ModValPool =
        {
            "1", "2", "3", "5", "8", "9", "3.5",            // numeric (incl. decimal)
            "200k", "true", "PERFECT",                       // non-numeric else branch
            SniperService.TierBoostShorthand,                // petItem tier-boost
            "69", "420", "400", "120",                       // cake years (69/420/400 are ImportantCakeYears)
            "0,1,2", "1,2,3,4", "GEM RUBY",                  // comma / space substring containment
        };

        /// <summary>
        /// Enchant types spanning ordinary, very-valuable (<c>ultimate_wise</c>), and a couple of common types so the
        /// enchant same-level / different-level / missing branches all fire.
        /// </summary>
        public static readonly EnchantmentType[] EnchPool =
        {
            EnchantmentType.sharpness, EnchantmentType.growth, EnchantmentType.protection,
            EnchantmentType.critical, EnchantmentType.ultimate_wise,
        };

        public static readonly ItemReferences.Reforge[] ReforgePool =
        {
            ItemReferences.Reforge.Any, ItemReferences.Reforge.Gilded,
            ItemReferences.Reforge.jaded, ItemReferences.Reforge.Necrotic,
        };

        /// <summary>
        /// Random priced-breakdown value: hits the 0 (GetValueOrDefault default path), tiny, and realistic-coin buckets.
        /// </summary>
        public static long RandomValue(Random rng)
        {
            int roll = rng.Next(6);
            return roll switch
            {
                0 => 0,                                  // exercises GetValueOrDefault default path
                1 => rng.Next(1, 50),                    // tiny
                _ => (long)rng.Next(1, 2000) * 1_000_000 // realistic coin values
            };
        }

        /// <summary>
        /// Builds an <see cref="AuctionKey"/> the same way the harness helpers do (enchants, reforge, modifiers, tier,
        /// count); a thin wrapper so callers don't repeat the cast dance.
        /// </summary>
        public static AuctionKey Key(int tier, int count, List<Enchant> enchants = null,
            List<KeyValuePair<string, string>> modifiers = null, ItemReferences.Reforge reforge = ItemReferences.Reforge.Any)
            => new(enchants ?? new(), reforge, modifiers ?? new(), (Tier)tier, (byte)count);

        /// <summary>
        /// The shared generator: a random <see cref="AuctionKey"/> together with its priced breakdown (the <c>cv</c>
        /// list of <see cref="SniperService.RankElem"/> produced by ComparisonValue). The <c>cv</c> mirrors the key's
        /// enchants/modifiers (1:1) plus an optional reforge RankElem, exactly as the production breakdown does, so the
        /// pair is consistent for both the score kernel (which consumes the <c>cv</c>) and IsHigherValue (which consumes
        /// only the key).
        /// </summary>
        public static (AuctionKey key, List<SniperService.RankElem> cv) RandomKeyAndBreakdown(Random rng)
        {
            var enchants = new List<Enchant>();
            var cv = new List<SniperService.RankElem>();
            int enchCount = rng.Next(0, 4);
            var usedTypes = new HashSet<EnchantmentType>();
            for (int i = 0; i < enchCount; i++)
            {
                var t = EnchPool[rng.Next(EnchPool.Length)];
                if (!usedTypes.Add(t)) continue; // keep enchant types unique within a key
                var e = new Enchant { Type = t, Lvl = (byte)rng.Next(1, 8) };
                enchants.Add(e);
                cv.Add(new(e, RandomValue(rng)));
            }

            var modifiers = new List<KeyValuePair<string, string>>();
            int modCount = rng.Next(0, 5);
            var usedKeys = new HashSet<string>();
            for (int i = 0; i < modCount; i++)
            {
                var k = ModKeyPool[rng.Next(ModKeyPool.Length)];
                if (!usedKeys.Add(k)) continue; // unique modifier keys within a key
                // petItem leans toward TIER_BOOST so the tier-boost branch is hit often.
                var v = k == SniperService.PetItemKey && rng.Next(2) == 0
                    ? SniperService.TierBoostShorthand
                    : ModValPool[rng.Next(ModValPool.Length)];
                var kv = new KeyValuePair<string, string>(k, v);
                modifiers.Add(kv);
                cv.Add(new(kv, RandomValue(rng)));
            }

            var reforge = ReforgePool[rng.Next(ReforgePool.Length)];
            if (reforge != ItemReferences.Reforge.Any && rng.Next(2) == 0)
                cv.Add(new(reforge, RandomValue(rng)));

            var key = Key(rng.Next(0, 12), rng.Next(1, 5), enchants, modifiers, reforge);
            return (key, cv);
        }
    }
}
