using System;
using System.Collections.Generic;
using AwesomeAssertions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// Proves the de-LINQ'd <see cref="SniperService.IsHigherValue"/> is boolean-bit-exact with the original LINQ
    /// implementation (<see cref="SniperService.IsHigherValueReference"/>) across randomized keys exercising every
    /// branch: tier/count, numeric &gt;/&lt; (inverted keys), comma-substring containment, new_years_cake/ImportantCakeYears,
    /// exp + petItem TIER_BOOST, and the PET_SPIRIT tag rule.
    /// </summary>
    public class IsHigherValueTests
    {
        private SniperService service = null!;

        [SetUp]
        public void Setup()
            => service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, null);

        [Test]
        public void Fuzz_BoolExact_DeLinq_VsReference()
        {
            int mismatches = 0; string first = "";
            string[] tags = { "SOME_ITEM", "PET_SPIRIT", "PET_ENDER_DRAGON", "NEW_YEAR_CAKE" };
            for (int seed = 1; seed <= 40_000; seed++)
            {
                var rng = new Random(seed);
                var tag = tags[rng.Next(tags.Length)];
                var a = AuctionKeyFuzz.RandomKeyAndBreakdown(rng).key;
                var b = AuctionKeyFuzz.RandomKeyAndBreakdown(rng).key;
                bool expected = service.IsHigherValueReference(tag, a, b);
                bool actual = service.IsHigherValue(tag, a, b);
                if (expected != actual)
                {
                    if (mismatches == 0)
                        first = $"seed={seed} tag={tag} expected={expected} actual={actual}\n  base={Describe(a)}\n  check={Describe(b)}";
                    mismatches++;
                }
            }
            mismatches.Should().Be(0, $"de-LINQ must equal the reference over 40000 pairs. First: {first}");
        }

        [Test]
        public void Edge_Cases_Match_Reference()
        {
            // numeric greater / inverted, comma-substring, cake years, exp+tierboost, enchant level dominance.
            var cases = new (string tag, AuctionKey a, AuctionKey b)[]
            {
                ("I", Key(1,1, mods: M(("kills","3"))), Key(1,1, mods: M(("kills","5")))),                 // numeric greater
                ("I", Key(1,1, mods: M(("edition","5"))), Key(1,1, mods: M(("edition","2")))),             // inverted: lower is higher
                ("I", Key(1,1, mods: M(("edition","5"))), Key(1,1)),                                       // inverted + absent
                ("I", Key(1,1, mods: M(("unlocked_slots","1,2"))), Key(1,1, mods: M(("unlocked_slots","0,1,2,3")))), // comma substring
                ("NEW_YEAR_CAKE", Key(1,1, mods: M(("new_years_cake","69"))), Key(1,1, mods: M(("new_years_cake","420")))), // important years
                ("PET_X", Key(3,1, mods: M(("exp","100"))), Key(5,1, mods: M(("exp","200"),("petItem","TIER_BOOST")))),     // exp + tierboost
                ("PET_SPIRIT", Key(1,1), Key((int)Tier.LEGENDARY,1)),                                      // PET_SPIRIT legendary rule
                ("I", Key(1,1, ench: E((EnchantmentType.sharpness,6))), Key(1,1, ench: E((EnchantmentType.sharpness,7)))),  // enchant level
                ("I", Key(1,1, ench: E((EnchantmentType.sharpness,7))), Key(1,1, ench: E((EnchantmentType.sharpness,6)))),  // enchant level lower
                // additional explicit cases requested for the columnar kernel:
                ("I", Key(1,1, mods: M(("candyUsed","3"))), Key(1,1)),                                     // inverted key absent -> excused via exception
                ("I", Key(1,1, mods: M(("captured_player","RUBY"))), Key(1,1, mods: M(("captured_player","GEM RUBY")))), // base value is substring of space check value
                ("I", Key(1,1, mods: M(("kills","5"))), Key(1,1, mods: M(("kills","3")))),                 // both-numeric, check lower -> not covered
            };
            foreach (var (tag, a, b) in cases)
            {
                bool reference = service.IsHigherValueReference(tag, a, b);
                service.IsHigherValue(tag, a, b).Should().Be(reference,
                    $"de-LINQ: tag={tag} base={Describe(a)} check={Describe(b)}");
                // The columnar kernel must also match the oracle on every explicit case.
                var interner = new ClosestScoreKernel.Interner();
                DominatorIndex.Dominates(DominatorIndex.BuildDomKey(a, interner), DominatorIndex.BuildDomKey(b, interner), tag == "PET_SPIRIT")
                    .Should().Be(reference, $"columnar kernel: tag={tag} base={Describe(a)} check={Describe(b)}");
            }
        }

        [Test]
        public void Fuzz_ColumnarKernel_VsReference()
        {
            int mismatches = 0; string first = "";
            int prefilterViolations = 0; string firstViolation = "";
            string[] tags = { "SOME_ITEM", "PET_SPIRIT", "PET_ENDER_DRAGON", "NEW_YEAR_CAKE" };
            var interner = new ClosestScoreKernel.Interner(); // single shared id space across all built DomKeys
            for (int seed = 1; seed <= 40_000; seed++)
            {
                var rng = new Random(seed);
                var tag = tags[rng.Next(tags.Length)];
                var a = AuctionKeyFuzz.RandomKeyAndBreakdown(rng).key;
                var b = AuctionKeyFuzz.RandomKeyAndBreakdown(rng).key;
                bool expected = service.IsHigherValueReference(tag, a, b);
                var domA = DominatorIndex.BuildDomKey(a, interner);
                var domB = DominatorIndex.BuildDomKey(b, interner);
                bool actual = DominatorIndex.Dominates(domA, domB, tag == "PET_SPIRIT");
                if (expected != actual)
                {
                    if (mismatches == 0)
                        first = $"seed={seed} tag={tag} expected={expected} actual={actual}\n  base={Describe(a)}\n  check={Describe(b)}";
                    mismatches++;
                }
                // Prefilter soundness: when a dominates b (base=a), the mask test must hold (never false-rejects).
                if (expected)
                {
                    bool maskOk = (domA.RequiredMask & domB.ProvidedMask) == domA.RequiredMask;
                    if (!maskOk)
                    {
                        if (prefilterViolations == 0)
                            firstViolation = $"seed={seed} tag={tag} required=0x{domA.RequiredMask:x} provided=0x{domB.ProvidedMask:x}\n  base={Describe(a)}\n  check={Describe(b)}";
                        prefilterViolations++;
                    }
                }
            }
            mismatches.Should().Be(0, $"columnar kernel must equal the reference over 40000 pairs. First: {first}");
            prefilterViolations.Should().Be(0, $"mask prefilter must never false-reject a true dominance over 40000 pairs. First: {firstViolation}");
        }

        // ---- generators ----
        // The random (AuctionKey) generator is the shared AuctionKeyFuzz.RandomKeyAndBreakdown (F2); only its key is
        // consumed here (IsHigherValue ignores the priced breakdown). Tag selection stays local to this harness.
        private static AuctionKey Key(int tier, int count, List<KeyValuePair<string, string>> mods = null, List<Enchant> ench = null)
            => new(ench ?? new(), ItemReferences.Reforge.Any, mods ?? new(), (Tier)tier, (byte)count);
        private static List<KeyValuePair<string, string>> M(params (string k, string v)[] kv)
        {
            var l = new List<KeyValuePair<string, string>>();
            foreach (var (k, v) in kv) l.Add(new(k, v));
            return l;
        }
        private static List<Enchant> E(params (EnchantmentType t, byte lvl)[] es)
        {
            var l = new List<Enchant>();
            foreach (var (t, lvl) in es) l.Add(new Enchant { Type = t, Lvl = lvl });
            return l;
        }
        private static string Describe(AuctionKey k)
            => $"t{k.Tier} c{k.Count} mods=[{string.Join(",", k.Modifiers.ConvertToString())}] ench=[{string.Join(",", k.Enchants.EnchToString())}]";
    }

    internal static class IsHigherValueTestExtensions
    {
        public static IEnumerable<string> ConvertToString(this System.Collections.ObjectModel.ReadOnlyCollection<KeyValuePair<string, string>> mods)
        {
            foreach (var m in mods) yield return m.Key + "=" + m.Value;
        }
        public static IEnumerable<string> EnchToString(this System.Collections.ObjectModel.ReadOnlyCollection<Enchant> ench)
        {
            foreach (var e in ench) yield return e.Type + ":" + e.Lvl;
        }
    }
}
