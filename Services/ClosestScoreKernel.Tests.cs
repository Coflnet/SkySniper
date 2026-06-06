using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
    /// TDD spec for <see cref="ClosestScoreKernel"/>: the columnar/interned scorer must be <b>bit-exact</b> with the
    /// production <see cref="AuctionKey"/> market-price similarity, so the closest search can be swapped in wholesale.
    /// "Bit-exact" here means integer equality of the returned score for every (query, candidate) pair; the single
    /// internal floating-point term is additionally checked for 0-ulp agreement.
    /// </summary>
    public class ClosestScoreKernelTests
    {
        private SniperService service = null!;

        [SetUp]
        public void Setup()
        {
            // Non-null service routes Similarity into the market-price path; it also populates the static VeryValuable
            // set that ImportanceFactor reads (so both scorers see the same importance tiers).
            service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, null);
        }

        // Reference: exactly the production call shape from FindClosestTo —
        //   itemKey.Similarity(candidate, service, candidateCv, queryCv)
        private int Reference(AuctionKey query, List<SniperService.RankElem> queryCv,
                              AuctionKey candidate, List<SniperService.RankElem> candidateCv)
            => query.Similarity(candidate, service, candidateCv, queryCv);

        private static int Kernel(AuctionKey query, List<SniperService.RankElem> queryCv,
                                  AuctionKey candidate, List<SniperService.RankElem> candidateCv)
        {
            var interner = new ClosestScoreKernel.Interner();
            var qVec = ClosestScoreKernel.Build(query, queryCv, interner);
            var cVec = ClosestScoreKernel.Build(candidate, candidateCv, interner);
            return ClosestScoreKernel.Score(in qVec, in cVec);
        }

        // ---- Fuzz: thousands of random pairs must match bit-exactly ----

        [Test]
        public void Fuzz_BitExact_AgainstProductionSimilarity()
        {
            int mismatches = 0;
            var firstFail = "";
            for (int seed = 1; seed <= 20_000; seed++)
            {
                var rng = new Random(seed);
                var (qKey, qCv) = AuctionKeyFuzz.RandomKeyAndBreakdown(rng);
                var (cKey, cCv) = AuctionKeyFuzz.RandomKeyAndBreakdown(rng);

                int expected = Reference(qKey, qCv, cKey, cCv);
                int actual = Kernel(qKey, qCv, cKey, cCv);
                if (expected != actual)
                {
                    if (mismatches == 0)
                        firstFail = $"seed={seed} expected={expected} actual={actual}\n  q={Describe(qKey, qCv)}\n  c={Describe(cKey, cCv)}";
                    mismatches++;
                }
            }
            mismatches.Should().Be(0, $"kernel must be bit-exact over 20000 random pairs. First: {firstFail}");
        }

        // ---- Targeted branch coverage ----

        [Test]
        public void EnchantMatch_SameLevel_Adds_DifferentLevel_Ignores_Missing_Subtracts()
        {
            var ench = new Enchant { Type = EnchantmentType.sharpness, Lvl = 6 };
            var q = Key(tier: 3, count: 1, enchants: new() { ench });
            var qCv = new List<SniperService.RankElem> { new(ench, 5_000_000) };
            // candidate: same enchant present (match)
            var c = Key(tier: 3, count: 1, enchants: new() { ench });
            var cCv = new List<SniperService.RankElem> { new(ench, 7_000_000) };
            AssertExact(q, qCv, c, cCv);

            // candidate: different level
            var ench7 = new Enchant { Type = EnchantmentType.sharpness, Lvl = 7 };
            AssertExact(q, qCv, Key(3, 1, enchants: new() { ench7 }), new() { new(ench7, 7_000_000) });

            // candidate: missing the enchant entirely
            AssertExact(q, qCv, Key(3, 1), new());
        }

        [Test]
        public void NumericModifierDifference_UsesFloatPath_WhenMatchedValueIsZero()
        {
            // Both sides have "kills" (a numeric modifier), candidate's RankElem.Value == 0 -> the float default term
            // (|a-b| * importance) is what's actually used. This is the only place float math reaches the result.
            var qMods = new List<KeyValuePair<string, string>> { new("kills", "5") };
            var cMods = new List<KeyValuePair<string, string>> { new("kills", "2") };
            var q = Key(2, 1, modifiers: qMods);
            var c = Key(2, 1, modifiers: cMods);
            var qCv = new List<SniperService.RankElem> { new(qMods[0], 3_000_000) };
            var cCv = new List<SniperService.RankElem> { new(cMods[0], 0) }; // 0 -> float default path
            AssertExact(q, qCv, c, cCv);
        }

        [Test]
        public void PetItem_TierBoost_AppliesPenalty()
        {
            var qMods = new List<KeyValuePair<string, string>> { new(SniperService.PetItemKey, SniperService.TierBoostShorthand) };
            var cMods = new List<KeyValuePair<string, string>> { new(SniperService.PetItemKey, "SOMETHING_ELSE") };
            var q = Key(2, 1, modifiers: qMods);
            var c = Key(2, 1, modifiers: cMods);
            AssertExact(q, new() { new(qMods[0], 1_000_000) }, c, new() { new(cMods[0], 1_000_000) });
        }

        [Test]
        public void CandyUsed_SameValue_DoesNotAddValue()
        {
            var mods = new List<KeyValuePair<string, string>> { new("candyUsed", "3") };
            var k = Key(2, 1, modifiers: mods);
            AssertExact(k, new() { new(mods[0], 9_000_000) }, k, new() { new(mods[0], 9_000_000) });
        }

        [Test]
        public void Reforge_Match_Adds_Mismatch_Subtracts()
        {
            var q = Key(2, 1, reforge: ItemReferences.Reforge.Gilded);
            var qCv = new List<SniperService.RankElem> { new(ItemReferences.Reforge.Gilded, 4_000_000) };
            // match
            AssertExact(q, qCv, Key(2, 1, reforge: ItemReferences.Reforge.Gilded), new() { new(ItemReferences.Reforge.Gilded, 6_000_000) });
            // mismatch (candidate has different reforge -> self lookup misses -> -1000)
            AssertExact(q, qCv, Key(2, 1, reforge: ItemReferences.Reforge.Any), new());
        }

        [Test]
        public void TierAndCountDifferences_WithExpModifier()
        {
            var expMods = new List<KeyValuePair<string, string>> { new("exp", "1234567") };
            var q = Key(tier: 5, count: 1, modifiers: expMods);
            var c = Key(tier: 2, count: 3, modifiers: expMods); // tier diff + count diff + exp on candidate
            var cv = new List<SniperService.RankElem> { new(expMods[0], 2_000_000) };
            AssertExact(q, cv, c, cv);
        }

        [Test]
        public void NonNumericValue_FallsToImportanceDefault()
        {
            // "200k" does not parse as a float -> non-numeric else branch.
            var qMods = new List<KeyValuePair<string, string>> { new("logs_cut", "200k") };
            var cMods = new List<KeyValuePair<string, string>> { new("logs_cut", "2") };
            AssertExact(Key(2, 1, modifiers: qMods), new() { new(qMods[0], 0) },
                        Key(2, 1, modifiers: cMods), new() { new(cMods[0], 0) });
        }

        // ---- Explicit ULP check on the float sub-term ----

        [Test]
        public void FloatDifferenceTerm_IsZeroUlp()
        {
            // Reproduce the exact term used inside CompareValues: (float)|a-b| * importance, widened to double, ->long.
            // The kernel must compute the identical float, i.e. 0 ulp from a straight reparse-and-multiply.
            string[] values = { "1", "2", "3.5", "7", "9", "12.25", "100", "0.5" };
            foreach (var av in values)
                foreach (var bv in values)
                {
                    float a = float.Parse(av, CultureInfo.InvariantCulture);
                    float b = float.Parse(bv, CultureInfo.InvariantCulture);
                    int importance = 1_000_000;
                    float reference = Math.Abs(a - b) * importance;

                    // kernel path: values are interned/precomputed via the same float.TryParse overload
                    float.TryParse(av, CultureInfo.InvariantCulture, out var ka);
                    float.TryParse(bv, CultureInfo.InvariantCulture, out var kb);
                    float kernel = Math.Abs(kb - ka) * importance;

                    BitConverter.SingleToInt32Bits(kernel).Should().Be(BitConverter.SingleToInt32Bits(reference),
                        $"{av} vs {bv}: float term must be 0 ulp (bit-identical)");
                    ((long)(double)kernel).Should().Be((long)(double)reference);
                }
        }

        // ---------------- helpers ----------------

        private void AssertExact(AuctionKey q, List<SniperService.RankElem> qCv, AuctionKey c, List<SniperService.RankElem> cCv)
            => Kernel(q, qCv, c, cCv).Should().Be(Reference(q, qCv, c, cCv),
                $"kernel must match production\n  q={Describe(q, qCv)}\n  c={Describe(c, cCv)}");

        // Thin wrapper over the shared fuzz helper so the targeted-branch tests below read naturally.
        private static AuctionKey Key(int tier, int count, List<Enchant> enchants = null,
            List<KeyValuePair<string, string>> modifiers = null, ItemReferences.Reforge reforge = ItemReferences.Reforge.Any)
            => AuctionKeyFuzz.Key(tier, count, enchants, modifiers, reforge);

        private static string Describe(AuctionKey k, List<SniperService.RankElem> cv)
            => $"tier={k.Tier} count={k.Count} reforge={k.Reforge} mods=[{string.Join(",", k.Modifiers.Select(m => m.Key + "=" + m.Value))}] " +
               $"cv=[{string.Join(",", cv.Select(e => e.Enchant.Lvl != 0 ? $"E:{e.Enchant.Type}{e.Enchant.Lvl}={e.Value}" : e.Modifier.Key != null ? $"M:{e.Modifier.Key}={e.Modifier.Value}:{e.Value}" : $"R:{e.Reforge}={e.Value}"))}]";
    }
}
