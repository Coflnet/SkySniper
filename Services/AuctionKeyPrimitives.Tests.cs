using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using AwesomeAssertions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R2 Phase 0 / F3 bit-exactness oracle for the cross-cutting <see cref="AuctionKey"/> primitives
    /// (<c>Equals</c> / <c>GetHashCode</c> / <c>ToString</c>). This is the gate workstream <b>R2-E</b>
    /// (zero-alloc primitives, <c>benchmarks/COMPUTE_FLOOR_SPEC_R2.md</c> §2.1 + §5 R2-E) is held to: the de-LINQ'd
    /// production methods must be <b>byte-identical</b> to the snapshot captured in
    /// <see cref="AuctionKeyPrimitivesReference"/> (a verbatim copy of the current impl) over a large fuzzed input set.
    ///
    /// What is gated and how (golden-output capture against the verbatim reference):
    /// <list type="bullet">
    ///   <item><b><see cref="AuctionKey.GetHashCode"/></b> — every fuzzed key's hash equals
    ///     <see cref="AuctionKeyPrimitivesReference.GetHashCodeReference"/>, AND is stable across element re-ordering
    ///     (Equals is order-independent, so the hash must be too — the dictionary-correctness property).</item>
    ///   <item><b><see cref="AuctionKey.ToString"/></b> — every fuzzed key's string equals
    ///     <see cref="AuctionKeyPrimitivesReference.ToStringReference"/> (the <c>MedianKey</c>/log string, 31% of strings).</item>
    ///   <item><b><see cref="AuctionKey.Equals(object)"/></b> (and <c>operator ==</c>/<c>!=</c>) — for every fuzzed PAIR
    ///     the production verdict equals <see cref="AuctionKeyPrimitivesReference.EqualsReference"/>, including the
    ///     null-collection branches, the order-independent set semantics, and reflexivity/symmetry.</item>
    ///   <item>The hash/equality <b>contract invariant</b>: <c>Equals(a,b) =&gt; GetHashCode(a)==GetHashCode(b)</c>,
    ///     asserted directly so a refactor cannot break dictionary lookups.</item>
    /// </list>
    ///
    /// Inputs come from the shared <see cref="AuctionKeyFuzz"/> generator (the same broad distribution every other
    /// bit-exactness harness uses) plus a hand-built set of edge keys (null collections, empty collections, the
    /// permutation/near-miss pairs that exercise the order-independent <c>All</c>/<c>Any</c> set comparison).
    /// </summary>
    public class AuctionKeyPrimitivesTests
    {
        [SetUp]
        public void Setup()
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
        }

        // ---------- single-key primitives: GetHashCode + ToString golden output ----------

        [Test]
        public void GetHashCode_And_ToString_BitExact_VsReference()
        {
            int checkedCount = 0;
            for (int seed = 1; seed <= 40_000; seed++)
            {
                var rng = new Random(seed);
                var (key, _) = AuctionKeyFuzz.RandomKeyAndBreakdown(rng);

                key.GetHashCode().Should().Be(AuctionKeyPrimitivesReference.GetHashCodeReference(key),
                    $"GetHashCode must equal the reference snapshot (seed={seed}, key={key})");
                key.ToString().Should().Be(AuctionKeyPrimitivesReference.ToStringReference(key),
                    $"ToString must equal the reference snapshot (seed={seed})");
                checkedCount++;
            }
            checkedCount.Should().Be(40_000);
        }

        // ---------- pairwise Equals golden output + contract invariant ----------

        [Test]
        public void Equals_BitExact_VsReference_AndHonorsHashContract()
        {
            int checkedPairs = 0, equalPairs = 0, reorderedEqualPairs = 0;
            for (int seed = 1; seed <= 40_000; seed++)
            {
                var rng = new Random(seed);
                var (a, _) = AuctionKeyFuzz.RandomKeyAndBreakdown(rng);
                var (b, _) = AuctionKeyFuzz.RandomKeyAndBreakdown(rng);

                // distinct-key pair (mostly unequal — exercises the unequal branches at every field)
                AssertPair(a, b, $"seed={seed}");
                checkedPairs++;

                // structural clone: forces the fully-equal branch through every field on every iteration.
                var aClone = CloneKey(a);
                AssertPair(a, aClone, $"seed={seed} self-clone");
                a.Equals(aClone).Should().BeTrue($"a key must equal its structural clone (seed={seed}, key={a})");
                equalPairs++; // the clone pair is always equal

                // a genuinely-equal-but-distinct pair built by re-ordering the collections — the high-entropy fuzz keys
                // essentially never collide by chance, so this is how the equal branch's order-independent set semantics
                // (Enchants/Modifiers .All/.Any) are exercised against the reference on non-clone instances.
                var reordered = ShuffleCollections(a, new Random(seed * 17 + 3));
                AssertPair(a, reordered, $"seed={seed} reordered");
                if (a.Equals(reordered)) reorderedEqualPairs++;
            }
            checkedPairs.Should().Be(40_000);
            equalPairs.Should().Be(40_000, "the clone pair exercises the equal branch on every iteration");
            reorderedEqualPairs.Should().Be(40_000,
                "every re-ordering of a key must compare equal (order-independent set semantics), proving the equal branch on distinct instances");
        }

        // ---------- order-independence (the set-semantics Equals depends on) ----------

        [Test]
        public void Permuted_KeysAreEqual_AndHashStable()
        {
            int permuted = 0;
            for (int seed = 1; seed <= 40_000; seed++)
            {
                var rng = new Random(seed);
                var (key, _) = AuctionKeyFuzz.RandomKeyAndBreakdown(rng);
                var shuffled = ShuffleCollections(key, new Random(seed * 31 + 7));

                // a re-ordering of the same enchants/modifiers must compare equal (order-independent set semantics)...
                key.Equals(shuffled).Should().BeTrue($"permuted key must equal original (seed={seed}, key={key})");
                AuctionKeyPrimitivesReference.EqualsReference(key, shuffled).Should().BeTrue();
                (key == shuffled).Should().BeTrue();
                // ...and therefore (hash contract) must hash identically, against BOTH prod and the reference.
                shuffled.GetHashCode().Should().Be(key.GetHashCode(),
                    $"order-independent Equals demands an order-independent hash (seed={seed}, key={key})");
                AuctionKeyPrimitivesReference.GetHashCodeReference(shuffled)
                    .Should().Be(AuctionKeyPrimitivesReference.GetHashCodeReference(key));
                if (key.Enchants.Count > 1 || key.Modifiers.Count > 1) permuted++;
            }
            permuted.Should().BeGreaterThan(0, "fuzz must yield keys with >1 enchant/modifier so permutation actually reorders something");
        }

        // ---------- hand-built edge keys (null / empty collections, near-miss pairs) ----------

        [Test]
        public void EdgeCases_BitExact_VsReference()
        {
            var keys = EdgeKeys();
            for (int i = 0; i < keys.Count; i++)
            {
                var k = keys[i];
                k.GetHashCode().Should().Be(AuctionKeyPrimitivesReference.GetHashCodeReference(k), $"hash edge[{i}]");
                k.ToString().Should().Be(AuctionKeyPrimitivesReference.ToStringReference(k), $"toString edge[{i}]");
            }
            for (int i = 0; i < keys.Count; i++)
                for (int j = 0; j < keys.Count; j++)
                    AssertPair(keys[i], keys[j], $"edge pair [{i}]x[{j}]");

            // explicit operator coverage incl. null operands (operator== short-circuits on null left)
            AuctionKey nullKey = null;
            (nullKey == keys[0]).Should().BeFalse();
            (keys[0] == nullKey).Should().BeFalse();
            keys[0].Equals(null).Should().Be(AuctionKeyPrimitivesReference.EqualsReference(keys[0], null));
        }

        // ---------- core pair assertion: production verdict + ops == reference, both directions ----------

        private static void AssertPair(AuctionKey a, AuctionKey b, string ctx)
        {
            var refAb = AuctionKeyPrimitivesReference.EqualsReference(a, b);
            var refBa = AuctionKeyPrimitivesReference.EqualsReference(b, a);

            a.Equals(b).Should().Be(refAb, $"Equals(a,b) must match reference [{ctx}]\n  a={a}\n  b={b}");
            b.Equals(a).Should().Be(refBa, $"Equals(b,a) must match reference [{ctx}]\n  a={a}\n  b={b}");
            (a == b).Should().Be(refAb, $"operator== must match reference [{ctx}]");
            (a != b).Should().Be(!refAb, $"operator!= must match reference [{ctx}]");

            if (a.Equals(b))
            {
                // symmetry (Equals is defined symmetric) ...
                b.Equals(a).Should().BeTrue($"Equals must be symmetric [{ctx}]");
                // ... and the hash/equality contract: equal keys MUST hash equally (prod and reference).
                a.GetHashCode().Should().Be(b.GetHashCode(),
                    $"equal keys must share a hash code [{ctx}]\n  a={a}\n  b={b}");
                AuctionKeyPrimitivesReference.GetHashCodeReference(a)
                    .Should().Be(AuctionKeyPrimitivesReference.GetHashCodeReference(b), $"reference hash contract [{ctx}]");
            }
        }

        // ---------- helpers ----------

        private static AuctionKey CloneKey(AuctionKey k) => new AuctionKey
        {
            Enchants = k.Enchants == null ? null : new(k.Enchants.ToList()),
            Reforge = k.Reforge,
            Modifiers = k.Modifiers == null ? null : new(k.Modifiers.ToList()),
            Tier = k.Tier,
            Count = k.Count,
        };

        private static AuctionKey ShuffleCollections(AuctionKey k, Random rng)
        {
            var ench = k.Enchants?.OrderBy(_ => rng.Next()).ToList();
            var mods = k.Modifiers?.OrderBy(_ => rng.Next()).ToList();
            return new AuctionKey
            {
                Enchants = ench == null ? null : new(ench),
                Reforge = k.Reforge,
                Modifiers = mods == null ? null : new(mods),
                Tier = k.Tier,
                Count = k.Count,
            };
        }

        private static List<AuctionKey> EdgeKeys()
        {
            var e0 = new Enchant { Type = Enchantment.EnchantmentType.sharpness, Lvl = 5 };
            var e1 = new Enchant { Type = Enchantment.EnchantmentType.growth, Lvl = 3 };
            var m0 = new KeyValuePair<string, string>("exp", "1000000");
            var m1 = new KeyValuePair<string, string>("candyUsed", "1");

            return new List<AuctionKey>
            {
                // null collections (the `Enchants == null` / `Modifiers == null` branches)
                new AuctionKey { Enchants = null, Modifiers = null, Reforge = ItemReferences.Reforge.Any, Tier = Tier.COMMON, Count = 1 },
                new AuctionKey { Enchants = null, Modifiers = new(new List<KeyValuePair<string,string>>()), Reforge = ItemReferences.Reforge.Any, Tier = Tier.COMMON, Count = 1 },
                // empty collections
                AuctionKeyFuzz.Key((int)Tier.COMMON, 1),
                // single element each
                AuctionKeyFuzz.Key((int)Tier.RARE, 2, new List<Enchant> { e0 }, new List<KeyValuePair<string,string>> { m0 }),
                // two-element collections (order-independence + near-miss diffs below)
                AuctionKeyFuzz.Key((int)Tier.EPIC, 1, new List<Enchant> { e0, e1 }, new List<KeyValuePair<string,string>> { m0, m1 }),
                AuctionKeyFuzz.Key((int)Tier.EPIC, 1, new List<Enchant> { e1, e0 }, new List<KeyValuePair<string,string>> { m1, m0 }), // permuted clone of prior
                // near-miss: differs only by enchant level
                AuctionKeyFuzz.Key((int)Tier.EPIC, 1, new List<Enchant> { new Enchant { Type = Enchantment.EnchantmentType.sharpness, Lvl = 6 }, e1 },
                    new List<KeyValuePair<string,string>> { m0, m1 }),
                // near-miss: differs only by reforge
                AuctionKeyFuzz.Key((int)Tier.EPIC, 1, new List<Enchant> { e0, e1 }, new List<KeyValuePair<string,string>> { m0, m1 }, ItemReferences.Reforge.Gilded),
                // near-miss: differs only by tier / count
                AuctionKeyFuzz.Key((int)Tier.LEGENDARY, 1, new List<Enchant> { e0, e1 }, new List<KeyValuePair<string,string>> { m0, m1 }),
                AuctionKeyFuzz.Key((int)Tier.EPIC, 3, new List<Enchant> { e0, e1 }, new List<KeyValuePair<string,string>> { m0, m1 }),
                // near-miss: differs only by a modifier value
                AuctionKeyFuzz.Key((int)Tier.EPIC, 1, new List<Enchant> { e0, e1 },
                    new List<KeyValuePair<string,string>> { new KeyValuePair<string,string>("exp", "2000000"), m1 }),
            };
        }
    }
}
