using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using NUnit.Framework;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// WS-B bit-exactness gate. Proves the de-LINQ'd / pooled key-extraction pipeline
    /// (<see cref="SniperService.DetailedKeyFromSaveAuction"/> -> <c>SelectValuable</c> / <c>CapKeyLength</c> /
    /// <c>ComparisonValue</c>) produces an <b>identical</b> <see cref="KeyWithValueBreakdown"/> — the <see cref="AuctionKey"/>
    /// at every reduction level (<c>GetReduced(0..3)</c>), the substracted value, AND the per-item value breakdown — vs the
    /// verbatim original kept as <see cref="SniperService.DetailedKeyFromSaveAuctionReference"/> (see
    /// <c>KeyExtraction.Reference.cs</c>).
    ///
    /// Coverage: every real Mock fixture's priced buckets (boots / HYPERION / juju / midas_sword), reconstructed into
    /// SaveAuctions, plus a randomized SaveAuction fuzzer hitting many-modifier / many-enchant items, unlocked_slots
    /// (the in-place-mutation gotcha), attributes (godroll), reforge, pets/exp + TIER_BOOST, recomb (rarity_upgrades),
    /// candyUsed, comma/space substring values, cake years and PET_SPIRIT.
    /// </summary>
    public class KeyExtractionTests
    {
        private SniperService service = null!;

        private class MockCraftCostService : ICraftCostService
        {
            public Dictionary<string, double> Costs { get; } = new();
            public Dictionary<string, Category> ItemCategories { get; set; } = new();
            public void AddCostForSpecialItems() { }
            public bool TryGetCost(string itemId, out double cost) => Costs.TryGetValue(itemId, out cost);
        }

        [SetUp]
        public void Setup()
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            SniperService.MIN_TARGET = 0;
            // Shift the epoch forward so AddLookupData keeps the (historical) loaded references, matching the harness.
            SniperService.StartTime = new DateTime(2021, 9, 25) + TimeSpan.FromDays(10_000);
            var itemService = new HypixelItemService(null, NullLogger<HypixelItemService>.Instance);
            service = new SniperService(itemService, null, NullLogger<SniperService>.Instance, new MockCraftCostService());
        }

        [TearDown]
        public void TearDown() => SniperService.MIN_TARGET = 200_000;

        // ---------- real-fixture parity (production-shaped data) ----------

        [TestCase("boots.json", "WISE_WITHER_BOOTS")]
        [TestCase("HYPERION.json", "HYPERION")]
        [TestCase("juju.json", "JUJU_SHORTBOW")]
        [TestCase("midas_sword.json", "MIDAS_SWORD")]
        public void RealFixtures_BitExact_VsReference(string mockFile, string tag)
        {
            if (!File.Exists($"Mock/{mockFile}"))
                Assert.Ignore($"fixture {mockFile} not present");
            var lookup = LoadLookupMock(mockFile);
            service.AddLookupData(tag, lookup);
            var live = service.Lookups.TryGetValue(tag, out var stored) ? stored : lookup;

            int checked_ = 0, mismatches = 0; string first = "";
            foreach (var key in live.Lookup.Keys.ToList())
            {
                var auction = AuctionFromKey(tag, key);
                if (!Compare(auction, out var why))
                {
                    if (mismatches == 0) first = why;
                    mismatches++;
                }
                checked_++;
            }
            checked_.Should().BeGreaterThan(0, "fixture should yield buckets to check");
            mismatches.Should().Be(0, $"{mockFile}: {mismatches}/{checked_} buckets diverged. First: {first}");
        }

        // ---------- randomized fuzz over every special branch ----------

        [Test]
        public void Fuzz_BitExact_VsReference()
        {
            // Prime a couple of lookups so GetCleanItemValue / ModifierEstimate cross-item paths have data to read,
            // exactly as production would; the parity must hold regardless.
            if (File.Exists("Mock/boots.json"))
                service.AddLookupData("WISE_WITHER_BOOTS", LoadLookupMock("boots.json"));

            string[] tags =
            {
                "WISE_WITHER_BOOTS", "HYPERION", "SOME_SWORD", "PET_SPIRIT", "PET_ENDER_DRAGON",
                "NEW_YEAR_CAKE", "ENCHANTED_BOOK", "PANDORAS_BOX", "ATTRIBUTE_SHARD", "DIVAN_HELMET",
            };

            int mismatches = 0; string first = "";
            for (int seed = 1; seed <= 25_000; seed++)
            {
                var rng = new Random(seed);
                var tag = tags[rng.Next(tags.Length)];
                var auction = RandomAuction(rng, tag);
                if (!Compare(auction, out var why))
                {
                    if (mismatches == 0) first = $"seed={seed} {why}";
                    mismatches++;
                }
            }
            mismatches.Should().Be(0, $"de-LINQ key extraction must equal the reference over 25000 fuzzed auctions. First: {first}");
        }

        // ---------- GetReduced de-LINQ parity (vs the verbatim LINQ reference) ----------

        /// <summary>
        /// Fuzzes the de-LINQ'd <see cref="KeyWithValueBreakdown.GetReduced"/> against the verbatim original
        /// (<see cref="SniperService.GetReducedReference"/>, the <c>.Where(...)</c>+<c>new([..])</c> form) over randomized
        /// breakdowns at every reduction level — including heavy-drop cases (many enchant/modifier breakdown entries,
        /// duplicate types/keys, NeverDrop / InvertedValueKey / AttributeKeys / rarity_upgrades / TIER_BOOST / reforge
        /// entries). Proves the reduced AuctionKey (enchants, modifiers, reforge, tier, count) AND the ValueSubstract are
        /// byte-identical. This is the dedicated gate for the GetReduced rewrite (the RealFixtures/Fuzz tests above run
        /// GetReduced on both sides but use the same — new — method, so they cannot catch a GetReduced regression).
        /// </summary>
        [Test]
        public void GetReduced_BitExact_VsReference()
        {
            int mismatches = 0; string first = "";
            for (int seed = 1; seed <= 50_000; seed++)
            {
                var rng = new Random(seed);
                var bk = RandomBreakdown(rng);
                for (int level = 0; level <= 5; level++)
                {
                    var refKey = SniperService.GetReducedReference(bk, level);
                    var newKey = bk.GetReduced(level);
                    if (!KeyEqual(refKey, newKey, out var kw) || refKey.ValueSubstract != newKey.ValueSubstract)
                    {
                        if (mismatches == 0)
                            first = $"seed={seed} level={level} {kw} subRef={refKey.ValueSubstract} subNew={newKey.ValueSubstract}\n  ref={refKey}\n  new={newKey}";
                        mismatches++;
                    }
                }
            }
            mismatches.Should().Be(0, $"de-LINQ'd GetReduced must equal the verbatim LINQ reference over 50000 fuzzed breakdowns. First: {first}");
        }

        /// <summary>Builds a randomized <see cref="KeyWithValueBreakdown"/>: a Key with random enchants/modifiers/reforge/
        /// tier/count and a ValueBreakdown whose entries (enchant / modifier / reforge RankElems, with deliberate
        /// duplicate types/keys and NeverDrop / InvertedValueKey / Attribute / rarity_upgrades / TIER_BOOST entries) drive
        /// every GetReduced branch and heavy-drop overlap.</summary>
        private static KeyWithValueBreakdown RandomBreakdown(Random rng)
        {
            // small pools that overlap with NeverDrop / InvertedValueKey / AttributeKeys / special-cased keys
            string[] modKeys = { "exp", "rarity_upgrades", SniperService.PetItemKey, "skin", "kills", "edition", "color", "MAGMA_LORD", "FROZEN_BLAZE", "art_of_war_count", "winning_bid" };
            EnchantmentType[] enchTypes = { EnchantmentType.sharpness, EnchantmentType.growth, EnchantmentType.critical, EnchantmentType.ultimate_wise, EnchantmentType.scavenger };
            var attribOver = Constants.AttributeKeys.Take(2).ToArray();

            var enchants = new List<Enchant>();
            int ec = rng.Next(0, 5);
            for (int i = 0; i < ec; i++)
                enchants.Add(new Enchant { Type = enchTypes[rng.Next(enchTypes.Length)], Lvl = (byte)rng.Next(1, 8) });
            var modifiers = new List<KeyValuePair<string, string>>();
            int mc = rng.Next(0, 5);
            for (int i = 0; i < mc; i++)
            {
                var k = modKeys[rng.Next(modKeys.Length)];
                modifiers.Add(new KeyValuePair<string, string>(k, rng.Next(2) == 0 ? SniperService.TierBoostShorthand : rng.Next(1, 5).ToString()));
            }
            var key = new AuctionKey
            {
                Enchants = enchants.AsReadOnly(),
                Modifiers = modifiers.AsReadOnly(),
                Reforge = ReforgePool[rng.Next(ReforgePool.Length)],
                Tier = (Tier)rng.Next(0, 12),
                Count = (byte)rng.Next(1, 6),
            };

            // breakdown: some entries reference the key's enchants/modifiers (so they drop), some don't; include reforge,
            // attribute (>50M to hit the -50M branch), rarity_upgrades, TIER_BOOST and NeverDrop entries.
            var breakdown = new List<SniperService.RankElem>();
            int bc = rng.Next(0, 9);
            for (int i = 0; i < bc; i++)
            {
                switch (rng.Next(6))
                {
                    case 0: // enchant entry (often matching a key enchant -> drop)
                        var et = enchants.Count > 0 && rng.Next(2) == 0 ? enchants[rng.Next(enchants.Count)].Type : enchTypes[rng.Next(enchTypes.Length)];
                        breakdown.Add(new SniperService.RankElem(new Enchant { Type = et, Lvl = (byte)rng.Next(1, 8) }, rng.Next(0, 200) * 1_000_000L));
                        break;
                    case 1: // modifier entry (often matching a key modifier -> drop)
                        var mk = modifiers.Count > 0 && rng.Next(2) == 0 ? modifiers[rng.Next(modifiers.Count)].Key : modKeys[rng.Next(modKeys.Length)];
                        breakdown.Add(new SniperService.RankElem(new KeyValuePair<string, string>(mk, rng.Next(1, 5).ToString()), rng.Next(0, 200) * 1_000_000L) { IsEstimate = rng.Next(2) == 0 });
                        break;
                    case 2: // reforge entry
                        breakdown.Add(new SniperService.RankElem(ReforgePool[rng.Next(ReforgePool.Length)], rng.Next(0, 50) * 1_000_000L));
                        break;
                    case 3: // attribute entry with a large value to hit the >50M attribute branch
                        breakdown.Add(new SniperService.RankElem(new KeyValuePair<string, string>(attribOver.Length > 0 ? attribOver[rng.Next(attribOver.Length)] : "MAGMA_LORD", rng.Next(1, 5).ToString()), 60_000_000L + rng.Next(0, 100) * 1_000_000L) { IsEstimate = rng.Next(2) == 0 });
                        break;
                    case 4: // TIER_BOOST modifier entry
                        breakdown.Add(new SniperService.RankElem(new KeyValuePair<string, string>(SniperService.PetItemKey, SniperService.TierBoostShorthand), rng.Next(0, 50) * 1_000_000L));
                        break;
                    default: // rarity_upgrades modifier entry
                        breakdown.Add(new SniperService.RankElem(new KeyValuePair<string, string>("rarity_upgrades", "1"), rng.Next(0, 50) * 1_000_000L));
                        break;
                }
            }
            return new KeyWithValueBreakdown { Key = key, ValueBreakdown = breakdown, SubstractedValue = rng.Next(-50, 200) * 1_000_000L };
        }

        // ---------- comparison core ----------

        /// <summary>
        /// Runs the reference first, the optimized second (caches are content-addressed, so order does not change the
        /// result), and asserts full equality of the breakdown + the key at every reduction level the snipe/query path
        /// uses (GetReduced 0..3).
        /// </summary>
        private bool Compare(SaveAuction auction, out string why)
        {
            why = "";
            // distinct auction clones so list mutation inside one path can never leak into the other
            var refKey = service.DetailedKeyFromSaveAuctionReference(Clone(auction));
            var newKey = service.ValueKeyForTest(Clone(auction));

            if (refKey.SubstractedValue != newKey.SubstractedValue)
            { why = $"SubstractedValue ref={refKey.SubstractedValue} new={newKey.SubstractedValue} [{Describe(auction)}]"; return false; }
            if (!BreakdownEqual(refKey.ValueBreakdown, newKey.ValueBreakdown, out var bw))
            { why = $"breakdown {bw} [{Describe(auction)}]"; return false; }

            for (int level = 0; level <= 3; level++)
            {
                var a = refKey.GetReduced(level);
                var b = newKey.GetReduced(level);
                if (!KeyEqual(a, b, out var kw))
                { why = $"GetReduced({level}) {kw}\n  ref={a}\n  new={b}\n  [{Describe(auction)}]"; return false; }
                if (a.ValueSubstract != b.ValueSubstract)
                { why = $"GetReduced({level}).ValueSubstract ref={a.ValueSubstract} new={b.ValueSubstract} [{Describe(auction)}]"; return false; }
            }
            return true;
        }

        private static bool KeyEqual(AuctionKey a, AuctionKey b, out string why)
        {
            why = "";
            if (a.Reforge != b.Reforge) { why = $"reforge {a.Reforge}!={b.Reforge}"; return false; }
            if (a.Tier != b.Tier) { why = $"tier {a.Tier}!={b.Tier}"; return false; }
            if (a.Count != b.Count) { why = $"count {a.Count}!={b.Count}"; return false; }
            // Enchants: order-sensitive (production sorts by type via OrderBy; we must preserve the exact sequence)
            if (a.Enchants.Count != b.Enchants.Count) { why = $"ench count {a.Enchants.Count}!={b.Enchants.Count}"; return false; }
            for (int i = 0; i < a.Enchants.Count; i++)
                if (a.Enchants[i].Type != b.Enchants[i].Type || a.Enchants[i].Lvl != b.Enchants[i].Lvl)
                { why = $"ench[{i}] {a.Enchants[i].Type}={a.Enchants[i].Lvl} != {b.Enchants[i].Type}={b.Enchants[i].Lvl}"; return false; }
            // Modifiers: order-sensitive too (key construction must reproduce the exact modifier sequence)
            if (a.Modifiers.Count != b.Modifiers.Count) { why = $"mod count {a.Modifiers.Count}!={b.Modifiers.Count}"; return false; }
            for (int i = 0; i < a.Modifiers.Count; i++)
                if (a.Modifiers[i].Key != b.Modifiers[i].Key || a.Modifiers[i].Value != b.Modifiers[i].Value)
                { why = $"mod[{i}] {a.Modifiers[i].Key}={a.Modifiers[i].Value} != {b.Modifiers[i].Key}={b.Modifiers[i].Value}"; return false; }
            return true;
        }

        private static bool BreakdownEqual(List<SniperService.RankElem> a, List<SniperService.RankElem> b, out string why)
        {
            why = "";
            if ((a == null) != (b == null)) { why = $"null mismatch ref={(a == null)} new={(b == null)}"; return false; }
            if (a == null) return true;
            if (a.Count != b.Count) { why = $"count {a.Count}!={b.Count}"; return false; }
            for (int i = 0; i < a.Count; i++)
            {
                var x = a[i]; var y = b[i];
                if (x.Value != y.Value || x.IsEstimate != y.IsEstimate
                    || x.Enchant.Type != y.Enchant.Type || x.Enchant.Lvl != y.Enchant.Lvl
                    || x.Modifier.Key != y.Modifier.Key || x.Modifier.Value != y.Modifier.Value
                    || x.Reforge != y.Reforge)
                { why = $"[{i}] ref=({x}) new=({y})"; return false; }
            }
            return true;
        }

        // ---------- fuzzer ----------

        // Modifier keys spanning the value branches and the special handlers: the in-place-mutation unlocked_slots /
        // gemstone_slots pair, attributes (godroll), recomb, pet exp + tier-boost, candy, cake years, edition, runes,
        // souls, plain numeric kill counters and a couple of non-key noise modifiers.
        private static readonly string[] ModKeyPool =
        {
            "exp", "candyUsed", "skin", "color", "ability_scroll", SniperService.PetItemKey,
            "kills", "logs_cut", "rarity_upgrades", "winning_bid", "edition", "new_years_cake",
            "unlocked_slots", "gemstone_slots", "captured_player", "art_of_war_count", "dye_item",
            "talisman_enrichment", "mana_disintegrator_count", "MAGMA_LORD", "FROZEN_BLAZE", "heldItem",
            "MINOS_INQUISITOR_750", "mana_pool", "veteran",
        };
        private static readonly EnchantmentType[] EnchPool =
        {
            EnchantmentType.sharpness, EnchantmentType.growth, EnchantmentType.protection,
            EnchantmentType.critical, EnchantmentType.ultimate_wise, EnchantmentType.scavenger,
            EnchantmentType.ultimate_legion, EnchantmentType.snipe,
        };
        private static readonly ItemReferences.Reforge[] ReforgePool =
        {
            ItemReferences.Reforge.Any, ItemReferences.Reforge.Gilded, ItemReferences.Reforge.None,
            ItemReferences.Reforge.jaded, ItemReferences.Reforge.Necrotic, ItemReferences.Reforge.warped_on_aote,
        };

        /// <summary>Realistic value for a given modifier key — production never feeds e.g. "edition" a non-integer, and a
        /// thrown <see cref="FormatException"/> can't be compared, so the fuzzer mirrors the production value shapes per
        /// key while still hitting every special branch (TIER_BOOST only on petItem, comma-substrings on slot lists, etc.).</summary>
        private static string ValueFor(string key, Random rng)
        {
            switch (key)
            {
                case "exp": return new[] { "0", "1000000", "5000000", "25900000", "100", "0.3", "0.6" }[rng.Next(7)];
                case "candyUsed": return new[] { "0", "1", "5", "10" }[rng.Next(4)];
                case "edition": return new[] { "1", "50", "99", "500", "5000", "50000", "120" }[rng.Next(7)];
                case "new_years_cake": return new[] { "1", "9", "69", "120", "400", "420" }[rng.Next(6)];
                case "winning_bid": return new[] { "100", "5000000", "25000000", "100000000" }[rng.Next(4)];
                case "kills":
                case "art_of_war_count":
                case "mana_disintegrator_count":
                case "mana_pool":
                case "veteran": return new[] { "1", "2", "5", "10", "100", "1000" }[rng.Next(6)];
                case "logs_cut": return new[] { "200000", "5", "100" }[rng.Next(3)];
                case "rarity_upgrades": return "1";
                case SniperService.PetItemKey:
                    return rng.Next(2) == 0 ? SniperService.TierBoostShorthand
                        : new[] { "MINING_SPREAD", "TEXTBOOK", "BIGGER_SCROLL" }[rng.Next(3)];
                case "heldItem":
                    return new[] { "PET_ITEM_TIER_BOOST", "MINOS_RELIC", "QUICK_CLAW" }[rng.Next(3)];
                case "unlocked_slots": return new[] { "0", "0,1", "0,1,2", "1,2,3,4", "COMBAT_0,COMBAT_1" }[rng.Next(5)];
                case "gemstone_slots": return new[] { "1", "2", "3", "4" }[rng.Next(4)];
                case "color": return new[] { "0:0:0", "255:128:64", "::" }[rng.Next(3)];
                case "skin": return new[] { "bear", "PANDA", "FROG" }[rng.Next(3)];
                case "dye_item": return new[] { "DYE_HOLLOW", "DYE_NECRON" }[rng.Next(2)];
                case "ability_scroll": return new[] { "IMPLOSION_SCROLL", "WITHER_SHIELD_SCROLL" }[rng.Next(2)];
                case "captured_player": return "abc123";
                case "talisman_enrichment": return new[] { "CRITICAL", "STRENGTH" }[rng.Next(2)];
                case "MAGMA_LORD":
                case "FROZEN_BLAZE": return new[] { "1", "5", "10" }[rng.Next(3)]; // attributes
                case "MINOS_INQUISITOR_750": return new[] { "1", "2", "3" }[rng.Next(3)]; // soul
                default: return new[] { "1", "true", "PERFECT", "5" }[rng.Next(4)];
            }
        }

        private static SaveAuction RandomAuction(Random rng, string tag)
        {
            var enchants = new List<Enchantment>();
            int enchCount = rng.Next(0, 7);
            var usedTypes = new HashSet<EnchantmentType>();
            for (int i = 0; i < enchCount; i++)
            {
                var t = EnchPool[rng.Next(EnchPool.Length)];
                if (!usedTypes.Add(t)) continue;
                enchants.Add(new Enchantment(t, (byte)rng.Next(1, 11)));
            }

            var nbt = new Dictionary<string, string>();
            int modCount = rng.Next(0, 8);
            for (int i = 0; i < modCount; i++)
            {
                var k = ModKeyPool[rng.Next(ModKeyPool.Length)];
                nbt[k] = ValueFor(k, rng);
            }

            var reforge = ReforgePool[rng.Next(ReforgePool.Length)];
            var auction = new SaveAuction
            {
                Tag = tag,
                Tier = (Tier)rng.Next(0, 12),
                Reforge = reforge,
                Count = (byte)rng.Next(1, 6),
                Category = rng.Next(3) == 0 ? Category.ARMOR : Category.UNKNOWN,
                FlatenedNBT = nbt,
                Enchantments = enchants,
                StartingBid = rng.Next(0, 2) == 0 ? 0 : rng.Next(1, 100) * 1_000_000L,
                HighestBidAmount = rng.Next(0, 2) == 0 ? 0 : rng.Next(1, 100) * 1_000_000L,
            };
            // ItemCreatedAt left at default (0001) < UnlockedIntroduction so the unlocked_slots in-place-mutation path runs.
            return auction;
        }

        private static SaveAuction AuctionFromKey(string tag, AuctionKey key)
        {
            var nbt = new Dictionary<string, string>();
            foreach (var m in key.Modifiers)
                nbt[m.Key] = m.Value;
            var a = new SaveAuction
            {
                Tag = tag,
                Tier = key.Tier,
                Reforge = key.Reforge,
                Count = Math.Max((int)key.Count, 1),
                Category = Category.UNKNOWN,
                FlatenedNBT = nbt,
                Enchantments = key.Enchants.Select(e => new Enchantment(e.Type, e.Lvl)).ToList(),
                StartingBid = 1000,
                HighestBidAmount = 1000,
            };
            return a;
        }

        private static SaveAuction Clone(SaveAuction a)
        {
            var nbt = a.FlatenedNBT == null ? null : new Dictionary<string, string>(a.FlatenedNBT);
            var clone = new SaveAuction
            {
                Tag = a.Tag,
                Tier = a.Tier,
                Reforge = a.Reforge,
                Count = a.Count,
                Category = a.Category,
                FlatenedNBT = nbt,
                Enchantments = a.Enchantments == null ? null : a.Enchantments.Select(e => new Enchantment(e.Type, e.Level)).ToList(),
                StartingBid = a.StartingBid,
                HighestBidAmount = a.HighestBidAmount,
                ItemCreatedAt = a.ItemCreatedAt,
                ItemName = a.ItemName,
            };
            return clone;
        }

        private static string Describe(SaveAuction a)
            => $"tag={a.Tag} tier={a.Tier} reforge={a.Reforge} count={a.Count} cat={a.Category} hb={a.HighestBidAmount} " +
               $"nbt=[{string.Join(",", (a.FlatenedNBT ?? new()).Select(m => m.Key + "=" + m.Value))}] " +
               $"ench=[{string.Join(",", (a.Enchantments ?? new()).Select(e => e.Type + ":" + e.Level))}]";

        // ---------- mock loading (mirrors DropOff.Tests.cs) ----------

        private static PriceLookup LoadLookupMock(string mockFileName)
        {
            var text = File.ReadAllText($"Mock/{mockFileName}");
            var parsed = JsonConvert.DeserializeObject<LookupLoad>(text);
            var dict = new Dictionary<AuctionKey, ReferenceAuctions>();
            foreach (var entry in parsed.Lookup)
            {
                AuctionKey key;
                try { key = ParseKey(entry); } catch { continue; }
                dict[key] = entry.Value;
            }
            return new PriceLookup
            {
                Lookup = new System.Collections.Concurrent.ConcurrentDictionary<AuctionKey, ReferenceAuctions>(dict),
                CleanPricePerDay = parsed.CleanPricePerDay,
                CleanKey = parsed.CleanKey
            };
        }

        private static AuctionKey ParseKey(KeyValuePair<string, ReferenceAuctions> e)
        {
            var parts = e.Key.Replace(", ", ",").Split(' ');
            return new AuctionKey
            {
                Enchants = new(parts[0].Split(',').Where(s => !string.IsNullOrWhiteSpace(s)).Select(s =>
                {
                    var eparts = s.Split('=');
                    return new Enchant { Type = Enum.Parse<EnchantmentType>(eparts.First()), Lvl = byte.Parse(eparts.Last()) };
                }).ToArray()),
                Reforge = Enum.Parse<ItemReferences.Reforge>(parts[1]),
                Tier = Enum.Parse<Tier>(parts.Reverse().Skip(1).First()),
                Count = byte.Parse(parts.Last()),
                Modifiers = new(parts.Skip(2).First().Split("],[").Select(s =>
                {
                    var mparts = s.Split(',', 2);
                    return new KeyValuePair<string, string>(mparts.First().TrimStart('['), mparts.Last().TrimEnd(']'));
                }).Where(m => !string.IsNullOrEmpty(m.Key)).ToList())
            };
        }

        public class LookupLoad
        {
            public Dictionary<string, ReferenceAuctions> Lookup { get; set; }
            public Dictionary<short, long> CleanPricePerDay { get; set; } = new();
            public AuctionKey CleanKey { get; set; }
        }
    }
}
