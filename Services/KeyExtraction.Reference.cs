using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// Bit-exactness oracle for WS-B (zero-alloc / de-LINQ key extraction).
    ///
    /// These <c>*Reference</c> methods are <b>verbatim copies of the original (pre-optimization) key-extraction
    /// pipeline</b> — <see cref="SniperService.DetailedKeyFromSaveAuction"/>, <c>SelectValuable</c>, <c>CapKeyLength</c>
    /// (with its inner <c>SortCombined</c>/<c>GetOrdered</c>), <c>ComparisonValue</c> and <c>AddReforgeValue</c>. They are
    /// retained <b>only</b> as the oracle the de-LINQ'd production methods are fuzzed against (see
    /// <c>KeyExtraction.Tests.cs</c>); do not call them from production. The post-processing in
    /// <see cref="DetailedKeyFromSaveAuctionReference"/> (rarity/tier/attribute handling) is identical to production —
    /// only the three hot helpers were rewritten, so only those need a reference twin.
    ///
    /// Everything they touch that is shared, read-only service state (caches, lookups, mapper, itemService) is used as-is
    /// so the reference path produces exactly what production produced before WS-B. The caches are content-addressed by
    /// the same keys, so running the reference path first does not change what the optimized path computes.
    /// </summary>
    public partial class SniperService
    {
        internal KeyWithValueBreakdown DetailedKeyFromSaveAuctionReference(SaveAuction auction, bool fastMode = false, int limit = 5)
        {
            var shouldIncludeReforge = Constants.RelevantReforges.Contains(auction.Reforge);
            long valueSubstracted = 0;
            bool removedRarity = false;
            List<RankElem> rankElems = [];
            List<Enchant> enchants;
            List<KeyValuePair<string, string>> modifiers;
            (enchants, modifiers) = SelectValuableReference(auction, fastMode);

            (valueSubstracted, removedRarity, shouldIncludeReforge, rankElems) = CapKeyLengthReference(enchants, modifiers, auction, limit);

            if (enchants == null)
                enchants = new List<Enchant>();
            var tier = auction.Tier;
            if (auction.Tag == "ENCHANTED_BOOK")
            {
                // rarities don't matter for enchanted books and often used for scamming
                tier = Tier.UNCOMMON;
            }
            if (auction.Tag == "PANDORAS_BOX")
                // pandoras box tier gets set based on the player
                tier = Tier.COMMON;
            // Reduce tier if PET_ITEM_TIER_BOOST was present (it's now removed from key)
            if (auction.FlatenedNBT?.TryGetValue("heldItem", out var heldItemValue) == true && heldItemValue == "PET_ITEM_TIER_BOOST")
            {
                tier = ReduceRarity(tier);
            }
            if (removedRarity)
            {
                tier = ReduceRarity(tier);
            }
            var reducedEnchants = RemoveNoEffectEnchants(auction, enchants);
            if (reducedEnchants.Count < enchants.Count)
            {
                rankElems = rankElems.Where(r => r.Enchant.Type == default || reducedEnchants.Any(re => re.Type == r.Enchant.Type)).ToList();
                enchants = reducedEnchants;
            }
            if (auction.Tag != null && AttributeToIgnoreOnLookup.TryGetValue(auction.Tag, out var ignore))
            {
                modifiers.RemoveAll(m => ignore.Contains(m.Key));
            }
            if (modifiers.Any(m => m.Key == "rarity_upgrades") && !Constants.DoesRecombMatter(auction.Category, auction.Tag))
            {
                modifiers.RemoveAll(m => m.Key == "rarity_upgrades");
                if (!IsRune(auction.Tag))
                    tier = ReduceRarity(tier);
            }
            // Remove PET_ITEM_TIER_BOOST if it somehow got into modifiers (it shouldn't be in the key)
            modifiers.RemoveAll(m => m.Value == "TIER_BOOST");

            return Constructkey(auction, enchants, modifiers, shouldIncludeReforge, valueSubstracted, rankElems, tier);
        }

        private (List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers) SelectValuableReference(SaveAuction auction, bool fastMode = false)
        {
            var enchants = auction.Enchantments
                            ?.Where(e => MinEnchantMap.TryGetValue(e.Type, out byte value) && e.Level >= value)
                            .OrderBy(e => e.Type)
                            .Select(e => new Models.Enchant() { Lvl = e.Level, Type = e.Type }).ToList();
            var flatNbt = auction.FlatenedNBT;

            var modifiers = new Dictionary<string, string>(5);
            if (flatNbt != null)
                foreach (var item in flatNbt)
                {
                    if (!IncludeKeys.Contains(item.Key) && item.Value != "PERFECT" && !IsRune(item.Key) && !IsSoul(item))
                    {
                        continue;
                    }
                    var normalized = NormalizeData(item, auction.Tag, flatNbt);
                    if (normalized.Key != Ignore.Key)
                        modifiers.Add(normalized.Key, normalized.Value);
                }
            if (auction.ItemCreatedAt < UnlockedIntroduction
                // safe guard for when the creation date is wrong
                && flatNbt != null
                && !flatNbt.ContainsKey("unlocked_slots"))
            {
                var allUnlockable = itemService?.GetUnlockableSlots(auction.Tag).ToList();
                if (flatNbt.TryGetValue("gemstone_slots", out var countString) && int.TryParse(countString, out var count))
                {
                    allUnlockable = allUnlockable.Take(count).ToList();
                    modifiers.Remove("gemstone_slots");
                }
                if (allUnlockable?.Count > 0)
                    modifiers.Add("unlocked_slots", string.Join(",", allUnlockable.OrderBy(s => s)));
            }
            var result = modifiers.ToList();
            return (enchants, result);
        }

        private (long valueSubstracted, bool removedRarity, bool includeReforge, List<RankElem> ranked) CapKeyLengthReference(
            List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers, SaveAuction auction, long threshold = 500000, int elements = 5)
        {
            long underlyingItemValue = GetCleanItemValue(auction, ref threshold);
            long valueSubstracted = HandleGems(modifiers, auction);
            IEnumerable<RankElem> combined = ComparisonValueReference(enchants, modifiers, auction.Tag, auction.FlatenedNBT);

            bool includeReforge = AddReforgeValueReference(auction.Reforge, ref combined);
            combined = SortCombined(combined);

            var modifierSum = underlyingItemValue + combined?.Select(m => m.IsEstimate ? m.Value / 20 : m.Value).DefaultIfEmpty(0).Sum() ?? 0;
            threshold = Math.Max(threshold, modifierSum / 22);
            var percentDiff = (double)auction.HighestBidAmount / modifierSum;
            if (auction.HighestBidAmount == 0 || percentDiff > 1)
                percentDiff = 1;
            // remove all but the top 5
            List<RankElem> toRemove = GetItemsToRemove(threshold, combined);
            bool removedRarity = false;
            foreach (var item in toRemove)
            {
                // use percentage of full value
                var adjustedRemoveValue = (long)(item.Value * percentDiff);
                // remove all but the top 5
                if (item.Enchant.Type != 0)
                {
                    if (enchants.Remove(item.Enchant))
                        valueSubstracted += adjustedRemoveValue;
                }
                else if (item.Reforge != ItemReferences.Reforge.None)
                {
                    includeReforge = false;
                }
                else
                {
                    if (item.Modifier.Key == "exp")
                        continue; // even if its valued at very little this needs to stay
                    if (adjustedRemoveValue > 50_000_000 && Constants.AttributeKeys.Contains(item.Modifier.Key))
                        adjustedRemoveValue -= 50_000_000;
                    if (item.IsEstimate)
                        adjustedRemoveValue /= 10;
                    if (modifiers.Remove(item.Modifier))
                        valueSubstracted += adjustedRemoveValue;
                    if (item.Modifier.Key == "skin")
                        modifiers.RemoveAll(m => m.Key == "candyUsed");
                    if (item.Modifier.Key == "rarity_upgrades")
                        removedRarity = true;
                }
            }
            List<RankElem> ordered = GetOrdered(elements, combined);
            return (valueSubstracted, removedRarity, includeReforge, ordered);

            static IEnumerable<RankElem> SortCombined(IEnumerable<RankElem> combined)
            {
                var list = combined as ICollection<RankElem> ?? combined.ToList();
                var filtered = new List<RankElem>(list.Count);
                foreach (var c in list)
                {
                    if (c.Value != 0)
                        filtered.Add(c);
                }
                filtered.Sort((a, b) => b.Value.CompareTo(a.Value));
                return filtered;
            }

            static List<RankElem> GetOrdered(int elements, IEnumerable<RankElem> combined)
            {
                return combined.Where(c => c.Value == 0).Concat(combined.Take(elements)).ToList();
            }

            // Verbatim copy of the original GetItemsToRemove (kept local so the reference does not depend on the
            // production helper, whose signature WS-B changed to a List<RankElem>).
            static List<RankElem> GetItemsToRemove(long threshold, IEnumerable<RankElem> combined)
            {
                var toRemove = new List<RankElem>(5);
                int i = 0;
                foreach (var c in combined)
                {
                    if ((i >= 5 && c.Value > 0)
                        || (i >= 1 && i < 5 && c.Value > 0 && c.Value < threshold)
                        || (i < 1 && c.Value > 0 && (c.Value < 500_000 || c.Value < threshold / 4)))
                    {
                        toRemove.Add(c);
                    }
                    i++;
                }
                return toRemove;
            }
        }

        bool AddReforgeValueReference(ItemReferences.Reforge reforge, ref IEnumerable<RankElem> combined)
        {
            bool includeReforge = Constants.RelevantReforges.Contains(reforge);
            if (includeReforge)
            {
                if (ReforgeValueLookup.TryGetValue(reforge, out var value))
                {
                    combined = combined.Append(value.Item1);
                    return includeReforge;
                }
                long reforgeValue = GetReforgeValue(reforge);
                var element = new RankElem(reforge, reforgeValue);
                combined = combined.Append(element);
                if (reforgeValue > 0)
                    ReforgeValueLookup[reforge] = (element, DateTime.UtcNow);
            }

            return includeReforge;
        }

        internal IEnumerable<RankElem> ComparisonValueReference(IEnumerable<Enchant> enchants, IList<KeyValuePair<string, string>> modifiers, string tag, Dictionary<string, string> flatNbt)
        {
            var valuePerEnchant = enchants?.Select(item => new RankElem(item, mapper.EnchantValue(new Core.Enchantment(item.Type, item.Lvl), null, BazaarPrices, tag)));

            List<RankElem> valuePerModifier = null;
            if (modifiers != null)
            {
                var relevant = new Dictionary<string, string>(modifiers.Count);
                foreach (var rm in modifiers)
                    relevant.TryAdd(rm.Key, rm.Value);

                valuePerModifier = new List<RankElem>(modifiers.Count);
                for (int mi = 0; mi < modifiers.Count; mi++)
                {
                    var m = modifiers[mi];
                    try
                    {
                        var lookupKey = new ModifierLookupKey() { ItemTag = tag, Modifier = m, RelevantModifiers = relevant };
                        if (ModifierValueLookup.TryGetValue(lookupKey, out var value))
                        {
                            valuePerModifier.Add(value.Item1);
                            continue;
                        }
                        var calculated = ModifierEstimate(modifiers, tag, flatNbt, m);
                        if (calculated.Value > 0)
                            ModifierValueLookup[lookupKey] = (calculated, DateTime.UtcNow);
                        valuePerModifier.Add(calculated);
                    }
                    catch (Exception e)
                    {
                        logger.LogInformation($"Error when calculating value for {m.Key} {m.Value} {tag}\n" + e);
                        valuePerModifier.Add(new RankElem(m, 0));
                    }
                }
            }
            IEnumerable<RankElem> combined = null;
            if (valuePerEnchant != null && valuePerModifier != null)
                combined = valuePerEnchant.Concat(valuePerModifier);
            else if (valuePerEnchant != null)
                combined = valuePerEnchant;
            else if (valuePerModifier != null)
                combined = valuePerModifier;
            return combined;
        }

        /// <summary>
        /// Verbatim copy of the original (pre-de-LINQ) <see cref="KeyWithValueBreakdown.GetReduced"/> — the LINQ
        /// <c>.Where(...)</c> + <c>new([.. ...])</c> form. Oracle for the de-LINQ'd production GetReduced; fuzzed against
        /// it in <c>KeyExtraction.Tests.cs</c> over randomized breakdowns / heavy-drop cases (test-only, Release-excluded).
        /// </summary>
        internal static AuctionKeyWithValue GetReducedReference(KeyWithValueBreakdown self, int level)
        {
            if (level == 0)
                return new AuctionKeyWithValue(self.Key) { ValueSubstract = self.SubstractedValue };
            IEnumerable<KeyValuePair<string, string>> modifiers = self.Key.Modifiers;
            IEnumerable<Enchant> enchants = self.Key.Enchants;
            var modifierchanged = false;
            var enchantChanged = false;
            var reforge = self.Key.Reforge;
            var tier = self.Key.Tier;
            var valueSubstracted = self.SubstractedValue;
            var breakdown = self.ValueBreakdown;
            for (int idx = (breakdown?.Count ?? 0) - 1, taken = 0; idx >= 0 && taken < level; idx--)
            {
                var item = breakdown[idx];
                taken++;
                if (NeverDrop.Contains(item.Modifier.Key))
                    continue;
                if (item.Enchant.Type != 0)
                {
                    enchants = enchants.Where(x => x.Type != item.Enchant.Type);
                    valueSubstracted -= item.Value;
                    enchantChanged = true;
                }
                else if (item.Reforge != ItemReferences.Reforge.None)
                {
                    reforge = ItemReferences.Reforge.Any;
                }
                else
                {
                    var adjustedRemoveValue = item.Value;
                    modifiers = modifiers.Where(x => x.Key != item.Modifier.Key);
                    modifierchanged = true;
                    if (adjustedRemoveValue > 50_000_000 && Constants.AttributeKeys.Contains(item.Modifier.Key))
                        adjustedRemoveValue -= 50_000_000;
                    if (item.IsEstimate)
                        adjustedRemoveValue /= 10;
                    if (!InvertedValueKey.Contains(item.Modifier.Key))
                        valueSubstracted += adjustedRemoveValue;
                    if (item.Modifier.Key == "rarity_upgrades")
                        tier = ReduceRarity(tier);
                    if (item.Modifier.Value == TierBoostShorthand)
                    {
                        tier = ReduceRarity(tier);
                        valueSubstracted -= item.Value * 2;
                    }
                }
            }
            return new AuctionKeyWithValue()
            {
                Enchants = enchantChanged ? new([.. enchants]) : self.Key.Enchants,
                Modifiers = modifierchanged ? new([.. modifiers]) : self.Key.Modifiers,
                Reforge = reforge,
                Tier = tier,
                Count = self.Key.Count,
                ValueSubstract = valueSubstracted
            };
        }
    }
}
