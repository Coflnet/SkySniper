using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Sniper.Models
{
    public class KeyWithValueBreakdown
    {
        public AuctionKey Key { get; set; }
        public List<SniperService.RankElem> ValueBreakdown { get; set; }
        public long SubstractedValue { get; set; }
        public static implicit operator AuctionKeyWithValue(KeyWithValueBreakdown key)
        {
            return new AuctionKeyWithValue(key.Key) { ValueSubstract = key.SubstractedValue };
        }

        // [ThreadStatic] scratch for the dropped enchant-types / modifier-keys collected during the reverse breakdown
        // walk. These are transient (consumed inside GetReduced, never stored on the returned key), so a per-worker
        // reused buffer is race-free and zero-alloc — the parse runs on the ingest worker threads, [ThreadStatic] is
        // per-worker. They are cleared on entry to every level>0 call. (The result Enchants/Modifiers lists below are
        // NOT pooled: the dispatch loop retains a reduced key in `lastKey` and compares it against the next reduced key,
        // so two reduced keys coexist — a shared scratch result would alias and corrupt that comparison.)
        [ThreadStatic] private static List<EnchantmentType> _droppedEnchantTypes;
        [ThreadStatic] private static List<string> _droppedModifierKeys;

        public AuctionKeyWithValue GetReduced(int level)
        {
            if (level == 0)
                return new AuctionKeyWithValue(Key) { ValueSubstract = SubstractedValue };
            var reforge = Key.Reforge;
            var tier = Key.Tier;
            var valueSubstracted = SubstractedValue;
            var droppedEnchantTypes = _droppedEnchantTypes ??= new List<EnchantmentType>();
            var droppedModifierKeys = _droppedModifierKeys ??= new List<string>();
            droppedEnchantTypes.Clear();
            droppedModifierKeys.Clear();
            // Walk the last `level` breakdown entries from the end, in reverse — equivalent to Reverse().Take(level)
            // but without the LINQ buffering/allocation (this runs up to 4× per auction). A NeverDrop entry still
            // consumes one of the `level` slots (matching Take), so `taken` is incremented before the skip.
            // De-LINQ: instead of chaining `enchants = enchants.Where(...)` / `modifiers = modifiers.Where(...)` (a Func
            // closure + iterator per dropped item, re-evaluated lazily at materialization), the dropped enchant-Types
            // and modifier-Keys are accumulated into the [ThreadStatic] scratch sets, then compacted in a single pass
            // below. Bit-identical: the original chained `.Where`s keep exactly the elements whose Type/Key matched none
            // of the dropped items (order preserved); set-membership over the same dropped items is the same predicate.
            var breakdown = ValueBreakdown;
            for (int idx = (breakdown?.Count ?? 0) - 1, taken = 0; idx >= 0 && taken < level; idx--)
            {
                var item = breakdown[idx];
                taken++;
                if (SniperService.NeverDrop.Contains(item.Modifier.Key))
                    continue;
                if (item.Enchant.Type != 0)
                {
                    droppedEnchantTypes.Add(item.Enchant.Type);
                    valueSubstracted -= item.Value;
                }
                else if (item.Reforge != ItemReferences.Reforge.None)
                {
                    reforge = ItemReferences.Reforge.Any;
                }
                else
                {
                    var adjustedRemoveValue = item.Value;
                    droppedModifierKeys.Add(item.Modifier.Key);
                    if (adjustedRemoveValue > 50_000_000 && Constants.AttributeKeys.Contains(item.Modifier.Key))
                        adjustedRemoveValue -= 50_000_000;
                    if (item.IsEstimate)
                        adjustedRemoveValue /= 10;
                    // only substract (and save adding) value for keys which are lower when removed
                    if (!SniperService.InvertedValueKey.Contains(item.Modifier.Key))
                        valueSubstracted += adjustedRemoveValue;
                    if (item.Modifier.Key == "rarity_upgrades")
                        tier = SniperService.ReduceRarity(tier);
                    if (item.Modifier.Value == SniperService.TierBoostShorthand)
                    {
                        tier = SniperService.ReduceRarity(tier);
                        valueSubstracted -= item.Value * 2;
                    }
                }
            }
            return new AuctionKeyWithValue()
            {
                Enchants = droppedEnchantTypes.Count > 0 ? CompactEnchants(Key.Enchants, droppedEnchantTypes) : Key.Enchants,
                Modifiers = droppedModifierKeys.Count > 0 ? CompactModifiers(Key.Modifiers, droppedModifierKeys) : Key.Modifiers,
                Reforge = reforge,
                Tier = tier,
                Count = Key.Count,
                ValueSubstract = valueSubstracted
            };
        }

        // Keep every enchant whose Type was not dropped, in the original order (== the chained `.Where(x => x.Type != t)`
        // result). Allocates a fresh ReadOnlyCollection only on the drop path (matching the old `new([.. enchants])`),
        // which is required because the reduced key escapes (retained as `lastKey` in the dispatch loop).
        private static ReadOnlyCollection<Enchant> CompactEnchants(ReadOnlyCollection<Enchant> source, List<EnchantmentType> dropped)
        {
            var result = new List<Enchant>(source.Count);
            for (int i = 0; i < source.Count; i++)
            {
                var e = source[i];
                bool drop = false;
                for (int j = 0; j < dropped.Count; j++)
                    if (dropped[j] == e.Type) { drop = true; break; }
                if (!drop)
                    result.Add(e);
            }
            return result.AsReadOnly();
        }

        // Keep every modifier whose Key was not dropped, in the original order (== the chained `.Where(x => x.Key != k)`).
        private static ReadOnlyCollection<KeyValuePair<string, string>> CompactModifiers(ReadOnlyCollection<KeyValuePair<string, string>> source, List<string> dropped)
        {
            var result = new List<KeyValuePair<string, string>>(source.Count);
            for (int i = 0; i < source.Count; i++)
            {
                var m = source[i];
                bool drop = false;
                for (int j = 0; j < dropped.Count; j++)
                    if (dropped[j] == m.Key) { drop = true; break; }
                if (!drop)
                    result.Add(m);
            }
            return result.AsReadOnly();
        }

        public override string ToString()
        {
            return $"{Key} - {SubstractedValue}";
        }
    }
}