using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;

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

        public AuctionKeyWithValue GetReduced(int level)
        {
            if (level == 0)
                return new AuctionKeyWithValue(Key) { ValueSubstract = SubstractedValue };
            IEnumerable<KeyValuePair<string, string>> modifiers = Key.Modifiers;
            IEnumerable<Enchant> enchants = Key.Enchants;
            var modifierchanged = false;
            var enchantChanged = false;
            var reforge = Key.Reforge;
            var tier = Key.Tier;
            var valueSubstracted = SubstractedValue;
            foreach (var item in (ValueBreakdown as IEnumerable<SniperService.RankElem>).Reverse().Take(level))
            {
                if (SniperService.NeverDrop.Contains(item.Modifier.Key))
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
                    modifiers = modifiers.Where(x => x.Key != item.Modifier.Key);
                    modifierchanged = true;
                    // only substract (and save adding) value for keys which are lower when removed
                    if (!SniperService.InvertedValueKey.Contains(item.Modifier.Key))
                        valueSubstracted += item.Value;
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
                Enchants = enchantChanged ? new([.. enchants]) : Key.Enchants,
                Modifiers = modifierchanged ? new([.. modifiers]) : Key.Modifiers,
                Reforge = reforge,
                Tier = tier,
                Count = Key.Count,
                ValueSubstract = valueSubstracted
            };
        }

        public override string ToString()
        {
            return $"{Key} - {SubstractedValue}";
        }
    }
}