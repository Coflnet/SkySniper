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
            var modifiers = Key.Modifiers.ToList();
            var enchants = Key.Enchants.ToList();
            var reforge = Key.Reforge;
            var tier = Key.Tier;
            var valueSubstracted = SubstractedValue;
            foreach (var item in (ValueBreakdown as IEnumerable<SniperService.RankElem>).Reverse().Take(level))
            {
                if(SniperService.NeverDrop.Contains(item.Modifier.Key))
                    continue;
                if (item.Enchant.Type != 0)
                {
                    if (enchants.Remove(item.Enchant))
                        valueSubstracted += item.Value;
                }
                else if (item.Reforge != ItemReferences.Reforge.None)
                {
                    reforge = ItemReferences.Reforge.Any;
                }
                else
                {
                    if (modifiers.Remove(item.Modifier))
                        // only substract (and save adding) value for keys which are lower when removed
                        if(!SniperService.InvertedValueKey.Contains(item.Modifier.Key))
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
                Enchants = new(enchants),
                Modifiers = new(modifiers),
                Reforge = reforge,
                Tier = tier,
                Count = Key.Count,
                ValueSubstract = valueSubstracted
            };
        }

        public string ToString()
        {
            return $"{Key} - {SubstractedValue}";
        }
    }
}