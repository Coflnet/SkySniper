using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;
using MessagePack;

namespace Coflnet.Sky.Sniper.Models
{
    [MessagePackObject]
    public class AuctionKey
    {
        [Key(0)]
        public List<Enchantment> Enchants = new();
        [Key(1)]
        public ItemReferences.Reforge Reforge;
        [Key(2)]
        public List<KeyValuePair<string, string>> Modifiers = new();
        [Key(3)]
        public Tier Tier;
        [Key(4)]
        public byte Count;

        public override bool Equals(object obj)
        {
            return obj is AuctionKey key &&
                    (key.Enchants == null && this.Enchants == null || (this.Enchants != null && key.Enchants != null && key.Enchants.SequenceEqual(this.Enchants))) &&
                   Reforge == key.Reforge &&
                   (key.Modifiers == null && this.Modifiers == null || (this.Modifiers != null && key.Modifiers != null && key.Modifiers.SequenceEqual(this.Modifiers))) &&
                   Tier == key.Tier &&
                   Count == key.Count;
        }

        public int Similarity(AuctionKey key)
        {
            if (key == null)
                return -100000;
            var sum = 0;
            if (this.Reforge == key.Reforge)
                sum++;
            else
                sum--;
            sum -= Math.Abs(this.Tier - key.Tier) * 10;
            if (this.Tier - key.Tier < 0)
                sum -= 10; // higher tier is very bad
            if (this.Count == key.Count)
                sum += this.Count;
            if (this.Enchants != null && key.Enchants != null)
            {
                sum += this.Enchants.Count(e => key.Enchants.Any(k => k.Lvl == e.Lvl && k.Type == e.Type)) * 3;
                sum += this.Enchants.Where(e => Constants.VeryValuableEnchant.TryGetValue(e.Type, out var lvl) && key.Enchants.Contains(e)).Count() * 10;
                // its important that the other key doesn't have more valuable enchants
                sum -= key.Enchants.Where(e => Constants.VeryValuableEnchant.TryGetValue(e.Type, out var lvl) && !this.Enchants.Contains(e)).Count() * 10;
            }
            sum -= (this.Enchants?.Count ?? 0) + (key.Enchants?.Count ?? 0);

            if (this.Modifiers != null && key.Modifiers != null)
            {
                //sum += this.Modifiers.Count(m => key.Modifiers.Any(k => k.Key == m.Key && k.Value == m.Value)) * 3;
                sum -= ModifierDifference(key, this.Modifiers);
                sum -= ModifierDifference(this, key.Modifiers);

                var veryValuable = this.Modifiers.Where(m => SniperService.VeryValuable.Contains(m.Key)).ToList();
                var valuableCount = veryValuable.Count();
                var valuableOtherCount = key.Modifiers.Where(m => SniperService.VeryValuable.Contains(m.Key)).Count();
                var exactMatching = veryValuable.Count(m => key.Modifiers.Any(k => k.Key == m.Key && k.Value == m.Value));
                var difference = veryValuable.Sum(m =>
                {
                    var match = key.Modifiers.Where(k => k.Key == m.Key).FirstOrDefault();
                    if (match.Key == null)
                        return 5;
                    if (int.TryParse(match.Value, out var matchValue) && int.TryParse(m.Value, out var value))
                        return Math.Abs(matchValue - value);
                    return 0;
                });

                // its important that the other key doesn't have more valuable modifiers
                sum += (exactMatching * 4 - valuableCount * 2 - valuableOtherCount) * 8;
                sum -= difference * 2;
            }
            else
                sum -= this.Modifiers?.Count ?? 0 - key.Modifiers?.Count ?? 0;
            sum -= Math.Abs(key.Count - Count);
            return sum;
        }

        private static int ModifierDifference(AuctionKey key, List<KeyValuePair<string, string>> leftMods)
        {
            return leftMods.Sum(m =>
            {
                var match = key.Modifiers.Where(k => k.Key == m.Key).FirstOrDefault();
                if (match.Key == null)
                    if (int.TryParse(m.Value, out var parsed))
                        return Math.Abs(parsed);
                    else
                        return 4;
                if (int.TryParse(match.Value, out var matchValue) && int.TryParse(m.Value, out var value))
                    return Math.Abs(matchValue - value);
                if (match.Value == m.Value)
                    if (m.Key == SniperService.PetItemKey && m.Value == "TB")
                        return -28; // tier boost is very valuable
                    else
                        return -2;
                return 1;
            });
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Enchants?.FirstOrDefault()?.Type, Reforge, Modifiers?.FirstOrDefault().Value, Tier, Count);
        }

        public override string ToString()
        {
            return $"{(Enchants == null ? "ne" : string.Join(',', Enchants.Select(m => $"{m.Type}={m.Lvl}")))} {Reforge} {(Modifiers == null ? "nm" : string.Join(',', Modifiers.Select(m => m.ToString())))} {Tier} {Count}";
        }

        public AuctionKey(List<Enchantment> enchants, ItemReferences.Reforge reforge, List<KeyValuePair<string, string>> modifiers, Tier tier, byte count)
        {
            Enchants = enchants;
            Reforge = reforge;
            Modifiers = modifiers;
            Tier = tier;
            Count = count;
        }


        public AuctionKey(AuctionKey key)
        {
            Enchants = key.Enchants?.Select(e => new Enchantment() { Lvl = e.Lvl, Type = e.Type }).ToList();
            Reforge = key.Reforge;
            Modifiers = key.Modifiers.Select(m => new KeyValuePair<string, string>(m.Key, m.Value)).ToList();
            Tier = key.Tier;
            Count = key.Count;
        }

        public AuctionKey()
        {
        }

        public static bool operator ==(AuctionKey a, AuctionKey b)
        {
            return a.Equals(b);
        }
        public static bool operator !=(AuctionKey a, AuctionKey b)
        {
            return !(a?.Equals(b) ?? true);
        }
    }

}