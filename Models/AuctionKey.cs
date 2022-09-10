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
        public List<Enchantment> Enchants;
        [Key(1)]
        public ItemReferences.Reforge Reforge;
        [Key(2)]
        public List<KeyValuePair<string, string>> Modifiers;
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
            sum -= Math.Abs(this.Tier - key.Tier) * 4;
            if (this.Count == key.Count)
                sum += this.Count;
            if (this.Enchants != null && key.Enchants != null)
            {
                sum += this.Enchants.Count(e => key.Enchants.Any(k => k.Lvl == e.Lvl && k.Type == e.Type)) * 3;
            }
            sum -= (this.Enchants?.Count ?? 0) + (key.Enchants?.Count ?? 0);

            if (this.Modifiers != null && key.Modifiers != null)
            {
                sum += this.Modifiers.Count(m => key.Modifiers.Any(k => k.Key == m.Key && k.Value == m.Value)) * 3;
                sum -= (this.Modifiers.Count() + key.Modifiers.Count());

                var valuableCount = this.Modifiers.Where(m => SniperService.VeryValuable.Contains(m.Key)).Count();
                var valuableOtherCount = key.Modifiers.Where(m => SniperService.VeryValuable.Contains(m.Key)).Count();
                var matching = this.Modifiers.Where(m => SniperService.VeryValuable.Contains(m.Key)).Count(m => key.Modifiers.Any(k => k.Key == m.Key && k.Value == m.Value));
                sum += (matching * 3 - valuableCount - valuableOtherCount) * 5;
            }
            else
                sum -= this.Modifiers?.Count ?? 0 - key.Modifiers?.Count ?? 0;
            return sum;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Enchants?.FirstOrDefault()?.Type, Reforge, Modifiers?.FirstOrDefault().Value, Tier, Count);
        }

        public override string ToString()
        {
            return $"{(Enchants == null ? "nm" : string.Join(',', Enchants.Select(m => $"{m.Type}={m.Lvl}")))} {Reforge} {(Modifiers == null ? "nm" : string.Join(',', Modifiers.Select(m => m.ToString())))} {Tier} {Count}";
        }

        public AuctionKey(List<Enchantment> enchants, ItemReferences.Reforge reforge, List<KeyValuePair<string, string>> modifiers, Tier tier, byte count)
        {
            Enchants = enchants;
            Reforge = reforge;
            Modifiers = modifiers;
            Tier = tier;
            Count = count;
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