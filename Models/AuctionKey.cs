using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;
using MessagePack;

namespace Coflnet.Sky.Sniper.Models
{
    public class AuctionKeyWithValue : AuctionKey
    {
        public AuctionKeyWithValue(AuctionKey key) : base(key)
        {
        }

        public AuctionKeyWithValue()
        {
        }

        public long ValueSubstract { get; set; }
    }
    [MessagePackObject]
    public class AuctionKey
    {
        [Key(0)]
        public ReadOnlyCollection<Enchant> Enchants { get; init; } = new(new List<Enchant>());
        [Key(1)]
        public ItemReferences.Reforge Reforge;
        [Key(2)]
        public ReadOnlyCollection<KeyValuePair<string, string>> Modifiers = new(new List<KeyValuePair<string, string>>());
        [Key(3)]
        public Tier Tier;
        [Key(4)]
        public byte Count;

        public static ReadOnlyCollection<KeyValuePair<string, string>> EmptyModifiers = new(new List<KeyValuePair<string, string>>());

        public AuctionKey WithEnchants(IEnumerable<Enchant> enchants)
        {
            return new AuctionKey(this)
            {
                Enchants = enchants.ToList().AsReadOnly()
            };
        }

        public bool IsClean()
        {
            return Enchants?.Count == 0 && Modifiers?.Count == 0 && Reforge == ItemReferences.Reforge.Any && Count <= 1;
        }

        /// <summary>
        /// The higher the better
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public int Similarity(AuctionKey key, SniperService service = null, List<SniperService.RankElem> keyvalue = null, List<SniperService.RankElem> self = null)
        {
            if (key == null)
                return -100000;

            if (service != null)
            {
                return SimilarityByMarketPrice(key, keyvalue, self);
            }
            var sum = 0;
            if (this.Reforge == key.Reforge)
                sum++;
            else
                sum--;
            var petDifference = Math.Abs(this.Tier - key.Tier);
            sum -= petDifference * 11;
            if (petDifference > 0 && key.Modifiers.Any(m => m.Key == "exp"))
                sum -= 555; // higher tier is very bad
            if (this.Count == key.Count)
                sum += this.Count;
            if (this.Enchants != null && key.Enchants != null)
            {
                //sum += this.Enchants.Count(e => key.Enchants.Any(k => k.Lvl == e.Lvl && k.Type == e.Type)) * 3;
                sum -= EnchantSimilarity(this.Enchants, key);
                sum -= EnchantSimilarity(key.Enchants, this);
                sum -= EnchantSimilarity(this.Enchants.Where(e => Constants.VeryValuableEnchant.TryGetValue(e.Type, out var lvl) && e.Lvl >= lvl).ToList(), key) * 10;
                sum -= EnchantSimilarity(key.Enchants.Where(e => Constants.VeryValuableEnchant.TryGetValue(e.Type, out var lvl) && e.Lvl >= lvl).ToList(), this) * 10;
            }
            sum -= (this.Enchants?.Count ?? 0) + (key.Enchants?.Count ?? 0);

            if (this.Modifiers != null && key.Modifiers != null)
            {
                //sum += this.Modifiers.Count(m => key.Modifiers.Any(k => k.Key == m.Key && k.Value == m.Value)) * 3;
                sum -= (int)ModifierDifference(key, this.Modifiers);
                sum -= (int)ModifierDifference(this, key.Modifiers);

                sum -= (int)(ModifierDifference(this, key.Modifiers.Where(m => SniperService.VeryValuable.Contains(m.Key)).ToList()) * 10);
                sum -= (int)(ModifierDifference(key, this.Modifiers.Where(m => SniperService.VeryValuable.Contains(m.Key)).ToList()) * 10);

                sum -= (int)ModifierDifference(this, key.Modifiers.Where(m => SniperService.Increadable.Contains(m.Key)).ToList()) * 100;
                sum -= (int)ModifierDifference(key, this.Modifiers.Where(m => SniperService.Increadable.Contains(m.Key)).ToList()) * 100;
            }
            else
                sum -= this.Modifiers?.Count ?? 0 - key.Modifiers?.Count ?? 0;
            sum -= Math.Abs(key.Count - Count);
            return sum;
        }

        private int SimilarityByMarketPrice(AuctionKey key, List<SniperService.RankElem> keyvalue, List<SniperService.RankElem> self)
        {
            var matchValue = 0L;
            matchValue = CompareValues(keyvalue, self, matchValue);
            matchValue = CompareValues(self, keyvalue, matchValue);

            foreach (var item in keyvalue.Where(k => k.Value == 0))
            {
                if (Random.Shared.NextDouble() < 0.1)
                    Console.WriteLine($"Key {item} has no value");
            }

            var petDiffere = Math.Abs(this.Tier - key.Tier);
            matchValue -= petDiffere * 11_000_000;
            if (petDiffere > 0 && key.Modifiers.Any(m => m.Key == "exp"))
                matchValue -= 100_000_000; // higher tier is very bad
            var countDiff = Math.Abs(this.Count - key.Count);
            matchValue -= countDiff * 1_000_000;
            return (int)(matchValue / 100);
        }

        private static long CompareValues(List<SniperService.RankElem> keyvalue, List<SniperService.RankElem> self, long matchValue)
        {
            foreach (var item in keyvalue.Where(k => k.Enchant.Lvl != default))
            {
                var enchMatch = self.FirstOrDefault(k => k.Enchant.Type == item.Enchant.Type);
                if (enchMatch?.Enchant.Lvl == default)
                {
                    matchValue -= item.Value;
                    continue;
                };
                if (enchMatch.Enchant.Lvl == item.Enchant.Lvl)
                    matchValue += enchMatch.Value;
            }
            foreach (var item in keyvalue.Where(k => k.Modifier.Key != default))
            {
                var modMatch = self.FirstOrDefault(k => k.Modifier.Key == item.Modifier.Key);
                var m = item.Modifier;
                if (modMatch == default)
                {
                    matchValue -= item.GetValueOrDefault(ImportanceFactor(m.Key));
                    continue;
                }

                if (modMatch.Modifier.Value == m.Value)
                    matchValue += modMatch.GetValueOrDefault(ImportanceFactor(m.Key));
                else if (modMatch.Modifier.Key == null && float.TryParse(m.Value, CultureInfo.InvariantCulture, out var parsed))
                    matchValue -= modMatch.GetValueOrDefault(Math.Abs(parsed) * ImportanceFactor(m.Key));
                else if (float.TryParse(modMatch.Modifier.Value, CultureInfo.InvariantCulture, out var mValue) && float.TryParse(m.Value, CultureInfo.InvariantCulture, out var value))
                    // numeric difference
                    matchValue -= modMatch.GetValueOrDefault(Math.Abs(mValue - value) * ImportanceFactor(m.Key));
                else
                    matchValue -= modMatch.GetValueOrDefault(ImportanceFactor(m.Key));
            }
            var reforge = keyvalue.FirstOrDefault(k => k.Reforge != default);
            if (reforge != default && reforge.Reforge != default)
            {
                var match = self.FirstOrDefault(k => k.Reforge == reforge.Reforge);
                if (match.Reforge == reforge.Reforge)
                    matchValue += match.Value;
                else
                    matchValue -= match.Value;
            }

            return matchValue;
        }

        private static int ImportanceFactor(string key)
        {
            if (SniperService.VeryValuable.Contains(key))
                return 10 * 1_000_000;
            if (SniperService.Increadable.Contains(key))
                return 100 * 1_000_000;
            return 1 * 1_000_000;
        }

        private int EnchantSimilarity(IEnumerable<Enchant> enchantsToCompare, AuctionKey key)
        {
            return enchantsToCompare.Sum(ench =>
            {
                var match = key.Enchants.FirstOrDefault(k => k.Type == ench.Type);
                if (match.Lvl == 0)
                    return 6;
                if (match.Lvl == ench.Lvl)
                    return -2;
                var multiplier = 1;
                if (match.Lvl > ench.Lvl)
                    multiplier = 2;
                return Math.Abs(match.Lvl - ench.Lvl) * multiplier;
            });
        }

        private static float ModifierDifference(AuctionKey key, IEnumerable<KeyValuePair<string, string>> leftMods)
        {
            return leftMods.Sum(m =>
            {
                var match = key.Modifiers.Where(k => k.Key == m.Key).FirstOrDefault();
                if (match.Key == null)
                    if (float.TryParse(m.Value, CultureInfo.InvariantCulture, out var parsed))
                        return Math.Abs(parsed) * 8;
                    else if (m.Value == SniperService.TierBoostShorthand)
                        return 58; // tier boost is very valuable
                    else
                        return 5 + m.Value.Length;
                if (float.TryParse(match.Value, CultureInfo.InvariantCulture, out var matchValue) && float.TryParse(m.Value, CultureInfo.InvariantCulture, out var value))
                    return Math.Abs(matchValue - value);
                if (match.Value == m.Value)
                    if (m.Key == SniperService.PetItemKey && m.Value == SniperService.TierBoostShorthand)
                        return -28; // tier boost is very valuable
                    else
                        return -2;
                return 3 + Math.Abs(match.Value.Length - m.Value.Length);
            });
        }

        public override int GetHashCode()
        {
            var enchRes = 0x02;
            if (Enchants != null)
                foreach (var item in Enchants)
                {
                    enchRes = enchRes * 31 + item.GetHashCode();
                }
            var modRes = 0x20;
            if (Modifiers != null)
                foreach (var item in Modifiers)
                {
                    modRes = modRes * 31 + (item.Value == null ? 0 : item.Value.GetHashCode());
                }
            return HashCode.Combine(enchRes, Reforge, modRes, Tier, Count);
        }

        public override string ToString()
        {
            return $"{(Enchants == null ? "ne" : string.Join(',', Enchants.Select(m => $"{m.Type}={m.Lvl}")))} {Reforge} {(Modifiers == null ? "nm" : string.Join(',', Modifiers.Select(m => m.ToString())))} {Tier} {Count}";
        }

        public override bool Equals(object obj)
        {
            return obj is AuctionKey key &&
                    (key.Enchants == null && this.Enchants == null || (this.Enchants != null && key.Enchants != null && key.Enchants.SequenceEqual(this.Enchants))) &&
                   Reforge == key.Reforge &&
                   (key.Modifiers == null && this.Modifiers == null || (this.Modifiers != null && key.Modifiers != null && key.Modifiers.SequenceEqual(this.Modifiers))) &&
                   Tier == key.Tier &&
                   Count == key.Count;
        }

        public AuctionKey(List<Enchant> enchants, ItemReferences.Reforge reforge, List<KeyValuePair<string, string>> modifiers, Tier tier, byte count)
        {
            Enchants = enchants?.AsReadOnly();
            Reforge = reforge;
            Modifiers = modifiers?.AsReadOnly();
            Tier = tier;
            Count = count;
        }

        public AuctionKey(AuctionKey key)
        {
            Enchants = key.Enchants?.Select(e => new Enchant() { Lvl = e.Lvl, Type = e.Type }).ToList().AsReadOnly();
            Reforge = key.Reforge;
            Modifiers = key.Modifiers?.Select(m => new KeyValuePair<string, string>(m.Key, m.Value)).ToList().AsReadOnly();
            Tier = key.Tier;
            Count = key.Count;
        }

        public AuctionKey()
        {
        }

        public static bool operator ==(AuctionKey a, AuctionKey b)
        {
            return a?.Equals(b) ?? false;
        }
        public static bool operator !=(AuctionKey a, AuctionKey b)
        {
            return !(a?.Equals(b) ?? true);
        }
    }
}