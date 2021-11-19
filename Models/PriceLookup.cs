using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using MessagePack;

namespace Coflnet.Sky.Sniper.Models
{
    [MessagePackObject]
    public class PriceLookup
    {
        [Key(0)]
        public ConcurrentDictionary<AuctionKey, ReferenceAuctions> Lookup = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>();
    }

    [MessagePackObject]
    public class AuctionKey
    {
        [Key(0)]
        public List<Enchantment> Enchants;
        [Key(1)]
        public hypixel.ItemReferences.Reforge Reforge;
        [Key(2)]
        public List<KeyValuePair<string, string>> Modifiers;
        [Key(3)]
        public hypixel.Tier Tier;
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

        bool Reached()
        {
            Console.WriteLine("reached");
            return true;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Enchants?.FirstOrDefault()?.Type, Reforge, Modifiers?.FirstOrDefault().Value, Tier, Count);
        }

        public override string ToString()
        {
            return Newtonsoft.Json.JsonConvert.SerializeObject(this);
        }

        public AuctionKey(List<Enchantment> enchants, hypixel.ItemReferences.Reforge reforge, List<KeyValuePair<string, string>> modifiers, hypixel.Tier tier, byte count)
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
    }

    [MessagePackObject]
    public class Enchantment
    {
        [Key(0)]
        public hypixel.Enchantment.EnchantmentType Type;
        [Key(1)]
        public byte Lvl;

        public override bool Equals(object obj)
        {
            return obj is Enchantment ench
                && ench.Lvl == Lvl
                && ench.Type == Type;
        }

        public override int GetHashCode()
        {
            return (int)Type << 4 + Lvl;
        }

        public override string ToString()
        {
            return base.ToString();
        }
    }

    [MessagePackObject]
    public class ReferenceAuctions
    {
        [Key(0)]
        public int Price;
        [Key(1)]
        public ConcurrentQueue<ReferencePrice> References = new ConcurrentQueue<ReferencePrice>();
        [Key(2)]
        public ReferencePrice LastLbin;
        /// <summary>
        /// Second lowest bin, used if the lowest bin got sold
        /// </summary>
        [Key(3)]
        public ReferencePrice SecondLbin;
    }

    [MessagePackObject]
    public struct ReferencePrice
    {
        [Key(0)]
        public long AuctionId;
        [Key(1)]
        public int Price;
        [Key(2)]
        public short Day;
        /// <summary>
        /// first two bytes of the sellerId
        /// used for antimarketmanipulation
        /// </summary>
        [Key(3)]
        public short Seller;
    }

}