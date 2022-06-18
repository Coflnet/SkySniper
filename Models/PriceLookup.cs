using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;
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
            return !a.Equals(b);
        }
    }

    [MessagePackObject]
    public class Enchantment
    {
        [Key(0)]
        public Core.Enchantment.EnchantmentType Type;
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
        [Obsolete("replaed by Lbins ")]
        public ReferencePrice LastLbin;
        /// <summary>
        /// Second lowest bin, used if the lowest bin got sold
        /// </summary>
        [Key(3)]
        [Obsolete("replaed by Lbins ", true)]
        public ReferencePrice SecondLbin;
        /// <summary>
        /// The day of the oldest used reference for <see cref="Price"/>
        /// </summary>
        [Key(4)]
        public short OldestRef;
        [Key(5)]
        public List<ReferencePrice> Lbins = new ();
        [IgnoreMember]
        public ReferencePrice Lbin => Lbins.FirstOrDefault();

        [IgnoreMember]
        public float Volume => (float)(References.TryPeek(out ReferencePrice price)
                        ? (float)References.Count / (SniperService.GetCurrentDay() - price.Day + 1)
                        : 0);
    }

    public class ReferenceComparer : IComparer<ReferencePrice>
    {
        public int Compare(ReferencePrice x, ReferencePrice y)
        {
            return x.Price.CompareTo(y.Price);
        }
    }

    [MessagePackObject]
    public struct ReferencePrice
    {
        [Key(0)]
        public long AuctionId { get; set; }
        [Key(1)]
        public int Price { get; set; }
        [Key(2)]
        public short Day { get; set; }
        /// <summary>
        /// first two bytes of the sellerId
        /// used for antimarketmanipulation
        /// </summary>
        [Key(3)]
        public short Seller { get; set; }

        public static ReferenceComparer Compare = new ReferenceComparer();

        public override bool Equals(object obj)
        {
            return obj is ReferencePrice price &&
                   AuctionId == price.AuctionId &&
                   Price == price.Price &&
                   Day == price.Day &&
                   Seller == price.Seller;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(AuctionId, Seller);
        }
    }

}