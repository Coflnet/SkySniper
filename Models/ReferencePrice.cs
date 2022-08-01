using System;
using MessagePack;

namespace Coflnet.Sky.Sniper.Models
{
    [MessagePackObject]
    public struct ReferencePrice
    {
        [Key(0)]
        public long AuctionId { get; set; }
        [Key(1)]
        public long Price { get; set; }
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