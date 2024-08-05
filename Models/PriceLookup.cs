using System.Collections.Concurrent;
using System.Collections.Generic;
using Coflnet.Sky.Core;
using MessagePack;

namespace Coflnet.Sky.Sniper.Models
{
    [MessagePackObject]
    public class PriceLookup
    {
        [Key(0)]
        public ConcurrentDictionary<AuctionKey, ReferenceAuctions> Lookup = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>(1, 3);
        /// <summary>
        /// What category this item would be in the AH
        /// </summary>
        [Key(1)]
        public Category Category;
        [Key(2)]
        public float Volume;

        [Key(3)]
        public Dictionary<short, long> CleanPricePerDay = new();
        /// <summary>
        /// Clean key (like dropped, usually with highest volume)
        /// </summary>
        [Key(4)]
        public AuctionKey CleanKey;
    }

}