using System;
using System.Collections.Generic;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Sniper.Services
{
    public static class TestExtensions
    {
        private static readonly Random random = new Random(1);
        private static int counter = 0;
        public static SaveAuction Dupplicate(this SaveAuction origin, long highestBid = 0, DateTime end = default)
        {
            return new SaveAuction(origin)
            {
                Uuid = random.Next().ToString(),
                UId = counter++,
                AuctioneerId = random.Next().ToString() + counter++,
                FlatenedNBT = new Dictionary<string, string>(origin.FlatenedNBT),
                Enchantments = origin.Enchantments == null ? null : new(origin.Enchantments),
                HighestBidAmount = highestBid == 0 ? origin.HighestBidAmount : highestBid,
                End = end == default ? origin.End : end
            };
        }
    }
}