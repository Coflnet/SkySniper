using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Sniper.Services;
using MessagePack;

namespace Coflnet.Sky.Sniper.Models
{
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
        public ReferencePrice Lbin => Lbins?.FirstOrDefault() ?? default;

        [IgnoreMember]
        public float Volume => (float)(References.TryPeek(out ReferencePrice price)
                        ? (float)References.Count / (SniperService.GetDay() - price.Day + 1)
                        : 0);
    }

}