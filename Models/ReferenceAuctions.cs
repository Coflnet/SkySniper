using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Sniper.Services;
using MessagePack;
using Newtonsoft.Json;

namespace Coflnet.Sky.Sniper.Models
{
    [MessagePackObject]
    public class ReferenceAuctions
    {
        [Key(0)]
        public long Price;
        [Key(1)]
        public ConcurrentQueue<ReferencePrice> References = new ConcurrentQueue<ReferencePrice>();
        [Key(2)]
        [Obsolete("replaed by Lbins ")]
        [JsonIgnore]
        public ReferencePrice LastLbin;
        /// <summary>
        /// Second lowest bin, used if the lowest bin got sold
        /// </summary>
        [IgnoreMember]
        [Obsolete("replaed by Lbins ", true)]
        [JsonIgnore]
        public ReferencePrice SecondLbin;
        /// <summary>
        /// The day of the oldest used reference for <see cref="Price"/>
        /// </summary>
        [Key(4)]
        public short OldestRef;
        [Key(5)]
        public List<ReferencePrice> Lbins = new();
        [Key(6)]
        public short HitsSinceCalculating = 0;
        [Key(7)]
        public byte Volatility = 0;
        [IgnoreMember]
        [JsonIgnore]
        public ReferencePrice Lbin => Lbins?.FirstOrDefault() ?? default;

        [IgnoreMember]
        public float Volume => (float)(References.TryPeek(out ReferencePrice price)
                        ? (float)References.Count / (SniperService.GetDay() - price.Day + 1)
                        : 0);
    }

}