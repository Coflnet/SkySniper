using Coflnet.Sky.Core;

namespace Coflnet.Sky.Sniper.Models
{
    public class LogEntry 
    {
        public string Uuid;
        public object Key;
        public long Median;
        public long LBin;
        public float Volume;
        public LowPricedAuction.FinderType Finder;
    }

}