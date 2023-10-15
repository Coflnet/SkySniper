namespace Coflnet.Sky.Sniper.Models
{
    public class PriceEstimate
    {
        public ReferencePrice Lbin { get; set; }
        public long Median { get; set; }
        public float Volume { get; set; }
        public string LbinKey { get; set; }
        public string MedianKey { get; set; }
        public string ItemKey { get; set; }
    }
}