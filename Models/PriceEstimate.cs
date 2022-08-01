namespace Coflnet.Sky.Sniper.Models
{
    public class PriceEstimate
    {
        public ReferencePrice Lbin { get; set; }
        public ReferencePrice SLbin { get; set; }
        public long Median { get; set; }
        public float Volume { get; set; }
        public long FourthPercentile { get; set; }
        public string LbinKey { get; set; }
        public string MedianKey { get; set; }
    }
}