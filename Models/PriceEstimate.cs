namespace Coflnet.Sky.Sniper.Models
{
    public class PriceEstimate
    {
        public ReferencePrice Lbin { get; set; }
        public ReferencePrice SLbin { get; set; }
        public int Median { get; set; }
        public float Volume { get; set; }
        public int FourthPercentile { get; set; }
    }

}