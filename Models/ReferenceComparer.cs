using System.Collections.Generic;

namespace Coflnet.Sky.Sniper.Models
{
    public class ReferenceComparer : IComparer<ReferencePrice>
    {
        public int Compare(ReferencePrice x, ReferencePrice y)
        {
            return x.Price.CompareTo(y.Price);
        }
    }

}