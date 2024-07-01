using System.Collections.Generic;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Sniper.Services
{
    public partial class SniperService
    {
        public class RankElem
        {
            public long Value { get; set; }
            public Models.Enchant Enchant { get; set; }
            public KeyValuePair<string, string> Modifier { get; set; }
            public ItemReferences.Reforge Reforge { get; set; }
            public bool IsEstimate { get; set; }

            public long GetValueOrDefault(double defaultVal)
            {
                if (Value == 0)
                    return (long)defaultVal;
                return Value;
            }

            public override string ToString()
            {
                return $"{Enchant.Type}-{Enchant.Lvl} {Modifier} {Value}";
            }

            public RankElem(Models.Enchant enchant, long value)
            {
                Enchant = enchant;
                Value = value;
            }

            public RankElem(KeyValuePair<string, string> modifier, long value)
            {
                Modifier = modifier;
                Value = value;
            }

            public RankElem(ItemReferences.Reforge reforge, long value)
            {
                Reforge = reforge;
                Value = value;
            }
        }
    }
}
