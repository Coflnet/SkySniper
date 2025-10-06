using System.Collections.Concurrent;
using MessagePack;

namespace Coflnet.Sky.Sniper.Services;

[MessagePackObject]
public class AttributeLookup
{
    [Key(0)]
    public ConcurrentDictionary<string, ConcurrentDictionary<string, double>> Values = new();
}
