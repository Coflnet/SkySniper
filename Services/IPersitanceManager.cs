using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services
{
    public interface IPersitanceManager
    {
        Task LoadLookups(SniperService service);
        Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups);
        Task<ConcurrentDictionary<string, AttributeLookup>> GetWeigths();
        Task SaveWeigths(ConcurrentDictionary<string, AttributeLookup> lookups);
        Task<List<KeyValuePair<string, PriceLookup>>> LoadGroup(int groupId);
    }
}