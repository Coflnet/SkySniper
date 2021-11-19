using System.Collections.Concurrent;
using System.Threading.Tasks;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services
{
    public interface IPersitanceManager
    {
        Task LoadLookups(SniperService service);
        Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups);
    }
}