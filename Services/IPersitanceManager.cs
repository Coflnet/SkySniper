using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
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
        Task<Dictionary<string, double>> LoadCraftCost();
        // Generic blob storage helpers for persisting models or other binary state
        Task SaveBlob(string key, Stream data);
        Task<Stream> LoadBlob(string key);
        Task FlushDueGroups(ConcurrentDictionary<string, PriceLookup> lookups, TimeSpan maxAge, CancellationToken cancellationToken = default);
    }
}