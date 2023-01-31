using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using System.Diagnostics;

namespace Coflnet.Sky.Sniper.Services
{
    public class ActiveUpdater
    {
        private Queue<AhStateSumary> RecentUpdates = new Queue<AhStateSumary>();
        private ActivitySource activitySource;
        private SniperService sniper;
        public ActiveUpdater(SniperService sniper, ActivitySource activitySource)
        {
            this.sniper = sniper;
            this.activitySource = activitySource;
        }
        public async Task ProcessSumary(AhStateSumary sum)
        {
            Console.WriteLine("\n-->Consumed update sumary " + sum.Time);
            using var spancontext = activitySource.StartActivity("AhSumaryUpdate");
            if (sum.Time < DateTime.UtcNow - TimeSpan.FromMinutes(5))
                return;
            RecentUpdates.Enqueue(sum);

            if (RecentUpdates.Min(r => r.Time) > DateTime.UtcNow - TimeSpan.FromMinutes(4) || RecentUpdates.Count < 5)
                return;
            var completeLookup = new Dictionary<long, long>();
            foreach (var sumary in RecentUpdates)
            {
                foreach (var item in sumary.ActiveAuctions)
                {
                    completeLookup[item.Key] = item.Value;
                }
            }
            await Task.Yield();

            foreach (var item in sniper.Lookups)
            {
                foreach (var lookup in item.Value.Lookup)
                {
                    if (lookup.Value.Lbins == null)
                        lookup.Value.Lbins = new();
                    foreach (var binAuction in lookup.Value.Lbins.ToList())
                    {
                        if (!completeLookup.ContainsKey(binAuction.AuctionId))
                        {
                            lookup.Value.Lbins.Remove(binAuction);
                        }
                    }
                    lookup.Value.Lbins.Sort(Models.ReferencePrice.Compare);
                }
            }

            if (RecentUpdates.Peek().Time < DateTime.UtcNow - TimeSpan.FromMinutes(5))
                RecentUpdates.Dequeue();

            sniper.PrintLogQueue();
            sniper.FinishedUpdate();
        }
    }
}