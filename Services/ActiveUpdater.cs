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
        private readonly Queue<AhStateSumary> RecentUpdates = new Queue<AhStateSumary>();
        private readonly ActivitySource activitySource;
        private readonly SniperService sniper;
        public ActiveUpdater(SniperService sniper, ActivitySource activitySource)
        {
            this.sniper = sniper;
            this.activitySource = activitySource;
        }

        private Task intermediateRefresh;

        public async Task ProcessSumary(AhStateSumary sum)
        {
            // copy the sumary to prevent it from being modified
            sum = sum.Clone();
            Console.WriteLine("\n-->Consumed update sumary " + sum.Time);
            using var spancontext = activitySource.StartActivity("AhSumaryUpdate");
            if (sum.Time < DateTime.UtcNow - TimeSpan.FromMinutes(3))
                return;
            RecentUpdates.Enqueue(sum);

            if (RecentUpdates.Where(r => r != null).Min(r => r.Time) > DateTime.UtcNow - TimeSpan.FromMinutes(3) || RecentUpdates.Count < 4)
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
                            int removed = lookup.Value.Lbins.RemoveAll(l => l.AuctionId == binAuction.AuctionId);
                            Console.WriteLine("Removed inactive " + AuctionService.Instance.GetUuid(binAuction.AuctionId) + " " + removed);
                        }
                    }
                    lookup.Value.Lbins.Sort(Models.ReferencePrice.Compare);
                }
            }


            sniper.PrintLogQueue();
            sniper.FinishedUpdate();

            if (intermediateRefresh == null)
                intermediateRefresh = Task.Run(async () =>
                {
                    // wait till shortly before next api refresh
                    await Task.Delay(TimeSpan.FromSeconds(50));
                    try
                    {
                        sniper.ProcessLbins();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Intermediate refresh failed");
                        Console.WriteLine(e);
                    }
                    intermediateRefresh = null;
                });

            sniper.SummaryUpdate();
            if (RecentUpdates.Peek().Time >= DateTime.UtcNow - TimeSpan.FromMinutes(3))
                return;
            Console.WriteLine("Removing old update data");
            while (RecentUpdates.Count > 4)
            {
                var elem = RecentUpdates.Dequeue();
                elem.ActiveAuctions.Clear();
                elem.ActiveAuctions = null;
            }
        }
    }
}