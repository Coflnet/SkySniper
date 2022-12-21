using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Coflnet.Sky.Sniper.Services
{
    public class InternalDataLoader : BackgroundService
    {
        SniperService sniper;
        private IConfiguration config;
        private IPersitanceManager persitance;
        private Queue<AhStateSumary> RecentUpdates = new Queue<AhStateSumary>();
        private string LowPricedAuctionTopic;
        private static ProducerConfig producerConfig;
        private ActivitySource activitySource;

        private ILogger<InternalDataLoader> logger;

        Prometheus.Counter foundFlipCount = Prometheus.Metrics
                    .CreateCounter("sky_sniper_found_flips", "Number of flips found");
        Prometheus.Counter auctionsReceived = Prometheus.Metrics
                    .CreateCounter("sky_sniper_auction_received", "Number of auctions received");
        Prometheus.Counter soldReceived = Prometheus.Metrics
                    .CreateCounter("sky_sniper_sold_received", "Number of sold auctions received");

        public InternalDataLoader(SniperService sniper, IConfiguration config, IPersitanceManager persitance, ILogger<InternalDataLoader> logger, ActivitySource activitySource)
        {
            this.sniper = sniper;
            this.config = config;
            this.persitance = persitance;
            LowPricedAuctionTopic = config["TOPICS:LOW_PRICED"];
            producerConfig = new ProducerConfig
            {
                BootstrapServers = config["KAFKA_HOST"],
                LingerMs = 5
            };
            this.logger = logger;
            this.activitySource = activitySource;
        }



        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var loadActive = Task.Run(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await LoadActiveAuctions(stoppingToken).ConfigureAwait(false);
                    await Task.Delay(TimeSpan.FromMinutes(30), stoppingToken).ConfigureAwait(false);
                }
            });
            Task newAuctions = ConsumeNewAuctions(stoppingToken);
            Task soldAuctions = LoadLookupsAndProcessSells(stoppingToken);
            var sellLoad = LoadSellHistory(stoppingToken);

            stoppingToken.Register(() =>
            {
                Console.WriteLine("saving");
                persitance.SaveLookup(sniper.Lookups).Wait();
                Console.WriteLine("saved");

            });

            await Task.WhenAny(newAuctions, soldAuctions, Task.WhenAll(ActiveUpdater(stoppingToken), StartProducer(stoppingToken), loadActive, sellLoad));
            throw new Exception("at least one task stopped");
        }

        private async Task StartProducer(CancellationToken stoppingToken)
        {

            using var lpp = new ProducerBuilder<string, LowPricedAuction>(producerConfig).SetValueSerializer(SerializerFactory.GetSerializer<LowPricedAuction>()).Build();
            sniper.FoundSnipe += flip =>
            {
                if (flip.Auction.Context != null)
                    flip.Auction.Context["fsend"] = (DateTime.Now - flip.Auction.FindTime).ToString();
                lpp.Produce(LowPricedAuctionTopic, new Message<string, LowPricedAuction>()
                {
                    Key = flip.Auction.Uuid,
                    Value = flip
                });
                foundFlipCount.Inc();
            };
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(2000);
            }
        }


        private async Task ConsumeNewAuctions(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    logger.LogInformation("consuming new ");
                    await Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(config["KAFKA_HOST"], config["TOPICS:NEW_AUCTION"], auctions =>
                    {
                        foreach (var a in auctions)
                        {
                            auctionsReceived.Inc();
                            if (!a.Bin)
                                continue;
                            if (a.Context != null)
                                a.Context["frec"] = (DateTime.Now - a.FindTime).ToString();
                            try
                            {
                                sniper.TestNewAuction(a);
                            }
                            catch (Exception e)
                            {
                                logger.LogError(e, "testing new auction failed");
                            }
                        }
                        return Task.CompletedTask;
                    }, stoppingToken, "sky-sniper").ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "consuming new auction");
                }
            logger.LogError("done with consuming");
        }

        private async Task LoadActiveAuctions(CancellationToken stoppingToken)
        {
            // load active auctions

            logger.LogInformation("loading active auctions");
            try
            {
                int topId = 0;
                using (var context = new HypixelContext())
                {
                    topId = (await context.Auctions.MaxAsync(a => a.Id)) - 5_000_000;
                    var active = context.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments)
                                        .Where(a => a.Id > topId && a.End > DateTime.Now && a.Bin == true)
                                        .AsNoTracking()
                                        .AsAsyncEnumerable();

                    var count = 0;
                    await foreach (var item in active)
                    {
                        sniper.TestNewAuction(item, false);
                        count++;
                    }
                    sniper.FinishedUpdate();
                    logger.LogInformation("finished loading active auctions " + count);
                }

                await Task.Delay(3000);

                using (var context = new HypixelContext())
                {
                    var sold = context.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments)
                                        .Where(a => a.Id > topId + 4_900_000 && a.End < DateTime.Now && a.Bin == true && a.HighestBidAmount > 0)
                                        .AsNoTracking()
                                        .AsAsyncEnumerable();
                    var count = 0;
                    await foreach (var item in sold)
                    {
                        sniper.AddSoldItem(item);
                        count++;
                    }
                    logger.LogInformation("finished loading sold auctions " + count);
                }
            }
            catch (Exception e)
            {
                logger.LogError(e, "loading active auctions");
            }
        }

        private async Task LoadSellHistory(CancellationToken stoppinToken)
        {
            var maxId = 0;
            using (var context = new HypixelContext())
            {
                maxId = context.Auctions.Max(a => a.Id);
            }
            await Task.Delay(TimeSpan.FromMinutes(1), stoppinToken);

            var batchSize = 15_000;
            for (var batchStart = maxId - 10_000_000; batchStart < maxId; batchStart += batchSize)
            {
                try
                {
                    using var context = new HypixelContext();
                    await LoadSellsBatch(context, batchSize, batchStart, stoppinToken);
                }
                catch (System.Exception e)
                {
                    logger.LogError(e, "failed to load sells batch " + batchStart);
                    await Task.Delay(2000);
                }
                await Task.Delay(500);
            }
        }

        private async Task LoadSellsBatch(HypixelContext context, int batchSize, int batchStart, CancellationToken stoppinToken)
        {
            var end = batchStart + batchSize;
            var sold = await context.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments)
                                    .Where(a => a.Id > batchStart && a.Id < end && a.Bin && a.HighestBidAmount > 0)
                                    .AsNoTracking()
                                    .ToListAsync(stoppinToken);
            foreach (var item in sold)
            {
                var references = sniper.GetBucketForAuction(item).References;
                if (!ShouldAuctionBeIncluded(item, references))
                    continue;
                sniper.AddSoldItem(item);
            }
            await Task.Delay(500);
            if ((batchStart / batchSize) % 10 == 0)
                Console.WriteLine($"Loaded batch {batchStart} - {end}");
        }

        /// <summary>
        /// Older auctions are not relevant because they are outdated
        /// </summary>
        /// <param name="item"></param>
        /// <param name="references"></param>
        /// <returns></returns>
        public bool ShouldAuctionBeIncluded(SaveAuction item, ConcurrentQueue<ReferencePrice> references)
        {
            return references.FirstOrDefault().Day < SniperService.GetDay(item.End);
        }

        private async Task ActiveUpdater(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    await Kafka.KafkaConsumer.Consume<AhStateSumary>(Program.KafkaHost, config["TOPICS:AH_SUMARY"], ProcessSumary, stoppingToken);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "processing inactive auctions");
                }
        }

        private async Task ProcessSumary(AhStateSumary sum)
        {

            Console.WriteLine("\n-->Consumed update sumary " + sum.Time);
            using var spancontext = activitySource.StartActivity("AhSumaryUpdate");
            if (sum.Time < DateTime.Now - TimeSpan.FromMinutes(5))
                return;
            RecentUpdates.Enqueue(sum);

            if (RecentUpdates.Min(r => r.Time) > DateTime.Now - TimeSpan.FromMinutes(4))
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

            if (RecentUpdates.Peek().Time < DateTime.Now - TimeSpan.FromMinutes(5))
                RecentUpdates.Dequeue();

            sniper.PrintLogQueue();
            sniper.FinishedUpdate();
        }

        private async Task LoadLookupsAndProcessSells(CancellationToken stoppingToken)
        {
            try
            {
                await persitance.LoadLookups(sniper);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            Console.WriteLine("loaded lookup");
            await Kafka.KafkaConsumer.Consume<SaveAuction>(config["KAFKA_HOST"], config["TOPICS:SOLD_AUCTION"], async a =>
            {
                soldReceived.Inc();
                sniper.AddSoldItem(a);
                if (a.UId % 10 == 0)
                    Console.Write("s");
                await SaveIfReached(a);

            }, stoppingToken, "sky-sniper");
            logger.LogInformation("processing sells stopped");
        }

        private static bool saving = false;
        private static int saveCount = 1;
        private Task SaveIfReached(SaveAuction a)
        {
            if (a.UId % 1000 != 0)
                return Task.CompletedTask;
            Console.WriteLine($"processed 1k {sniper.Lookups.Sum(l => l.Value.Lookup.Count)} {saveCount} -");
            saveCount++;
            if (!saving && saveCount % 20 == 0)
            {
                saving = true;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await persitance.SaveLookup(sniper.Lookups);
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, "could not save ");
                    }
                    await Task.Delay(TimeSpan.FromMinutes(2));
                    saving = false;
                }).ConfigureAwait(false);
            }
            return Task.CompletedTask;
        }
    }
}
