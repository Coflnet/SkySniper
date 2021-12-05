using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTracing.Util;

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

        private ILogger<InternalDataLoader> logger;

        Prometheus.Counter foundFlipCount = Prometheus.Metrics
                    .CreateCounter("sky_sniper_found_flips", "Number of flips found");
        Prometheus.Counter auctionsReceived = Prometheus.Metrics
                    .CreateCounter("sky_sniper_auction_received", "Number of auctions received");

        public InternalDataLoader(SniperService sniper, IConfiguration config, IPersitanceManager persitance, ILogger<InternalDataLoader> logger)
        {
            this.sniper = sniper;
            this.config = config;
            this.persitance = persitance;
            LowPricedAuctionTopic = config["TOPICS:LOW_PRICED"];
            producerConfig = new ProducerConfig
            {
                BootstrapServers = config["KAFKA_HOST"],
                LingerMs = 1
            };
            this.logger = logger;
        }



        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var loadActive = Task.Run(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await LoadActiveAuctions(stoppingToken);
                    await Task.Delay(TimeSpan.FromMinutes(30), stoppingToken);
                }
            });
            Task newAuctions = ConsumeNewAuctions(stoppingToken);
            Task soldAuctions = LoadLookupsAndProcessSells(stoppingToken);

            stoppingToken.Register(() =>
            {
                Console.WriteLine("saving");
                persitance.SaveLookup(sniper.Lookups).Wait();
                Console.WriteLine("saved");

            });


            return Task.WhenAll(newAuctions, soldAuctions, ActiveUpdater(stoppingToken), StartProducer(stoppingToken), loadActive);
        }

        private async Task StartProducer(CancellationToken stoppingToken)
        {

            using var lpp = new ProducerBuilder<string, LowPricedAuction>(producerConfig).SetValueSerializer(hypixel.SerializerFactory.GetSerializer<LowPricedAuction>()).Build();
            sniper.FoundSnipe += flip =>
            {

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

            logger.LogInformation("loaded active auctions, starting consuming");
            await Kafka.KafkaConsumer.Consume<hypixel.SaveAuction>(config["KAFKA_HOST"], config["TOPICS:NEW_AUCTION"], a =>
            {
                if (!a.Bin)
                    return Task.CompletedTask;
                sniper.TestNewAuction(a);
                auctionsReceived.Inc();
                return Task.CompletedTask;
            }, stoppingToken, "sky-sniper");
        }

        private async Task LoadActiveAuctions(CancellationToken stoppingToken)
        {
            // load active auctions
            using (var context = new hypixel.HypixelContext())
            {
                logger.LogInformation("loading active auctions");
                try
                {
                    var topId = (await context.Auctions.MaxAsync(a => a.Id)) - 5_000_000;
                    var active = await context.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments)
                                        .Where(a => a.Id > topId && a.End > DateTime.Now)
                                        .ToListAsync(stoppingToken);
                    foreach (var item in active)
                    {
                        sniper.TestNewAuction(item, false);
                    }
                    logger.LogInformation("finished loading active auctions " + active);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "loading active auctions");
                }
            }
        }

        private async Task ActiveUpdater(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    await Kafka.KafkaConsumer.Consume<AhStateSumary>(hypixel.Program.KafkaHost, config["TOPICS:AH_SUMARY"], async sum =>
                    {
                        Console.WriteLine("\n-->Consumed update sumary " + sum.Time);
                        using var spancontext = GlobalTracer.Instance.BuildSpan("AhSumaryUpdate").StartActive();
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
                                if (!completeLookup.ContainsKey(lookup.Value.SecondLbin.AuctionId))
                                {
                                    lookup.Value.SecondLbin = default;
                                }
                                if (!completeLookup.ContainsKey(lookup.Value.LastLbin.AuctionId))
                                {
                                    lookup.Value.LastLbin = lookup.Value.SecondLbin;
                                    lookup.Value.SecondLbin = default;
                                }
                            }
                        }

                        if (RecentUpdates.Peek().Time < DateTime.Now - TimeSpan.FromMinutes(5))
                            RecentUpdates.Dequeue();

                    }, stoppingToken);
                }
                catch (Exception e)
                {
                    dev.Logger.Instance.Error(e, "processing inactive auctions");
                }
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
            await Kafka.KafkaConsumer.Consume<hypixel.SaveAuction>(config["KAFKA_HOST"], config["TOPICS:SOLD_AUCTION"], async a =>
            {
                sniper.AddSoldItem(a);
                Console.Write("s");
                await saveifreached(a);
            }, stoppingToken, "sky-sniper");
        }

        private static bool saving = false;
        private static int saveCount = 1;
        private async Task saveifreached(hypixel.SaveAuction a)
        {
            if (a.UId % 1000 != 0)
                return;
            Console.WriteLine($"processed 1k {sniper.Lookups.Sum(l => l.Value.Lookup.Count)} {saveCount} -");
            saveCount++;
            if (!saving && saveCount % 10 == 0)
            {
                saving = true;
                Console.WriteLine("consumed sold");
                try
                {
                    await persitance.SaveLookup(sniper.Lookups);
                }
                catch (Exception e)
                {
                    Console.WriteLine("could not save " + e.Message);
                    Console.WriteLine(e.StackTrace);
                }
                await Task.Delay(TimeSpan.FromMinutes(2));
                saving = false;
            }
        }
    }
}