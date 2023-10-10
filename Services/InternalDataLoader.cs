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
using Coflnet.Kafka;

namespace Coflnet.Sky.Sniper.Services
{
    public class InternalDataLoader : BackgroundService
    {
        SniperService sniper;
        private IConfiguration config;
        private IPersitanceManager persitance;
        private string LowPricedAuctionTopic;
        private ActivitySource activitySource;
        private ActiveUpdater activeUpdater;
        private Kafka.KafkaCreator kafkaCreator;
        private PartialCalcService partialCalcService;
        private IMayorService mayorService;
        public event Action<LowPricedAuction> FoundPartialFlip;

        private ILogger<InternalDataLoader> logger;
        private IProducer<string, LowPricedAuction> FlipProducer;

        Prometheus.Counter foundFlipCount = Prometheus.Metrics
                    .CreateCounter("sky_sniper_found_flips", "Number of flips found");
        Prometheus.Counter auctionsReceived = Prometheus.Metrics
                    .CreateCounter("sky_sniper_auction_received", "Number of auctions received");
        Prometheus.Counter soldReceived = Prometheus.Metrics
                    .CreateCounter("sky_sniper_sold_received", "Number of sold auctions received");

        public InternalDataLoader(
            SniperService sniper,
            IConfiguration config,
            IPersitanceManager persitance,
            ILogger<InternalDataLoader> logger,
            ActivitySource activitySource,
            ActiveUpdater activeUpdater,
            KafkaCreator kafkaCreator,
            PartialCalcService partialCalcService,
            IMayorService mayorService)
        {
            this.sniper = sniper;
            this.config = config;
            this.persitance = persitance;
            LowPricedAuctionTopic = config["TOPICS:LOW_PRICED"];
            this.logger = logger;
            this.activitySource = activitySource;
            this.activeUpdater = activeUpdater;
            this.kafkaCreator = kafkaCreator;
            this.partialCalcService = partialCalcService;
            this.mayorService = mayorService;
        }



        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var loadActive = Task.Run(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await LoadActiveAuctions(stoppingToken).ConfigureAwait(false);
                    await Task.Delay(TimeSpan.FromMinutes(30), stoppingToken).ConfigureAwait(false);
                    await partialCalcService.Load();
                }
            });
            Task soldAuctions = LoadLookupsAndProcessSells(stoppingToken);
            Task newAuctions = ConsumeNewAuctions(stoppingToken);
            var sellLoad = LoadSellHistory(stoppingToken);

            stoppingToken.Register(() =>
            {
                Console.WriteLine("saving");
                //partialCalcService.Save();
                persitance.SaveLookup(sniper.Lookups).Wait();
                Console.WriteLine("saved");

            });

            var result = await Task.WhenAny(newAuctions, soldAuctions,
                Task.WhenAll(ActiveUpdater(stoppingToken),
                             StartProducer(stoppingToken),
                             ConsumeBazaar(stoppingToken),
                             loadActive,
                             sellLoad));
            if (!stoppingToken.IsCancellationRequested)
                throw new Exception("at least one task stopped " + result.Status + " " + result.Exception);
        }

        private async Task StartProducer(CancellationToken stoppingToken)
        {
            await kafkaCreator.CreateTopicIfNotExist(LowPricedAuctionTopic);
            FlipProducer = kafkaCreator.BuildProducer<string, LowPricedAuction>(true, pb => pb);
            sniper.FoundSnipe += flip =>
            {
                Produceflip(flip, FlipProducer);
            };
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(2000);
            }
            FlipProducer.Dispose();
        }

        private void Produceflip(LowPricedAuction flip, IProducer<string, LowPricedAuction> lpp)
        {
            if (flip.Auction.Context != null)
                flip.Auction.Context["fsend"] = (DateTime.UtcNow - flip.Auction.FindTime).ToString();
            lpp.Produce(LowPricedAuctionTopic, new Message<string, LowPricedAuction>()
            {
                Key = flip.Auction.Uuid,
                Value = flip
            });
            foundFlipCount.Inc();
        }

        private async Task ConsumeNewAuctions(CancellationToken stoppingToken)
        {
            while (sniper.State != SniperState.Ready)
                await Task.Delay(1000);
            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    logger.LogInformation("consuming new ");
                    await Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(ConsumerConfig, new string[] { config["TOPICS:NEW_AUCTION"] }, auctions =>
                    {
                        foreach (var a in auctions)
                        {
                            auctionsReceived.Inc();
                            if (!a.Bin)
                                continue;
                            if (a.Context != null)
                                a.Context["frec"] = (DateTime.UtcNow - a.FindTime).ToString();
                            try
                            {
                                sniper.TestNewAuction(a);
                                CheckForPartial(a);
                            }
                            catch (Exception e)
                            {
                                logger.LogError(e, "testing new auction failed " + a.Uuid);
                            }
                        }
                        return Task.CompletedTask;
                    }, stoppingToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "consuming new auction");
                }
            logger.LogError("done with consuming");
        }

        private void CheckForPartial(SaveAuction a)
        {
            var breakdown = partialCalcService.GetPrice(a, true);
            if (breakdown.Price > a.StartingBid * 1.5 && breakdown.Price - a.StartingBid > 3_000_000)
            {
                var flip = new LowPricedAuction()
                {
                    Auction = a,
                    AdditionalProps = new() { { "breakdown", string.Join('\n', breakdown.BreakDown) } },
                    Finder = LowPricedAuction.FinderType.AI,
                    TargetPrice = (long)(breakdown.Price * 0.8)
                };
                Produceflip(flip, FlipProducer);
                FoundPartialFlip?.Invoke(flip);
            }
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
                                        .Where(a => a.Id > topId && a.End > DateTime.UtcNow && a.Bin == true)
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
                                        .Where(a => a.Id > topId + 4_900_000 && a.End < DateTime.UtcNow && a.Bin == true && a.HighestBidAmount > 0)
                                        .AsNoTracking()
                                        .AsAsyncEnumerable();
                    var count = 0;
                    await foreach (var item in sold)
                    {
                        sniper.AddSoldItem(item, true);
                        count++;
                    }
                    logger.LogInformation("finished loading sold auctions " + count);
                }

                foreach (var item in sniper.Lookups)
                {
                    foreach (var bucket in item.Value.Lookup)
                    {
                        // make sure all medians are up to date
                        sniper.UpdateMedian(bucket.Value, (item.Key, bucket.Key));
                    }
                }
            }
            catch (Exception e)
            {
                logger.LogError(e, "loading active auctions");
            }
        }

        private async Task LoadSellHistory(CancellationToken stoppinToken)
        {
            await Task.Delay(TimeSpan.FromSeconds(10), stoppinToken);
            var maxId = 0;
            using (var context = new HypixelContext())
            {
                maxId = await context.Auctions.MaxAsync(a => a.Id);
            }

            var batchSize = 20_000;
            var totalSize = 15_000_000;
            var allStart = maxId - totalSize;
            var differential = 10;
            logger.LogInformation("loading sell history " + allStart + " " + maxId + " " + batchSize);
            // split batches into 10 distributed groups
            for (var i = 0; i < differential; i++)
            {
                await LoadOnepass(maxId, batchSize, allStart, differential, i, stoppinToken);
            }
        }

        private async Task LoadOnepass(int maxId, int batchSize, int allStart, int differential, int i, CancellationToken stoppinToken)
        {
            for (var batchStart = allStart + batchSize * i; batchStart < maxId; batchStart += batchSize * differential)
            {
                try
                {
                    await LoadSellsBatch(batchSize, batchStart, stoppinToken);
                }
                catch (System.Exception e)
                {
                    logger.LogError(e, "failed to load sells batch " + batchStart);
                    await Task.Delay(2000);
                }
            }
            _ = Task.Run(async () =>
            {
                try
                {
                    await partialCalcService.CapAtCraftCost();
                }
                catch (System.Exception e)
                {
                    logger.LogError(e, "capping at craft cost");
                }
            });
            logger.LogInformation($"Loaded {i}/{differential}th of sell history");
            // ready if more than 20% loaded
            if (i >= differential / 5)
            {
                sniper.State = SniperState.Ready;
                UpdateAllMedian();
                await Task.Delay(500);
            }
        }

        public async Task<Dictionary<string, Dictionary<string, double>>> PartialAnalysis(string targetTag, CancellationToken stoppinToken, DateTime totalStart = default)
        {
            var context = new HypixelContext();
            using var scope = logger.BeginScope("partial analysis");
            logger.LogInformation("loading aote from db");
            var id = ItemDetails.Instance.GetItemIdForTag(targetTag);
            if (totalStart == default)
                totalStart = DateTime.UtcNow - TimeSpan.FromDays(300);

            var samples = new List<SaveAuction>();
            for (var start = totalStart; start < DateTime.UtcNow; start += TimeSpan.FromDays(10))
            {
                var end = start + TimeSpan.FromDays(10);
                samples.AddRange(await LoadpartialBatch(context, id, start, end, stoppinToken, samples));
                if (samples.Count > 5000)
                {
                    var commonalityCount = samples.GroupBy(s => s.Tag).ToDictionary(g => g.Key, g => g.Count());
                    samples = samples.OrderBy(s => Random.Shared.NextDouble() / commonalityCount.GetValueOrDefault(s.Tag, 1)).Take(2500).ToList();
                }
            }
            logger.LogInformation(Newtonsoft.Json.JsonConvert.SerializeObject(partialCalcService.GetAttributeCosts(targetTag), Newtonsoft.Json.Formatting.Indented));

            return partialCalcService.GetAttributeCosts(targetTag);
        }

        private async Task<List<SaveAuction>> LoadpartialBatch(HypixelContext context, int id, DateTime start, DateTime end, CancellationToken stoppinToken, List<SaveAuction> lastSample)
        {
            var batch = await context.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments)
                        .Where(a => a.End > start && a.End < end && a.Bin && a.HighestBidAmount > 0 && id == a.ItemId)
                        .Take(20_000)
                        .AsNoTracking()
                        .ToListAsync(stoppinToken);
            Console.WriteLine("applying aote");
            // filter underpriced ones
            batch = batch.Where(s => s.StartingBid != 0).ToList();
            var newSample = batch.OrderBy(s => Random.Shared.NextDouble()).Take(100).ToList();
            batch = batch.Concat(lastSample).ToList();
            const int MillisecondsDelay = 500;
            //ApplyData(sold, 0.2);
            for (int i = 0; i < 5; i++)
            {
                ApplyData(batch, 0.023);
                await Task.Delay(MillisecondsDelay);
            }
            batch = batch.Where(s => s.End > DateTime.UtcNow - TimeSpan.FromDays(30)).ToList();
            if (batch.Count == 0)
                return newSample;
            for (int i = 0; i < 5; i++)
            {
                ApplyData(batch, 0.03);
                await Task.Delay(MillisecondsDelay);
            }
            var lastWeek = batch.Where(s => s.End > DateTime.UtcNow - TimeSpan.FromDays(5)).ToList();
            if (lastWeek.Count > 20)
                batch = lastWeek;
            for (int i = 0; i < 50; i++)
            {
                ApplyData(batch, 0.015);
                await Task.Delay(MillisecondsDelay);
            }
            var recent = batch.Where(s => s.End > DateTime.UtcNow - TimeSpan.FromDays(1))
                .OrderByDescending(s => s.End).Take(50).ToList();
            if (recent.Count > 10)
                for (int i = 0; i < 20; i++)
                {
                    ApplyData(recent, 0.005);
                }
            return newSample;
        }


        private void ApplyData(List<SaveAuction> sold, double v)
        {
            if (v == 1)
                foreach (var item in sold)
                {
                    var bucket = sniper.GetBucketForAuction(item);
                    var references = bucket.auctions;
                    if (!ShouldAuctionBeIncluded(item, references.References))
                        continue;
                    sniper.AddAuctionToBucket(item, true, references, bucket.key.ValueSubstract);
                }
            partialCalcService.SetLearningRate(v);
            Parallel.ForEach(sold, item =>
            {
                try
                {
                    partialCalcService.AddSell(item);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                }
            });
            partialCalcService.CapAtCraftCost();
        }

        private void UpdateAllMedian()
        {
            foreach (var lookup in sniper.Lookups)
            {
                foreach (var item in lookup.Value.Lookup)
                {
                    try
                    {
                        sniper.UpdateMedian(item.Value);
                    }
                    catch (System.Exception e)
                    {
                        logger.LogError(e, "updating median");
                    }
                }
            }
        }

        private async Task LoadSellsBatch(int batchSize, int batchStart, CancellationToken stoppinToken)
        {
            var end = batchStart + batchSize;
            using var context = new HypixelContext();
            var shouldLog = (batchStart % batchSize) % 10 == 1;
            if (shouldLog)
                Console.WriteLine($"Start Loading batch {batchStart} - {end}");
            var sold = await context.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments)
                                    .Where(a => a.Id > batchStart && a.Id < end && a.Bin && a.HighestBidAmount > 0)
                                    .AsNoTracking()
                                    .ToListAsync(stoppinToken);
            if (shouldLog)
                Console.WriteLine($"Loaded batch {batchStart} - {end}");
            foreach (var item in sold)
            {
                var add = sniper.GetBucketForAuction(item);
                var references = add.auctions;
                if (!ShouldAuctionBeIncluded(item, references.References))
                    continue;
                sniper.AddAuctionToBucket(item, true, references, add.key.ValueSubstract);
            }
            partialCalcService.SetLearningRate(0.01);
            foreach (var item in sold)
                partialCalcService.AddSell(item);

            if (shouldLog)
                Console.WriteLine($"Applied batch {batchStart} - {end}");
        }

        /// <summary>
        /// Older auctions are not relevant because they are outdated
        /// </summary>
        /// <param name="item"></param>
        /// <param name="references"></param>
        /// <returns></returns>
        public bool ShouldAuctionBeIncluded(SaveAuction item, ConcurrentQueue<ReferencePrice> references)
        {
            return references.Select(d => d.Day).DefaultIfEmpty(default).Min() < SniperService.GetDay(item.End) || references.Count < 12;
        }

        private async Task ActiveUpdater(CancellationToken stoppingToken)
        {
            await RunTilStopped(
                Kafka.KafkaConsumer.Consume<AhStateSumary>(config, config["TOPICS:AH_SUMARY"], activeUpdater.ProcessSumary, stoppingToken, ConsumerConfig.GroupId, AutoOffsetReset.Latest)
            , stoppingToken);
        }

        private async Task ConsumeBazaar(CancellationToken stoppingToken)
        {
            Console.WriteLine("starting bazaar");
            await RunTilStopped(
                Coflnet.Kafka.KafkaConsumer.ConsumeBatch<dev.BazaarPull>(ConsumerConfig, new string[] { config["TOPICS:BAZAAR"] }, batch =>
                {
                    foreach (var item in batch)
                    {
                        if (item.Timestamp > DateTime.UtcNow - TimeSpan.FromMinutes(1))
                            sniper.UpdateBazaar(item);
                    }
                    return Task.CompletedTask;
                }, stoppingToken, 5)
            , stoppingToken);
            throw new Exception("stopped processing bazaar");
        }

        private ConsumerConfig ConsumerConfig =>
            new ConsumerConfig(KafkaCreator.GetClientConfig(config))
            {
                SessionTimeoutMs = 9_000,
                AutoOffsetReset = AutoOffsetReset.Latest,
                GroupId = System.Net.Dns.GetHostName(),
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };


        private async Task RunTilStopped(Task todo, CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    await todo;
                }
                catch (Exception e)
                {
                    logger.LogError(e, "processing bazaar");
                }
        }

        private async Task LoadLookupsAndProcessSells(CancellationToken stoppingToken)
        {
            try
            {
                await persitance.LoadLookups(sniper);
                await partialCalcService.Load();
            }
            catch (Exception e)
            {
                Console.WriteLine("lookup load failed");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            Console.WriteLine("loaded lookup");
            if (sniper.Lookups.FirstOrDefault().Value?.Lookup?.Select(l => l.Value.References.Count()).FirstOrDefault() > 0)
                sniper.State = SniperState.Ready;
            await Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(ConsumerConfig, new string[] { config["TOPICS:SOLD_AUCTION"] }, async batch =>
            {
                foreach (var a in batch)
                {
                    soldReceived.Inc();
                    sniper.AddSoldItem(a);
                    if (a.UId % 10 == 0)
                        Console.Write("s");
                    await SaveIfReached(a);
                }

            }, stoppingToken, 4);
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