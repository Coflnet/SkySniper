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
using Newtonsoft.Json;

namespace Coflnet.Sky.Sniper.Services
{
    public class InternalDataLoader : BackgroundService
    {
        readonly SniperService sniper;
        private readonly IConfiguration config;
        private readonly IPersitanceManager persitance;
        private readonly string LowPricedAuctionTopic;
        private readonly ActivitySource activitySource;
        private readonly ActiveUpdater activeUpdater;
        private readonly Kafka.KafkaCreator kafkaCreator;
        private readonly PartialCalcService partialCalcService;
        private readonly IMayorService mayorService;
        public event Action<LowPricedAuction> FoundPartialFlip;
        public readonly ISelfLearningFlipFinderService flipFinder;
        public readonly ICraftCostService craftCostService;
        private readonly ItemDetails itemDetails;

        private readonly ILogger<InternalDataLoader> logger;
        private IProducer<string, LowPricedAuction> FlipProducer;
        private readonly SemaphoreSlim persistenceLock = new(1, 1);
        // R2-PAR: long-lived tag-shard dispatcher that processes each Kafka batch across N worker threads. Created once
        // in ConsumeNewAuctions (not per batch) and disposed on shutdown. See ShardedAuctionDispatcher for the
        // one-worker-per-resolved-group-tag invariant that makes the parallel snipe set bit-identical to single-threaded.
        private ShardedAuctionDispatcher snipeDispatcher;

        readonly Prometheus.Counter foundFlipCount = Prometheus.Metrics
                    .CreateCounter("sky_sniper_found_flips", "Number of flips found");
        readonly Prometheus.Counter auctionsReceived = Prometheus.Metrics
                    .CreateCounter("sky_sniper_auction_received", "Number of auctions received");
        readonly Prometheus.Counter soldReceived = Prometheus.Metrics
                    .CreateCounter("sky_sniper_sold_received", "Number of sold auctions received");

        public InternalDataLoader(
            SniperService sniper,
            IConfiguration config,
            IPersitanceManager persitance,
            ILogger<InternalDataLoader> logger,
            ActivitySource activitySource,
            ActiveUpdater activeUpdater,
            Kafka.KafkaCreator kafkaCreator,
            PartialCalcService partialCalcService,
            IMayorService mayorService,
            ItemDetails itemDetails,
            ISelfLearningFlipFinderService flipFinder,
            ICraftCostService craftCostService)
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
            this.itemDetails = itemDetails;
            this.flipFinder = flipFinder;
            this.craftCostService = craftCostService;
        }



        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var bazaarConsume = ConsumeBazaar(stoppingToken);
            Task soldAuctions = LoadLookupsAndProcessSells(stoppingToken);
            Task newAuctions = ConsumeNewAuctions(stoppingToken);
            var sellLoad = LoadSellHistory(stoppingToken);
            var dueGroupFlush = FlushDueGroups(stoppingToken);

            var loadActive = Task.Run(async () =>
            {
                // wait shortly to let lookups load first
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                while (!stoppingToken.IsCancellationRequested)
                {
                    await LoadActiveAuctions(stoppingToken).ConfigureAwait(false);
                    await Task.Delay(TimeSpan.FromMinutes(30), stoppingToken).ConfigureAwait(false);
                    await partialCalcService.Load();
                }
            });

            stoppingToken.Register(() =>
            {
                Console.WriteLine("saving");
                //partialCalcService.Save();
                persitance.SaveLookup(sniper.Lookups).Wait();
                Console.WriteLine("saved");

            });
            await sniper.Init();

            var result = await Task.WhenAny(newAuctions, soldAuctions,
                Task.WhenAll(ActiveUpdater(stoppingToken),
                             StartProducer(stoppingToken),
                             bazaarConsume,
                             loadActive,
                             sellLoad,
                             dueGroupFlush));
            if (!stoppingToken.IsCancellationRequested)
                throw new Exception("at least one task stopped " + result.Status + " " + result.Exception);
        }

        private bool ShouldProduceFound()
        {
            return !(bool.TryParse(config["NO_PRODUCE"], out var noProduce) && noProduce);
        }

        private async Task StartProducer(CancellationToken stoppingToken)
        {
            await kafkaCreator.CreateTopicIfNotExist(LowPricedAuctionTopic);
            FlipProducer = kafkaCreator.BuildProducer<string, LowPricedAuction>(true, pb => pb);
            if (ShouldProduceFound())
            {
                Console.WriteLine("starting producer\n-----------------");
                sniper.FoundSnipe += flip =>
                {
                    Produceflip(flip, FlipProducer);
                };
            }
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

        /// <summary>
        /// Resolves the number of shard workers for <see cref="snipeDispatcher"/>. Configurable via
        /// <c>SNIPER:SHARDS</c>; otherwise defaults to the host's logical processor count (the dispatcher scales with
        /// cores, capped only by hot-tag imbalance). Always at least 1.
        /// </summary>
        private int ResolveShardCount()
        {
            if (int.TryParse(config["SNIPER:SHARDS"], out var configured) && configured >= 1)
                return configured;
            return Math.Max(1, Environment.ProcessorCount);
        }

        private async Task ConsumeNewAuctions(CancellationToken stoppingToken)
        {
            while (sniper.State < SniperState.Ready)
                await Task.Delay(1000);

            // R2-PAR: create the tag-shard dispatcher ONCE (not per batch) and reuse it for the lifetime of the
            // consumer. Each Kafka batch is fanned out across the workers by resolved group tag; the same tag always
            // lands on the same worker so its PriceLookup is never mutated concurrently (snipe set bit-identical to
            // single-threaded — proven by the SNIPE_REPLAY_SHARDS parity harness).
            var shardCount = ResolveShardCount();
            snipeDispatcher = new ShardedAuctionDispatcher(sniper, shardCount);
            logger.LogInformation("snipe dispatcher started with {shards} shard workers (cores: {cores})",
                shardCount, Environment.ProcessorCount);
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                    try
                    {
                        logger.LogInformation("consuming new ");
                        await Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(ConsumerConfig, new string[] { config["TOPICS:NEW_AUCTION"] }, auctions =>
                        {
                            // Phase 1: select the auctions this batch will actually process (bins only) and stamp
                            // receive-latency context, preserving the original per-auction bookkeeping. We materialize
                            // the kept list so we can (a) feed it to the dispatcher and (b) run CheckForPartial over it
                            // afterwards on this (consumer) thread.
                            var kept = new List<SaveAuction>();
                            foreach (var a in auctions)
                            {
                                auctionsReceived.Inc();
                                if (!a.Bin)
                                    continue;
                                if (a.Context != null)
                                    a.Context["frec"] = (DateTime.UtcNow - a.FindTime).ToString();
                                kept.Add(a);
                            }

                            // WS-D: measure the intra-batch parse duplication on the REAL kept Kafka batch (re-lists /
                            // stack-splits put many same-content auctions in one batch). distinct-vs-total here is the
                            // genuine production dup number the synthetic replay cannot represent. Pure telemetry behind
                            // SNIPER_BATCH_DUP_COUNT; off in production by default (one HashSet build per batch).
                            if (SniperService.BatchDupCount)
                                SniperService.MeasureBatchDup(kept);

                            // Phase 2: process the whole batch across the shard workers and BLOCK until every kept
                            // auction has been fully processed (TestNewAuction returned). This callback must not return
                            // before the batch fully drains: KafkaConsumer commits the offset immediately after the
                            // callback completes (EnableAutoCommit=false, manual Commit per batch), so returning early
                            // would commit an offset for auctions still in flight — breaking at-least-once. On
                            // cancellation ProcessBatchAndWait throws OperationCanceledException, which propagates out of
                            // the callback so the offset is NOT committed and Kafka re-delivers the batch (no loss).
                            try
                            {
                                snipeDispatcher.ProcessBatchAndWait(kept, stoppingToken);
                            }
                            catch (OperationCanceledException)
                            {
                                throw; // shutdown: do not commit this batch
                            }
                            catch (Exception e)
                            {
                                // A worker faulted on some auction in the batch. Log and let the offset commit (the
                                // single-threaded path also swallowed per-auction failures); the snipe set for the
                                // other auctions is correct because each tag is serialized on its own worker.
                                logger.LogError(e, "shard worker failed processing a new-auction batch");
                            }

                            // Phase 3: CheckForPartial runs HERE — on the consumer thread, after the batch has fully
                            // drained — NOT on the shard workers. Rationale (threading decision): CheckForPartial calls
                            // the ML flip finder (flipFinder.EstimateAsync) and writes to the shared Kafka FlipProducer
                            // via Produceflip; neither is contended-safe to run from N worker threads, and its blocking
                            // .GetAwaiter().GetResult() would stall a shard worker (serializing that worker's whole tag
                            // queue and killing throughput scaling). It is also OFF the snipe-finding path the dispatcher
                            // parallelizes, so keeping it single-threaded here preserves both its original semantics and
                            // the bit-exact snipe-set parity. It runs strictly after drain, so prices it reads from the
                            // sniper reflect this batch's writes — identical ordering to the original foreach.
                            foreach (var a in kept)
                            {
                                try
                                {
                                    CheckForPartial(a);
                                }
                                catch (Exception e)
                                {
                                    logger.LogError(e, "CheckForPartial failed " + a.Uuid);
                                }
                            }
                            return Task.CompletedTask;
                        }, stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break; // clean shutdown
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, "consuming new auction");
                    }
            }
            finally
            {
                // Clean shutdown: drain any items already enqueued on the workers (Dispose joins the threads after
                // CompleteAdding) so nothing in flight is dropped or double-processed, then release the threads/queues.
                snipeDispatcher?.Dispose();
                snipeDispatcher = null;
            }
            logger.LogError("done with consuming");
        }

        private void CheckForPartial(SaveAuction a)
        {
            if (!flipFinder.IsRelevantItem(a.Tag))
                return;
            var cflip = SaveAuctionExtensions.ToComplicatedFlip(a, includeBreakdown: true, sniper: sniper, mayorService: mayorService, craftCostService: craftCostService);
            var estimate = flipFinder.EstimateAsync(cflip).GetAwaiter().GetResult();
            if (estimate == null)
                return;
            // Exclude candyUsed from the attribute sum cap — its weight is a pricing signal
            // for the ML model, not an actual coin value. Including it inflates the cap
            // and causes cheap pets to be reported as flips.
            var attrSum = cflip.AttributeValues
                .Where(a => !a.Key.StartsWith("candyUsed:", StringComparison.OrdinalIgnoreCase))
                .Sum(a => a.Value);
            var value = Math.Min(estimate.EstimatedValue, attrSum);

            // Log suspicious cases where the AI estimate greatly exceeds the attribute sum cap
            // These are likely items with attributes that default to high estimate values
            if (estimate.EstimatedValue > attrSum * 3 && estimate.EstimatedValue > 50_000_000)
            {
                logger.LogWarning("AI overvaluation detected for {Tag} ({Uuid}): estimate={Estimate:F0}, attrSumCap={AttrSum}, bid={Bid}, attrs=[{Attrs}]",
                    a.Tag, a.Uuid, estimate.EstimatedValue, attrSum, a.StartingBid,
                    string.Join(", ", cflip.AttributeValues.Select(kv => $"{kv.Key}={kv.Value}")));
            }

            if (value > a.StartingBid * 1.1 && value - a.StartingBid > 3_000_000)
            {
                logger.LogInformation("found potential ai flip for {content} {metadata}", JsonConvert.SerializeObject(estimate), JsonConvert.SerializeObject(cflip));
                var flip = new LowPricedAuction()
                {
                    Auction = a,
                    AdditionalProps = new() { { "samples", JsonConvert.SerializeObject(estimate)}, {"cflip", JsonConvert.SerializeObject(cflip)} },
                    Finder = LowPricedAuction.FinderType.AI,
                    TargetPrice = (long)(value * 0.9)
                };
                Produceflip(flip, FlipProducer);
                FoundPartialFlip?.Invoke(flip);
            }
        }

        private async Task LoadActiveAuctions(CancellationToken stoppingToken)
        {
            if (DateTime.UtcNow < new DateTime(2025, 2, 6))
                return; // database issues, don't disable auctions
            logger.LogInformation("loading active auctions");
            try
            {
                int topId = 0;
                HashSet<long> ActiveLookup = new HashSet<long>();
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
                        ActiveLookup.Add(item.UId);
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
                        ActiveLookup.Remove(item.UId);
                    }
                    logger.LogInformation("finished loading sold auctions " + count);
                }
                foreach (var item in sniper.Lookups.SelectMany(l => l.Value.Lookup))
                {
                    foreach (var auction in item.Value.Lbins.ToList())
                    {
                        if (!ActiveLookup.Contains(auction.AuctionId))
                        {
                            item.Value.Lbins.Remove(auction);
                            logger.LogInformation("removed inactive " + auction.AuctionId);
                        }
                    }
                }

                UpdateAllMedian();
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

            var batchSize = 10_000;
            var totalSize = RetrainService.IsManager ? 20_000_000 : 5_000_000;
            var allStart = maxId - totalSize;
            var differential = 10;
            logger.LogInformation("loading sell history " + allStart + " " + maxId + " " + batchSize);
            // split batches into 10 distributed groups
            for (var i = 0; i < differential; i++)
            {
                await LoadOnepass(maxId, batchSize, allStart, differential, i, stoppinToken);
            }
            if (RetrainService.IsManager)
            {
                try
                {
                    await persitance.SaveLookup(sniper.Lookups);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "could not save ");
                }
            }
            sniper.State = SniperState.FullyLoaded;
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
            if (i > differential / 5)
            {
                UpdateAllMedian();
                sniper.State = SniperState.Ready;
                await Task.Delay(500);
            }
        }

        public async Task<Dictionary<string, Dictionary<string, double>>> PartialAnalysis(string targetTag, CancellationToken stoppinToken, DateTime totalStart = default)
        {
            var context = new HypixelContext();
            using var scope = logger.BeginScope("partial analysis");
            logger.LogInformation("loading aote from db");
            var id = itemDetails.GetItemIdForTag(targetTag);
            if (totalStart == default)
                totalStart = DateTime.UtcNow - TimeSpan.FromDays(100);

            var samples = new List<SaveAuction>();
            var tickSize = TimeSpan.FromDays(2);
            for (var start = totalStart; start < DateTime.UtcNow; start += tickSize)
            {
                var end = start + tickSize;
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
                        .Take(10_000)
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
                await ApplyData(batch, 0.023);
                await Task.Delay(MillisecondsDelay);
            }
            batch = batch.Where(s => s.End > DateTime.UtcNow - TimeSpan.FromDays(30)).ToList();
            if (batch.Count == 0)
                return newSample;
            for (int i = 0; i < 5; i++)
            {
                await ApplyData(batch, 0.03);
                await Task.Delay(MillisecondsDelay);
            }
            var lastWeek = batch.Where(s => s.End > DateTime.UtcNow - TimeSpan.FromDays(5)).ToList();
            if (lastWeek.Count > 20)
                batch = lastWeek;
            for (int i = 0; i < 50; i++)
            {
                await ApplyData(batch, 0.015);
                await Task.Delay(MillisecondsDelay);
            }
            var recent = batch.Where(s => s.End > DateTime.UtcNow - TimeSpan.FromDays(1))
                .OrderByDescending(s => s.End).Take(50).ToList();
            if (recent.Count > 10)
                for (int i = 0; i < 20; i++)
                {
                    await ApplyData(recent, 0.005);
                }
            return newSample;
        }


        private async Task ApplyData(List<SaveAuction> sold, double v)
        {
            if (v == 1)
                foreach (var item in sold)
                {
                    AddHistoricalSoldAuction(item);
                }
            partialCalcService.SetLearningRate(v);
            Parallel.ForEach(sold, async item =>
            {
                try
                {
                    await partialCalcService.AddSell(item);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                }
            });
            if (v > 0.01 && sold.Count > 1000)
                await partialCalcService.CapAtCraftCost();
        }

        private void AddHistoricalSoldAuction(SaveAuction item)
        {
            var bucket = sniper.GetBucketForAuction(item);
            if (!ShouldAuctionBeIncluded(item, bucket.auctions.References))
                return;

            sniper.AddSoldItem(item, true);
        }

        private void UpdateAllMedian()
        {
            foreach (var lookup in sniper.Lookups)
            {
                foreach (var item in lookup.Value.Lookup)
                {
                    try
                    {
                        sniper.UpdateMedian(item.Value, (lookup.Key, sniper.GetBreakdownKey(item.Key, lookup.Key)));
                    }
                    catch (System.Exception e)
                    {
                        logger.LogError(e, "updating median");
                    }
                }
            }
            logger.LogInformation("updated all median");
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
                AddHistoricalSoldAuction(item);
            }
            partialCalcService.SetLearningRate(0.01);
            foreach (var item in sold)
                await partialCalcService.AddSell(item);

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
            return references.Select(d => d.Day).DefaultIfEmpty(default).Min() < SniperService.GetDay(item.End) || references.Count < 15;
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
                Kafka.KafkaConsumer.ConsumeBatch<dev.BazaarPull>(ConsumerConfig, [config["TOPICS:BAZAAR"]], batch =>
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
            new(KafkaCreator.GetClientConfig(config))
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
            while (true)
                try
                {
                    await persitance.LoadLookups(sniper);
                    await partialCalcService.Load();
                    break;
                }
                catch (Exception e)
                {
                    Console.WriteLine("lookup load failed");
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                    await Task.Delay(2000, stoppingToken);
                }
            Console.WriteLine("loaded lookup");
            if (sniper.Lookups.FirstOrDefault().Value?.Lookup?.Select(l => l.Value.References.Count()).FirstOrDefault() > 0 && sniper.State < SniperState.Ready)
                sniper.State = SniperState.LadingLookup;
            await Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(ConsumerConfig, new string[] { config["TOPICS:SOLD_AUCTION"] }, async batch =>
            {
                foreach (var a in batch)
                {
                    soldReceived.Inc();
                    var listingTime = sniper.AddSoldItem(a);
                    if (a.UId % 10 == 0)
                        Console.Write("s");
                    await SaveIfReached(a);
                    if (listingTime > 2)
                    {
                        await partialCalcService.AddSell(a);
                    }
                }

            }, stoppingToken, 4);
            logger.LogInformation("processing sells stopped");
        }

        private int saveCount = 1;
        private Task SaveIfReached(SaveAuction a)
        {
            if (a.UId % 1000 != 0)
                return Task.CompletedTask;
            Console.WriteLine($"processed 1k {sniper.Lookups.Sum(l => l.Value.Lookup.Count)} {saveCount} -");
            if (!RetrainService.IsManager)
                return Task.CompletedTask; // only manager saves
            saveCount++;
            if (saveCount % 20 == 0)
                QueuePersistenceWork(() => persitance.SaveLookup(sniper.Lookups), "could not save ");
            return Task.CompletedTask;
        }

        private async Task FlushDueGroups(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (sniper.Lookups.IsEmpty)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                        continue;
                    }

                    var maxAge = RetrainService.IsManager ? TimeSpan.FromHours(1) : TimeSpan.FromHours(6);
                    var failureMessage = RetrainService.IsManager
                        ? "could not flush manager groups"
                        : "could not flush worker fallback groups";
                    QueuePersistenceWork(() => persitance.FlushDueGroups(sniper.Lookups, maxAge, stoppingToken), failureMessage);
                    await Task.Delay(TimeSpan.FromMinutes(10) + TimeSpan.FromSeconds(Random.Shared.Next(0, 60)), stoppingToken);
                }
                catch (TaskCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception e)
                {
                    logger.LogError(e, "processing persisted group flush schedule");
                    await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                }
            }
        }

        private void QueuePersistenceWork(Func<Task> persistWork, string errorMessage)
        {
            _ = Task.Run(async () =>
            {
                if (!await persistenceLock.WaitAsync(0).ConfigureAwait(false))
                    return;

                try
                {
                    await persistWork().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    logger.LogError(e, errorMessage);
                }
                finally
                {
                    persistenceLock.Release();
                }
            });
        }
    }
}