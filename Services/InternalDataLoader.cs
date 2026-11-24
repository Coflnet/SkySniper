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
        public event Action<LowPricedAuction> FoundPartialFlip;
        public readonly IAiFinder aiFinder;
        // new-auction batches the shard workers are processing; the AI finder stands down while there is one
        private int batchesInFlight;

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
            IAiFinder aiFinder)
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
            this.aiFinder = aiFinder;
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
                             ProcessCancelled(stoppingToken),
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
            aiFinder.Start(flip =>
            {
                Produceflip(flip, FlipProducer);
                FoundPartialFlip?.Invoke(flip);
            }, IsProcessingNewAuctions);
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

        /// <summary>True while the shard workers process a batch of new auctions</summary>
        internal bool IsProcessingNewAuctions() => Volatile.Read(ref batchesInFlight) > 0;

        /// <summary>Processes a batch on the shard workers and counts it as in flight meanwhile</summary>
        internal void ProcessInFlight(Action process)
        {
            Interlocked.Increment(ref batchesInFlight);
            try
            {
                process();
            }
            finally
            {
                Interlocked.Decrement(ref batchesInFlight);
            }
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
                await RunTilStopped("new auctions", backoff => Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(
                    ConsumerConfig, new string[] { config["TOPICS:NEW_AUCTION"] },
                    auctions => ProcessNewAuctionBatch(auctions, backoff, stoppingToken), stoppingToken), stoppingToken).ConfigureAwait(false);
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

        private Task ProcessNewAuctionBatch(IEnumerable<SaveAuction> auctions, ConsumeBackoff backoff, CancellationToken stoppingToken)
        {
            // Phase 1: select the auctions this batch will actually process (bins only) and stamp
            // receive-latency context, preserving the original per-auction bookkeeping. We materialize
            // the kept list to feed it to the dispatcher.
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
                ProcessInFlight(() => snipeDispatcher.ProcessBatchAndWait(kept, stoppingToken));
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

            // The AI finder is not run here: the sniper hands every tested auction to the finder's own queue
            // (SniperService.AuctionTested), and its background worker values it and publishes flips, see StartProducer.
            backoff.Reset(); // only after the whole batch was handled
            return Task.CompletedTask;
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
            // the bulk load feeds the AI finder old sales in no particular order, so it must not fit on them before the replay ends
            using var aiReplays = new AiReplayScopes(aiFinder);
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
            await ReplayAiHistory(aiReplays, stoppinToken);
        }

        /// <summary>
        /// The AI finder keeps more history per tag (90 days by default) than the bulk load reads. The training instance replays
        /// it per tag into the finder only, so the sniper's medians do not count these sales twice. A tag's replay scope ends
        /// when its replay does, so it can be fit while the next tag replays.
        /// </summary>
        internal async Task ReplayAiHistory(AiReplayScopes scopes, CancellationToken stoppingToken)
        {
            var started = Stopwatch.StartNew();
            foreach (var tag in scopes.Tags)
            {
                await ReplayAiHistory(tag, stoppingToken);
                scopes.End(tag);
            }
            if (scopes.Tags.Count > 0)
                logger.LogInformation("AI finder history of {Count} tags replayed in {Minutes:0.0} min", scopes.Tags.Count, started.Elapsed.TotalMinutes);
        }

        private async Task ReplayAiHistory(string tag, CancellationToken stoppingToken)
        {
            try
            {
                await aiFinder.ReplayHistory(tag, stoppingToken);
            }
            catch (Exception e) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogError(e, "AI finder history replay failed for {Tag}", tag);
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
            if (i > differential / 5)
            {
                UpdateAllMedian();
                sniper.State = SniperState.Ready;
                await Task.Delay(500);
            }
        }

        public async Task<Dictionary<string, Dictionary<string, double>>> PartialAnalysis(string targetTag, CancellationToken stoppinToken, DateTime totalStart = default)
        {
            using var scope = logger.BeginScope("partial analysis");
            logger.LogInformation("loading aote from db");
            await aiFinder.Retrain(targetTag, stoppinToken, totalStart, partialCalcService.AddSell);
            logger.LogInformation(Newtonsoft.Json.JsonConvert.SerializeObject(partialCalcService.GetAttributeCosts(targetTag), Newtonsoft.Json.Formatting.Indented));

            return partialCalcService.GetAttributeCosts(targetTag);
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
            await RunTilStopped("ah summary", backoff =>
                Kafka.KafkaConsumer.Consume<AhStateSumary>(config, config["TOPICS:AH_SUMARY"], async sum =>
                {
                    await activeUpdater.ProcessSumary(sum);
                    backoff.Reset();
                }, stoppingToken, ConsumerConfig.GroupId, AutoOffsetReset.Latest)
            , stoppingToken);
        }

        private async Task ConsumeBazaar(CancellationToken stoppingToken)
        {
            Console.WriteLine("starting bazaar");
            await RunTilStopped("bazaar", backoff =>
                Kafka.KafkaConsumer.ConsumeBatch<dev.BazaarPull>(ConsumerConfig, [config["TOPICS:BAZAAR"]], batch =>
                {
                    foreach (var item in batch)
                    {
                        if (item.Timestamp > DateTime.UtcNow - TimeSpan.FromMinutes(1))
                            sniper.UpdateBazaar(item);
                    }
                    backoff.Reset();
                    return Task.CompletedTask;
                }, stoppingToken, 5)
            , stoppingToken);
            Console.WriteLine("stopped processing bazaar");
        }

        private ConsumerConfig ConsumerConfig =>
            new(KafkaCreator.GetClientConfig(config))
            {
                SessionTimeoutMs = 9_000,
                AutoOffsetReset = AutoOffsetReset.Latest,
                GroupId = System.Net.Dns.GetHostName(),
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };


        private Task RunTilStopped(string name, Func<ConsumeBackoff, Task> consumeOnce, CancellationToken stoppingToken)
            => RunTilStopped(name, consumeOnce, logger, Task.Delay, stoppingToken);

        /// <summary>
        /// Starts a fresh consume call after every failure (or unexpected return) with a 2s to 60s backoff,
        /// only <paramref name="stoppingToken"/> ends it.
        /// </summary>
        internal static Task RunTilStopped(
            string name,
            Func<ConsumeBackoff, Task> consumeOnce,
            ILogger logger,
            Func<TimeSpan, CancellationToken, Task> delay,
            CancellationToken stoppingToken)
        {
            var backoff = new ConsumeBackoff();
            return ConsumeRetryLoop.RunAsync(
                () => consumeOnce(backoff),
                backoff,
                (e, wait) => logger.LogError(e, "consumer {name} stopped, restarting in {wait}", name, wait),
                delay,
                stoppingToken);
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
            await RunTilStopped("sold auctions", backoff => Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(ConsumerConfig, new string[] { config["TOPICS:SOLD_AUCTION"] }, async batch =>
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
                backoff.Reset();
            }, stoppingToken, 4), stoppingToken);
            logger.LogInformation("processing sells stopped");
        }

        private async Task ProcessCancelled(CancellationToken stoppingToken)
        {
            var topic = config["TOPICS:MISSING_AUCTION"];
            if (string.IsNullOrEmpty(topic))
                return;
            await RunTilStopped("cancelled auctions", backoff => Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(ConsumerConfig, new string[] { topic }, batch =>
            {
                foreach (var a in batch)
                    sniper.RemoveActive(a);
                backoff.Reset();
                return Task.CompletedTask;
            }, stoppingToken, 20), stoppingToken);
            logger.LogInformation("processing cancellations stopped");
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