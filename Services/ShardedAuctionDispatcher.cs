using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Coflnet.Sky.Core;
using Prometheus;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// WS-D — tag-shard dispatcher in front of <see cref="SniperService.TestNewAuction"/> for throughput scaling.
    ///
    /// <para><b>Why this is safe.</b> <c>TestNewAuctionInternal</c> resolves the incoming tag to a <em>group tag</em>
    /// via <see cref="SniperService.GetAuctionGroupTag"/> and then mutates exactly one per-tag
    /// <see cref="Models.PriceLookup"/> (<c>Lookups.GetOrAdd(groupTag, …)</c>): bucket adds, lbin enqueues, the
    /// per-bucket <c>HitsSinceCalculating++</c> and <c>ScoreVecCache</c>. Different group tags touch <em>disjoint</em>
    /// PriceLookup objects, so two different tags can run concurrently with no shared mutation. The only state shared
    /// across tags on the snipe path is already concurrency-safe: <c>Lookups</c> / <c>ComparisonValueLookup</c> /
    /// <c>ScoreVecLookup</c> are <see cref="ConcurrentDictionary{TKey,TValue}"/>, the score interner uses
    /// GetOrAdd + Interlocked, the <c>pricingEpoch</c> is Interlocked, the WS-A <c>CandidateIndex</c> is an immutable
    /// snapshot published by a single atomic reference write (concurrent rebuilds produce identical content), and
    /// <c>Logs</c>/<c>LbinUpdates</c> are <see cref="ConcurrentQueue{T}"/>.</para>
    ///
    /// <para>The one rule that makes the bucket-level mutations race-free is: <b>a given group tag is ALWAYS handled by
    /// exactly one worker.</b> We hash the resolved group tag to a fixed worker index, so all auctions of that tag are
    /// serialized through one queue/thread. Two workers never mutate the same PriceLookup. Different tags run in
    /// parallel; per-auction latency is unchanged.</para>
    ///
    /// <para><b>Determinism / parity.</b> The snipe set is independent of worker count: each tag's auctions are
    /// processed in the same submission order on its single worker (FIFO queue), and the snipe-finding path emits all
    /// of an auction's snipes synchronously within its own <c>TestNewAuction</c> call (the risky closest finder is
    /// awaited when <c>MIN_TARGET == 0</c>). The <em>cross-tag</em> interleaving is non-deterministic, but the
    /// shared structures it touches are commutative reads/idempotent caches, so the multiset of snipes (by finder,
    /// by auction) is bit-identical to single-threaded. See <c>ShardedReplay</c> in the harness for the parity assert.</para>
    ///
    /// <para><b>R8 WS-SHARD-OBS — shard-balance observability (additive, no behavior change).</b> The suspected capacity
    /// bottleneck is shard balance: under per-tag serialization a few hot tags can skew most of the load onto one worker.
    /// To make that measurable, this class exports two Prometheus metrics, both labelled by <c>shard</c> (the worker
    /// index, <c>0..WorkerCount-1</c>):
    /// <list type="bullet">
    /// <item><c>sky_sniper_shard_processed_total</c> (Counter): per-worker count of auctions handed to
    /// <c>TestNewAuction</c> (incremented in <see cref="WorkerLoop"/> alongside — but additional to — the existing
    /// global <see cref="Processed"/> counter, which is left exactly as-is for the harness parity assert). A skewed
    /// split of this counter across shards is the hot-tag-skew signal.</item>
    /// <item><c>sky_sniper_shard_queue_depth</c> (Gauge): current in-flight backlog per shard, incremented on every
    /// enqueue (<see cref="Enqueue"/> and the <see cref="ProcessBatchAndWait"/> enqueue loop) and decremented once per
    /// item after it is dequeued+processed in <see cref="WorkerLoop"/>. A persistently deep queue on one shard is the
    /// backlog side of the same skew.</item>
    /// </list>
    /// The metric declarations are <c>static readonly</c> (process-global by name+labels; registering the same name
    /// twice returns the same metric, so multiple dispatcher instances share them and the <c>shard</c> label keeps
    /// shards distinct within a process). To keep the snipe hot path allocation-free, the per-shard <c>Child</c> handles
    /// are pre-resolved once into instance arrays in the constructor — no <c>WithLabels</c> call per auction. These
    /// metrics are purely observational; they do not touch routing, serialization, the countdown/batch logic, fault
    /// handling, or which auctions produce which snipes.</para>
    /// </summary>
    public sealed class ShardedAuctionDispatcher : IDisposable
    {
        /// <summary>
        /// A unit of work on a shard queue: the auction to process plus an optional <see cref="CountdownEvent"/> that the
        /// worker signals once this auction's <c>TestNewAuction</c> has fully returned. The countdown is how
        /// <see cref="ProcessBatchAndWait"/> knows a whole batch has drained without ever calling
        /// <c>CompleteAdding</c> (which is terminal). When <see cref="batch"/> is null the item is fire-and-forget
        /// (the legacy <see cref="Enqueue"/> path); the same struct serves both so routing/serialization is identical.
        /// </summary>
        private readonly struct WorkItem
        {
            public readonly SaveAuction Auction;
            public readonly CountdownEvent Batch;
            public WorkItem(SaveAuction auction, CountdownEvent batch)
            {
                Auction = auction;
                Batch = batch;
            }
        }

        private readonly SniperService service;
        private readonly BlockingCollection<WorkItem>[] queues;
        private readonly Thread[] workers;
        private readonly bool triggerEvents;
        private readonly bool fastMode;
        private long processed;
        private volatile Exception workerError;

        // --- R8 WS-SHARD-OBS: shard-balance observability (see class summary) ---
        // Process-global metric declarations (static readonly, declared once; same name+labels returns the same metric
        // across dispatcher instances — the per-shard label disambiguates shards within one process).
        private static readonly Counter ShardProcessedTotal = Metrics.CreateCounter(
            "sky_sniper_shard_processed_total",
            "Per-shard-worker count of auctions handed to TestNewAuction (shard-balance / hot-tag-skew signal)",
            new CounterConfiguration { LabelNames = new[] { "shard" } });
        private static readonly Gauge ShardQueueDepth = Metrics.CreateGauge(
            "sky_sniper_shard_queue_depth",
            "Current in-flight queue backlog per shard worker (inc on enqueue, dec after dequeue+process)",
            new GaugeConfiguration { LabelNames = new[] { "shard" } });

        // Per-instance, pre-resolved per-shard child handles (indexed by worker idx) so the snipe hot path never calls
        // WithLabels per auction. Built in the constructor loop where the queues/workers are created.
        private readonly Counter.Child[] perShardProcessed;
        private readonly Gauge.Child[] perShardDepth;

        /// <summary>Number of shard workers (and queues).</summary>
        public int WorkerCount { get; }

        /// <summary>Total auctions drained and handed to <see cref="SniperService.TestNewAuction"/> across all workers.</summary>
        public long Processed => Interlocked.Read(ref processed);

        /// <param name="service">The single shared service. Its cross-tag structures are already concurrent; only one
        /// worker ever mutates a given tag's PriceLookup because routing is by resolved group tag.</param>
        /// <param name="workerCount">Number of parallel shard workers.</param>
        /// <param name="triggerEvents">Forwarded to <c>TestNewAuction</c> (the snipe-finding hot path uses true).</param>
        /// <param name="fastMode">Forwarded to <c>TestNewAuction</c>.</param>
        /// <param name="queueBound">Optional per-queue bound for back-pressure; 0/unset = unbounded.</param>
        public ShardedAuctionDispatcher(SniperService service, int workerCount, bool triggerEvents = true,
            bool fastMode = false, int queueBound = 0)
        {
            if (workerCount < 1) throw new ArgumentOutOfRangeException(nameof(workerCount));
            this.service = service ?? throw new ArgumentNullException(nameof(service));
            this.triggerEvents = triggerEvents;
            this.fastMode = fastMode;
            WorkerCount = workerCount;

            queues = new BlockingCollection<WorkItem>[workerCount];
            workers = new Thread[workerCount];
            // R8 WS-SHARD-OBS: pre-resolve per-shard metric children once (no per-auction WithLabels on the hot path).
            perShardProcessed = new Counter.Child[workerCount];
            perShardDepth = new Gauge.Child[workerCount];
            for (int i = 0; i < workerCount; i++)
            {
                queues[i] = queueBound > 0
                    ? new BlockingCollection<WorkItem>(queueBound)
                    : new BlockingCollection<WorkItem>();
                perShardProcessed[i] = ShardProcessedTotal.WithLabels(i.ToString());
                perShardDepth[i] = ShardQueueDepth.WithLabels(i.ToString());
            }
            for (int i = 0; i < workerCount; i++)
            {
                int idx = i;
                workers[i] = new Thread(() => WorkerLoop(idx))
                {
                    IsBackground = true,
                    Name = $"snipe-shard-{idx}"
                };
                workers[i].Start();
            }
        }

        /// <summary>
        /// Routes <paramref name="auction"/> to the worker that owns its resolved group tag. Blocks only if the target
        /// queue is bounded and full. The same group tag always lands on the same worker, so its PriceLookup is never
        /// mutated by two threads.
        /// </summary>
        public void Enqueue(SaveAuction auction)
        {
            int shard = ShardFor(auction.Tag);
            // R8 WS-SHARD-OBS: count this item into the target shard's backlog before it is added (dec'd after process).
            perShardDepth[shard].Inc();
            queues[shard].Add(new WorkItem(auction, null));
        }

        /// <summary>
        /// Enqueues an entire Kafka batch across the shard workers (each auction routed to the worker that owns its
        /// resolved group tag, exactly as <see cref="Enqueue"/>) and BLOCKS until every auction in the batch has been
        /// fully processed by its worker. This is the production batch primitive: the caller (the Kafka batch callback)
        /// must not return until the batch has drained, otherwise the offset would commit before processing — breaking
        /// at-least-once. Unlike <see cref="Complete"/>/<see cref="WaitForCompletion"/> this does NOT terminate the
        /// queues, so the same dispatcher instance is reused for every batch for the lifetime of the consumer.
        ///
        /// <para>The per-tag-serial invariant is unchanged: items still route by resolved group tag, so a given tag is
        /// processed by exactly one worker in submission order. Within a batch, two auctions of the same tag keep their
        /// relative submission order (single queue, FIFO), so the snipe set is bit-identical to single-threaded —
        /// the only non-determinism is the cross-tag interleaving, which touches commutative/idempotent shared state.</para>
        ///
        /// <para>On cancellation (<paramref name="cancellationToken"/>), this returns early WITHOUT having waited for the
        /// full drain; the caller must then NOT let the offset commit (it propagates the cancellation by throwing). Items
        /// already enqueued keep being processed by the workers — clean <see cref="Dispose"/> joins them so nothing is
        /// dropped mid-flight; the uncommitted offset means Kafka re-delivers the batch (at-least-once).</para>
        /// </summary>
        /// <param name="auctions">The auctions in this Kafka batch.</param>
        /// <param name="cancellationToken">Cancelled on shutdown; aborts the wait (the batch must then not commit).</param>
        /// <returns><c>true</c> if the whole batch drained (safe to commit); <c>false</c> if cancelled before drain.</returns>
        public bool ProcessBatchAndWait(IEnumerable<SaveAuction> auctions, CancellationToken cancellationToken = default)
        {
            if (auctions == null) throw new ArgumentNullException(nameof(auctions));
            // Materialize so we know the exact count and can route each item; the count seeds the countdown.
            var list = auctions as IReadOnlyList<SaveAuction> ?? new List<SaveAuction>(auctions);
            if (list.Count == 0)
                return true;

            // +1 initial count so we can enqueue all items before any worker can drive the countdown to zero; we
            // Signal() once after enqueueing to remove the guard. Workers Signal() once per processed auction.
            // NOTE: not a `using` — on cancellation, workers still hold WorkItems referencing this countdown and will
            // Signal() it after we return. We only Dispose() once we KNOW the batch fully drained (no in-flight item
            // can touch it); on the cancellation path we leave it for finalization (the worker Signal() is guarded).
            var done = new CountdownEvent(list.Count + 1);
            for (int i = 0; i < list.Count; i++)
            {
                int shard = ShardFor(list[i].Tag);
                // R8 WS-SHARD-OBS: count this item into the target shard's backlog before it is added (dec'd after process).
                perShardDepth[shard].Inc();
                queues[shard].Add(new WorkItem(list[i], done));
            }
            done.Signal(); // remove the enqueue guard; now only worker completions remain

            // Block until every item is processed, or until cancellation. Surface a worker fault so a partial (wrong)
            // batch never commits — and CLEAR it (per-batch scoped) so a single bad auction does not poison every
            // subsequent batch for the lifetime of the long-lived production dispatcher.
            bool drained;
            try
            {
                drained = done.Wait(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Cancelled mid-drain: do NOT dispose `done` (in-flight workers still Signal it). The batch did not
                // fully drain on this call; the caller must not commit. Re-throw so the consumer treats it as cancel.
                throw;
            }
            if (drained)
                done.Dispose();
            var fault = Interlocked.Exchange(ref workerError, null);
            if (fault != null)
                throw new AggregateException("A shard worker faulted", fault);
            return drained;
        }

        /// <summary>Raw tag → shard memo. GetAuctionGroupTag walks price lookups and allocates a substring per call —
        /// pure waste on the enqueue path where only the shard number matters. It also FREEZES routing per raw tag:
        /// CombinableStarred grows at runtime, and without the memo an auction of a tag admitted mid-batch could route
        /// to a different shard than an earlier same-tag auction of the same batch (two workers concurrently mutating
        /// one PriceLookup — the exact race the per-tag-serial invariant exists to prevent).</summary>
        private readonly ConcurrentDictionary<string, int> shardByTag = new();

        /// <summary>The worker index that owns <paramref name="tag"/> (after group-tag resolution). Public so the
        /// harness can pre-bucket a workload and assert routing stability.</summary>
        public int ShardFor(string tag)
        {
            if (shardByTag.TryGetValue(tag, out var cached))
                return cached;
            var groupTag = service.GetAuctionGroupTag(tag).tag;
            // FNV-1a over the resolved group tag: stable across runs/processes, no dependence on string.GetHashCode's
            // per-process randomization, so the same tag deterministically maps to the same shard.
            uint hash = 2166136261u;
            for (int i = 0; i < groupTag.Length; i++)
            {
                hash ^= groupTag[i];
                hash *= 16777619u;
            }
            var shard = (int)(hash % (uint)WorkerCount);
            shardByTag.TryAdd(tag, shard); // first resolution wins; races agree except across a CombinableStarred grow, where pinning IS the point
            return shard;
        }

        /// <summary>Signals that no more auctions will be enqueued. After this, drain with
        /// <see cref="WaitForCompletion"/>.</summary>
        public void Complete()
        {
            foreach (var q in queues)
                q.CompleteAdding();
        }

        /// <summary>Joins all worker threads (call after <see cref="Complete"/>). Rethrows the first worker exception,
        /// if any, so failures never silently produce a wrong (partial) snipe set.</summary>
        public void WaitForCompletion()
        {
            foreach (var t in workers)
                t.Join();
            if (workerError != null)
                throw new AggregateException("A shard worker faulted", workerError);
        }

        private void WorkerLoop(int idx)
        {
            // R8 WS-SHARD-OBS: cache this worker's pre-resolved per-shard metric children once per loop (no per-auction
            // WithLabels resolution on the hot path). The global `processed`/`Processed` counter below is left exactly
            // as-is; these are additive, observation-only increments.
            var shardProcessed = perShardProcessed[idx];
            var shardDepth = perShardDepth[idx];
            foreach (var item in queues[idx].GetConsumingEnumerable())
            {
                try
                {
                    service.TestNewAuction(item.Auction, triggerEvents, fastMode);
                    Interlocked.Increment(ref processed);
                    shardProcessed.Inc(); // R8 WS-SHARD-OBS: per-worker throughput (in addition to the global counter)
                }
                catch (Exception e)
                {
                    // First fault wins; surfaced from WaitForCompletion / ProcessBatchAndWait. We DON'T abandon the loop:
                    // the production consumer reuses this dispatcher across batches, and a single bad auction must not
                    // permanently kill a worker (which would silently drop every future auction routed to its tags).
                    // Record the error and keep processing; ProcessBatchAndWait rethrows it so the offending batch never
                    // commits. We still count it as processed so the per-batch countdown can complete and the consumer
                    // can surface the fault instead of hanging.
                    Interlocked.CompareExchange(ref workerError, e, null);
                    Interlocked.Increment(ref processed);
                    shardProcessed.Inc(); // R8 WS-SHARD-OBS: count the faulted-but-counted item on this shard too
                }
                finally
                {
                    // R8 WS-SHARD-OBS: this item has left the queue and been processed — drop it from the shard's
                    // in-flight backlog (mirrors the inc-on-enqueue; runs once per item even on a TestNewAuction fault).
                    shardDepth.Dec();
                    // Always signal the batch countdown — even on a TestNewAuction fault — so ProcessBatchAndWait never
                    // deadlocks waiting on an item that threw. Guard against a disposed countdown (only possible if a
                    // batch was both drained-and-disposed and somehow still had an in-flight item; defensive).
                    try { item.Batch?.Signal(); } catch (ObjectDisposedException) { }
                }
            }
        }

        public void Dispose()
        {
            // Best-effort: ensure adding is completed so worker threads can exit even if the caller forgot Complete().
            foreach (var q in queues)
            {
                try { if (!q.IsAddingCompleted) q.CompleteAdding(); } catch { /* already disposed */ }
            }
            foreach (var t in workers)
            {
                if (t.IsAlive) t.Join();
            }
            foreach (var q in queues)
                q.Dispose();
        }
    }
}
