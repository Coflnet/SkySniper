using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Core;
using Prometheus;
using Coflnet.Sky.Core.Services;
using Newtonsoft.Json;
using System.Net;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Buffers;
using System.Runtime.InteropServices;

namespace Coflnet.Sky.Sniper.Services
{
    public partial class SniperService
    {
        public const string PetItemKey = "petItem";
        public const string TierBoostShorthand = "TIER_BOOST";
        private const int SizeToKeep = 80;
        private const long LoadedBucketRebuildMinimumDelta = 1_500_000;
        private const int LoadedBucketRebuildRatioNumerator = 5;
        private const int LoadedBucketRebuildRatioDenominator = 4;
        private const int DrillPartRemovalCost = 50_000;

        public static int WorkingSize { get; set; } = 60;
        public const int PetExpMaxlevel = 4_225_538 * 6;
        private const int GoldenDragonMaxExp = 30_036_483 * 7;
        public static short CurrentDayCache = 0;
        public static int MIN_TARGET = 200_000;
        private static DateTime _startTime = new DateTime(2021, 9, 25);
        public static DateTime StartTime
        {
            get => _startTime; set
            {
                _startTime = value;
                UpdateToday(); // important for tests
            }
        }
        private readonly ILogger<SniperService> logger;
        public ConcurrentDictionary<string, PriceLookup> Lookups = new ConcurrentDictionary<string, PriceLookup>(3, 2000);

        private readonly ConcurrentQueue<LogEntry> Logs = new ConcurrentQueue<LogEntry>();
        public static readonly DeferredLogger deferred = new();
        private readonly ConcurrentQueue<(SaveAuction, ReferenceAuctions, AuctionKeyWithValue)> LbinUpdates = new();
        private readonly AuctionKey defaultKey = new AuctionKey();
        public SniperState State { get; set; } = SniperState.LoadingLbin;
        private readonly PropertyMapper mapper = new();
        private readonly (string, int)[] EmptyArray = [];
        private readonly Dictionary<string, double> BazaarPrices = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (PriceEstimate result, DateTime addedAt)> ClosetLbinMapLookup = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (PriceEstimate result, DateTime addedAt)> ClosetMedianMapLookup = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (ReferencePrice result, DateTime addedAt)> HigherValueLbinMapLookup = new();
        private readonly ConcurrentDictionary<ModifierLookupKey, (RankElem, DateTime)> ModifierValueLookup = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (List<RankElem> value, DateTime addedAt)> ComparisonValueLookup = new();
        // Columnar/interned score vectors for the closest-match kernel, derived 1:1 from ComparisonValueForKey. One
        // shared interner keeps ids consistent across every vec compared together. Swept on the same cadence as the
        // value caches it derives from (see FinishedUpdate).
        private readonly ClosestScoreKernel.Interner scoreInterner = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (ClosestScoreKernel.ScoreVec vec, DateTime addedAt)> ScoreVecLookup = new();
        // WS-A: monotonic "pricing epoch" bumped on every bucket price transition (the funnel is UpdateMedian, plus a
        // few out-of-band writes). PriceLookup.CandidateIndex caches the epoch it was built at; a mismatch (or a dict
        // Count change, or vec-TTL expiry) triggers a rebuild. Service-wide rather than per-lookup so the single bump
        // in UpdateMedian — which doesn't have the owning PriceLookup on every branch — covers all tags; the snipe
        // hot path never reprices, so the epoch is stable across the many FindClosestTo calls between sales.
        private long pricingEpoch;
        // Reuse window for BOTH the candidate index and the per-bucket vecs it captures (minutes). At half the 10-min vec
        // freshness bound, reuse-window + index-TTL (<=8 min) stays under it, so a vec reused into the index is never
        // older at use-time than what a fresh GetBucketScoreVec(now-10min) dict scan would have accepted — making the
        // index bit-exact with the pre-WS-A per-call scan while avoiding the force-rebuild of every vec on each rebuild
        // (the latter caused a rare-path allocation regression: novel keys churn the candidate set -> frequent rebuilds).
        private const double CandidateIndexMaxAgeMinutes = 4;
        // R6/MEMO2: TTL bound for the GetCleanItemPrice recompute-memo ENTRIES (CleanItemPriceMemo). Gated on the SAME
        // triggers as the candidate index (pricing epoch + live dict Count + this TTL) PLUS lookup.Volume, applied
        // PER-ENTRY (not per-window) so the dict is never re-allocated on an epoch bump. Kept identical to the candidate
        // index so a fresh entry can never reflect a lookup state the candidate/dominator indexes would have rebuilt
        // for. The memo caches only the PURE recompute output (deterministic for fixed inputs + a matching stamp), so
        // serving an entry is bit-exact for force + non-force.
        private const double CleanPriceMemoMaxAgeMinutes = CandidateIndexMaxAgeMinutes;
        // R6/MEMO2: optional, OFF by default. When on, counts GetCleanItemPrice calls that took the recompute path (memo
        // miss/build) vs those served by the memo, so the replay can report "recomputes eliminated per auction". Pure
        // telemetry — never changes the returned value.
        internal static readonly bool CleanPriceMemoCount
            = Environment.GetEnvironmentVariable("SNIPER_CLEANPRICE_MEMO_COUNT") is "1" or "true" or "TRUE";
        // R6/MEMO2 soak/test switch: when on, every memo HIT is cross-checked against a fresh recompute and throws on
        // divergence (the parity guard, like SNIPER_VERIFY_CLOSEST_INDEX). Off in production (adds a full recompute/hit).
        internal static readonly bool VerifyCleanPriceMemo
            = Environment.GetEnvironmentVariable("SNIPER_VERIFY_CLEANPRICE_MEMO") is "1" or "true" or "TRUE";
        internal static long CleanPriceMemoHits;
        internal static long CleanPriceMemoRecomputes;
        // R7/WS-A (+ folded R6 WS-PROBE) — the (contentHash, pricingEpoch) parse memo for DetailedKeyFromSaveAuction.
        // ONE dict for the whole service lifetime; entries carry their epoch stamp (the R6/MEMO2 entry-epoch pattern —
        // NEVER realloc the dict per epoch, the clean3-warm lesson). A stale-epoch (or absent) entry is a MISS. The
        // pricingEpoch rail makes a hit provably bit-exact: CapKeyLength's drop SET is epoch-stable, so within one epoch
        // the same content caps to the identical AuctionKey + breakdown. Bounded by the FinishedUpdate sweep + epoch.
        private readonly ParseMemo parseMemo = new();
        // WS-A: enable the cross-auction parse memo on the hot path. Default OFF (counting-only): the dup number + the
        // warm A/B decide whether the cross-auction memo earns its keep (see the report). When OFF, the probe still
        // measures distinct-vs-total + a rolling cross-auction hit-rate; the parse runs unmemoized (bit-identical to the
        // pre-R7 path). When ON, DetailedKeyFromSaveAuction serves epoch-matching hits from parseMemo.
        internal static readonly bool ParseMemoEnabled
            = Environment.GetEnvironmentVariable("SNIPER_PARSE_MEMO") is "1" or "true" or "TRUE";
        // WS-A/PROBE: count distinct content hashes vs total parses + a rolling-window cross-auction hit-rate (would-be
        // hits at the current epoch). Pure telemetry; never changes the returned value. Like SNIPER_CLEANPRICE_MEMO_COUNT.
        internal static readonly bool ParseMemoCount
            = Environment.GetEnvironmentVariable("SNIPER_PARSE_MEMO_COUNT") is "1" or "true" or "TRUE";
        // WS-A parity guard (the primary correctness contract): every DetailedKeyFromSaveAuction call computes BOTH the
        // memoized and a fresh result and asserts equal AuctionKey + breakdown. Off in production (adds a full recompute
        // per call). Green across COLD + WARM + RARE is the acceptance bar; catches a wrong NonContentKeys exclusion.
        internal static readonly bool VerifyParseMemo
            = Environment.GetEnvironmentVariable("SNIPER_VERIFY_PARSE_MEMO") is "1" or "true" or "TRUE";
        internal static long ParseMemoHits;       // would-be (probe) or actual (enabled) epoch-matching memo hits
        internal static long ParseMemoMisses;     // full parses run (distinct (hash,epoch) or memo-off)
        internal static long ParseMemoCalls;      // total DetailedKeyFromSaveAuction invocations counted
        // Whether ANY parse-memo machinery is active (enabled / counting / verify). When false (the shipped production
        // default), the per-entry content hashing fused into SelectValuable's traversal is SKIPPED entirely, so the
        // shipped path is byte-for-byte the pre-R7 parse (zero added hashing cost). When true, the hash shares the one
        // traversal (the WS-A "fuse, no second pass" rail).
        internal static readonly bool ParseMemoActive = ParseMemoEnabled || ParseMemoCount || VerifyParseMemo;
        /// <summary>
        /// Soak/test switch: when true, <see cref="FindClosestTo"/> cross-checks the contiguous-index arg-max against a
        /// fresh dictionary scan (<c>FindClosestToReference</c>) and throws on divergence — proves the index never
        /// drops/adds a candidate (the new-bucket trap) nor mis-scores. Off in production (adds a full extra scan).
        /// </summary>
        public static bool VerifyClosestIndex = Environment.GetEnvironmentVariable("SNIPER_VERIFY_CLOSEST_INDEX") == "1";
        /// <summary>
        /// R4 soak/test switch: when true, the finder dominator scans (<see cref="GetLbinCap"/>,
        /// <c>PotentialSnipeHigherValueScan</c>, <c>CheckLowerKeyFull</c>, the median lower-value scan) cross-check their
        /// flat-index dominator set against a fresh <see cref="IsHigherValue"/> dictionary filter and throw on divergence.
        /// Off in production. Proves the <see cref="DominatorIndex"/> never drops/adds a candidate vs the live dict.
        /// </summary>
        public static bool VerifyDominatorIndex = Environment.GetEnvironmentVariable("SNIPER_VERIFY_DOMINATOR_INDEX") == "1";
        private readonly ConcurrentDictionary<ItemReferences.Reforge, (RankElem, DateTime)> ReforgeValueLookup = new();
        private readonly ConcurrentDictionary<(string, KeyValuePair<string, string>), (long, DateTime)> AttributeValueLookup = new();

        private readonly Counter sellClosestSearch = Metrics.CreateCounter("sky_sniper_sell_closest_search", "Number of searches for closest sell");
        private readonly Counter closestMedianBruteCounter = Metrics.CreateCounter("sky_sniper_closest_median_brute", "Number of brute force searches for closest median");
        private readonly Counter closestLbinBruteCounter = Metrics.CreateCounter("sky_sniper_closest_lbin_brute", "Number of brute force searches for closest median");

        // --- Performance instrumentation (latency histograms for profiling/benchmarking) ---
        // Exponential buckets from ~1µs to ~4s (factor 4, 12 buckets) so both cache hits and brute-force scans land in distinct buckets.
        private static readonly double[] DurationBuckets = Histogram.ExponentialBuckets(0.000_001, 4, 12);
        private readonly Histogram getPriceDuration = Metrics.CreateHistogram(
            "sky_sniper_get_price_duration_seconds", "Wall-clock duration of SniperService.GetPrice",
            new HistogramConfiguration { Buckets = DurationBuckets });
        private readonly Histogram closestMedianSearchDuration = Metrics.CreateHistogram(
            "sky_sniper_closest_median_search_duration_seconds", "Wall-clock duration of the brute-force closest-median search",
            new HistogramConfiguration { Buckets = DurationBuckets });
        private readonly Histogram closestLbinSearchDuration = Metrics.CreateHistogram(
            "sky_sniper_closest_lbin_search_duration_seconds", "Wall-clock duration of the brute-force closest-lbin search",
            new HistogramConfiguration { Buckets = DurationBuckets });
        // Snipe-finding (ingest) path: latency of the per-auction find and its core finder, plus a throughput counter.
        private readonly Histogram testNewAuctionDuration = Metrics.CreateHistogram(
            "sky_sniper_test_new_auction_duration_seconds", "Wall-clock duration of TestNewAuction (snipe-finding per incoming auction)",
            new HistogramConfiguration { Buckets = DurationBuckets });
        private readonly Histogram findFlipDuration = Metrics.CreateHistogram(
            "sky_sniper_find_flip_duration_seconds", "Wall-clock duration of FindFlip (the core matched-bucket finder)",
            new HistogramConfiguration { Buckets = DurationBuckets });
        private readonly Counter snipesFoundCounter = Metrics.CreateCounter(
            "sky_sniper_snipes_found_total", "Snipes/flips emitted by FoundAFlip, labelled by finder type",
            new CounterConfiguration { LabelNames = new[] { "finder" } });

        /// <summary>
        /// When enabled, <see cref="GetPrice"/> and the brute-force closest searches emit per-call timing logs.
        /// Toggleable at runtime, or via the <c>SNIPER_PROFILE_VERBOSE=1</c> environment variable.
        /// Intended for local/staging profiling; leave off in production to avoid log spam (timing always feeds the
        /// Prometheus histograms and trace spans regardless of this flag).
        /// </summary>
        public static bool VerboseProfiling { get; set; }
            = Environment.GetEnvironmentVariable("SNIPER_PROFILE_VERBOSE") is "1" or "true" or "TRUE";
        private IMayorService mayorService;
        private int pauseFlipFindingCount;
        private readonly ConcurrentDictionary<string, int> pausedFlipFindingTags = new(StringComparer.Ordinal);

        public event Action<LowPricedAuction> FoundSnipe;
        public event Action<PotentialCraftFlip> CappedKey;
        public event Action<(SaveAuction, AuctionKeyWithValue)> OnSold;
        public event Action OnSummaryUpdate;
        public readonly string ServerDnsName = Dns.GetHostName();

        public bool IsFlipFindingPaused => Volatile.Read(ref pauseFlipFindingCount) > 0;

        public IDisposable PauseFlipFinding()
        {
            Interlocked.Increment(ref pauseFlipFindingCount);
            return new DisposeAction(() => Interlocked.Decrement(ref pauseFlipFindingCount));
        }

        public bool IsFlipFindingPausedFor(string itemTag)
        {
            if (IsFlipFindingPaused)
                return true;
            if (string.IsNullOrWhiteSpace(itemTag))
                return false;
            var groupTag = GetAuctionGroupTag(itemTag).tag;
            return pausedFlipFindingTags.TryGetValue(groupTag, out var count) && count > 0;
        }

        public IDisposable PauseFlipFindingFor(IEnumerable<string> itemTags)
        {
            var normalizedTags = itemTags
                .Where(tag => !string.IsNullOrWhiteSpace(tag))
                .Select(tag => GetAuctionGroupTag(tag).tag)
                .Where(tag => !string.IsNullOrWhiteSpace(tag))
                .Distinct()
                .ToArray();

            foreach (var tag in normalizedTags)
            {
                pausedFlipFindingTags.AddOrUpdate(tag, 1, (_, count) => count + 1);
            }

            return new DisposeAction(() =>
            {
                foreach (var tag in normalizedTags)
                {
                    while (pausedFlipFindingTags.TryGetValue(tag, out var count))
                    {
                        if (count <= 1)
                        {
                            if (pausedFlipFindingTags.TryRemove(new KeyValuePair<string, int>(tag, count)))
                                break;
                        }
                        else if (pausedFlipFindingTags.TryUpdate(tag, count - 1, count))
                        {
                            break;
                        }
                    }
                }
            });
        }

        public void RefreshLookup(string itemTag)
        {
            if (!Lookups.TryGetValue(itemTag, out var lookup) || lookup.Lookup == null || lookup.Lookup.Count == 0)
                return;

            UpdateCleanKey(lookup);
            foreach (var item in lookup.Lookup.ToList())
            {
                if (item.Value.References.Count == 0 && (item.Value.Lbins == null || item.Value.Lbins.Count == 0))
                    continue;
                CapBucketSize(item.Value);
                UpdateMedian(item.Value, (itemTag, GetBreakdownKey(item.Key, itemTag)));
                GetLbinCap(itemTag, lookup, item.Key);
            }
        }

        /// <summary>
        /// R5 P2 write-path benchmark entry point (used only by the SNIPE_REPLAY_WRITE replay mode). Runs the repricing
        /// of a SINGLE bucket exactly as <see cref="RefreshLookup"/>'s loop body does — <c>CapBucketSize</c> +
        /// <c>UpdateMedian(bucket, (tag, GetBreakdownKey(key, tag)))</c> + <c>GetLbinCap</c> — so the isolating harness
        /// drives the real production repricing primitive rather than a copy of it. Behavior-neutral: it only re-runs
        /// the same calls RefreshLookup already makes, so it changes no emitted snipe/price (it does not exist on any
        /// production path). <paramref name="lookup"/> may be null (GetLbinCap is then skipped, as RefreshLookup never
        /// passes null but the harness only resolves it best-effort).
        /// </summary>
        internal void UpdateMedian_DriveForBench(string itemTag, AuctionKey key, ReferenceAuctions bucket, PriceLookup lookup)
        {
            CapBucketSize(bucket);
            UpdateMedian(bucket, (itemTag, GetBreakdownKey(key, itemTag)));
            if (lookup != null)
                GetLbinCap(itemTag, lookup, key);
        }

        /// <summary>
        /// R6 WS-LOH benchmark entry point (used only by the deterministic LOH probe in the replay harness). Forces a
        /// FULL <see cref="GetOrBuildCandidateIndex"/> rebuild — exactly the per-auction work a novel-key (rare-path)
        /// auction triggers — by bumping the pricing epoch (the production invalidation trigger) before the build, so the
        /// cache always misses and the columns are re-allocated. Returns the rebuilt index's <c>Count</c>. Behavior-
        /// neutral: it only re-runs the same private build production already performs (it exists on no production path);
        /// the epoch bump is the same one any out-of-band price write issues.
        /// </summary>
        internal int BuildCandidateIndex_DriveForBench(string itemTag, PriceLookup lookup)
        {
            Interlocked.Increment(ref pricingEpoch); // force a cache miss -> a real rebuild (the rare-path cost)
            return GetOrBuildCandidateIndex(lookup, itemTag).Count;
        }

        public void MockFoundFlip(LowPricedAuction auction)
        {
            FoundSnipe?.Invoke(auction);
        }

        public ConcurrentQueue<Dictionary<string, string>> AllocatedDicts = new();

        private sealed class DisposeAction : IDisposable
        {
            private Action action;

            public DisposeAction(Action action)
            {
                this.action = action;
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref action, null)?.Invoke();
            }
        }

        internal readonly string[] CrimsonArmors = new string[] { "CRIMSON_", "TERROR_", "AURORA_", "FERVOR_" };
        private readonly HashSet<string> IncludeKeys = new HashSet<string>()
        {
            "baseStatBoostPercentage", // has an effect on drops from dungeons, is filtered to only max level, skelotor master and ice sprays
            "dye_item",
            // "backpack_color", no difference anymore
            "party_hat_color",
            "party_hat_emoji",
            "color", // armour
            "model", // abicase
            // potion "level", // not engough impact
            "item_tier", // mostly found on armor, dungeon floor it dropped on
            "talisman_enrichment", // talismans can be enriched with additional stats
            "divan_powder_coating",
            "ability_scroll", // applied to hyperions worth ~250m https://discord.com/channels/267680588666896385/1031668335731019886/1031668607479975976
            "power_ability_scroll",
            // magma armor is to cheap "magmaCubesKilled"
            "captured_player", // cake soul 
            "event", // year+eventtype
            "wood_singularity_count", // self explanatory
            "art_of_war_count", //       ^^
            "artOfPeaceApplied",
            "new_years_cake", // year of the cake
            // "heldItem", // pet held item - now handled like drill parts (removed from key, added via GetExtraValue)
            "skin", // cosmetic skins
            "candyUsed", // candy count
            "exp", // collected experience of pets
            "rarity_upgrades", // recomb
            "winning_bid", // price paid for midas
            "dungeon_item_level", "upgrade_level", // "stars"
            "farming_for_dummies_count",
            "unlocked_slots", // available gemstone slots
            "gemstone_slots", // old unlocked slots
            "zombie_kills", // slayer kills
            "spider_kills", // slayer kills
            "eman_kills", // slayer kills
            //"expertise_kills", // kills for expertise enchant counting sea creatures, replaced by enchant value caluclation
            "bow_kills", // huricane bow
            "raider_kills", // raiders axe
            "sword_kills",
            "blood_god_kills",
            "yogsKilled", // yog armor
            "mined_crops", // eg THEORETICAL_HOE_WARTS_3
            "blocksBroken", // promising (pick)axe
            "collected_coins", // crown of avarice
            "ethermerge",
            "edition", // great spook stuff
            "hpc", // hot potato books
            "thunder_charge", // Pulse ring modifer
            //"tuned_transmission", // aotv upgrade
            //"power_ability_scroll", // disabled as suggested by Coyu because comonly not worth 1m (up to 2m at most)
            "captured_player", // cake souls
            "handles_found", // NECRON ladder
            "RUNE_MUSIC",
            "RUNE_MEOW_MUSIC",
            "RUNE_DRAGON",
            "RUNE_TIDAL",
            "RUNE_GRAND_SEARING",
            "RUNE_ENCHANT",
            "RUNE_BARK_TUNES",
            "RUNE_BARK_SMITTEN",
            "RUNE_BARK_ICE_SKATES",
            "RUNE_SPELLBOUND",
            "RUNE_GRAND_FREEZING",
            "RUNE_PRIMAL_FEAR",
            "RUNE_RAINY_DAY",
            "bass_weight",
            "polarvoid",
            "intelligence_earned",
            "logs_cut",
            "chimera_found", // Diana's Bookshelf
            "is_shiny", // cosmetic effect on wither armor ~5% drop chance on Master Mode 7
            // test lower value modifiers (~200k)
            "tuned_transmission",
            "seconds_held",
        };

        private Dictionary<string, Func<ModifierMetadata, RankElem>> Converters = new()
        {
            { "full_bid", m => new (m.Modifier, (int)(float.Parse(m.Modifier.Value) * 48_000_000))},
            { "winning_bid", m => new (m.Modifier, (int)(float.Parse(m.Modifier.Value) * 25_000_000))},
            //{ "rarity_upgrades", m => new (m.Modifier, 50_000_000)}, item value
            { "eman_kills", m => new (m.Modifier, 3_000_000 * (int)Math.Pow(2, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "expertise_kills", m => new (m.Modifier, 3_000_000 * (int)Math.Pow(2, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "mined_crops", m => new (m.Modifier, 13_000_000 * (int)Math.Pow(2, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "blocksBroken", m => new (m.Modifier, 1_000_000 * (int)Math.Pow(2, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "item_tier", m=> new (m.Modifier, 2_300_000 * int.Parse(m.Modifier.Value)){IsEstimate=true}},
            { "color", m => new (m.Modifier, 50_000_000) {IsEstimate=true}}, // if its default its removed
            { "pgems", m => new (m.Modifier, 100_000_000){IsEstimate=true}},
            { "is_shiny", m => new (m.Modifier, 88_000_000) {IsEstimate=true}},
            { "party_hat_color", m => new (m.Modifier, 20_000_000) {IsEstimate=true}},
            { "thunder_charge", m => new (m.Modifier, 50_000_000 * int.Parse(m.Modifier.Value) + 10_000_000){IsEstimate=true} },
            { "baseStatBoost", m => new (m.Modifier, (int)((float.Parse(m.Modifier.Value) - 40) * 800_000)) {IsEstimate=true}},
            { "logs_cut", m=>  new(m.Modifier, m.Modifier.Value == "200k" ? 8_000_000 : 2_000_000){IsEstimate=true}},
            { "hotpc", m => new ("hotpc", m.Modifier.Value, m.Modifier.Value switch {
                "1" => 5_000_000,
                "0.1" => 2_000_000,
                _ => 800_000 // no fuming
            }) {IsEstimate=true}},
            { "chimera_found", m => new ("chimera_found", m.Modifier.Value, int.Parse(m.Modifier.Value) * 10_000_000) {IsEstimate=true}},
            { "new_years_cake", m => new (m.Modifier, int.Parse(m.Modifier.Value) switch
                {
                    < 20 => 20_000_000,
                    69 => 10_000_000,
                    < 120 => 2_000_000,
                    400 => 10_000_000, // check this in the future
                    _ => 600_000,
                }){IsEstimate=true} },
            { "party_hat_emoji", m => new (m.Modifier, 8_000_000){IsEstimate=true} },
            { "edition", m => new (m.Modifier, 8_000_000){IsEstimate=true} },
            { "collected_coins", m => new (m.Modifier, (long)(20_000_000 * Math.Pow(10, int.Parse(m.Modifier.Value)) + 1_000_000)){IsEstimate=true} },
        };

        public static readonly HashSet<string> InvertedValueKey = new()
        {
            "edition",
            "new_years_cake",
            "candyUsed",
        };

        internal static readonly HashSet<string> ImportantCakeYears = new()
        { "69", "420", "400"};

        private readonly struct RemovableItemKey
        {
            public readonly string Key;
            public readonly string AliasKey;
            public readonly bool IsPetItem;

            public RemovableItemKey(string key, bool isPetItem, string aliasKey = null)
            {
                Key = key;
                IsPetItem = isPetItem;
                AliasKey = aliasKey;
            }
        }

        /// <summary>
        /// Keys containing itemTags that should be added separately (cause its removable)
        /// </summary>
        private static readonly RemovableItemKey[] RemovableItems =
        [
            new("drill_part_engine", false, "engine.id"),
            new("drill_part_fuel_tank", false, "fuel_tank.id"),
            new("drill_part_upgrade_module", false, "upgrade_module.id"),
            new("line.part", false),
            new("sinker.part", false),
            new("hook.part", false),
            new("heldItem", true),
        ];

        private static readonly HashSet<string> KillKeys = [
            "blaze_kills",
            "blood_god_kills",
            "bow_kills",
            "eman_kills",
            "expertise_kills",
            "raider_kills",
            "runic_kills",
            "skeletorKills",
            "spider_kills",
            "sword_kills",
            "zombie_kills"
            ];

        private static readonly Dictionary<string, short> ShardAttributes = new(){
            {"mana_pool", 1},
            {"breeze", 2},
            {"speed", 2},
            {"life_regeneration", 2}, // especially valuable in combination with mana_pool
            {"fishing_experience", 1}, // golem said lvl 1 important
            {"ignition", 2},
            {"blazing_fortune", 2},
            {"double_hook", 3},
            {"mana_regeneration", 2},
            {"mending", 3},
            {"dominance", 3},
            {"trophy_hunter", 5},
            {"fisherman", 5},
            {"magic_find", 2},
            {"veteran", 4},
            {"lifeline", 3} // low volume but valuable
            // life recovery 3
        };

        // combos that are worth more starting at lvl 1 because they are together
        private readonly KeyValuePair<string, string>[] AttributeCombos = [
            new("blazing_fortune", "fishing_experience"),
            new("life_regeneration", "mana_pool"),
            new("veteran", "mending"),
            new("veteran", "magic_find"),
            new("mana_regeneration", "mana_pool"),
            new("dominance", "speed"),
            new("mending", "dominance"),
            new("double_hook", "fishing_speed"),
            new("trophy_hunter", "fishing_speed"),
            new("dominance", "mana_pool"),
            new("breeze", "mana_pool"),
            new("dominance", "magic_find"),
            new("dominance", "veteran"),
            new("mending", "mana_pool"),
        ];

        private readonly KeyValuePair<List<string>, List<KeyValuePair<string, string>>>[] ItemSpecificAttribCombo =
        [
            new(["LAVA_SHELL_NECKLACE", "MOLTEN_BELT"], new (){new("lifeline", "mana_pool"), new("lifeline", "lifeline")}),
            new(["LAVA_SHELL_NECKLACE"], [new("lifeline", "mana_regeneration")]),
            new(new (){"TERROR_BOOTS", "TERROR_LEGGINGS", "TERROR_CHESTPLATE"}, new (){new("lifeline", "mana_pool")}),
            new(new (){"MAGMA_LORD_BOOTS", "MAGMA_LORD_LEGGINGS", "MAGMA_LORD_CHESTPLATE", "MAGMA_LORD_HELMET"},
                new (){new("blazing_fortune", "mana_pool"), new("blazing_fortune", "fishing_experience"),
                       new("blazing_fortune", "magic_find"), new("blazing_fortune","blazing_fortune")}),
            new(new (){"AURORA_BOOTS", "AURORA_LEGGINGS", "AURORA_CHESTPLATE", "AURORA_HELMET", // not high but still noticable
                    "CRIMSON_BOOTS", "CRIMSON_LEGGINGS", "CRIMSON_CHESTPLATE", "CRIMSON_HELMET"}, new (){new("veteran", "mana_regeneration")}),
            new(new (){"CRIMSON_BOOTS", "CRIMSON_LEGGINGS", "CRIMSON_CHESTPLATE", "CRIMSON_HELMET", "MOLTEN_BRACELET"},
                    new (){new("magic_find", "mending")}), // vitality = mending
            new(new(){"MOLTEN_BRACELET", "MOLTEN_CLOAK", "MAGMA_LORD_GAUNTLET"}, new (){new("lifeline", "mana_pool")}),
            new(new(){"TAURUS_HELMET", "FLAMING_CHESTPLATE", "MOOGMA_LEGGINGS", "SLUG_BOOTS"}, new (){new("magic_find", "magic_find")}),
            new(new(){"GAUNTLET_OF_CONTAGION"}, new (){new("veteran", "mana_regeneration"),new("veteran", "breeze"),new("veteran", "mana_pool"),new("lifeline", "mana_pool")}),
        ];

        private readonly List<KeyValuePair<List<string>, List<string>>> AttributesToIgnoreOn = [
            // https://discord.com/channels/267680588666896385/1303058935430774904/1303072543145459784
            new(["SHIMMERING_LIGHT_TROUSERS", "SHIMMERING_LIGHT_TUNIC", "SHIMMERING_LIGHT_HOOD", "SHIMMERING_LIGHT_SLIPPERS"], Constants.AttributeKeys.ToList()),
            new(["VELVET_TOP_HAT","CASHMERE_JACKET", "SATIN_TROUSERS", "OXFORD_SHOES"],["color"]) // random https://hypixel-skyblock.fandom.com/wiki/Seymour%27s_Special_Armor
        ];
        private readonly Dictionary<string, string[]> AttributeToIgnoreOnLookup = new();

        public readonly Dictionary<string, List<KeyValuePair<string, string>>> ItemSpecificAttributeComboLookup = new();
        public readonly ConcurrentDictionary<string, HashSet<string>> AttributeComboLookup = new();

        public void FinishedUpdate()
        {
            ProcessLbins();
            UpdateToday();
            var removeBefore = DateTime.UtcNow.AddMinutes(-10);
            foreach (var item in HigherValueLbinMapLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                HigherValueLbinMapLookup.TryRemove(item.Key, out _);
            }
            foreach (var item in ModifierValueLookup.Where(c => c.Value.Item2 < removeBefore).ToList())
            {
                ModifierValueLookup.TryRemove(item.Key, out _);
            }
            foreach (var item in ComparisonValueLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                ComparisonValueLookup.TryRemove(item.Key, out _);
            }
            foreach (var item in ScoreVecLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                ScoreVecLookup.TryRemove(item.Key, out _);
            }
            foreach (var item in AttributeValueLookup.Where(c => c.Value.Item2 < removeBefore).ToList())
            {
                AttributeValueLookup.TryRemove(item.Key, out _);
            }
            removeBefore = DateTime.UtcNow.AddMinutes(-0.10);
            foreach (var item in ClosetLbinMapLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                ClosetLbinMapLookup.TryRemove(item.Key, out _);
            }
            removeBefore = DateTime.UtcNow.AddHours(-0.5);
            foreach (var item in ClosetMedianMapLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                ClosetMedianMapLookup.TryRemove(item.Key, out _);
            }
            foreach (var item in ReforgeValueLookup.Where(c => c.Value.Item2 < removeBefore).ToList())
            {
                ReforgeValueLookup.TryRemove(item.Key, out _);
            }
            // WS-A: bound the parse memo. A stale-epoch entry is already a read-MISS (never served), so this is purely a
            // memory bound — drop every entry whose epoch is no longer the current pricing epoch (it can never hit again
            // until overwritten by a fresh parse, which the dict supports in place). One dict for the lifetime; never
            // re-allocated (the clean3-warm lesson) — only its dead entries are pruned here on the 10-min cadence.
            if (ParseMemoEnabled || VerifyParseMemo)
            {
                var currentEpoch = Interlocked.Read(ref pricingEpoch);
                foreach (var item in parseMemo.Values.Where(c => c.Value.Epoch != currentEpoch).ToList())
                {
                    parseMemo.Values.TryRemove(item.Key, out _);
                }
            }
        }

        private static void UpdateToday()
        {
            CurrentDayCache = (short)(DateTime.UtcNow - StartTime).TotalDays; // os time for UtcNow takes time
        }

        public void ProcessLbins()
        {
            var count = LbinUpdates.Count;
            while (LbinUpdates.TryDequeue(out var update))
            {
                var (auction, bucket, key) = update;
                // For storing: subtract full bazaar price of removable items
                var valueToSubtract = GetFullRemovableValue(auction, key);
                var item = CreateReferenceFromAuction(auction, valueToSubtract, 0);
                if (bucket.Lbins == null)
                    bucket.Lbins = new();
                if (!bucket.Lbins.Contains(item))
                {
                    item.SellTime = (short)(auction.Start - auction.End.Date).TotalMinutes;
                    bucket.Lbins.Add(item);
                    bucket.Lbins.Sort(ReferencePrice.Compare);
                    if (bucket.Lbins.First().AuctionId == item.AuctionId && (logger?.IsEnabled(LogLevel.Information) ?? false))
                    {
                        logger.LogInformation($"New lowest lbin {auction.Uuid} {auction.StartingBid} from {bucket.Lbins.Skip(1).FirstOrDefault().Price}");
                    }
                }
                if (Lookups.TryGetValue(auction.Tag, out var lookup) && lookup.Category == Category.UNKNOWN)
                {
                    lookup.Category = auction.Category;
                }
            }
            if (logger?.IsEnabled(LogLevel.Information) ?? false)
                logger.LogInformation($"Finished processing {count} lbin updates");
        }

        private readonly Dictionary<string, string> ModifierItemPrefixes = new()
        {
            {"drill_part_engine", string.Empty},
            {"drill_part_fuel_tank", string.Empty},
            {"drill_part_upgrade_module", string.Empty},
            {"hook.part", string.Empty},
            {"line.part", string.Empty},
            {"sinker.part", string.Empty},
            {"skin", "PET_SKIN_"},
            {"dye_item", string.Empty},
            {"petItem", "PET_ITEM_"},
            {"heldItem", string.Empty}
        };
        private readonly HypixelItemService itemService;
        private readonly ActivitySource activitySource;
        private readonly ICraftCostService craftCostService;
        private readonly Dictionary<Core.Enchantment.EnchantmentType, byte> MinEnchantMap = new();

        /** NOTES
yogsKilled - needs further be looked into
skeletorKills - to low volume to include 
farmed_cultivating - tells the state of the cultivating enchant (already taken care of)

*/
        /* select helper
SELECT l.AuctionId,l.KeyId,l.Value,a.StartingBid, a.HighestBidAmount,a.Uuid,a.Tag,a.End FROM `NBTLookups` l, Auctions a
where KeyId = 128
and auctionid > 305000000  
and AuctionId = a.Id  
ORDER BY l.`AuctionId`  DESC;

        */

        // stuff changing value by 10+M
        public static HashSet<string> VeryValuable = new HashSet<string>()
        {
            "upgrade_level", // lvl 8+ are over 10m
            "rarity_upgrades",
            "winning_bid",
            "exp",
            "color",
            "dye_item",
            "ethermerge",
            "unlocked_slots",
            "skin",
            "candyUsed",
            "GRAND_SEARING",
            "new_years_cake" // not that valuable but the only attribute
        };

        // 200m+
        public static HashSet<string> Increadable = new HashSet<string>()
        {
            "ability_scroll",
            "color"
        };

        public static HashSet<string> NeverDrop = new()
        {
            "virtual", // virtual keys for calculations
            "exp", // this helps with closest match 
            "party_hat_color", // closest to clean would mix them up
            "ability_scroll", // most expensive attribues in the game
            "candyUsed", // candy count on pets can't be accounted
            "new_years_cake" // not that valuable but the only attribute
        };

        public static KeyValuePair<string, string> Ignore { get; } = new KeyValuePair<string, string>(string.Empty, string.Empty);


        public SniperService(HypixelItemService itemService, ActivitySource activitySource, ILogger<SniperService> logger, ICraftCostService craftCostService)
        {
            this.FoundSnipe += la =>
            {
                if (la.Finder == LowPricedAuction.FinderType.SNIPER && (float)la.Auction.StartingBid / la.TargetPrice < 0.8 && la.TargetPrice > 1_000_000
                    && (logger?.IsEnabled(LogLevel.Information) ?? false))
                    logger.LogInformation($"A: {la.Auction.Uuid} {la.Auction.StartingBid} -> {la.TargetPrice}  {KeyFromSaveAuction(la.Auction)}");
            };
            foreach (var item in AttributeCombos.ToList())
            {
                AttributeComboLookup.GetOrAdd(item.Key, a => new()).Add(item.Value);
                AttributeComboLookup.GetOrAdd(item.Value, a => new()).Add(item.Key);
                VeryValuable.Add(item.Key);
                VeryValuable.Add(item.Value);
            }
            foreach (var item in AttributeCombos)
            {
                IncludeKeys.Add(item.Key);
            }
            foreach (var elements in ItemSpecificAttribCombo)
            {
                foreach (var item in elements.Key)
                {
                    var lookup = ItemSpecificAttributeComboLookup.GetValueOrDefault(item, new());
                    lookup.AddRange(elements.Value);
                    ItemSpecificAttributeComboLookup[item] = lookup;
                }
            }
            foreach (var sample in AttributesToIgnoreOn)
            {
                foreach (var item in sample.Key)
                {
                    AttributeToIgnoreOnLookup[item] = sample.Value.ToArray();
                }
            }
            foreach (var item in Constants.AttributeKeys)
            {
                if (!ShardAttributes.ContainsKey(item))
                    ShardAttributes.Add(item, 9);
            }
            foreach (var item in ShardAttributes)
            {
                IncludeKeys.Add(item.Key);
            }

            foreach (var enchant in Enum.GetValues<Core.Enchantment.EnchantmentType>())
            {
                MinEnchantMap[enchant] = 6;
            }

            foreach (var item in Constants.RelevantEnchants)
            {
                MinEnchantMap[item.Type] = item.Level;
            }

            string[] armorPieces = ["HELMET", "CHESTPLATE", "LEGGINGS", "BOOTS"];
            string[] tiers = ["", "HOT_", "BURNING_", "FIERY_", "INFERNAL_"];
            foreach (var toAdd in CrimsonArmors)
            {
                // each kind of armor piece is combinable with each other of the same type
                // eg "CRIMSON_HELMET", "AURORA_HELMET", "FERVOR_HELMET" etc.
                foreach (var item in armorPieces)
                {
                    var list = new List<string>();
                    foreach (var adding in CrimsonArmors)
                    {
                        list.Add($"{adding}{item}");
                    }
                    foreach (var tier in tiers)
                    {
                        SharedAttributeGroup[$"{tier}{toAdd}{item}"] = list.ToArray();
                    }
                }
            }

            this.itemService = itemService;
            this.activitySource = activitySource;
            this.logger = logger;
            this.craftCostService = craftCostService;

            Converters["exp"] = m => new(m.Modifier, GetExpValue(m.ItemTag, m.Modifier)) { IsEstimate = true };
            Converters["candyUsed"] = m => new(m.Modifier, GetCandyPrice(m.ItemTag, m.RelevantModifiers.ToDictionary())) { IsEstimate = true };
            /*
            { "upgrade_level", m => new (m.Modifier, EstStarCost(m.ItemTag, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "unlocked_slots", m => new (m.Modifier, itemService.GetSlotCostSync(m.ItemTag, m.FlatNbt, m.Modifier.Value.Split(',').ToList()).Item2){IsEstimate=true}},
            { "scroll_count", m => new (m.Modifier, (GetPriceForItem("IMPLOSION_SCROLL") + GetPriceForItem("SHADOW_WARP_SCROLL") + GetPriceForItem("WITHER_SHIELD_SCROLL")) / 3 * int.Parse(m.Modifier.Value)){IsEstimate=true}},
            */
            Converters["upgrade_level"] = m => new(m.Modifier, EstStarCost(m.ItemTag, int.Parse(m.Modifier.Value)));
            Converters["unlocked_slots"] = m => new(m.Modifier, GetGemstoneSlotWorth(m.RelevantModifiers, m.ItemTag, m.Modifier));
            Converters["scroll_count"] = m => new(m.Modifier, (GetPriceForItem("IMPLOSION_SCROLL") + GetPriceForItem("SHADOW_WARP_SCROLL") + GetPriceForItem("WITHER_SHIELD_SCROLL")) / 3 * int.Parse(m.Modifier.Value));
            Converters["bass_weight"] = m => new(m.Modifier, 5_000_000 * int.Parse(m.Modifier.Value.Split(',')[0]));
        }

        /// <summary>
        /// Sets the mayor service for Diana-related item adjustments.
        /// </summary>
        public void SetMayorService(IMayorService service)
        {
            this.mayorService = service;
        }

        public void SummaryUpdate()
        {
            OnSummaryUpdate?.Invoke();
        }

        public PriceEstimate GetPrice(SaveAuction auction)
        {
            using var activity = activitySource?.StartActivity("GetPrice", ActivityKind.Internal);
            var startTimestamp = Stopwatch.GetTimestamp();
            try
            {
                var result = GetPriceInternal(auction);
                if (activity != null)
                {
                    activity.SetTag("sky.item_tag", auction?.Tag);
                    activity.SetTag("sky.median", result?.Median);
                    activity.SetTag("sky.volume", result?.Volume);
                }
                return result;
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
                getPriceDuration.Observe(elapsed.TotalSeconds);
                if (VerboseProfiling)
                    logger?.LogInformation("[profile] GetPrice tag={Tag} took {Micros:F1}µs", auction?.Tag, elapsed.TotalMicroseconds);
            }
        }

        private PriceEstimate GetPriceInternal(SaveAuction auction)
        {
            if (auction == null || auction.Tag == null)
                return null;
            if (BazaarPrices.TryGetValue(auction.Tag, out var bazaar))
                return new() { Median = (long)bazaar * auction.Count };
            var tagGroup = GetAuctionGroupTag(auction.Tag);

            var result = new PriceEstimate();
            if (!Lookups.TryGetValue(tagGroup.Item1, out PriceLookup lookup))
            {
                return result;
            }
            var l = lookup.Lookup;
            if (auction.Category == Category.UNKNOWN)
            {
                auction.Category = lookup.Category;
            }
            var detailedKey = DetailedKeyFromSaveAuction(auction);
            var itemKey = detailedKey.GetReduced(0);
            result.ItemKey = itemKey.ToString();

            // add back removed value (gem,drills and rod parts)
            var gemVal = GetExtraValue(auction, itemKey);
            // use full removable value for LBIN to match what was subtracted during storage
            var fullRemovableVal = GetFullRemovableValue(auction, itemKey);
            if (l.TryGetValue(itemKey, out ReferenceAuctions bucket))
            {
                if (result.Lbin.AuctionId == default && bucket.Lbin.AuctionId != default)
                {
                    var lbinGemValue = fullRemovableVal;
                    if (itemKey.Modifiers.Any(m => m.Key == "pgems" && m.Value == "5"))
                    {// gems are already accounted for
                        lbinGemValue = 0;
                    }
                    result.Lbin = new(bucket.Lbin)
                    {
                        Price = bucket.Lbin.Price + lbinGemValue
                    };
                    result.LbinKey = itemKey.ToString();
                    var slbin = bucket.Lbins.Skip(1).FirstOrDefault();
                    if (slbin.AuctionId != default)
                        result.SLbin = new(slbin) { Price = slbin.Price + lbinGemValue };
                    else
                        result.SLbin = slbin;
                }
                if (result.Median == default && bucket.Price != default)
                {
                    AssignMedian(result, itemKey, bucket, gemVal);
                }
            }
            if (result.Median == default)
            {
                var now = DateTime.UtcNow;
                var res = ClosetMedianMapLookup
                            .GetOrAdd(((string, AuctionKey))(auction.Tag, itemKey),
                                      _ => GetEstimatedMedian(auction, result, lookup, detailedKey, gemVal, now));
                if (res.addedAt != now)
                {
                    result.Median = res.result.Median;
                    result.MedianKey = res.result.MedianKey;
                    result.Volume = res.result.Volume;
                }
            }
            if (result.Lbin.Price == default && l.Count > 0)
            {
                var now = DateTime.UtcNow;
                var res = ClosetLbinMapLookup.GetOrAdd(((string, AuctionKey))(auction.Tag, itemKey), a =>
                {
                    return ClosestLbin(auction, result, l, itemKey, now);
                });
                if (res.addedAt != now)
                {
                    result.Lbin = res.result.Lbin;
                    result.LbinKey = res.result.LbinKey;
                }
            }
            ReferencePrice lbinCap = GetLbinCap(tagGroup.tag, lookup, itemKey);
            if (lbinCap.Price != 0 && result.Lbin.Price > lbinCap.Price + fullRemovableVal)
            {
                result.Lbin = new(lbinCap) { Price = lbinCap.Price + fullRemovableVal };
                result.LbinKey += $"+HV";
            }
            // correct for combined items
            if (tagGroup.Item2 != 0)
            {
                result.Median += tagGroup.Item2;
                result.MedianKey += $"&comb";
            }
            // Adjust Diana-related items down by 10% when Diana's term is ending or just ended
            if (mayorService != null
                && MayorService.DianaRelatedItems.Contains(auction.Tag)
                && mayorService.IsDianaItemsAdjustmentActive(DateTime.UtcNow))
            {
                result.Median = (long)(result.Median * 0.9);
                result.MedianKey += "&diana-adj";
            }
            return result;
        }

        public ReferencePrice GetLbinCap(string tag, PriceLookup lookup, AuctionKey itemKey)
        {
            var lbinCap = HigherValueLbinMapLookup.GetOrAdd((tag, itemKey), a =>
            {
                // R4 WS-SHARE: replaces `l.Where(Lbin!=0 && IsHigherValue(itemKey,k) && reforge==).OrderBy(Lbin.Price)
                // .FirstOrDefault()` with a flat scan of the shared DominatorIndex. Direction A: candidates that DOMINATE
                // the query item (Dominates(query, cand)). The mask prefilter is a proven-sound necessary condition so it
                // never drops a true dominator; the exact kernel decides survivors. Bit-exact: the qualifying set is
                // identical, and a strict-'<' first-wins min over the index (same dict-enumeration order) reproduces the
                // stable OrderBy(Lbin.Price).FirstOrDefault(). Lbin/reforge read so the result equals the LINQ one:
                // reforge is key-derived (immutable, cached column); Lbin.Price is read LIVE off the bucket.
                var index = GetOrBuildDominatorIndex(lookup);
                var query = DominatorIndex.BuildDomKey(itemKey, scoreInterner);
                ulong qReq = query.RequiredMask;
                bool petSpirit = tag == "PET_SPIRIT";
                int qReforge = (int)itemKey.Reforge;
                AuctionKey bestKey = null;
                ReferenceAuctions bestBucket = null;
                long bestLbin = 0;
                for (int i = 0; i < index.Count; i++)
                {
                    if (index.Reforge[i] != qReforge)
                        continue;
                    var bucket = index.Buckets[i];
                    long lbin = bucket.Lbin.Price; // LIVE
                    if (lbin == 0)
                        continue;
                    if ((qReq & index.ProvidedMask[i]) != qReq)
                        continue; // sound presence prefilter
                    if (!DominatorIndex.Dominates(in query, in index.Doms[i], petSpirit))
                        continue;
                    if (bestKey is null || lbin < bestLbin) // `is null`, NOT `== null`: AuctionKey.operator== is value
                    {                                       // equality (returns false for null) — see AuctionKey.cs:408
                        bestKey = index.Keys[i];
                        bestBucket = bucket;
                        bestLbin = lbin;
                    }
                }
                if (VerifyDominatorIndex)
                    AssertDominatorParity(lookup.Lookup, itemKey, tag, index, baseIsQuery: true);
                if (bestKey is null)
                    return (default, DateTime.UtcNow); // OrderBy.FirstOrDefault() of empty -> default KVP
                if (bestKey == a.Item2)
                    return (default, DateTime.UtcNow); // best match is itself, skip
                return (bestBucket?.Lbin ?? default, DateTime.UtcNow);
            }).result;
            return lbinCap;
        }

        private (PriceEstimate result, DateTime addedAt) GetEstimatedMedian(SaveAuction auction, PriceEstimate result, PriceLookup lookup, KeyWithValueBreakdown itemKey, long gemVal, DateTime now)
        {
            var l = lookup.Lookup;
            closestMedianBruteCounter.Inc();
            using var searchActivity = activitySource?.StartActivity("ClosestMedianSearch", ActivityKind.Internal);
            var searchStart = Stopwatch.GetTimestamp();
            // The consumer takes the FIRST closest candidate that yields Median > 0 (almost always the single top-scored
            // one). So try the top-1 via a zero-alloc arg-max first; only if it does not produce a positive median do we
            // materialize+sort the full ordered candidate set (FindClosestOrdered) and continue from the SECOND element.
            // Bit-exact: ProcessMedianCandidate is the identical per-candidate body, the arg-max top == FindClosestOrdered's
            // first element (same score, stable first-wins), and the fallback iterates the same order skipping that first.
            var top = FindClosestArgMax(l, itemKey, auction.Tag);
            if (top.Key != null)
            {
                ProcessMedianCandidate(new KeyValuePair<AuctionKey, ReferenceAuctions>(top.Key, top.Value));
                if (result.Median <= 0)
                {
                    bool skippedTop = false;
                    foreach (var scored in FindClosestOrdered(l, itemKey, auction.Tag))
                    {
                        if (!skippedTop) { skippedTop = true; continue; } // first element already processed by the arg-max
                        ProcessMedianCandidate(new KeyValuePair<AuctionKey, ReferenceAuctions>(scored.Key, scored.Value));
                        if (result.Median > 0)
                            break;
                    }
                }
            }

            void ProcessMedianCandidate(KeyValuePair<AuctionKey, ReferenceAuctions> c)
            {
                AssignMedian(result, c.Key, c.Value, gemVal);
                GetDifferenceSum(auction, result, itemKey, c, out var diffExp, out var changeAmount);
                if (changeAmount != 0)
                {
                    result.MedianKey += diffExp;
                    var lookup = Lookups[this.GetAuctionGroupTag(auction.Tag).Item1];
                    var cleanItemValue = GetCleanItemPrice(auction.Tag, itemKey, lookup);
                    if (changeAmount > result.Median / 4 * 3)
                    {
                        IEnumerable<RankElem> closestCraftCost = ComparisonValueForKey(auction.Tag, c.Key);
                        AddReforgeValue(c.Key.Reforge, ref closestCraftCost);
                        var percentDiff = (float)(cleanItemValue + itemKey.ValueBreakdown.Sum(v => v.Value)) / (cleanItemValue + closestCraftCost.Sum(m => m.Value) + 1);
                        result.Median = (long)(result.Median * Math.Min(percentDiff, 1.5));
                        result.MedianKey += "*" + percentDiff.ToString("F2", System.Globalization.CultureInfo.InvariantCulture);
                    }
                    else
                        result.Median -= changeAmount;
                }
                foreach (var item in itemKey.Key.Modifiers.Where(m => Constants.AttributeKeys.Contains(m.Key)))
                {
                    // "scrap for parts"
                    var key = VirtualAttributeKey(item);
                    if (!l.TryGetValue(key, out var references) || references.Price == 0)
                        continue;
                    var median = references.Price;
                    if (result.Median < median)
                    {
                        result.Median = median;
                        result.MedianKey = key.ToString();
                    }
                }
            }
            if (result.Median > 0)
            {
                // check lower value keys
                // R3-READ: single-pass arg-max over the live dict replaces l.Where(IsHigherValue && Reforge match)
                //   .OrderByDescending(b => b.Value.Price).FirstOrDefault() — the consumer needs only the highest-priced
                // higher-value bucket, so the Where/OrderBy/FirstOrDefault chain (predicate/selector closures, a full
                // sort) is replaced by an arg-max. Bit-exact: same filter, same dict-enumeration order, OrderByDescending
                // is stable so .FirstOrDefault() picks the first source candidate tied for the max Price, which a
                // strict-'>' first-wins scan reproduces.
                AuctionKey maxKey = null;
                ReferenceAuctions maxBucket = null;
                long maxPrice = 0;
                // R4 WS-SHARE: flat scan of the shared DominatorIndex. Direction B: candidates DOMINATED BY the query
                // (Dominates(cand, query) == IsHigherValue(tag, cand, itemKey)) — i.e. lower-value keys the full item
                // contains. The implicit KeyWithValueBreakdown->AuctionKey conversion is hoisted once (was the dominant
                // read-path allocator). Bit-exact: same filter (reforge== AND dominance) over the same dict-enumeration
                // order, strict-'>' first-wins reproduces the stable OrderByDescending(Price).FirstOrDefault(). Price
                // read LIVE off each bucket; reforge is the immutable cached column.
                AuctionKey itemKeyAsKey = itemKey;
                var index = GetOrBuildDominatorIndex(lookup);
                var query = DominatorIndex.BuildDomKey(itemKeyAsKey, scoreInterner);
                ulong qProv = query.ProvidedMask;
                bool petSpirit = auction.Tag == "PET_SPIRIT";
                int itemKeyReforge = (int)itemKey.Key.Reforge;
                for (int i = 0; i < index.Count; i++)
                {
                    if (index.Reforge[i] != itemKeyReforge)
                        continue;
                    if ((index.RequiredMask[i] & qProv) != index.RequiredMask[i])
                        continue; // sound presence prefilter (candidate is the base side)
                    if (!DominatorIndex.Dominates(in index.Doms[i], in query, petSpirit))
                        continue;
                    var bucket = index.Buckets[i];
                    long price = bucket.Price; // LIVE
                    if (maxKey is null || price > maxPrice)
                    {
                        maxKey = index.Keys[i];
                        maxBucket = bucket;
                        maxPrice = price;
                    }
                }
                if (VerifyDominatorIndex)
                    AssertDominatorParity(l, itemKeyAsKey, auction.Tag, index, baseIsQuery: false);
                if (maxBucket != null && maxBucket.Price > result.Median)
                {
                    result.Median = maxBucket.Price;
                    result.MedianKey += $"+HV-{maxKey}";
                    // De-LINQ'd .Where(...).Sum(...): explicit loop over the value breakdown, skipping the modifier/enchant
                    // already covered by the matched higher-value key. Bit-exact with the original LINQ accumulation.
                    long add = 0;
                    var breakdown = itemKey.ValueBreakdown;
                    for (int bi = 0; bi < breakdown.Count; bi++)
                    {
                        var m = breakdown[bi];
                        if (!maxKey.Modifiers.Contains(m.Modifier) && !maxKey.Enchants.Contains(m.Enchant))
                            add += m.IsEstimate ? m.Value / 20 : m.Value;
                    }
                    result.Median += add / 9;
                }
            }
            RecordSearchDuration(closestMedianSearchDuration, searchStart, searchActivity, auction?.Tag, l.Count, "ClosestMedianSearch");
            return (result, now);
        }

        private (PriceEstimate result, DateTime addedAt) ClosestLbin(SaveAuction auction, PriceEstimate result, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKeyWithValue itemKey, DateTime now)
        {
            closestLbinBruteCounter.Inc();
            using var searchActivity = activitySource?.StartActivity("ClosestLbinSearch", ActivityKind.Internal);
            var searchStart = Stopwatch.GetTimestamp();
            // R3-READ: only the top-1 closest lbin is consumed here, so a single-pass arg-max over the live dict avoids
            // the LINQ Where().OrderByDescending() (full sort + boxed score keys + delegate closures) GetClosestLbins
            // built only to take its first element. Byte-identical: OrderByDescending is stable and .FirstOrDefault()
            // returns the first source element among those tied for the max score, which a strict-'>' first-wins scan
            // reproduces (same filter, same dict-enumeration order, same float key Similarity(key)+Min(Volume,2)).
            var closest = ClosestLbinArgMax(l, itemKey);
            if (closest.Key != default)
            {
                result.Lbin = closest.Value.Lbin;
                result.LbinKey = closest.Key.ToString();

                GetDifferenceSum(auction, result, itemKey, closest, out var diffExp, out var changeAmount);
                if (changeAmount != 0)
                {
                    var lbinPrice = result.Lbin.Price - changeAmount;
                    if (lbinPrice < 0)
                        lbinPrice = result.Lbin.Price;
                    result.Lbin = new ReferencePrice()
                    {
                        AuctionId = result.Lbin.AuctionId,
                        Day = result.Lbin.Day,
                        Price = lbinPrice,
                        Seller = result.Lbin.Seller
                    };
                    result.LbinKey += diffExp;
                }
            }
            RecordSearchDuration(closestLbinSearchDuration, searchStart, searchActivity, auction?.Tag, l.Count, "ClosestLbinSearch");
            return (result, now);
        }

        /// <summary>
        /// Records the duration of a brute-force closest search to its Prometheus histogram, the active trace span and
        /// (when <see cref="VerboseProfiling"/> is on) the log. <paramref name="bucketSize"/> is the number of candidate
        /// keys scanned, which is the dominant cost driver for these O(n·log n) searches.
        /// </summary>
        private void RecordSearchDuration(Histogram histogram, long startTimestamp, Activity activity, string tag, int bucketSize, string label)
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
            histogram.Observe(elapsed.TotalSeconds);
            if (activity != null)
            {
                activity.SetTag("sky.item_tag", tag);
                activity.SetTag("sky.bucket_size", bucketSize);
                activity.SetTag("sky.duration_us", elapsed.TotalMicroseconds);
            }
            if (VerboseProfiling)
                logger?.LogInformation("[profile] {Label} tag={Tag} buckets={Buckets} took {Micros:F1}µs", label, tag, bucketSize, elapsed.TotalMicroseconds);
        }

        public IEnumerable<(AuctionKey, ReferencePrice lbin)> ClosestLbinKeys(string tag, AuctionKey baseKey)
        {
            var tagGroup = GetAuctionGroupTag(tag);

            var result = new PriceEstimate();
            if (!Lookups.TryGetValue(tagGroup.Item1, out PriceLookup lookup))
            {
                return null;
            }
            var l = lookup.Lookup;
            var itemKey = GetBreakdownKey(baseKey, tag);
            return GetClosestLbins(l, itemKey).Select(c => (c.Key, c.Value.Lbin));
        }

        private static IOrderedEnumerable<KeyValuePair<AuctionKey, ReferenceAuctions>> GetClosestLbins(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKeyWithValue itemKey)
        {
            return l.Where(l => l.Key != null && l.Value?.Price > 0 && l.Value?.Lbin.Price > 0)
                            .OrderByDescending(m => itemKey.Similarity(m.Key) + Math.Min(m.Value.Volume, 2));
        }

        /// <summary>
        /// R3-READ: zero-alloc single-pass arg-max equivalent of <c>GetClosestLbins(l, itemKey).FirstOrDefault()</c>.
        /// <see cref="ClosestLbin"/> consumes only the top candidate, so building the whole ordered enumerable (a full
        /// sort plus a boxed <see cref="float"/> score key and predicate/selector closures per candidate) is wasted.
        /// Bit-exact with the LINQ form: same candidate filter (<c>Key != null &amp;&amp; Price &gt; 0 &amp;&amp; Lbin.Price &gt; 0</c>),
        /// same per-candidate score (<c>itemKey.Similarity(key) + Math.Min(Volume, 2)</c>, computed in <see cref="float"/>
        /// exactly as the LINQ selector), enumerated in the same dictionary order. <c>OrderByDescending</c> is a stable
        /// sort and <c>.FirstOrDefault()</c> takes the first source element among those tied for the maximum, so a
        /// strict-greater-than ("first wins") scan returns the identical <see cref="KeyValuePair{TKey,TValue}"/> — and
        /// <c>default</c> (Key == null) for an empty/no-candidate set, matching <c>FirstOrDefault()</c>.
        /// </summary>
        private static KeyValuePair<AuctionKey, ReferenceAuctions> ClosestLbinArgMax(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKeyWithValue itemKey)
        {
            // NOTE: AuctionKey overloads operator==/!= to do a *value* compare that returns false for two null
            // references, so `key == null` does NOT reliably detect a null reference. Use `is null` / `is not null`
            // (reference checks the overload cannot intercept) for every nullity test here.
            float bestScore = 0f;
            AuctionKey bestKey = null;
            ReferenceAuctions bestBucket = null;
            foreach (var m in l)
            {
                var v = m.Value;
                if (m.Key is null || !(v?.Price > 0) || !(v.Lbin.Price > 0))
                    continue;
                float score = itemKey.Similarity(m.Key) + Math.Min(v.Volume, 2);
                if (bestKey is null || score > bestScore)
                {
                    bestScore = score;
                    bestKey = m.Key;
                    bestBucket = v;
                }
            }
            return bestKey is null
                ? default
                : new KeyValuePair<AuctionKey, ReferenceAuctions>(bestKey, bestBucket);
        }

        private void GetDifferenceSum(SaveAuction auction, PriceEstimate result, AuctionKeyWithValue itemKey, KeyValuePair<AuctionKey, ReferenceAuctions> c, out string diffExp, out long changeAmount)
        {
            (var modVal, var modExp) = AdjustMedianForModifiers(result, itemKey, c, auction);
            (var enchal, var enchExp) = AdjustForMissingEnchants(result, auction.Tag, itemKey, c);
            var reforgediff = 0L;
            if (c.Key.Reforge != itemKey.Reforge)
                reforgediff = GetReforgeValue(c.Key.Reforge) - GetReforgeValue(itemKey.Reforge) / 20;
            diffExp = modExp + enchExp;
            changeAmount = modVal + enchal + reforgediff;
        }

        private (long substract, string add) AdjustForMissingEnchants(PriceEstimate result, string itemTag, AuctionKey itemKey, KeyValuePair<AuctionKey, ReferenceAuctions> closest)
        {
            // closest should be bigger 
            var missingEnchants = closest.Key.Enchants.Where(m => !itemKey.Enchants.Any(e => e.Type == m.Type && e.Lvl >= m.Lvl)).ToList();
            if (missingEnchants.Count > 0)
            {
                var enchCost = GetPriceSumForEnchants(missingEnchants, itemTag);
                if (enchCost > 0)
                {
                    return (enchCost, $"-{string.Join(",", missingEnchants.Select(m => $"{m.Type}{m.Lvl}"))}");
                }
            }
            return (0, string.Empty);
        }

        private (long sub, string exp) AdjustMedianForModifiers(PriceEstimate result, AuctionKey itemKey, KeyValuePair<AuctionKey, ReferenceAuctions> closest, SaveAuction auction)
        {
            var missingModifiers = closest.Key.Modifiers.Where(m => !itemKey.Modifiers.Contains(m)).ToList();
            if (missingModifiers.Count > 0)
            {
                long toSubstract = GetPriceSumForModifiers(missingModifiers, itemKey.Modifiers, auction, true);
                toSubstract += AdjustForAttributes(result.Median, itemKey, missingModifiers, auction);
                if (toSubstract != 0)
                {
                    return (toSubstract, $"- {string.Join(",", missingModifiers.Select(m => m.Value))}");
                }
            }
            return (0, string.Empty);
        }

        private long GetPriceSumForModifiers(List<KeyValuePair<string, string>> missingModifiers, IEnumerable<KeyValuePair<string, string>> modifiers, SaveAuction auction, bool calculate = false)
        {
            if (missingModifiers == null)
                return 0;
            return missingModifiers.Select(m =>
            {
                if (Constants.AttributeKeys.Contains(m.Key)
                    || m.Key == "exp" || m.Key == "candyUsed" || KillKeys.Contains(m.Key))
                {
                    return 0;
                }
                var elem = ModifierEstimate(missingModifiers.ToList(), auction.Tag, auction.FlatenedNBT, m);
                if (elem.Value == 0)
                {
                    deferred.Log($"Missing modifier value {m.Key} {m.Value} {auction.Uuid}");
                    return 4_000_000_000; // not found potentially very valuable
                }
                var value = elem.Value;
                if (m.Key == "upgrade_level" && modifiers.FirstOrDefault(e => e.Key == m.Key).Value != default)
                    value -= ModifierEstimate(modifiers.ToList(), auction.Tag, auction.FlatenedNBT, modifiers.FirstOrDefault(e => e.Key == m.Key)).Value;
                if (calculate && elem.IsEstimate)
                    return value / 20;
                return value;
            }).Sum();
        }

        private IEnumerable<(string tag, int amount)> GetItemKeysForModifier(string tag, KeyValuePair<string, string> m)
        {
            if (m.Key == "upgrade_level")
            {
                return EmptyArray;
            }
            if (m.Key == null)
                return EmptyArray;
            if (ModifierItemPrefixes.TryGetValue(m.Key, out var prefix))
                if (prefix == string.Empty)
                    return new (string, int)[] { (prefix + m.Value.ToUpper(), 1) };
                else
                    // some of the items actually don't have the prefix, skins on pets may but other skins don't
                    return new (string, int)[] { (prefix + m.Value.ToUpper(), 1), (m.Value.ToUpper(), 1) };

            if (mapper.TryGetIngredients(tag, m.Key, m.Value, null, out var ingredients))
            {
                return ingredients.GroupBy(i => i).Select(i => (i.Key, i.Count()));
            }
            return EmptyArray;
        }

        public long GetPriceForItem(string item)
        {
            if (Lookups.TryGetValue(item, out var lookup))
            {
                return lookup.Lookup.Values.FirstOrDefault(p => p.Price != 0)?.Price ?? 0;
            }
            return 0;
        }

        /// <summary>
        /// R4 — the bucket's cached interned <see cref="DomKey"/> for the dominance kernel, built lazily and reused (the
        /// DomKey is a pure function of the immutable key, so it never goes stale — no TTL). Same cache the
        /// <see cref="DominatorIndex"/> uses, so a finder scanning Lists (CheckCombined) and the index share one DomKey
        /// per bucket. Atomic-ref publish (tear-safe).
        /// </summary>
        private DomKey GetBucketDomKey(ReferenceAuctions bucket, AuctionKey key)
        {
            var box = bucket.DomKeyCache;
            if (box != null)
                return box.Value;
            var dom = DominatorIndex.BuildDomKey(key, scoreInterner);
            bucket.DomKeyCache = new DomKeyBox(dom);
            return dom;
        }

        /// <summary>
        /// Cached <see cref="ComparisonValue"/> for a stable candidate/query <see cref="AuctionKey"/> on the
        /// flatNbt-independent path. The brute-force closest searches price every candidate's enchants+modifiers once
        /// per incoming auction; the keys are stable, so memoizing the breakdown (TTL-swept in
        /// <see cref="FinishedUpdate"/> like the other value caches) removes the dominant per-candidate cost — and the
        /// per-modifier <see cref="ModifierLookupKey"/> hashing along with it. The returned list must be treated as
        /// read-only by callers (the closest searches only read/copy it).
        /// </summary>
        private List<RankElem> ComparisonValueForKey(string itemTag, AuctionKey key)
        {
            if (ComparisonValueLookup.TryGetValue((itemTag, key), out var cached))
                return cached.value;
            var computed = ComparisonValue(key.Enchants, key.Modifiers, itemTag, null)?.ToList() ?? new List<RankElem>();
            ComparisonValueLookup[(itemTag, key)] = (computed, DateTime.UtcNow);
            return computed;
        }

        /// <summary>
        /// Interned, columnar <see cref="ClosestScoreKernel.ScoreVec"/> for a key, derived 1:1 from
        /// <see cref="ComparisonValueForKey"/> so the kernel score is bit-exact with <c>AuctionKey.Similarity</c>.
        /// Cached/TTL-swept like the value caches; uses the shared <see cref="scoreInterner"/> so ids are consistent.
        /// </summary>
        private ClosestScoreKernel.ScoreVec ScoreVecForKey(string itemTag, AuctionKey key)
        {
            if (ScoreVecLookup.TryGetValue((itemTag, key), out var cached))
                return cached.vec;
            var vec = ClosestScoreKernel.Build(key, ComparisonValueForKey(itemTag, key), scoreInterner);
            ScoreVecLookup[(itemTag, key)] = (vec, DateTime.UtcNow);
            return vec;
        }

        /// <summary>
        /// Score vector for a candidate bucket, cached <b>on the bucket</b> so the closest-search scan reads it with no
        /// per-candidate dictionary lookup. Rebuilt when older than <paramref name="cutoff"/> (the value-cache TTL) —
        /// the vec depends only on (tag, key, cross-item prices), so this matches <see cref="ScoreVecForKey"/>'s
        /// freshness exactly while avoiding the hash+lookup per candidate.
        /// </summary>
        private ClosestScoreKernel.ScoreVec GetBucketScoreVec(string itemTag, AuctionKey key, ReferenceAuctions bucket, DateTime cutoff)
        {
            var box = bucket.ScoreVecCache; // single atomic read of the reference
            if (box != null && box.At >= cutoff)
                return box.Vec;
            var vec = ClosestScoreKernel.Build(key, ComparisonValueForKey(itemTag, key), scoreInterner);
            bucket.ScoreVecCache = new ClosestScoreKernel.ScoreVecCache(vec, DateTime.UtcNow); // atomic ref publish
            return vec;
        }

        private KeyValuePair<AuctionKey, ReferenceAuctions> FindClosestTo(PriceLookup lookup, AuctionKey itemKey, string itemTag, int maxAge = 8)
        {
            // WS-A: single-pass arg-max over a CONTIGUOUS candidate store instead of iterating the ConcurrentDictionary
            // per auction. Bit-exact with the old dict scan: the index holds exactly the priced, non-virtual buckets a
            // fresh filter would, each scored by the same ClosestScoreKernel.Score (built 1:1 from ComparisonValueForKey)
            // with the same '> minDay ? 0 : -10' age adjustment applied at scan time. Strict '>' keeps the first max.
            // The index is rebuilt only when the candidate set could have changed (pricing epoch or dict Count) or the
            // vec TTL elapsed; the snipe hot path never reprices, so it is reused across calls — the proven ~2.2x scan
            // (SoaScanBenchmarks) over the residual ~50-60 ns/bucket of dict-node + heap pointer-chasing.
            var minDay = GetDay() - maxAge;
            var queryVec = ScoreVecForKey(itemTag, itemKey); // query isn't a bucket -> small per-call cache
            var index = GetOrBuildCandidateIndex(lookup, itemTag);

            // WS-C: scalar branch-and-bound prefilter. For each candidate compute a cheap SOUND upper bound
            // UB = PosCap(query) + PosCap(cand) − exact tier/exp/count penalties (+100·age folded in); if UB/100 can't
            // beat the running best, the exact kernel can't either (exact ≤ UB), so skip it. Bit-exact with the plain
            // scan: same first-wins arg-max, pruning only provable losers. Prunes ~46–96% of exact-score calls
            // (WS-C-SIMD-FINDINGS.md: 2.3×–11× over the contiguous scan), zero extra allocation.
            var bestScore = long.MinValue;
            int bestIdx = -1;
            // R6 WS-LOH: columns are now chunked (per-block <85 KB, off the LOH). The indexer / VecRef(i) hide the
            // block-offset split; `in index.VecRef(i)` keeps the 112-byte ScoreVec read zero-copy, so the BnB scan is
            // unchanged byte-for-byte — same age adjust, same upper bound, same first-wins arg-max.
            int qTier = queryVec.Tier, qCount = queryVec.Count;
            long qCap = ClosestScoreKernel.PosCap(in queryVec);
            for (int i = 0; i < index.Count; i++)
            {
                ref readonly var vec = ref index.VecRef(i);
                int age = index.OldestRef[i] > minDay ? 0 : -10;
                long ub = qCap + index.Cap[i];
                int dTier = qTier - vec.Tier; int adt = dTier < 0 ? -dTier : dTier;
                ub -= (long)adt * 11_000_000;
                if (adt > 0 && vec.HasExpModifier) ub -= 100_000_000;
                int dc = qCount - vec.Count; ub -= (long)(dc < 0 ? -dc : dc) * 1_000_000;
                ub += 100L * age; // fold age so ub/100 is an upper bound on Score()+age
                if (ub / 100 <= bestScore) continue; // provably cannot beat best -> skip the exact kernel
                long score = ClosestScoreKernel.Score(in queryVec, in vec) + age;
                if (score > bestScore)
                {
                    bestScore = score;
                    bestIdx = i;
                }
            }
            var best = bestIdx < 0
                ? default
                : new KeyValuePair<AuctionKey, ReferenceAuctions>(index.Keys[bestIdx], index.Buckets[bestIdx]);

            if (VerifyClosestIndex)
                AssertClosestParity(lookup.Lookup, itemKey, itemTag, maxAge, index, bestScore);
            return best;
        }

        /// <summary>
        /// Returns the cached <see cref="ClosestCandidateIndex"/> for <paramref name="lookup"/>, rebuilding it when the
        /// candidate set could have changed (pricing <see cref="pricingEpoch"/> or live dict Count) or the vec TTL
        /// (10 min) elapsed. A rebuild force-publishes a fresh <see cref="ClosestScoreKernel.ScoreVec"/> to every
        /// candidate bucket, all stamped with one build time, so a concurrent dict scan via
        /// <see cref="GetBucketScoreVec"/> reads back the identical vec for the index's whole &lt;=10 min life — that is
        /// what keeps the index scan bit-exact with the dict scan (and lets <see cref="AssertClosestParity"/> verify it).
        /// </summary>
        private ClosestCandidateIndex GetOrBuildCandidateIndex(PriceLookup lookup, string itemTag)
        {
            var l = lookup.Lookup;
            var epoch = Interlocked.Read(ref pricingEpoch);
            int liveCount = l.Count;
            var now = DateTime.UtcNow;
            var cached = lookup.CandidateIndex; // single atomic read of the reference
            if (cached != null && cached.BuiltEpoch == epoch && cached.BuiltLookupCount == liveCount
                && cached.BuiltAt > now.AddMinutes(-CandidateIndexMaxAgeMinutes))
                return cached;

            // Reuse each bucket's cached vec when it is fresh (<= CandidateIndexMaxAgeMinutes old), rebuilding only the
            // stale ones — exactly GetBucketScoreVec's per-bucket lazy policy. A rebuild therefore costs ~one old-style
            // dict scan (cache hits), not a full re-derivation of every vec; bit-exact because reuse-window + index-TTL
            // keep every reused vec within the 10-min freshness a fresh dict scan would accept (see the const comment).
            var vecCutoff = now.AddMinutes(-CandidateIndexMaxAgeMinutes);
            // R6 WS-LOH: each column is built into BlockSize-element blocks (chunked) instead of one flat array. The flat
            // ScoreVec[] (112 B/elem) crossed the ~85 KB LOH threshold on hot tags (>~759 buckets) and promoted to gen2
            // every novel-key rebuild (the rare AllocLarge pause tail); the chunked blocks stay in the SOH and die in gen0.
            // The builder auto-grows by appending fresh blocks, so a dict that grows mid-enumeration never drops a
            // candidate (replacing the old Array.Resize). Membership / order / score / TTL are unchanged.
            var vecsB = new ClosestCandidateIndex.Builder<ClosestScoreKernel.ScoreVec>(liveCount);
            var keysB = new ClosestCandidateIndex.Builder<AuctionKey>(liveCount);
            var bucketsB = new ClosestCandidateIndex.Builder<ReferenceAuctions>(liveCount);
            var oldestB = new ClosestCandidateIndex.Builder<short>(liveCount);
            var capB = new ClosestCandidateIndex.Builder<long>(liveCount);
            int n = 0;
            foreach (var m in l)
            {
                if (m.Key == null || m.Value?.References == null || !(m.Value.Price > 0) || m.Key.Modifiers.Any(mod => mod.Key == "virtual"))
                    continue;
                var vec = GetBucketScoreVec(itemTag, m.Key, m.Value, vecCutoff); // reuses fresh bucket caches, publishes rebuilds
                vecsB.Add(in vec); keysB.Add(m.Key); bucketsB.Add(m.Value); oldestB.Add(m.Value.OldestRef);
                capB.Add(ClosestScoreKernel.PosCap(in vec)); // WS-C: query-independent upper-bound cap, precomputed once
                n++;
            }
            var index = new ClosestCandidateIndex(vecsB.Build(), keysB.Build(), bucketsB.Build(), oldestB.Build(),
                capB.Build(), n, epoch, liveCount, now);
            lookup.CandidateIndex = index; // atomic publish
            return index;
        }

        /// <summary>
        /// R4 — returns the cached <see cref="DominatorIndex"/> for <paramref name="lookup"/>, rebuilding it when the
        /// candidate set could have changed (pricing <see cref="pricingEpoch"/> or live dict Count) or the TTL elapsed —
        /// the SAME invalidation triggers as <see cref="GetOrBuildCandidateIndex"/>, the proven cached-snapshot pattern.
        /// The index caches only the IMMUTABLE, key-derived data (the interned <c>DomKey</c> + dominance masks + Keys +
        /// Buckets + Reforge/Tier); the finders that consume it read all MUTABLE bucket state (Price / Lbin / References)
        /// LIVE off <c>Buckets[i]</c> during their scan, so the shared index never makes a finder act on stale prices or
        /// references — it is bit-exact with the old per-finder <c>foreach (var x in l)</c> while removing the dict
        /// enumeration + the per-candidate string <see cref="IsHigherValue"/>. Atomic-ref published (tear-safe).
        /// </summary>
        private DominatorIndex GetOrBuildDominatorIndex(PriceLookup lookup)
        {
            var l = lookup.Lookup;
            var now = DateTime.UtcNow;
            var ttlCutoff = now.AddMinutes(-CandidateIndexMaxAgeMinutes);
            // NOTE: deliberately NOT gated on pricingEpoch (unlike GetOrBuildCandidateIndex). This index's membership is
            // price-INDEPENDENT (all non-null buckets) and every column the finders read (Doms/masks/Reforge/Keys/Buckets)
            // is immutable key-derived — the finders read all MUTABLE state (Price/Lbin/References) LIVE off the bucket.
            // So a price transition changes neither membership nor any used column; rebuilding on it would be pure churn.
            //
            // R5c (idx-grow): the per-auction full rebuild (six fresh O(N) column arrays -> LOH -> gen2 AllocLarge, the #1
            // rare-pause driver) is replaced with the append-amortized growable store. On the RARE path a novel key is
            // ADDED every auction (Count +1) — an APPEND of just that bucket's row into doubling-capacity backing arrays,
            // not a full re-alloc. The store falls back to a full rebuild only on a removal/replacement (a prior member is
            // gone), a Count shrink, or the TTL — its per-bucket membership stamp lets it verify soundly (seenPrior ==
            // priorCount) that the change is a PURE add, so an append is BIT-EXACT with a fresh Build (proven by
            // AssertDominatorParity over the live dict in the soak). The Count-keyed HIT keeps the same TTL-bounded
            // staleness contract as the old code on net-zero swaps/replacements.
            var store = lookup.DominatorIndexStore;
            if (store == null)
            {
                var created = new DominatorIndexStore();
                // First writer wins; any loser uses the published instance (the store itself is internally synchronized).
                store = Interlocked.CompareExchange(ref lookup.DominatorIndexStore, created, null) ?? created;
            }
            var index = store.GetOrBuild(l, scoreInterner, Interlocked.Read(ref pricingEpoch), now, ttlCutoff);
            lookup.DominatorIndex = index; // keep the legacy field pointing at the latest view (back-compat / inspection)
            return index;
        }

        /// <summary>
        /// R4 parity guard (gated by <see cref="VerifyDominatorIndex"/>): asserts the flat-index dominator set produced by
        /// scanning <paramref name="index"/> with the mask prefilter + <c>DominatorIndex.Dominates</c> equals a fresh
        /// <see cref="IsHigherValue"/> filter over the live dict, in BOTH directions. Side-effect-free. <paramref
        /// name="baseIsQuery"/> selects the direction: true = "candidates that dominate the query"
        /// (<c>IsHigherValue(tag, query, cand)</c>, used by GetLbinCap / PotentialSnipe); false = "candidates dominated by
        /// the query" (<c>IsHigherValue(tag, cand, query)</c>, used by CheckLowerKeyFull / the median lower-value scan).
        /// </summary>
        private void AssertDominatorParity(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKey query,
            string itemTag, DominatorIndex index, bool baseIsQuery)
        {
            bool petSpirit = itemTag == "PET_SPIRIT";
            var q = DominatorIndex.BuildDomKey(query, scoreInterner);
            // index dominator set (the exact predicate over the flat arrays; mask prefilter is proven sound by the fuzz
            // so applying it here would only confirm it — the guard checks the EXACT scan vs the live dict)
            var fromIndex = new HashSet<AuctionKey>();
            for (int i = 0; i < index.Count; i++)
            {
                bool dom = baseIsQuery
                    ? DominatorIndex.Dominates(in q, in index.Doms[i], petSpirit)
                    : DominatorIndex.Dominates(in index.Doms[i], in q, petSpirit);
                if (dom) fromIndex.Add(index.Keys[i]);
            }
            var fromDict = new HashSet<AuctionKey>();
            foreach (var m in l)
            {
                if (m.Key == null || m.Value?.References == null) continue;
                bool dom = baseIsQuery ? IsHigherValue(itemTag, query, m.Key) : IsHigherValue(itemTag, m.Key, query);
                if (dom) fromDict.Add(m.Key);
            }
            if (!fromIndex.SetEquals(fromDict))
                throw new InvalidOperationException(
                    $"DominatorIndex parity violation for {itemTag} (baseIsQuery={baseIsQuery}): index={fromIndex.Count} vs dict={fromDict.Count}");
        }

        /// <summary>
        /// Soak/test parity guard (gated by <see cref="VerifyClosestIndex"/>), side-effect-free so it faithfully reflects
        /// production. Checks the two invariants that actually matter, without re-deriving vecs (which would mutate the
        /// bucket caches and falsely flag the within-tolerance vec reuse):
        ///   (1) <b>BnB correctness</b> — a FULL scan over the index's OWN vecs (no prefilter) reproduces the pruned
        ///       arg-max; catches any wrongly-pruned winner.
        ///   (2) <b>Membership</b> — a fresh dictionary filter yields the same candidate count as the index; catches a
        ///       dropped/extra candidate, including a price flip (0&lt;-&gt;positive) that failed to bump the pricing epoch
        ///       (the new-bucket trap). Per-element score correctness is covered by ClosestScoreKernelTests (20k fuzz)
        ///       and the reuse-freshness invariant documented on <see cref="CandidateIndexMaxAgeMinutes"/>.
        /// </summary>
        private void AssertClosestParity(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKey itemKey, string itemTag, int maxAge, ClosestCandidateIndex index, long indexBestScore)
        {
            var minDay = GetDay() - maxAge;
            var queryVec = ScoreVecForKey(itemTag, itemKey);
            long fullBest = long.MinValue;
            for (int i = 0; i < index.Count; i++)
            {
                long score = ClosestScoreKernel.Score(in queryVec, in index.VecRef(i)) + (index.OldestRef[i] > minDay ? 0 : -10);
                if (score > fullBest) fullBest = score;
            }
            int freshCount = 0;
            foreach (var m in l)
                if (m.Key != null && m.Value?.References != null && m.Value.Price > 0 && !m.Key.Modifiers.Any(mod => mod.Key == "virtual"))
                    freshCount++;
            if (fullBest != indexBestScore || freshCount != index.Count)
                throw new InvalidOperationException(
                    $"ClosestCandidateIndex parity violation for {itemTag}: BnB(score={indexBestScore}) vs full(score={fullBest}); index.Count={index.Count} vs fresh={freshCount}");
        }
        public IEnumerable<KeyValuePair<AuctionKey, ReferenceAuctions>> FindClosest(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKey itemKey, string itemTag, int maxAge = 8)
        {
            var minDay = GetDay() - maxAge;
            var values = ComparisonValueForKey(itemTag, itemKey);
            return l.Where(l => l.Key != null && l.Value?.References != null && l.Value.Price > 0 && !l.Key.Modifiers.Any(m => m.Key == "virtual"))
                            .OrderByDescending(m => itemKey.Similarity(m.Key, this, ComparisonValueForKey(itemTag, m.Key), values) + (m.Value.OldestRef > minDay ? 0 : -10));
        }

        /// <summary>
        /// R3-READ: buffer-materialized, bit-exact equivalent of <see cref="FindClosest"/> for the hot read-path
        /// consumer (<see cref="GetEstimatedMedian"/>), which iterates the ordering with an early break. The LINQ form
        /// allocates a <c>Where</c> + <c>OrderByDescending</c> pair (predicate/selector closures, lazy enumerators) and
        /// re-invokes the score selector during the sort; this scans the dict once into pooled-shape local arrays, scores
        /// each candidate once with the identical kernel (<c>itemKey.Similarity(key, this, ComparisonValueForKey(key),
        /// values) + age</c>), and sorts descending. Bit-exact ordering: <c>OrderByDescending</c> is a STABLE sort, so
        /// ties keep source (dict-enumeration) order; we sort on <c>(score desc, sourceIndex asc)</c> over candidates
        /// collected in that same dict order, reproducing the exact sequence — including which candidate the consumer's
        /// "first with Median &gt; 0" break selects. Returns the candidates in order (the consumer indexes/breaks).
        /// </summary>
        private Scored[] FindClosestOrdered(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKey itemKey, string itemTag, int maxAge = 8)
        {
            var minDay = GetDay() - maxAge;
            var values = ComparisonValueForKey(itemTag, itemKey);
            // Single scored buffer (over-sized to dict count, trimmed to n) — one allocation instead of the LINQ
            // Where+OrderBy machinery (predicate/selector closures + lazy enumerators + internal key/map buffers).
            var buf = new Scored[l.Count];
            int n = 0;
            foreach (var m in l)
            {
                var v = m.Value;
                if (m.Key == null || v?.References == null || !(v.Price > 0) || ModifiersContainVirtual(m.Key.Modifiers))
                    continue;
                if (n >= buf.Length) // dict grew during enumeration -> grow so a candidate is never dropped
                    Array.Resize(ref buf, n + 8);
                int score = itemKey.Similarity(m.Key, this, ComparisonValueForKey(itemTag, m.Key), values)
                            + (v.OldestRef > minDay ? 0 : -10);
                buf[n] = new Scored(score, n, m.Key, v);
                n++;
            }
            // Stable descending order: Array.Sort is unstable, so the (score desc, sourceIndex asc) total order in
            // Scored.CompareTo makes the result deterministic and identical to OrderByDescending's stable order (ties
            // keep source/dict-enumeration order). Sort only the populated prefix [0, n).
            Array.Sort(buf, 0, n);
            if (n != buf.Length)
                Array.Resize(ref buf, n);
            return buf;
        }

        /// <summary>R3-READ scored candidate for <see cref="FindClosestOrdered"/>; sorts by (score desc, source index asc)
        /// to reproduce <c>OrderByDescending</c>'s stable order with a single buffer and no comparison delegate.</summary>
        private readonly struct Scored : IComparable<Scored>
        {
            public readonly int Score;
            public readonly int Index;
            public readonly AuctionKey Key;
            public readonly ReferenceAuctions Value;
            public Scored(int score, int index, AuctionKey key, ReferenceAuctions value)
            { Score = score; Index = index; Key = key; Value = value; }
            public int CompareTo(Scored o)
            {
                if (Score != o.Score) return o.Score.CompareTo(Score); // descending by score
                return Index.CompareTo(o.Index);                       // ties: ascending source index (stable)
            }
        }

        private static bool ModifiersContainVirtual(IReadOnlyList<KeyValuePair<string, string>> modifiers)
        {
            if (modifiers == null)
                return false;
            for (int i = 0; i < modifiers.Count; i++)
                if (modifiers[i].Key == "virtual")
                    return true;
            return false;
        }

        /// <summary>
        /// R3-READ: zero-alloc single-pass arg-max returning the SAME element <see cref="FindClosestOrdered"/> would put
        /// first — the top-scored closest candidate. <see cref="GetEstimatedMedian"/> almost always consumes only this
        /// one (it breaks on the first positive median), so the buffer+sort of FindClosestOrdered is avoided on the
        /// common path. Bit-exact with FindClosestOrdered[0]: same filter, same score
        /// (<c>itemKey.Similarity(key, this, ComparisonValueForKey(key), values) + age</c>), same dict-enumeration order;
        /// the ordered array's first element is the max score with ties broken by lowest source index, which a strict-'>'
        /// (first-wins) scan reproduces. Returns (null, null) when there is no candidate.
        /// </summary>
        private (AuctionKey Key, ReferenceAuctions Value) FindClosestArgMax(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKey itemKey, string itemTag, int maxAge = 8)
        {
            var minDay = GetDay() - maxAge;
            var values = ComparisonValueForKey(itemTag, itemKey);
            int bestScore = 0;
            AuctionKey bestKey = null;
            ReferenceAuctions bestValue = null;
            foreach (var m in l)
            {
                var v = m.Value;
                if (m.Key == null || v?.References == null || !(v.Price > 0) || ModifiersContainVirtual(m.Key.Modifiers))
                    continue;
                int score = itemKey.Similarity(m.Key, this, ComparisonValueForKey(itemTag, m.Key), values)
                            + (v.OldestRef > minDay ? 0 : -10);
                if (bestKey is null || score > bestScore)
                {
                    bestScore = score;
                    bestKey = m.Key;
                    bestValue = v;
                }
            }
            return (bestKey, bestValue);
        }

        void AssignMedian(PriceEstimate result, AuctionKey key, ReferenceAuctions bucket, long gemVal)
        {
            result.Median = bucket.Price + gemVal + (Math.Max((key as AuctionKeyWithValue)?.ValueSubstract - gemVal * 20 / 19 ?? 0, 0) / 3);
            result.Volume = bucket.Volume;
            result.MedianKey = key.ToString();
            result.Volatility = bucket.Volatility;
            result.LastSale = bucket.References.LastOrDefault();
            result.AvgSellTime = (short)bucket.TimeToSell;
        }

        internal void Move(string tag, long auctionId, AuctionKey from, string toTag, AuctionKey to)
        {
            var oldBucket = Lookups[tag].Lookup[from];
            var newBucket = GetOrAdd(to, Lookups[toTag]);

            var toChange = oldBucket.References.Where(e => e.AuctionId == auctionId).First();
            var newList = oldBucket.References.Where(e => e.AuctionId != auctionId).ToList();
            oldBucket.SetReferences(newList);

            if (!newBucket.References.Contains(toChange))
                newBucket.EnqueueReference(toChange);

            UpdateMedian(oldBucket, (GetAuctionGroupTag(tag).tag, GetBreakdownKey(from, tag)));
            UpdateMedian(newBucket, (GetAuctionGroupTag(toTag).tag, GetBreakdownKey(to, tag)));
        }

        public IEnumerable<long> GetReferenceUids(SaveAuction auction)
        {
            if (TryGetReferenceAuctions(auction, out ReferenceAuctions bucket))
                return bucket.References.Select(r => r.AuctionId);
            return new long[0];
        }

        /// <summary>
        /// Adds persisted lookup data
        /// </summary>
        /// <param name="itemTag"></param>
        /// <param name="loadedVal"></param>
        public void AddLookupData(string itemTag, PriceLookup loadedVal)
        {
            foreach (var item in loadedVal.Lookup.Keys)
            {
                var value = loadedVal.Lookup.GetValueOrDefault(item);
                if (value == null)
                    continue;
                if (value.References.Count == 0 && value.Lbins.Count == 0 || value.References.All(r => r.Day == 1047) // lost nbt data that day
                    || value.References.All(r => r.Day < GetDay() - 21) && !item.IsClean())
                    loadedVal.Lookup.TryRemove(item, out _); // unimportant
                if (NBT.IsPet(itemTag) && !item.Modifiers.Any(m => m.Key == "exp"))
                    loadedVal.Lookup.TryRemove(item, out _); // bugged
            }
            UpdateFraggedAndRune(itemTag);
            if (craftCostService != null)
                craftCostService.ItemCategories[itemTag] = loadedVal.Category;
            if (IsRune(itemTag))
            {
                foreach (var item in loadedVal.Lookup.Keys.ToList())
                {
                    // the rune level should never be removed
                    if (!item.Modifiers.Any(m => m.Key.Contains("RUNE")))
                        loadedVal.Lookup.TryRemove(item, out _);
                }
            }
            var current = Lookups.AddOrUpdate(itemTag, (id) =>
            {
                foreach (var item in loadedVal.Lookup)
                {
                    item.Value.SetReferences(item.Value.References.Where(r => r.Price > 0).OrderBy(r => r.Day));
                    if (item.Key.Count == 0 && item.Key.Tier == Tier.UNKNOWN && item.Value.References.Count == 0)
                        continue;
                    loadedVal.Lookup[item.Key] = item.Value;
                }
                if (loadedVal.CleanKey?.Count == default || !loadedVal.Lookup.TryGetValue(loadedVal.CleanKey, out var b) || b?.Volume * 10 < loadedVal.Lookup.Max(l => l.Value.Volume))
                {
                    UpdateCleanKey(loadedVal);
                }
                return loadedVal;
            }, (tag, value) =>
            {
                foreach (var item in loadedVal.Lookup)
                {
                    if (!value.Lookup.TryGetValue(item.Key, out ReferenceAuctions existingBucket))
                    {
                        item.Value.SetReferences(item.Value.References.Where(r => r.Price > 0).OrderBy(r => r.Day));
                        if (item.Key.Count == 0 && item.Key.Tier == Tier.UNKNOWN && item.Value.References.Count == 0)
                            continue;
                        value.Lookup[item.Key] = item.Value;
                        continue;
                    }
                    CombineBuckets(item, existingBucket);
                    item.Value.Lbins.Sort(ReferencePrice.Compare);
                }
                return value;
            });
            var idLookup = current.Lookup.SelectMany(l => l.Value.References.Select(r => r.AuctionId)).GroupBy(a => a).Where(g => g.Count() > 1).Select(g => g.Key).ToHashSet();
            foreach (var item in current.Lookup.Keys.ToList())
            {
                try
                {
                    // remove all with perfect
                    if (item.Modifiers.Any(m => m.Value == "PERFECT"))
                        current.Lookup.TryRemove(item, out _);
                    // remove all with too many enchants
                    if (item.Enchants.Count > 5)
                        current.Lookup.TryRemove(item, out _);
                    //Deduplicate(itemTag, current, item); probably not needed anymore
                    if (item.Modifiers.Count <= 1 && current.Lookup.TryGetValue(item, out var tocheck) && tocheck.References.Any(r => idLookup.Contains(r.AuctionId)))
                    {
                        tocheck.SetReferences(tocheck.References.Where(r => !idLookup.Contains(r.AuctionId)).ToList());
                    }
                }
                catch (System.Exception e)
                {
                    dev.Logger.Instance.Error(e, $"Could not deduplicate");
                }
            }

            // Persisted lookup snapshots are mostly trusted on load.
            // Only rebuild buckets that are clearly stale against recent references so startup
            // does not behave like a full RefreshLookup for every persisted bucket.
            RefreshClearlyStaleLoadedBuckets(itemTag, current);

            void CombineBuckets(KeyValuePair<AuctionKey, ReferenceAuctions> item, ReferenceAuctions existingBucket)
            {
                var existingRef = existingBucket.References;
                existingBucket.SetReferences(item.Value.References);
                if (existingRef != null)
                {
                    existingBucket.SetReferences(existingRef.Concat(item.Value.References).ToList()
                        .DistinctBy(d => d.AuctionId)
                        .OrderBy(r => r.Day));

                    var today = GetDay();
                    if (existingBucket.References.Count > 7 && existingBucket.References.TryPeek(out var r) && r.Day < today - 30)
                    {
                        var cleaned = ApplyAntiMarketManipulation(existingBucket);
                        var cleanedMedian = GetMedian(cleaned, null);
                        var median = GetMedian(existingBucket.References.Where(ri => ri.Day > today - 30).ToList(), null);
                        var increaseRate = (double)median / cleanedMedian;
                        var allowedIncrease = 1.5;
                        if (increaseRate > allowedIncrease)
                        {
                            increaseRate /= allowedIncrease;
                        }
                        else if (increaseRate < 1)
                        {
                            increaseRate = 1;
                        }
                        while (existingBucket.References.Count > 7 && existingBucket.References.TryPeek(out r) && r.Day < today - 30 * increaseRate)
                        {
                            existingBucket.TryDequeueReference(out _);
                        }
                    }
                }
                existingBucket.Price = item.Value.Price;
                Interlocked.Increment(ref pricingEpoch); // WS-A: out-of-band price write -> invalidate candidate indexes
                if (item.Value.Lbins == null)
                    item.Value.Lbins = new();
                // load all non-empty lbins
                foreach (var binAuction in item.Value.Lbins)
                {
                    if (!existingBucket.Lbins.Contains(binAuction) && binAuction.Price > 0)
                        existingBucket.Lbins.Add(binAuction);
                }
                CapBucketSize(existingBucket);
            }
        }

        /// <summary>
        /// Repairs persisted bucket medians only when recent deduplicated references show the
        /// loaded value is clearly stale. Clean buckets repair stale-high medians; kill buckets
        /// also repair stale-low medians because their progression premium is easy to understate
        /// in serialized snapshots. The check stays selective to avoid full startup churn.
        /// </summary>
        private void RefreshClearlyStaleLoadedBuckets(string itemTag, PriceLookup lookup)
        {
            if (lookup?.Lookup == null)
                return;

            foreach (var item in lookup.Lookup.ToList())
            {
                var bucket = item.Value;
                if (bucket == null || bucket.Price <= 0 || bucket.References.Count < 6)
                    continue;

                var isCleanKey = item.Key.Enchants.Count == 0 && item.Key.Modifiers.Count == 0;
                var hasKillModifier = item.Key.Modifiers.Any(m => KillKeys.Contains(m.Key));
                if (!isCleanKey && !hasKillModifier)
                    continue;

                var deduplicated = ApplyAntiMarketManipulation(bucket);
                DropUnderlistings(deduplicated);
                var recent = deduplicated.Where(r => r.Day >= GetDay() - 30)
                    .OrderByDescending(r => r.Day)
                    .ToList();
                if (recent.Count < 4)
                    continue;

                var recentMedian = GetMedian(GetShortTermBatch(recent, bucket.Volume), null);
                if (recentMedian == 0)
                    continue;

                var isClearlyStaleHigh = isCleanKey
                    && bucket.Price > recentMedian * LoadedBucketRebuildRatioNumerator / LoadedBucketRebuildRatioDenominator
                    && bucket.Price - recentMedian > LoadedBucketRebuildMinimumDelta;
                var isClearlyStaleLow = hasKillModifier
                    && recentMedian > bucket.Price * LoadedBucketRebuildRatioNumerator / LoadedBucketRebuildRatioDenominator
                    && recentMedian - bucket.Price > LoadedBucketRebuildMinimumDelta;
                if (!isClearlyStaleHigh && !isClearlyStaleLow)
                    continue;

                UpdateMedian(bucket, (itemTag, GetBreakdownKey(item.Key, itemTag)));
            }
        }

        private static void UpdateFraggedAndRune(string itemTag)
        {
            if (itemTag.StartsWith("STARRED_") &&
            // midas and daedalus needs golden fragments which are expensive
            !MidasTags.Contains(itemTag) && !itemTag.StartsWith("STARRED_DAEDALUS_AXE"))
            {
                CombinableStarred.Add(itemTag);
            }
            if (itemTag.Contains("RUNE_"))
            {
                RuneLookup.Add(itemTag);
                RuneLookup.Add(itemTag.Replace("UNIQUE_", ""));
            }
        }

        private static bool IsRune(string itemTag)
        {
            return RuneLookup.Contains(itemTag);
        }

        private static void CapBucketSize(ReferenceAuctions bucket)
        {
            while (bucket.References.Count > SizeToKeep && bucket.TryDequeueReference(out _)) { }
        }

        public short AddSoldItem(SaveAuction auction, bool preventMedianUpdate = false)
        {
            UpdateFraggedAndRune(auction.Tag);
            (ReferenceAuctions bucket, var key) = GetBucketForAuction(auction);
            var extraValue = GetExtraValue(auction, key);
            var time = AddAuctionToBucket(auction, preventMedianUpdate, bucket, key.ValueSubstract, extraValue);
            HigherValueLbinMapLookup.TryRemove(((string, AuctionKey))(auction.Tag, key), out _);
            try
            {
                var attributesOnAuction = auction.FlatenedNBT.Where(a => Constants.AttributeKeys.Contains(a.Key)).ToList();
                if (attributesOnAuction.Count == 0)
                    return time;
                if (key.Enchants.Count > 1 || key.Modifiers.Count > 2)
                    return time; // only add attributes for (almost) clean items, one allowed for things that drop with extra enchants
                var groupTag = GetAuctionGroupTag(auction.Tag);
                var itemGroupTag = groupTag.Item1;
                foreach (var item in attributesOnAuction)
                {
                    AuctionKey itemKey = VirtualAttributeKey(item);
                    var bucketForAttribute = GetOrAdd(itemKey, Lookups[itemGroupTag]);
                    var level = int.Parse(item.Value);
                    var power = Math.Pow(2, level - 1);
                    var toSubstractForLvl1 = auction.HighestBidAmount - auction.HighestBidAmount / power;
                    AddAuctionToBucket(auction, preventMedianUpdate, bucketForAttribute, (long)toSubstractForLvl1);
                    foreach (var updateItem in AttributeValueLookup.Where(l =>
                        (l.Key.Item1 == groupTag.tag || groupTag.tag == "ATTRIBUTE_SHARD")
                        && l.Key.Item2.Key == itemKey.Modifiers.First().Key).ToList())
                    {
                        AttributeValueLookup.TryRemove(updateItem.Key, out _);
                    }
                }
                OnSold?.Invoke((auction, key));
            }
            catch (System.Exception e)
            {
                dev.Logger.Instance.Error(e, $"Occured when trying to store attribue value");
            }
            return time;
        }

        private static AuctionKey VirtualAttributeKey(KeyValuePair<string, string> item)
        {
            return new AuctionKey()
            {
                Modifiers = new(new List<KeyValuePair<string, string>>()
                        {
                            new(item.Key, "1"),
                            new("virtual",string.Empty)
                        })
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="auction"></param>
        /// <param name="preventMedianUpdate"></param>
        /// <param name="bucket"></param>
        /// <param name="valueSubstract">Extra value to substract based on what is not matching in the bucket</param>
        public short AddAuctionToBucket(SaveAuction auction, bool preventMedianUpdate, ReferenceAuctions bucket, long valueSubstract = 0, long extraValue = 0)
        {
            if (bucket.References.Where(r => r.AuctionId == auction.UId).Any())
                return -1; // duplicate
            var reference = CreateReferenceFromAuction(auction, valueSubstract, extraValue);
            if (reference.Price < 0 && valueSubstract > 1_000_000)
            {
                logger.LogInformation($"Negative price {JsonConvert.SerializeObject(auction)} {reference.Price} {valueSubstract}");
            }
            // move reference to sold
            var activeLbin = bucket.Lbins.FirstOrDefault(l => l.AuctionId == auction.UId);
            if (activeLbin.AuctionId != default)
            {
                var listedAt = StartTime.AddDays(activeLbin.Day) + TimeSpan.FromMinutes(activeLbin.SellTime);
                var soldAt = auction.End;
                reference.SellTime = (short)(soldAt - listedAt).TotalMinutes;
            }
            else if (auction.Start != default)
            {
                reference.SellTime = (short)((auction.End - auction.Start).TotalMinutes + 1); // add one in case it sold within a minute
            }
            bucket.EnqueueReference(reference);
            bucket.Lbins.RemoveAll(l => l.AuctionId == auction.UId);
            CapBucketSize(bucket);
            if (!preventMedianUpdate)
            {
                var key = DetailedKeyFromSaveAuction(auction);
                if (NBT.IsPet(auction.Tag) && !key.Key.Modifiers.Any(m => m.Key == "exp"))
                {
                    logger.LogError($"Exp lost {auction.Uuid} {JsonConvert.SerializeObject(key)}");
                }
                UpdateMedian(bucket, (GetAuctionGroupTag(auction.Tag).tag, key));
            }
            return reference.SellTime;
        }

        public void UpdateMedian(ReferenceAuctions bucket, (string tag, KeyWithValueBreakdown key) keyCombo = default)
        {
            // WS-A: UpdateMedian is the funnel for (almost) every bucket price transition (priced<->unpriced), which is
            // exactly what changes the closest-search candidate set. Bumping the pricing epoch here invalidates the
            // contiguous candidate indexes so a newly-priced/unpriced bucket is never missed (the reverted set-caching
            // trap). Service-wide and unconditional: UpdateMedian isn't on the snipe hot path, so spurious bumps only
            // cost an occasional index rebuild; correctness is the priority.
            Interlocked.Increment(ref pricingEpoch);
            var size = bucket.References.Count;
            if (size < 4)
            {
                if (keyCombo != default)
                    bucket.Price = 0;
                return; // can't have enough volume
            }
            List<ReferencePrice> deduplicated = ApplyAntiMarketManipulation(bucket);
            DropUnderlistings(deduplicated);

            size = deduplicated.Count();
            if (size <= 3 || deduplicated.Count(d => d.Day >= GetDay() - 20) < 3 && !(keyCombo.Item2?.Key.IsClean() ?? false) && !IsMaxAttrib(keyCombo))
            {
                bucket.Price = 0; // to low vol
                return;
            }
            var days = GetDay() - deduplicated.OrderBy(d => d.Day).First().Day + 1;
            if (days == 0)
                days = 1;
            bucket.Volume = deduplicated.Count / days;
            // short term protects against price drops after updates
            List<ReferencePrice> shortTermList = GetShortTermBatch(deduplicated, bucket.Volume).OrderByDescending(b => b.Day).ToList();
            PriceLookup lookup;
            Dictionary<short, long> cleanPriceLookup;
            bool isCleanitem;
            GetCleanPriceLookup(keyCombo, out lookup, out cleanPriceLookup, out isCleanitem);
            var shortTermPrice = GetMedian(shortTermList, cleanPriceLookup);
            if (IsDropping(bucket, shortTermPrice, out var rate))
            {
                shortTermPrice = (long)Math.Max((shortTermPrice * Math.Max(rate, 0.82)),
                    lookup?.CleanPricePerTier?.GetValueOrDefault(keyCombo.key.Key.Tier) * 0.99 ?? 0);
            }
            bucket.OldestRef = shortTermList.Take(4).Min(s => s.Day);
            if (shortTermList.Count >= 3 && bucket.OldestRef - shortTermList.First().Day <= -5
                && shortTermList.First().AuctionId != shortTermList.OrderByDescending(o => o.Price).First().AuctionId
                && bucket.References.OrderByDescending(r => r.Day).Skip(5).FirstOrDefault().Day <= GetDay() - 5 // check if anti market manipulation made a hole
                && bucket.Volume > 0.3) // 5 day gaps are to be expected at ~0.2 volume
            {
                // probably derpy or weird price drop
                var reduced = (shortTermList.OrderBy(s => s.Price).First().Price + shortTermPrice * 2) / 3;
                shortTermPrice = Math.Max(shortTermPrice * 8 / 10, reduced);
            }
            // long term protects against market manipulation
            var monthSpan = deduplicated.Where(d => d.Day >= GetDay() - 30).ToList();
            var longSpanPrice = monthSpan.Count switch
            {
                > 24 => HighReferenceCount(cleanPriceLookup, monthSpan),
                > 5 => GetMedian(monthSpan, cleanPriceLookup),
                _ => GetMedian(deduplicated.Take(29).ToList(), cleanPriceLookup)
            };
            var today = GetDay();
            if (deduplicated.All(d => d.Day >= today))
            {
                // all prices are from today, use 25th percentile instead
                longSpanPrice = deduplicated.OrderBy(d => d.Price).Take((int)Math.Max(deduplicated.Count() * 0.25, 1)).Max(d => d.Price);
            }
            var uncappedMedian = Math.Min(shortTermPrice, longSpanPrice);
            var medianPrice = uncappedMedian;
            if (keyCombo == default)
            {
                bucket.Price = medianPrice; // bazaar price
                return;
            }
            var lbinMedian = bucket.Lbins.Where(l => l.Price > medianPrice / 2 && l.Day > GetDay() + 5).OrderBy(l => l.Price).Skip(2).FirstOrDefault();
            if (lbinMedian.AuctionId != default)
            {
                medianPrice = Math.Min(medianPrice, lbinMedian.Price);
                shortTermPrice = Math.Min(shortTermPrice, lbinMedian.Price);
            }
            (bucket.Volatility, medianPrice) = GetVolatility(lookup, bucket, shortTermPrice, longSpanPrice);
            bucket.HitsSinceCalculating = 0;
            bucket.StonksHits = 0;
            bucket.DeduplicatedReferenceCount = (short)deduplicated.Count();
            PreCalculateVolume(keyCombo);
            bucket.TimeToSell = (int)EstimateTimeTosell(deduplicated, medianPrice, bucket.Volume);
            // get price of item without enchants and add enchant value 
            if (keyCombo != default)
            {
                var breakdown = keyCombo.key.ValueBreakdown;
                var volatMedian = medianPrice;
                var limitedPrice = CapPriceAtHigherLevelKey(keyCombo, medianPrice, bucket);

                var itemTier = keyCombo.key.Key.Tier;
                if (keyCombo.key.Key.Modifiers.Any(m => m.Value == TierBoostShorthand))
                {
                    itemTier--;
                }

                // check higher value keys for lower price 
                // refresh clean price before tierval check to prevent stale inflation
                lookup.CleanPricePerTier ??= new();
                var cleanPrice = GetCleanItemPrice(keyCombo.tag, keyCombo.key, lookup, true);
                if (cleanPrice > 0)
                    lookup.CleanPricePerTier[keyCombo.key.Key.Tier] = cleanPrice;
                if (keyCombo.key.Key.Modifiers.Any(m => m.Key == "pgems"))
                {
                    var lookupKey = new AuctionKey(keyCombo.key)
                    {
                        Modifiers = new(keyCombo.key.Key.Modifiers.Where(m => m.Key != "pgems").ToList())
                    };
                    if (lookup.Lookup.TryGetValue(lookupKey, out var lookupBucket))
                    {
                        limitedPrice = Math.Max(limitedPrice, lookupBucket.Price);
                    }
                }
                else if (lookup.CleanPricePerTier?.TryGetValue(itemTier, out var tierval) ?? false)
                {
                    // (removed: a dead `lowest = Lookup.Where(...).OrderBy(...).Take(5).ToList()` that was computed but
                    //  never read — it allocated a KeyValuePair[] buffer (OrderBy) every reprice for the jaded-clean branch.)
                    // check clean item value is higher
                    if (limitedPrice < tierval / 1.2 && !keyCombo.key.Key.Modifiers.Any(m => m.Key == "virtual" || Constants.AttributeKeys.Contains(m.Key))
                        && !IsMidas(keyCombo.tag))
                        limitedPrice = Math.Max(limitedPrice, tierval);
                }
                limitedPrice = CapAtCraftCost(keyCombo.tag, limitedPrice, keyCombo.key, bucket.Price);
                var craftCostCap = limitedPrice;
                if (limitedPrice == 0 || IsLevel100CleanPet(keyCombo.key))
                {
                    limitedPrice = medianPrice;
                }

                if (size > 40 || bucket.Volatility <= 8 && size > 8)
                {
                    bucket.RiskyEstimate = Get66thPercentile(cleanPriceLookup, monthSpan, limitedPrice, medianPrice);
                }
                else if (size > 10 && bucket.Volatility < 18)
                {
                    var riskyEst = Get66thPercentile(cleanPriceLookup, monthSpan, limitedPrice, medianPrice);
                    bucket.RiskyEstimate = (limitedPrice + riskyEst) / 2;
                }
                else
                {
                    bucket.RiskyEstimate = limitedPrice;
                }

                if (limitedPrice == 0)
                {
                    if (longSpanPrice != 0)
                        logger.LogWarning($"Price capped {keyCombo.tag} -> {limitedPrice} ({craftCostCap}) {keyCombo.key.Key} {medianPrice} {bucket.Price} - {volatMedian} {shortTermPrice} {longSpanPrice}");
                    limitedPrice = 11;
                }
                var preLimitedPrice = medianPrice;
                medianPrice = limitedPrice;
                if (medianPrice < 0)
                {
                    logger.LogWarning($"Negative price {keyCombo.tag} -> {limitedPrice}  {keyCombo.key} {medianPrice} {bucket.Price}");
                }
                else
                    bucket.Price = medianPrice;

                var recent = bucket.References.AsEnumerable().Reverse().Take(Math.Max(bucket.Volume < 0.5 ? 6 : 12, (int)bucket.Volume)).ToList();
                var percentileRecent = GetMedian(recent, cleanPriceLookup, 3f);
                bool lbinListedForShort = bucket.Lbins.Where(l => l.Price < medianPrice * 1.1).Select(l => l.Day - GetDay()).Take(4).DefaultIfEmpty(3).Average() < 4;
                if (bucket.Volume >= 4 && bucket.Lbin.AuctionId != default && lbinListedForShort && percentileRecent > medianPrice)
                { // volume high enought to risk higher percentile
                    var cappedPrice = preLimitedPrice == medianPrice ? preLimitedPrice * 12 / 10 : limitedPrice;
                    medianPrice = Math.Min(Math.Max(bucket.RiskyEstimate, medianPrice), Math.Min(cappedPrice, percentileRecent));
                    var deduplicatedRecent = deduplicated.OrderByDescending(r => r.Day).Take(9).OrderBy(r => r.Price).Skip(6).FirstOrDefault();
                    if (deduplicatedRecent.AuctionId != default && deduplicatedRecent.Price < medianPrice)
                    {
                        medianPrice = Math.Min(medianPrice, deduplicatedRecent.Price);
                    }
                }
                else if (keyCombo.key.Key.Modifiers.Count <= 1) // this condition could be extended to everything but the calculation can cause undervaluations on eg gems
                    medianPrice = Math.Min(medianPrice, percentileRecent);
                // De-LINQ: the old `Lookup.Where(Tier!=UNKNOWN).GroupBy(Tier).Count() > 2` buffered every
                // KeyValuePair of the ConcurrentDictionary into an array (the dominant warm KeyValuePair[] allocator).
                // Equivalent explicit pass: detect ">2 distinct non-UNKNOWN tiers" by collecting the first 3 distinct
                // tiers and early-exiting once a 3rd appears (bit-exact: same predicate, same distinct semantics).
                lookup.HasMultipleRarities = HasMoreThanTwoDistinctTiers(lookup.Lookup);

                var keyWithNoEnchants = new AuctionKey(keyCombo.Item2)
                {
                    Enchants = new(new List<Enchant>())
                };

                if (keyCombo.Item2.Key.Count > 1)
                {
                    var lowerCountKey = new AuctionKey(keyCombo.Item2)
                    {
                        Count = 1
                    };
                    if (lookup.Lookup.TryGetValue(lowerCountKey, out var lowerCountBucket)
                        && lowerCountBucket.Price != 0
                        && lowerCountBucket.Price * keyCombo.Item2.Key.Count < medianPrice)
                    {
                        medianPrice = Math.Min(medianPrice, lowerCountBucket.Price * keyCombo.Item2.Key.Count);
                    }
                }
            }
            else
            {
                logger.LogWarning($"No key combo {keyCombo.tag} {bucket.Price} {bucket.References.First().AuctionId}");
            }
            if (isCleanitem)
            {
                lookup.CleanPricePerDay ??= new();
                if (medianPrice > 0)
                    lookup.CleanPricePerDay[shortTermList.OrderByDescending(s => s.Day).First().Day] = uncappedMedian;
            }
            if (medianPrice < 0)
            {
                logger.LogWarning($"Negative price {keyCombo.tag} -> {medianPrice}  {keyCombo.key} {shortTermPrice} {longSpanPrice}");
            }
            else
                bucket.Price = medianPrice;

            bool IsMaxAttrib((string tag, KeyWithValueBreakdown key) keyCombo)
            {
                var matchCount = keyCombo.key?.Key?.Modifiers?.Where(m => Constants.AttributeKeys.Contains(m.Key) && m.Value == "10").Count();
                return matchCount == 2;
            }

            long CapPriceAtHigherLevelKey((string tag, KeyWithValueBreakdown key) keyCombo, long limitedPrice, ReferenceAuctions bucket)
            {
                var oldestDay = bucket.OldestRef;
                if (keyCombo.key.Key.Modifiers.Any(m => m.Key == "new_years_cake" && ImportantCakeYears.Contains(m.Value)))
                {
                    return limitedPrice;
                }
                // De-LINQ: the old `Lookup.Where(<predicate>).OrderBy(b => b.Value.Price).FirstOrDefault()` buffered every
                // matching ConcurrentDictionary KeyValuePair into an array (the OrderBy sort buffer — a top warm KVP[]
                // allocator) and boxed a Modifiers enumerator per candidate (the `.Any(m=>m.Key=="virtual")` — the
                // KeyValuePair<string,string> Enumerator allocator). Single-pass arg-min preserves OrderBy's *stable*
                // tie-break exactly: among entries tied for the minimum Value.Price, FirstOrDefault returns the one
                // enumerated first, so we replace only on a STRICTLY smaller price (never on equal) — and a foreach over
                // the ConcurrentDictionary visits entries in the same order the LINQ pipeline did. Bit-exact.
                var cheaperHigherValue = default(KeyValuePair<AuctionKey, ReferenceAuctions>);
                bool foundCheaper = false;
                long bestCheaperPrice = 0;
                foreach (var k in Lookups[keyCombo.tag].Lookup)
                {
                    var v = k.Value;
                    if (v.Price < limitedPrice && v.Price != 0
                        && keyCombo.key.Key != k.Key
                        && !ModifiersContainVirtual(k.Key.Modifiers)
                        && v.OldestRef >= oldestDay // only relevant if price dropped recently
                        && v.DeduplicatedReferenceCount > 3
                        && v.Volume * 5 >= bucket.Volume
                        && IsHigherValue(keyCombo.tag, keyCombo.key, k.Key) && k.Key.Reforge == keyCombo.key.Key.Reforge)
                    {
                        if (!foundCheaper || v.Price < bestCheaperPrice)
                        {
                            cheaperHigherValue = k;
                            bestCheaperPrice = v.Price;
                            foundCheaper = true;
                        }
                    }
                }
                if (cheaperHigherValue.Value != default
                    && cheaperHigherValue.Key != keyCombo.key.Key
                    && cheaperHigherValue.Value.Price < limitedPrice)
                {
                    limitedPrice = cheaperHigherValue.Value.Price;
                }

                return limitedPrice;
            }

            void GetCleanPriceLookup((string tag, KeyWithValueBreakdown key) keyCombo, out PriceLookup lookup, out Dictionary<short, long> cleanPriceLookup, out bool isCleanitem)
            {
                if (keyCombo == default)
                {
                    lookup = default;
                    cleanPriceLookup = default;
                    isCleanitem = false;
                    return;
                }
                lookup = Lookups.GetOrAdd(keyCombo.tag, new PriceLookup());
                cleanPriceLookup = lookup.CleanPricePerDay;
                if (lookup.CleanKey?.Count == default && lookup.Lookup.Count > 1)
                {
                    UpdateCleanKey(lookup);
                }
                isCleanitem = keyCombo.key?.Key == lookup.CleanKey;
                if (lookup.CleanKey == default)
                    cleanPriceLookup = new();
                else if (isCleanitem)
                {
                    // For single-variant items with sufficient price history, apply cleanPricePerDay
                    // adjustment to account for declining market trends. SelectAdjustedPrice only
                    // adjusts downward (when clean > today), so stable/rising markets are unaffected.
                    var meaningfulBuckets = lookup.Lookup.Count(l => l.Value.References.Count >= 4);
                    if (meaningfulBuckets > 1 || cleanPriceLookup.Count < 5)
                        cleanPriceLookup = new();
                }
            }

            static long HighReferenceCount(Dictionary<short, long> cleanPriceLookup, List<ReferencePrice> monthSpan)
            {
                var lastTwoWeeks = monthSpan.Where(d => d.Day >= GetDay() - 10).ToList();
                if (lastTwoWeeks.Count < 5)
                {
                    return GetMedian(monthSpan, cleanPriceLookup);
                }
                return GetMedian(lastTwoWeeks, cleanPriceLookup);
            }

            static long Get66thPercentile(Dictionary<short, long> cleanPriceLookup, List<ReferencePrice> monthSpan, long limitedPrice, long medianPrice)
            {
                var riskyLongTerm = GetMedian(monthSpan.Where(d => d.Day >= GetDay() - 10).ToList(), cleanPriceLookup, 3f);
                var riskyShort = GetMedian(monthSpan.Where(d => d.Day >= GetDay() - 2).ToList(), cleanPriceLookup, 3f);
                var marketManipLimit = limitedPrice * 10 / 9 + 1_000_000;
                if (medianPrice > limitedPrice)
                {// already capped by craft cost reduce limit
                    marketManipLimit = limitedPrice * 11 / 10;
                }
                var estimate = Math.Min(Math.Min(riskyShort, riskyLongTerm), marketManipLimit);
                return estimate;
            }

            static double EstimateTimeTosell(List<ReferencePrice> deduplicated, long medianPrice, float volume)
            {
                var relevant = deduplicated
                    .Where(d => d.SellTime > 0 && d.Price > medianPrice * 0.96 && d.Price < medianPrice * 1.1);
                return relevant.DefaultIfEmpty().Average(d => d.SellTime) * Math.Max(1, 2 / volume);
            }
        }

        /// <summary>
        /// Returns whether the lookup contains more than two DISTINCT non-UNKNOWN tiers — bit-exact replacement for
        /// <c>Lookup.Where(l =&gt; l.Key.Tier != Tier.UNKNOWN).GroupBy(l =&gt; l.Key.Tier).Count() &gt; 2</c>, without the
        /// per-call KeyValuePair[] LINQ buffering over the ConcurrentDictionary. Collects up to three distinct tiers
        /// and short-circuits the moment a third appears.
        /// </summary>
        private static bool HasMoreThanTwoDistinctTiers(ConcurrentDictionary<AuctionKey, ReferenceAuctions> lookup)
        {
            Tier t0 = Tier.UNKNOWN, t1 = Tier.UNKNOWN;
            bool have0 = false, have1 = false;
            foreach (var kv in lookup)
            {
                var tier = kv.Key.Tier;
                if (tier == Tier.UNKNOWN)
                    continue;
                if (!have0) { t0 = tier; have0 = true; continue; }
                if (tier == t0)
                    continue;
                if (!have1) { t1 = tier; have1 = true; continue; }
                if (tier == t1)
                    continue;
                // a third distinct non-UNKNOWN tier
                return true;
            }
            return false;
        }

        static void DropUnderlistings(List<ReferencePrice> deduplicated, int scanSize = 5)
        {
            var bucketSize = deduplicated.Count;
            List<ReferencePrice> toRemove = null;
            // De-LINQ: `deduplicated.ToLookup(d => d.Buyer)` was only used for `Contains(seller)` + `[seller].FirstOrDefault()`,
            // i.e. "the FIRST reference (in list order) whose Buyer == seller". A first-occurrence dict is bit-identical and
            // drops the ILookup + its grouping arrays.
            Dictionary<short, ReferencePrice> firstByBuyer = null;
            for (int i = 0; i < bucketSize; i++)
            {
                var targetAuction = deduplicated[i];
                // batch = deduplicated[i .. i+scanSize); only its Count and a filtered average over it are used.
                int batchCount = Math.Min(scanSize, bucketSize - i);
                if (batchCount < 3)
                    break;
                firstByBuyer ??= BuildFirstByBuyer(deduplicated);
                var hit = firstByBuyer.TryGetValue(targetAuction.Seller, out var firstHit) ? firstHit : default;
                if (hit.AuctionId == default)
                    continue;
                if (i < 3)
                {
                    // average of batch prices excluding hit.AuctionId (the filtered set is non-empty: batchCount>=3, one id excluded).
                    long sum = 0;
                    int cnt = 0;
                    int end = i + batchCount;
                    for (int j = i; j < end; j++)
                    {
                        var a = deduplicated[j];
                        if (a.AuctionId != hit.AuctionId)
                        {
                            sum += a.Price;
                            cnt++;
                        }
                    }
                    if ((double)sum / cnt < hit.Price)
                        continue;// skip if median would be pulled down, the point of this is to remove to low value
                }
                (toRemove ??= new List<ReferencePrice>()).Add(hit);
            }
            int removeCount = toRemove?.Count ?? 0;
            if (removeCount == 0 || deduplicated.Count - removeCount < 4)
            {
                return;
            }
            for (int i = 0; i < toRemove.Count; i++)
            {
                deduplicated.Remove(toRemove[i]);
            }

            static Dictionary<short, ReferencePrice> BuildFirstByBuyer(List<ReferencePrice> refs)
            {
                var map = new Dictionary<short, ReferencePrice>();
                for (int i = 0; i < refs.Count; i++)
                {
                    var r = refs[i];
                    if (!map.ContainsKey(r.Buyer))
                        map[r.Buyer] = r;
                }
                return map;
            }
        }

        /// <summary>
        /// WS-CHURN-C: in-place, array-backed twin of <see cref="DropUnderlistings(List{ReferencePrice}, int)"/> used by the
        /// hot clean-price recompute (pets are common). It operates on the already-contiguous flatten buffer
        /// <paramref name="arr"/>[0..<paramref name="n"/>) and returns the new logical length, avoiding the per-call
        /// <c>List&lt;ReferencePrice&gt;(n)</c> allocation + element copy the List path required just to mutate-in-place.
        ///
        /// <para>BIT-EXACT with the List overload — same survivor SET and ORDER for every input. The selection of
        /// <c>toRemove</c> is byte-for-byte the List logic with index access substituted for <c>deduplicated[i]</c> /
        /// <c>.Count</c>. The removal phase reproduces <see cref="List{T}.Remove"/> EXACTLY: for each hit in
        /// <c>toRemove</c> order it removes the FIRST still-present element (in current order) whose
        /// <see cref="ReferencePrice.Equals(object)"/> matches, shifting survivors left like <c>List.RemoveAt</c>. (Verified
        /// against the List overload over 2M+ randomized cases incl. Equals-duplicate clusters, default(0) AuctionIds and
        /// SellTime-only differences; the production memo/golden fuzz gates it end-to-end.)</para>
        /// </summary>
        static int DropUnderlistings(ReferencePrice[] arr, int n, int scanSize = 5)
        {
            var bucketSize = n;
            List<ReferencePrice> toRemove = null;
            Dictionary<short, ReferencePrice> firstByBuyer = null;
            for (int i = 0; i < bucketSize; i++)
            {
                var targetAuction = arr[i];
                int batchCount = Math.Min(scanSize, bucketSize - i);
                if (batchCount < 3)
                    break;
                firstByBuyer ??= BuildFirstByBuyer(arr, n);
                var hit = firstByBuyer.TryGetValue(targetAuction.Seller, out var firstHit) ? firstHit : default;
                if (hit.AuctionId == default)
                    continue;
                if (i < 3)
                {
                    long sum = 0;
                    int cnt = 0;
                    int end = i + batchCount;
                    for (int j = i; j < end; j++)
                    {
                        var a = arr[j];
                        if (a.AuctionId != hit.AuctionId)
                        {
                            sum += a.Price;
                            cnt++;
                        }
                    }
                    if ((double)sum / cnt < hit.Price)
                        continue;
                }
                (toRemove ??= new List<ReferencePrice>()).Add(hit);
            }
            int removeCount = toRemove?.Count ?? 0;
            if (removeCount == 0 || n - removeCount < 4)
                return n;
            // Mirror List.Remove: for each hit (in toRemove order) remove the FIRST present element (current order)
            // that Equals(hit), shifting the tail left exactly like List.RemoveAt — identical survivor set AND order.
            int len = n;
            for (int t = 0; t < toRemove.Count; t++)
            {
                var hit = toRemove[t];
                for (int i = 0; i < len; i++)
                {
                    if (arr[i].Equals(hit))
                    {
                        Array.Copy(arr, i + 1, arr, i, len - i - 1);
                        len--;
                        break;
                    }
                }
            }
            return len;

            static Dictionary<short, ReferencePrice> BuildFirstByBuyer(ReferencePrice[] refs, int count)
            {
                var map = new Dictionary<short, ReferencePrice>();
                for (int i = 0; i < count; i++)
                {
                    var r = refs[i];
                    if (!map.ContainsKey(r.Buyer))
                        map[r.Buyer] = r;
                }
                return map;
            }
        }

        private bool IsLevel100CleanPet(KeyWithValueBreakdown key)
        {
            return key.Key.Modifiers.Count == 1 && key.Key.Modifiers[0].Key == "exp" && key.Key.Modifiers[0].Value == "6";
        }

        private bool IsDropping(ReferenceAuctions bucket, long shortTermPrice, out float rate)
        {
            var relevant = bucket.References.Reverse().Take(6).ToList();
            var buyLookup = relevant.GroupBy(r => r.Buyer).ToDictionary(g => g.Key, g => g.First());
            List<float> drops = new();
            foreach (var item in relevant)
            {
                if (buyLookup.TryGetValue(item.Seller, out var purchase))
                {
                    if (purchase.Price > item.Price * 0.97 && item.Price < shortTermPrice)
                    {
                        drops.Add((float)item.Price / Math.Max(purchase.Price, shortTermPrice * 0.97f) * 0.97f);
                    }
                }
            }
            if (drops.Count <= 1)
            {
                rate = 1;
                return false;
            }
            rate = drops.OrderByDescending(e => e).Skip(1).First();
            return true;
        }

        internal static List<ReferencePrice> ApplyAntiMarketManipulation(ReferenceAuctions bucket)
        {
            // WS-AMM (R6): de-LINQ of the verbatim LINQ original (preserved below as
            // ApplyAntiMarketManipulationReference, the bit-exactness oracle in
            // Services/ApplyAntiMarketManipulation.Tests.cs — a ≥40k heavy-tie + buyer-collision fuzz). Every stage
            // reproduces LINQ's observable semantics EXACTLY:
            //  - GroupBy: groups in first-encounter key order; elements in source order within a group.
            //  - OrderBy / OrderByDescending: STABLE (ties keep source order). Reproduced with index-tiebroken sorts.
            //  - The deferred side-effecting counter: GroupBy(a => a.Buyer == 0 ? buyerCounter++ : a.Buyer). When the
            //    GroupBy is enumerated ONCE (as here), the key selector runs once per element in source order, so the
            //    counter increments per Buyer==0 row in encounter order. CRITICAL: the counter shares the int keyspace
            //    with real (non-zero) Buyer keys, so a counter value can COLLIDE with a real buyer and merge groups
            //    (proven in the fuzz) — we therefore simulate the actual counter, not "one singleton per Buyer==0".
            var references = bucket.ReferenceSnapshot();
            int n = references.Length;
            if (n == 0)
                return new List<ReferencePrice>();

            // ---- Stage 1: back-and-forth detection ----------------------------------------------------------------
            // GroupBy(key) where key = Buyer>Seller ? Buyer<<(15+Seller) : Seller<<(15+Buyer)  (C# precedence: << is
            // lower than +). Keep groups with Count>1 whose sellers are not all equal to the first element's seller.
            // ToLookup(g => g.First().Seller).Contains(x)  ≡  membership in {firstSeller of each surviving group}.
            // We only need that membership SET of shorts.
            var combos = AmmComboBuffers.Combos; // Dictionary<int,(ReferencePrice first,int count,bool allSame,short firstSeller)>
            combos.Clear();
            var comboOrder = AmmComboBuffers.ComboOrder; // first-encounter key order (only used to keep determinism; not strictly needed for a set)
            comboOrder.Clear();
            for (int i = 0; i < n; i++)
            {
                var a = references[i];
                int key = a.Buyer > a.Seller ? a.Buyer << (15 + a.Seller) : a.Seller << (15 + a.Buyer);
                if (combos.TryGetValue(key, out var st))
                {
                    st.count++;
                    if (a.Seller != st.firstSeller)
                        st.allSame = false;
                    combos[key] = st;
                }
                else
                {
                    combos[key] = (a, 1, true, a.Seller);
                    comboOrder.Add(key);
                }
            }
            var manipulatedSellers = AmmComboBuffers.ManipulatedSellers; // set of g.First().Seller of surviving groups
            manipulatedSellers.Clear();
            for (int oi = 0; oi < comboOrder.Count; oi++)
            {
                var st = combos[comboOrder[oi]];
                // surviving group: Count>1 AND NOT all sellers equal first's seller
                if (st.count > 1 && !st.allSame)
                    manipulatedSellers.Add(st.firstSeller);
            }

            // ---- Stage 2: isPersonManipulating ------------------------------------------------------------------
            // references.OrderByDescending(Price).Take(n/2).GroupBy(Seller)
            //   .Where(Count >= Max(n/3,3)).OrderByDescending(Count).Select(First().Seller).FirstOrDefault()
            short isPersonManipulating = ComputeIsPersonManipulating(references, n);

            // ---- Stage 3: dedup ---------------------------------------------------------------------------------
            // references.Reverse().Where(notManipulated).OrderByDescending(Day) -> stable order S.
            // Build S explicitly: reverse FIFO, drop rows whose Seller or Buyer is in manipulatedSellers, then a
            // STABLE descending sort by Day.
            var stage = AmmComboBuffers.StageList; // List<ReferencePrice>
            stage.Clear();
            for (int i = n - 1; i >= 0; i--)
            {
                var d = references[i];
                if (manipulatedSellers.Contains(d.Seller) || manipulatedSellers.Contains(d.Buyer))
                    continue;
                stage.Add(d);
            }
            // Stable OrderByDescending(Day): sort by (-Day) with original index as tiebreak to preserve stability.
            StableSortByDayDescending(stage);

            // GroupBy(Seller) over S (first-encounter seller order; within-group order = S order). Per group:
            //   OrderBy(Price) [stable] .Skip(count/3).First()  -> one element per seller.
            var sellerSelected = AmmComboBuffers.SellerSelected; // List<ReferencePrice>, in seller-group encounter order
            sellerSelected.Clear();
            GroupSelectCheapest(stage, bySeller: true, output: sellerSelected, counterStartsAt: 0, out _);

            // GroupBy(Buyer==0 ? counter++ : Buyer) over sellerSelected, single enumeration => counter increments per
            // Buyer==0 in sellerSelected order, sharing the int keyspace with real buyer keys (collisions matter).
            // Per group: OrderBy(Price)[stable].Skip(count/3).First().  Then Take(WorkingSize).
            var buyerSelected = AmmComboBuffers.BuyerSelected;
            buyerSelected.Clear();
            GroupSelectCheapest(sellerSelected, bySeller: false, output: buyerSelected, counterStartsAt: 0, out _);

            int take = Math.Min(WorkingSize, buyerSelected.Count);
            var deduplicated = new List<ReferencePrice>(take);
            for (int i = 0; i < take; i++)
            {
                var elem = buyerSelected[i];
                if (isPersonManipulating != default && elem.Seller == isPersonManipulating)
                    elem.Price /= 2;
                deduplicated.Add(elem);
            }
            return deduplicated;
        }

        // ---- WS-AMM de-LINQ helpers ----------------------------------------------------------------------------

        /// <summary>Per-call scratch buffers for the AMM de-LINQ. [ThreadStatic] so the bulk-reprice loop reuses them
        /// without per-bucket allocation (AMM is called single-threaded per bucket within a shard worker; each shard
        /// has its own thread, hence its own copy). Every consumer clears before use.</summary>
        private static class AmmComboBuffers
        {
            [ThreadStatic] private static Dictionary<int, (ReferencePrice first, int count, bool allSame, short firstSeller)> _combos;
            [ThreadStatic] private static List<int> _comboOrder;
            [ThreadStatic] private static HashSet<short> _manip;
            [ThreadStatic] private static List<ReferencePrice> _stage;
            [ThreadStatic] private static List<ReferencePrice> _sellerSel;
            [ThreadStatic] private static List<ReferencePrice> _buyerSel;
            [ThreadStatic] private static List<(int key, int firstIdx, int memberStart)> _groupMeta;
            [ThreadStatic] private static List<(int groupIdx, int srcIdx)> _members;

            public static Dictionary<int, (ReferencePrice first, int count, bool allSame, short firstSeller)> Combos
                => _combos ??= new();
            public static List<int> ComboOrder => _comboOrder ??= new();
            public static HashSet<short> ManipulatedSellers => _manip ??= new();
            public static List<ReferencePrice> StageList => _stage ??= new();
            public static List<ReferencePrice> SellerSelected => _sellerSel ??= new();
            public static List<ReferencePrice> BuyerSelected => _buyerSel ??= new();
            public static List<(int key, int firstIdx, int memberStart)> GroupMeta => _groupMeta ??= new();
            public static List<(int groupIdx, int srcIdx)> Members => _members ??= new();
        }

        /// <summary>Stable descending sort of <paramref name="list"/> by Day (ties keep current order), matching
        /// <c>Enumerable.OrderByDescending(b =&gt; b.Day)</c>. Implemented as a stable insertion-into-sorted via an
        /// index-tiebroken key array + Array.Sort on a parallel key (Array.Sort is unstable, so we encode the original
        /// index into the comparison to force stability).</summary>
        [ThreadStatic] private static long[] _ammDayKeys;
        [ThreadStatic] private static ReferencePrice[] _ammDayItems;

        private static void StableSortByDayDescending(List<ReferencePrice> list)
        {
            int c = list.Count;
            if (c < 2)
                return;
            // keys[i] packs (-Day, i) as the signed long (-Day)*2^32 + i, so an ascending Array.Sort yields
            // Day-descending with the original index as a stability tiebreak. Day is short so (-Day) fits and the
            // low 32 bits (i < c) never collide with the (-Day) field. Pooled scratch ([ThreadStatic]) — no per-call alloc.
            var keys = _ammDayKeys;
            var items = _ammDayItems;
            if (keys == null || keys.Length < c)
            {
                keys = _ammDayKeys = new long[Math.Max(c, 32)];
                items = _ammDayItems = new ReferencePrice[Math.Max(c, 32)];
            }
            for (int i = 0; i < c; i++)
            {
                items[i] = list[i];
                keys[i] = ((long)(-(int)list[i].Day) << 32) | (uint)i;
            }
            Array.Sort(keys, items, 0, c);
            list.Clear();
            for (int i = 0; i < c; i++)
                list.Add(items[i]);
        }

        /// <summary>
        /// Reproduces <c>source.GroupBy(keySel).Select(g =&gt; g.OrderBy(Price)[stable].Skip(g.Count()/3).First())</c>
        /// for the two AMM dedup stages.
        /// <para>When <paramref name="bySeller"/> is true the key is <c>(int)Seller</c>. When false the key is the
        /// deferred-counter key <c>Buyer==0 ? counter++ : Buyer</c> — the counter increments once per Buyer==0 element
        /// in <paramref name="source"/> order and shares the int keyspace with real buyer keys (so a counter value can
        /// collide with a real buyer, exactly as LINQ does).</para>
        /// Output groups are emitted in first-encounter key order; within a group members keep source order; the
        /// per-group pick is the rank-(count/3) element of a STABLE ascending Price sort.
        /// </summary>
        private static void GroupSelectCheapest(List<ReferencePrice> source, bool bySeller, List<ReferencePrice> output, int counterStartsAt, out int counterEnd)
        {
            output.Clear();
            int sc = source.Count;
            int counter = counterStartsAt;
            if (sc == 0)
            {
                counterEnd = counter;
                return;
            }
            var meta = AmmComboBuffers.GroupMeta;   // (key, firstIdx, unused) per group, in encounter order
            var members = AmmComboBuffers.Members;   // (groupIdx, srcIdx) appended in source order
            meta.Clear();
            members.Clear();
            // key -> groupIdx (encounter order), in the SAME int keyspace LINQ uses (so counter/buyer collisions merge).
            var map = _ammKeyMap ??= new Dictionary<int, int>();
            map.Clear();
            for (int i = 0; i < sc; i++)
            {
                var e = source[i];
                int key = bySeller
                    ? e.Seller
                    : (e.Buyer == 0 ? counter++ : e.Buyer);
                if (!map.TryGetValue(key, out int gi))
                {
                    gi = meta.Count;
                    map[key] = gi;
                    meta.Add((key, i, 0));
                }
                members.Add((gi, i));
            }
            counterEnd = counter;
            // For each group in encounter order, collect its members (in source order), stable-sort by Price, pick
            // index = count/3.
            int groupCount = meta.Count;
            // Build per-group member index lists by a single pass keeping source order (members already in src order).
            // We iterate groups in order; for each, scan members for matching groupIdx. To keep it O(n) overall we
            // bucket members by group first.
            var perGroup = _ammPerGroup ??= new List<List<int>>();
            // grow/reset perGroup buckets
            while (perGroup.Count < groupCount)
                perGroup.Add(new List<int>());
            for (int g = 0; g < groupCount; g++)
                perGroup[g].Clear();
            for (int mi = 0; mi < members.Count; mi++)
            {
                var m = members[mi];
                perGroup[m.groupIdx].Add(m.srcIdx);
            }
            for (int g = 0; g < groupCount; g++)
            {
                var idxs = perGroup[g];
                int cnt = idxs.Count;
                // Stable ascending sort by Price; tiebreak = position within the group's member list (== source order).
                // Pick element at rank skip = cnt/3.
                int skip = cnt / 3;
                int chosenLocal = SelectStableByPrice(source, idxs, skip);
                output.Add(source[idxs[chosenLocal]]);
            }
        }

        [ThreadStatic] private static Dictionary<int, int> _ammKeyMap;
        [ThreadStatic] private static List<List<int>> _ammPerGroup;
        [ThreadStatic] private static int[] _ammSortKeys;
        [ThreadStatic] private static int[] _ammSortPos;

        /// <summary>
        /// Returns the LOCAL position (within <paramref name="idxs"/>) of the element that
        /// <c>idxs.OrderBy(Price)[stable].Skip(skip).First()</c> would select — i.e. the rank-<paramref name="skip"/>
        /// element of a STABLE ascending sort of the group's members by Price (ties keep member/source order).
        /// </summary>
        private static int SelectStableByPrice(List<ReferencePrice> source, List<int> idxs, int skip)
        {
            int cnt = idxs.Count;
            if (cnt == 1)
                return 0;
            // Stable sort the local positions [0..cnt) by (Price asc, localPos asc). Materialize order, return order[skip].
            var keysArr = _ammSortKeys;
            var posArr = _ammSortPos;
            if (keysArr == null || keysArr.Length < cnt)
            {
                keysArr = _ammSortKeys = new int[Math.Max(cnt, 16)];
                posArr = _ammSortPos = new int[Math.Max(cnt, 16)];
            }
            // We must sort by Price (long). Pack into a stable comparison via parallel arrays: sort an index array
            // [0..cnt) using a comparison on (Price, localPos). Use Array.Sort with a Comparison over a local index
            // array to keep it allocation-light; localPos tiebreak makes it stable.
            for (int i = 0; i < cnt; i++)
                posArr[i] = i;
            // Insertion sort is stable and cnt is small (one seller/buyer group); avoids Comparison delegate alloc and
            // is robust to long Price without packing-overflow concerns.
            for (int i = 1; i < cnt; i++)
            {
                int cur = posArr[i];
                long curPrice = source[idxs[cur]].Price;
                int j = i - 1;
                while (j >= 0 && source[idxs[posArr[j]]].Price > curPrice)
                {
                    posArr[j + 1] = posArr[j];
                    j--;
                }
                posArr[j + 1] = cur;
            }
            // posArr[0..cnt) now holds local positions in (Price asc, localPos asc) stable order.
            return posArr[skip];
        }

        /// <summary>De-LINQ of stage 2 (isPersonManipulating): the seller whose count among the top-n/2 by Price is
        /// the largest qualifying (>= Max(n/3,3)) group, tie-broken by earliest encounter (stable), else default(0).</summary>
        private static short ComputeIsPersonManipulating(ReferencePrice[] references, int n)
        {
            int half = n / 2;
            if (half == 0)
                return default;
            // OrderByDescending(Price) stable, Take(half). Stable: ties keep source (FIFO) order.
            // Dedicated buffer (StageList is built later for stage 3; must not clobber it).
            var top = _ammTop ??= new List<ReferencePrice>();
            top.Clear();
            for (int i = 0; i < n; i++)
                top.Add(references[i]);
            // stable descending by Price
            StableSortByPriceDescending(top);
            // Take(half): top[0..half). GroupBy(Seller) first-encounter order, count per seller.
            var sellerCounts = _ammSellerCounts ??= new Dictionary<short, int>();
            sellerCounts.Clear();
            var sellerOrder = _ammSellerOrder ??= new List<short>();
            sellerOrder.Clear();
            for (int i = 0; i < half; i++)
            {
                short s = top[i].Seller;
                if (sellerCounts.TryGetValue(s, out int c))
                    sellerCounts[s] = c + 1;
                else
                {
                    sellerCounts[s] = 1;
                    sellerOrder.Add(s);
                }
            }
            int threshold = Math.Max(n / 3, 3);
            // Where(count>=threshold).OrderByDescending(count) stable.Select(First().Seller).FirstOrDefault().
            // Stable OrderByDescending over groups in encounter order: pick the qualifying seller with the max count,
            // tie -> earliest in sellerOrder.
            short best = default;
            int bestCount = -1;
            bool found = false;
            for (int i = 0; i < sellerOrder.Count; i++)
            {
                short s = sellerOrder[i];
                int c = sellerCounts[s];
                if (c < threshold)
                    continue;
                if (c > bestCount) // strict > keeps the earliest-encountered on ties (stable)
                {
                    bestCount = c;
                    best = s;
                    found = true;
                }
            }
            return found ? best : default;
        }

        [ThreadStatic] private static List<ReferencePrice> _ammTop;
        [ThreadStatic] private static Dictionary<short, int> _ammSellerCounts;
        [ThreadStatic] private static List<short> _ammSellerOrder;

        [ThreadStatic] private static (long price, int idx)[] _ammPriceKeys;
        [ThreadStatic] private static ReferencePrice[] _ammPriceItems;

        /// <summary>Stable descending sort by Price (ties keep current order), matching OrderByDescending(r =&gt; r.Price).
        /// Pooled scratch ([ThreadStatic]) — no per-call alloc.</summary>
        private static void StableSortByPriceDescending(List<ReferencePrice> list)
        {
            int c = list.Count;
            if (c < 2)
                return;
            var items = _ammPriceItems;
            var keys = _ammPriceKeys;
            if (keys == null || keys.Length < c)
            {
                keys = _ammPriceKeys = new (long price, int idx)[Math.Max(c, 32)];
                items = _ammPriceItems = new ReferencePrice[Math.Max(c, 32)];
            }
            for (int i = 0; i < c; i++)
            {
                items[i] = list[i];
                keys[i] = (list[i].Price, i);
            }
            // ascending sort on (-price, idx) == descending price, stable by original index.
            Array.Sort(keys, items, 0, c, AmmPriceDescComparer.Instance);
            list.Clear();
            for (int i = 0; i < c; i++)
                list.Add(items[i]);
        }

        private sealed class AmmPriceDescComparer : IComparer<(long price, int idx)>
        {
            public static readonly AmmPriceDescComparer Instance = new();
            public int Compare((long price, int idx) x, (long price, int idx) y)
            {
                // descending price
                if (x.price != y.price)
                    return y.price.CompareTo(x.price);
                // ascending original index => stability
                return x.idx.CompareTo(y.idx);
            }
        }

        /// <summary>
        /// VERBATIM reference implementation of the anti-market-manipulation dedup (WS-AMM oracle, R6).
        /// DO NOT change its behavior — it is the bit-exactness golden the fuzz compares the de-LINQ against.
        /// </summary>
        internal static List<ReferencePrice> ApplyAntiMarketManipulationReference(ReferenceAuctions bucket)
        {
            var buyerCounter = 0;
            // Iterate the zero-alloc cached snapshot (FIFO-identical to bucket.References) for every scan below.
            var references = bucket.ReferenceSnapshot();
            // check for back and forth selling
            var buyerSellerCombos = references.GroupBy(a => a.Buyer > a.Seller ? a.Buyer << 15 + a.Seller : a.Seller << 15 + a.Buyer)
                .Where(g => g.Count() > 1 && !g.All(gi => gi.Seller == g.First().Seller))
                .ToLookup(l => l.First().Seller);
            var isPersonManipulating = references.OrderByDescending(r => r.Price).Take(references.Length / 2)
                        .GroupBy(r => r.Seller).Where(g => g.Count() >= Math.Max(references.Length / 3, 3)).OrderByDescending(g => g.Count()).Select(g => g.First().Seller).FirstOrDefault();
            var deduplicated = references.Reverse()
                .Where(d => !buyerSellerCombos.Contains(d.Seller) && !buyerSellerCombos.Contains(d.Buyer))
                .OrderByDescending(b => b.Day)
                .GroupBy(a => a.Seller)
                .Select(a => a.OrderBy(ai => ai.Price).Skip(a.Count() / 3).First())  // only use one (the cheapest) price from each seller
                .GroupBy(a => a.Buyer == 0 ? buyerCounter++ : a.Buyer)
                .Select(a => a.OrderBy(ai => ai.Price).Skip(a.Count() / 3).First())  // only use cheapest price from each buyer
                .Take(WorkingSize)
                .ToList();
            if (isPersonManipulating != default)
            {
                for (int i = 0; i < deduplicated.Count; i++)
                {
                    if (deduplicated[i].Seller == isPersonManipulating)
                    {
                        var elem = deduplicated[i];
                        elem.Price /= 2;
                        deduplicated[i] = elem;
                    }
                }
            }
            return deduplicated;
        }

        private static void UpdateCleanKey(PriceLookup lookup)
        {
            lookup.CleanKey = lookup.Lookup.Where(l => !l.Key.Modifiers.Any(m => m.Key == "virtual")).OrderByDescending(l => l.Value.Volume - l.Key.Modifiers.Count * 5).Select(l => l.Key).FirstOrDefault();
            lookup.CleanPricePerDay = new();
        }

        private void PreCalculateVolume((string tag, KeyWithValueBreakdown key) keyCombo)
        {
            if (keyCombo.tag == null || !Lookups.TryGetValue(keyCombo.tag, out var itemLookup))
            {
                return;
            }
            itemLookup.Volume = (float)itemLookup.Lookup.Sum(l => l.Value.References.Count) / 60;
        }

        private static (byte, long) GetVolatility(PriceLookup lookup, ReferenceAuctions bucket, long shortTermPrice, long longTerm)
        {
            var oldMedian = GetMedian(bucket.References.AsEnumerable().Take(5).ToList(), lookup?.CleanPricePerDay);
            var secondNewestMedian = 0L;
            if (bucket.References.Count > 8)
            {
                var secondSample = bucket.References.AsEnumerable().Reverse().Skip(5).Take(5).ToList();
                secondNewestMedian = GetMedian(secondSample, lookup?.CleanPricePerDay);
            }
            var thirdMedian = 0L;
            if (bucket.References.Count > 11)
            {
                var thirdSample = bucket.References.AsEnumerable().Reverse().Skip(9).Take(4).ToList();
                thirdMedian = GetMedian(thirdSample, lookup?.CleanPricePerDay);
            }
            var medianList = new float[] { oldMedian, secondNewestMedian, longTerm, shortTermPrice }.OrderByDescending(m => m).ToList();
            var mean = medianList.Average();
            medianList = medianList.Select(m => m / mean).ToList();
            mean = medianList.Average();
            var variance = medianList.Select(m => Math.Pow(m - mean, 2)).Sum() / medianList.Count;
            var volatility = Math.Sqrt(variance);
            var volatilityReduced = (byte)Math.Clamp(volatility * 100, -120, 120);
            var newMedian = Math.Min(shortTermPrice, longTerm);
            if (IsTrendDownwards(shortTermPrice, longTerm, thirdMedian, secondNewestMedian))
            {
                var difference = secondNewestMedian - shortTermPrice;
                var differenceToLong = longTerm - shortTermPrice;
                difference = Math.Min(difference, differenceToLong);
                var inPercent = (float)difference / secondNewestMedian / 2;
                if (difference > 0 && newMedian > difference && inPercent < 0.2)
                {
                    newMedian = newMedian - (long)(newMedian * inPercent);
                    deferred.Log($"Trend downwards {bucket.References.First().AuctionId} - {bucket.Price} {shortTermPrice} {longTerm} {secondNewestMedian} diff:{difference} {inPercent}% {newMedian}");
                }
            }

            return (volatilityReduced, newMedian);

            static bool IsTrendDownwards(long shortTermPrice, long longTerm, long oldMedian, long secondNewestMedian)
            {
                return oldMedian > secondNewestMedian && longTerm > shortTermPrice && secondNewestMedian > shortTermPrice;
            }
        }

        private long CapAtCraftCost(string tag, long medianPrice, KeyWithValueBreakdown key, long currentPrice)
        {
            List<RankElem> breakdown = key.ValueBreakdown;
            var limitedPrice = 0L;
            if (!Lookups.TryGetValue(tag, out var lookup))
            {
                logger.LogInformation($"Could not cap, No lookup for {tag} keeping {currentPrice} on {key.Key}");
                return Math.Min(medianPrice, currentPrice + 10_000 + currentPrice / 100);
            }
            // stackables
            if (key.Key.Enchants.Count == 0 && key.Key.Modifiers.Count == 0 && !lookup.HasMultipleRarities
                && (craftCostService?.TryGetCost(tag, out double craftCost) ?? false) && craftCost > 0)
            {
                var stackSize = key.Key.Count;
                if (stackSize == 0)
                {
                    return medianPrice; // don't limit bazaar items
                }
                var stackCost = craftCost * stackSize * 1.08;
                if (stackCost < 500_000)
                {
                    stackCost *= 2; // allow higher limit for low cost items
                }
                stackCost += 750_000; // lazyness factor
                if (stackCost < medianPrice)
                {
                    logger.LogDebug($"Capped {tag} at {stackCost} {stackSize} craft cost {craftCost} {key}");
                    return (long)stackCost;
                }
            }
            if (breakdown.Any(v => v.Value == 0) || breakdown.Count <= 0)
                return medianPrice; // can't cap nothin added, basically clean

            // determine craft cost 
            long minValue = GetCleanItemPrice(tag, key, lookup);
            if (minValue == 0 || currentPrice == minValue)
                return medianPrice;
            if (IsRune(tag))
            {
                return LimitRuneToFuseCost(medianPrice, breakdown, lookup);
            }
            var modifierSum = breakdown.Select(v =>
            {
                if (v.Modifier.Key == "candyUsed")
                {
                    return 0;
                }
                if (!Constants.AttributeKeys.Contains(v.Modifier.Key))
                    return v.Value;
                return AttributeValueEstimateForCap(tag, v, breakdown, lookup);
            }).Sum();
            if (modifierSum > 0)
            {
                if (minValue + modifierSum * 1.08 < medianPrice
                    && key.Key.Modifiers.All(m => !Constants.AttributeKeys.Contains(m.Key))
                )
                {
                    CappedKey?.Invoke(new(
                        tag,
                         key,
                         minValue,
                         modifierSum,
                         lookup,
                         medianPrice
                    ));
                }
                limitedPrice = Math.Min(minValue + modifierSum * 11 / 10, medianPrice);
            }
            if (limitedPrice > 0)
                return limitedPrice;
            return medianPrice;

            static long LimitRuneToFuseCost(long medianPrice, List<RankElem> breakdown, PriceLookup lookup)
            {
                // runes are fused and can't be crafted
                if (breakdown.First().Modifier.Value == "1")
                    return medianPrice;
                var lvl1 = lookup.Lookup.Where(v => v.Key.Modifiers.FirstOrDefault().Value == "1").FirstOrDefault().Value?.Price;
                if (lvl1 == null)
                    return medianPrice;
                var targetLevel = int.Parse(breakdown.First().Modifier.Value);
                var targetPrice = lvl1 * Math.Pow(3.5, targetLevel - 1);
                if (targetPrice < medianPrice)
                    return (long)targetPrice;
                return medianPrice;
            }
        }

        internal long GetCleanItemPrice(string tag, KeyWithValueBreakdown key, PriceLookup lookup, bool force = false)
        {
            // Persistent per-tier cache (warm hit). Unchanged: tier reduction picks the REDUCED tier the ingest path
            // wrote under, and `force` bypasses this cache exactly as before. This is the ONLY place the reduced tier
            // and the query Modifiers enter the decision; the recompute below is a pure function of (tag, UNREDUCED tier).
            var tier = key.Key.Tier;
            if (key.Key.Modifiers.Any(m => m.Value == TierBoostShorthand || m.Key == "rarity_upgrades"))
                tier = ReduceRarity(tier);
            if (!force && lookup.CleanPricePerTier.TryGetValue(tier, out var cleanPrice))
            {
                return cleanPrice;
            }

            // R6/MEMO2: persistent cache missed (or force) -> we would re-sort. Serve a memoized RECOMPUTE result keyed
            // by (tag, UNREDUCED query tier) — the exact inputs the recompute body depends on — IF the memo entry is
            // still fresh (its stamp matches the live pricing epoch + dict Count + Volume and is within TTL). One memo
            // dict per lookup for its lifetime: the dict is NEVER re-allocated on an epoch bump (the warm fix), each
            // entry carries its own freshness stamp. A stale or absent entry is a MISS — recompute via the clean2
            // partial-select and overwrite the entry with the fresh value+stamp. Serves force + non-force identically
            // because the recompute is deterministic for fixed inputs + a matching stamp (verified by the memo soak).
            // The memo is a cache of the PURE recompute only — it never serves the persistent CleanPricePerTier path
            // above. On warm the epoch changes every auction, so the entry is always stale -> always recompute the fast
            // clean2 path, with ZERO dict realloc/alloc churn (a no-op overhead-wise vs not having the memo at all).
            var memo = GetOrCreateCleanPriceMemo(lookup);
            var memoKey = (tag, key.Key.Tier);
            var epoch = Interlocked.Read(ref pricingEpoch);
            var liveCount = lookup.Lookup.Count;
            var volume = lookup.Volume;
            var now = DateTime.UtcNow;
            if (memo.Values.TryGetValue(memoKey, out var entry)
                && entry.IsFresh(epoch, liveCount, volume, now, CleanPriceMemoMaxAgeMinutes))
            {
                if (VerifyCleanPriceMemo)
                {
                    var fresh = RecomputeCleanItemPrice(tag, key, lookup);
                    if (fresh != entry.Value)
                        throw new InvalidOperationException($"CLEANPRICE MEMO DIVERGENCE tag={tag} tier={key.Key.Tier} memo={entry.Value} fresh={fresh} epoch={epoch} count={liveCount} vol={volume}");
                }
                if (CleanPriceMemoCount)
                    Interlocked.Increment(ref CleanPriceMemoHits);
                return entry.Value;
            }
            if (CleanPriceMemoCount)
                Interlocked.Increment(ref CleanPriceMemoRecomputes);
            var recomputed = RecomputeCleanItemPrice(tag, key, lookup);
            // Stamp the entry with the inputs it was computed against and overwrite in place (no dict realloc). The
            // recompute is deterministic for fixed inputs within a stamp, so a concurrent writer for the same (key,
            // stamp) stores the identical value; last-writer-wins is harmless. A stamp captured a hair before the recompute
            // can only make a future read recompute again (a conservative miss) — never serve a value the live state
            // wouldn't, which is exactly the candidate-index freshness contract.
            memo.Values[memoKey] = new CleanItemPriceMemo.Entry(recomputed, epoch, liveCount, volume, now);
            return recomputed;
        }

        /// <summary>
        /// R6/MEMO2 — returns the per-lookup <see cref="CleanItemPriceMemo"/>, lazily creating + atomic-publishing it
        /// on first use. The dict is created ONCE per lookup and never re-allocated thereafter (freshness is per-ENTRY,
        /// not per-window — the warm fix vs the deferred clean3). A concurrent first-touch race just discards a still-
        /// empty loser dict (no lost work); after publication the reference is stable so readers never see a torn/swapped
        /// dict.
        /// </summary>
        private CleanItemPriceMemo GetOrCreateCleanPriceMemo(PriceLookup lookup)
        {
            var existing = lookup.CleanPriceMemo; // single atomic read of the reference
            if (existing != null)
                return existing;
            var created = new CleanItemPriceMemo();
            // Atomic publish; if a concurrent caller won, use theirs and drop ours (it is empty, so nothing is lost).
            return Interlocked.CompareExchange(ref lookup.CleanPriceMemo, created, null) ?? created;
        }

        /// <summary>
        /// The pure clean-price RECOMPUTE — exactly what the old method body returned on a persistent-cache miss/force.
        /// Bit-exact: the flatten/filter/sort/select, sizing/devider, DropUnderlistings and Midas handling are byte-for-byte
        /// the previous clean2 code (only the persistent-cache gate moved up to the caller). A pure function of
        /// (tag, key.Key.Tier-UNREDUCED, lookup.Lookup references, lookup.Volume) — the contract the memo keys on.
        /// </summary>
        private long RecomputeCleanItemPrice(string tag, KeyWithValueBreakdown key, PriceLookup lookup)
        {
            // R2-D: de-LINQ'd, output-identical rewrite of the clean-price recompute. The original chain
            //   lookup.Lookup.Where(..).Select(.Value).ToList() -> SelectMany(.References).ToList()
            //   -> OrderByDescending(Day).ThenBy(Price).Take(size).OrderBy(Price).Skip(size/devider+1).FirstOrDefault()
            // allocated a List<ReferenceAuctions>, a List<ReferencePrice>, enumerator/closure objects and the
            // OrderBy index/buffer arrays per call (the #1 warm allocator). This version flattens into a pooled
            // ReferencePrice[] and reproduces LINQ's stable two-phase ordering with pooled index buffers. The
            // filters, DropUnderlistings, sizing, devider and Midas handling are unchanged.
            var matchRarity = tag == "THEORETICAL_HOE_WHEAT_3";
            var isPet = NBT.IsPet(tag);
            var minRarity = matchRarity ? key.Key.Tier : key.Key.Tier - 1;
            var ownTier = key.Key.Tier;

            // Flatten the matching buckets' references into a pooled buffer, preserving the
            // (bucket-enumeration-order, then reference-queue-order) flatten order LINQ produced.
            ReferencePrice[] rented = null;
            int n = 0;
            try
            {
                foreach (var entry in lookup.Lookup)
                {
                    if (isPet)
                    {
                        if (ownTier != entry.Key.Tier || CleanItemPriceHasTierBoost(entry.Key.Modifiers))
                            continue;
                    }
                    else
                    {
                        if (minRarity > entry.Key.Tier || CleanItemPriceHasPgemsOrAttribute(entry.Key.Modifiers))
                            continue;
                    }
                    var references = entry.Value.ReferenceSnapshot();
                    foreach (var r in references)
                    {
                        if (rented == null)
                            rented = ArrayPool<ReferencePrice>.Shared.Rent(Math.Max(references.Length, 16));
                        else if (n == rented.Length)
                        {
                            var bigger = ArrayPool<ReferencePrice>.Shared.Rent(rented.Length * 2);
                            Array.Copy(rented, bigger, n);
                            ArrayPool<ReferencePrice>.Shared.Return(rented);
                            rented = bigger;
                        }
                        rented[n++] = r;
                    }
                }

                Span<ReferencePrice> refs;
                if (isPet || matchRarity)
                {
                    // WS-CHURN-C: drop in place on the already-contiguous rented buffer (bit-exact array twin of the
                    // List-based DropUnderlistings), avoiding the per-call List<ReferencePrice>(n) alloc + copy. The
                    // span over the survivors [0..dropped) is in the SAME order the List path produced.
                    int dropped = rented == null ? 0 : DropUnderlistings(rented, n, 18);
                    refs = rented == null ? Span<ReferencePrice>.Empty : rented.AsSpan(0, dropped);
                }
                else
                {
                    refs = rented == null ? Span<ReferencePrice>.Empty : rented.AsSpan(0, n);
                }

                var size = (int)Math.Min(Math.Max(lookup.Volume * 10, 50), refs.Length);
                var devider = matchRarity ? 10 : 30;
                if (CanHaveGems(tag) && tag != "MELON_DICER_3")
                    devider = Math.Min(14, devider);

                var target = CleanItemPriceSelectTarget(refs, size, size / devider + 1);
                if (IsMidas(tag))
                    return target.Price + 80_000_000; // midas gets undersold very very often
                return target.Price;
            }
            finally
            {
                if (rented != null)
                    ArrayPool<ReferencePrice>.Shared.Return(rented);
            }
        }

        /// <summary>Non-allocating equivalent of <c>!modifiers.Any(m =&gt; m.Value == TierBoostShorthand)</c>'s negation.</summary>
        private static bool CleanItemPriceHasTierBoost(IReadOnlyList<KeyValuePair<string, string>> modifiers)
        {
            for (int i = 0; i < modifiers.Count; i++)
                if (modifiers[i].Value == TierBoostShorthand)
                    return true;
            return false;
        }

        /// <summary>Non-allocating equivalent of <c>modifiers.Any(m =&gt; m.Key == "pgems" || AttributeKeys.Contains(m.Key))</c>.</summary>
        private static bool CleanItemPriceHasPgemsOrAttribute(IReadOnlyList<KeyValuePair<string, string>> modifiers)
        {
            for (int i = 0; i < modifiers.Count; i++)
            {
                var k = modifiers[i].Key;
                if (k == "pgems" || Constants.AttributeKeys.Contains(k))
                    return true;
            }
            return false;
        }

        // ====================================================================================================
        // R5B (partial selection) — bit-exact replacement for the two-full-sort selection.
        //
        // The original (now kept verbatim as CleanItemPriceSelectTargetReference + phases below, the oracle the
        // 40k fuzz in Services/CleanItemPrice.Tests.cs gates against) does TWO full stable sorts of n then size
        // elements, then takes a SINGLE order statistic. That is wasteful: only a prefix is ever consumed and the
        // final answer is one element. R5B replaces it with two bounded selections (quickselect-style partitions),
        // proven equivalent to the reference total order below.
        //
        // Reference semantics (strict total orders — every tie is broken to a unique element):
        //   Phase1 sorts ALL n by  K1 = (Day DESC, Price ASC, idx ASC).  The "phase-1 prefix" P = the size
        //          smallest by K1; an element's prefix-position is its rank within P in K1 order.
        //   Phase2 sorts P by (Price ASC, prefix-position ASC) and the answer is the element at rank `skip`.
        //
        // Reduction (the whole correctness argument):
        //   Within P the K1 order == prefix-position order, so the Phase2 key (Price ASC, prefix-position ASC)
        //   restricted to P equals (Price ASC, K1 ASC) = (Price ASC, Day DESC, Price ASC, idx ASC) = K2 where
        //       K2 = (Price ASC, Day DESC, idx ASC).
        //   Both K1 and K2 are STRICT (idx is unique), so the answer index a* is fully determined by MEMBERSHIP
        //   of P alone (which intrinsic-keyed elements are the size smallest by K1) and the K2 rank within it —
        //   the phase-1 internal ordering never needs to be materialized. Therefore:
        //       a* = the element, among the size K1-smallest indices, at K2-rank `skip`.
        //   Because every key is strict, there is no ordering ambiguity at either selection boundary: the result
        //   is bit-identical to the reference for EVERY input, including heavy Day/Price ties. No per-case
        //   fallback is required (and the 40k fuzz proves it). Pivoting/partition order is irrelevant to the
        //   answer since the comparators are strict.
        // ====================================================================================================

        /// <summary>
        /// Bit-exact partial-selection replacement for <see cref="CleanItemPriceSelectTargetReference"/>: avoids the
        /// two full stable sorts by (1) partitioning the size K1-smallest indices (K1 = Day DESC, Price ASC, idx ASC)
        /// then (2) selecting the K2-rank-<paramref name="skip"/> element among them (K2 = Price ASC, Day DESC, idx ASC).
        /// See the block comment above for the equivalence proof. Uses pooled index buffers; zero managed alloc.
        /// </summary>
        internal static ReferencePrice CleanItemPriceSelectTarget(ReadOnlySpan<ReferencePrice> refs, int size, int skip)
        {
            if (size <= 0 || skip >= size)
                return default; // FirstOrDefault over an empty / too-short sequence
            int n = refs.Length;
            // size is min(.., n) at the only call-site, so size <= n and skip < size <= n hold here.
            // Materialize the intrinsic sort keys once into pooled local arrays so the partition can index them
            // cheaply (and so a ReadOnlySpan does not need to be captured by a delegate).
            short[] days = ArrayPool<short>.Shared.Rent(n);
            long[] prices = ArrayPool<long>.Shared.Rent(n);
            int[] idx = ArrayPool<int>.Shared.Rent(n);
            try
            {
                for (int i = 0; i < n; i++)
                {
                    days[i] = refs[i].Day;
                    prices[i] = refs[i].Price;
                    idx[i] = i;
                }

                // Phase 1 (partial): partition idx[0..n) so that idx[0..size) is exactly the membership set P =
                // the `size` smallest by K1. We do NOT need P ordered — only the set is consumed by phase 2.
                if (size < n)
                    CleanItemPricePartitionPhase1(idx, 0, n - 1, size - 1, days, prices);

                // Phase 2 (selection): among P = idx[0..size), select the element at K2-rank `skip`.
                CleanItemPricePartitionPhase2(idx, 0, size - 1, skip, days, prices);
                return refs[idx[skip]];
            }
            finally
            {
                ArrayPool<short>.Shared.Return(days);
                ArrayPool<long>.Shared.Return(prices);
                ArrayPool<int>.Shared.Return(idx);
            }
        }

        /// <summary>K1 strict order on original indices a,b: Day DESC, then Price ASC, then idx ASC. Returns &lt;0 if a precedes b.</summary>
        private static int CleanItemPriceCompareK1(int a, int b, short[] days, long[] prices)
        {
            int c = days[b].CompareTo(days[a]); // Day DESC
            if (c != 0) return c;
            c = prices[a].CompareTo(prices[b]); // Price ASC
            if (c != 0) return c;
            return a.CompareTo(b);              // idx ASC (a,b ARE the original indices)
        }

        /// <summary>K2 strict order on original indices a,b: Price ASC, then Day DESC, then idx ASC.</summary>
        private static int CleanItemPriceCompareK2(int a, int b, short[] days, long[] prices)
        {
            int c = prices[a].CompareTo(prices[b]); // Price ASC
            if (c != 0) return c;
            c = days[b].CompareTo(days[a]);         // Day DESC
            if (c != 0) return c;
            return a.CompareTo(b);                  // idx ASC
        }

        /// <summary>
        /// Quickselect partition by K1 so that, on return, idx[0..k] are the k+1 smallest by K1 (set, unordered) and
        /// idx[lo..hi] is partitioned around the element that belongs at position k. Median-of-three pivot guards the
        /// already-(reverse-)sorted adversarial inputs the reference sort tolerated. K1 is strict so the boundary at k
        /// is unambiguous.
        /// </summary>
        private static void CleanItemPricePartitionPhase1(int[] idx, int lo, int hi, int k, short[] days, long[] prices)
        {
            while (lo < hi)
            {
                int p = CleanItemPricePivotPhase1(idx, lo, hi, days, prices);
                if (p == k) return;
                if (p < k) lo = p + 1; else hi = p - 1;
            }
        }

        private static int CleanItemPricePivotPhase1(int[] idx, int lo, int hi, short[] days, long[] prices)
        {
            // median-of-three pivot selection (by K1) moved to hi-1, classic Hoare-ish Lomuto with sentinels.
            int mid = lo + ((hi - lo) >> 1);
            if (CleanItemPriceCompareK1(idx[mid], idx[lo], days, prices) < 0) (idx[lo], idx[mid]) = (idx[mid], idx[lo]);
            if (CleanItemPriceCompareK1(idx[hi], idx[lo], days, prices) < 0) (idx[lo], idx[hi]) = (idx[hi], idx[lo]);
            if (CleanItemPriceCompareK1(idx[hi], idx[mid], days, prices) < 0) (idx[mid], idx[hi]) = (idx[hi], idx[mid]);
            // pivot = idx[mid]; park it at hi-1 (Lomuto over [lo, hi-1) with idx[hi] already >= pivot)
            int pivot = idx[mid];
            (idx[mid], idx[hi - 1]) = (idx[hi - 1], idx[mid]);
            int store = lo;
            for (int j = lo; j < hi - 1; j++)
            {
                if (CleanItemPriceCompareK1(idx[j], pivot, days, prices) < 0)
                {
                    (idx[store], idx[j]) = (idx[j], idx[store]);
                    store++;
                }
            }
            (idx[store], idx[hi - 1]) = (idx[hi - 1], idx[store]); // restore pivot to its sorted position
            return store;
        }

        /// <summary>Quickselect partition by K2 over idx[lo..hi] so idx[k] holds the K2-rank-k element. K2 is strict.</summary>
        private static void CleanItemPricePartitionPhase2(int[] idx, int lo, int hi, int k, short[] days, long[] prices)
        {
            while (lo < hi)
            {
                int p = CleanItemPricePivotPhase2(idx, lo, hi, days, prices);
                if (p == k) return;
                if (p < k) lo = p + 1; else hi = p - 1;
            }
        }

        private static int CleanItemPricePivotPhase2(int[] idx, int lo, int hi, short[] days, long[] prices)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (CleanItemPriceCompareK2(idx[mid], idx[lo], days, prices) < 0) (idx[lo], idx[mid]) = (idx[mid], idx[lo]);
            if (CleanItemPriceCompareK2(idx[hi], idx[lo], days, prices) < 0) (idx[lo], idx[hi]) = (idx[hi], idx[lo]);
            if (CleanItemPriceCompareK2(idx[hi], idx[mid], days, prices) < 0) (idx[mid], idx[hi]) = (idx[hi], idx[mid]);
            int pivot = idx[mid];
            (idx[mid], idx[hi - 1]) = (idx[hi - 1], idx[mid]);
            int store = lo;
            for (int j = lo; j < hi - 1; j++)
            {
                if (CleanItemPriceCompareK2(idx[j], pivot, days, prices) < 0)
                {
                    (idx[store], idx[j]) = (idx[j], idx[store]);
                    store++;
                }
            }
            (idx[store], idx[hi - 1]) = (idx[hi - 1], idx[store]);
            return store;
        }

        // ---- Reference (original two-full-sort) selection: the bit-exactness oracle, kept verbatim ----

        /// <summary>
        /// Reproduces, byte-for-byte, the original LINQ selection:
        /// <c>refs.OrderByDescending(Day).ThenBy(Price).Take(size).OrderBy(Price).Skip(skip).FirstOrDefault()</c>.
        /// Both LINQ <c>OrderBy</c>/<c>ThenBy</c> are stable; the original-index / prefix-position tiebreaks below make
        /// the (unstable) <see cref="Array.Sort"/> produce the identical total order. Uses pooled index buffers.
        /// This is the oracle the partial-selection <see cref="CleanItemPriceSelectTarget"/> is gated against.
        /// </summary>
        internal static ReferencePrice CleanItemPriceSelectTargetReference(ReadOnlySpan<ReferencePrice> refs, int size, int skip)
        {
            if (size <= 0 || skip >= size)
                return default; // FirstOrDefault over an empty / too-short sequence
            int n = refs.Length;
            int[] order1 = ArrayPool<int>.Shared.Rent(n);
            int[] order2 = ArrayPool<int>.Shared.Rent(size);
            try
            {
                for (int i = 0; i < n; i++)
                    order1[i] = i;
                // Phase 1: OrderByDescending(Day).ThenBy(Price), stable via original-index tiebreak.
                CleanItemPriceStableSortPhase1(order1, n, refs);
                // Phase 2: first `size` of phase 1 -> OrderBy(Price), stable via prefix-position tiebreak.
                for (int p = 0; p < size; p++)
                    order2[p] = p;
                CleanItemPriceStableSortPhase2(order2, size, order1, refs);
                return refs[order1[order2[skip]]];
            }
            finally
            {
                ArrayPool<int>.Shared.Return(order1);
                ArrayPool<int>.Shared.Return(order2);
            }
        }

        private static void CleanItemPriceStableSortPhase1(int[] order, int n, ReadOnlySpan<ReferencePrice> refs)
        {
            // Materialize the sort keys into local arrays so the index comparer can capture them
            // (a Comparison/IComparer cannot capture a Span). The comparer is stable via the original index.
            short[] days = ArrayPool<short>.Shared.Rent(n);
            long[] prices = ArrayPool<long>.Shared.Rent(n);
            try
            {
                for (int i = 0; i < n; i++)
                {
                    days[i] = refs[i].Day;
                    prices[i] = refs[i].Price;
                }
                Array.Sort(order, 0, n, new CleanItemPricePhase1Comparer(days, prices));
            }
            finally
            {
                ArrayPool<short>.Shared.Return(days);
                ArrayPool<long>.Shared.Return(prices);
            }
        }

        private static void CleanItemPriceStableSortPhase2(int[] order2, int size, int[] order1, ReadOnlySpan<ReferencePrice> refs)
        {
            long[] prices = ArrayPool<long>.Shared.Rent(size);
            try
            {
                for (int p = 0; p < size; p++)
                    prices[p] = refs[order1[p]].Price;
                Array.Sort(order2, 0, size, new CleanItemPricePhase2Comparer(prices));
            }
            finally
            {
                ArrayPool<long>.Shared.Return(prices);
            }
        }

        private sealed class CleanItemPricePhase1Comparer : IComparer<int>
        {
            private readonly short[] _days;
            private readonly long[] _prices;
            public CleanItemPricePhase1Comparer(short[] days, long[] prices) { _days = days; _prices = prices; }
            public int Compare(int a, int b)
            {
                // OrderByDescending(Day)
                int c = _days[b].CompareTo(_days[a]);
                if (c != 0) return c;
                // ThenBy(Price)
                c = _prices[a].CompareTo(_prices[b]);
                if (c != 0) return c;
                // stable: preserve original order
                return a.CompareTo(b);
            }
        }

        private sealed class CleanItemPricePhase2Comparer : IComparer<int>
        {
            private readonly long[] _prices; // indexed by prefix position
            public CleanItemPricePhase2Comparer(long[] prices) { _prices = prices; }
            public int Compare(int a, int b)
            {
                // OrderBy(Price)
                int c = _prices[a].CompareTo(_prices[b]);
                if (c != 0) return c;
                // stable: preserve position within the phase-1 prefix
                return a.CompareTo(b);
            }
        }

        private bool CanHaveGems(string tag)
        {
            return itemService?.GetUnlockableSlots(tag).Any() ?? false;
        }

        private long AttributeValueEstimateForCap(string tag, RankElem v, List<RankElem> breakdown, PriceLookup lookup)
        {
            var comboValue = 0L;
            var both = breakdown.Where(b => Constants.AttributeKeys.Contains(b.Modifier.Key)).ToDictionary(e => e.Modifier.Key, e => e.Modifier.Value);
            if (HasAttributeCombo(v.Modifier, both, tag))
            {
                var lowestTwo = lookup.Lookup
                    .Where(p => p.Value.Price > 0)
                    .Where(l => both.All(b => l.Key.Modifiers.Any(m => m.Key == b.Key)))
                    .OrderBy(p => p.Value.Price).Take(2).SelectMany(l => l.Value.References).ToList();
                var percentile = lowestTwo.OrderBy(r => r.Price)
                            .Skip(lowestTwo.Count / 2).Select(r => r.Price)
                            // deviding by two because both attributes count
                            .DefaultIfEmpty(0).Min() / 2;
                comboValue = percentile;
            }
            var key = (tag, v.Modifier);
            if (AttributeValueLookup.TryGetValue(key, out var value))
            {
                return value.Item1 + comboValue;
            }
            var elementValue = NewMethod(tag, v, lookup);
            if (elementValue > 0)
                AttributeValueLookup[key] = (elementValue, DateTime.UtcNow);
            return elementValue + comboValue;

            static double GetPercentile(RankElem v, IEnumerable<KeyValuePair<AuctionKey, ReferenceAuctions>> options)
            {
                var values = options.Where(l => l.Value.Price > 0
                                            && (l.Key.Modifiers.Count == 2 && l.Key.Modifiers.Last().Key == "virtual" || l.Key.Modifiers.Count == 1) && l.Key.Modifiers.Any(m => m.Key == v.Modifier.Key))
                                .SelectMany(l => l.Value.References.Select(r => r.Price / Math.Pow(2, int.Parse(l.Key.Modifiers.First().Value))))
                                .ToList();
                var quarterPercentile = values.Count > 0 ? values.OrderBy(v => v).Skip(values.Count / 5).First() : 0;
                return quarterPercentile;
            }

            double CheckPaths(RankElem v, IEnumerable<KeyValuePair<AuctionKey, ReferenceAuctions>> options)
            {
                double quarterPercentile = GetPercentile(v, options);
                var shards = GetPercentile(v, Lookups.GetValueOrDefault("ATTRIBUTE_SHARD", new PriceLookup()).Lookup);
                if (shards > 0 && shards < quarterPercentile)
                {
                    quarterPercentile = shards;
                }

                return quarterPercentile;
            }

            long NewMethod(string tag, RankElem v, PriceLookup lookup)
            {
                var baseLevel = int.Parse(v.Modifier.Value);
                // check lowest value path
                var options = lookup.Lookup.AsEnumerable();
                if (CrimsonArmors.Any(tag.StartsWith))
                {
                    // these 4 types can be combined amongst each other
                    var secondType = tag.Split("_")[1];
                    options = CrimsonArmors.SelectMany(s => Lookups.TryGetValue(s + secondType, out var lookup) ? lookup.Lookup.AsEnumerable() : []);
                }
                if (tag == "HELLFIRE_ROD")
                {
                    // hellfire rod can be easily upgraded from magma rod
                    if (Lookups.TryGetValue("INFERNO_ROD", out var inferno))
                        options = options.Concat(inferno.Lookup);
                    if (Lookups.TryGetValue("MAGMA_ROD", out var magma))
                        options = options.Concat(magma.Lookup);
                }
                double quarterPercentile = CheckPaths(v, options);
                return (long)(Math.Pow(2, baseLevel) * quarterPercentile * 1.20);
            }
        }

        private static List<ReferencePrice> GetShortTermBatch(List<ReferencePrice> deduplicated, float volume)
        {
            // if more than half of the references are less than 12 hours old, use more references
            if (deduplicated.Where(d => d.Day >= GetDay(DateTime.Now - TimeSpan.FromHours(12))).Count() > SizeToKeep / 2 || volume > 4)
                return deduplicated.Take(6).ToList();
            return deduplicated.Take(5).ToList();
        }

        public (ReferenceAuctions auctions, AuctionKeyWithValue key) GetBucketForAuction(SaveAuction auction)
        {
            var group = GetAuctionGroupTag(auction.Tag);
            var itemGroupTag = group.Item1;
            if (!Lookups.TryGetValue(itemGroupTag, out var lookup) || lookup == null)
            {
                lookup = new PriceLookup();
                Lookups[itemGroupTag] = lookup;
            }
            if (auction.Category == Category.UNKNOWN)
            {
                auction.Category = lookup.Category;
            }
            var key = KeyFromSaveAuction(auction);
            key.ValueSubstract += group.Item2;
            return (GetOrAdd(key, lookup), key);
        }

        private static long GetMedian(List<ReferencePrice> deduplicated, Dictionary<short, long> cleanPricePerDay, float skipAdjust = 2)
        {
            if (deduplicated.Count == 0)
            {
                // can get here if there are no sells in the last 10 days in a bucket that has more than 20 in the last month (sus, so no median is fine)
                return 0;
            }
            var today = cleanPricePerDay?.GetValueOrDefault(GetDay()) ?? cleanPricePerDay?.GetValueOrDefault((short)(GetDay() - 1)) ?? 0;
            return (long)deduplicated
                .OrderByDescending(b => SelectAdjustedPrice(cleanPricePerDay, b, today))
                .Skip((int)(deduplicated.Count / skipAdjust))
                .Select(b => SelectAdjustedPrice(cleanPricePerDay, b, today))
                .First();

            static float SelectAdjustedPrice(Dictionary<short, long> cleanPricePerDay, ReferencePrice b, long today)
            {
                if (today == 0 || !cleanPricePerDay.TryGetValue(b.Day, out var clean))
                    return b.Price;
                var percentDiff = (float)(today - clean) / clean;
                return b.Price - (clean > today ? Math.Min(clean - today, b.Price - percentDiff * b.Price) : 0);
            }
        }

        private ReferenceAuctions CreateAndAddBucket(SaveAuction auction, AuctionKey key)
        {
            var itemBucket = Lookups.GetOrAdd(GetAuctionGroupTag(auction.Tag).tag, new PriceLookup());
            return GetOrAdd(key, itemBucket);
        }

        private static ReferenceAuctions GetOrAdd(AuctionKey key, PriceLookup itemBucket)
        {
            if (itemBucket.Lookup == null)
                itemBucket.Lookup = new();
            return itemBucket.Lookup.GetOrAdd(key, (k) => new ReferenceAuctions());
        }

        private static ReferencePrice CreateReferenceFromAuction(SaveAuction auction, long valueSubstract = 0, long extraValue = 0)
        {
            var basePrice = auction.HighestBidAmount == 0 ? auction.StartingBid : auction.HighestBidAmount;
            // remove at most 50% of the value
            if (basePrice < valueSubstract)
                valueSubstract = Math.Min(valueSubstract, basePrice / 2);
            var buyer = auction.Bids?.OrderByDescending(b => b.Amount).FirstOrDefault();
            if (basePrice - valueSubstract - extraValue < 0)
            {
                extraValue = 0;
            }
            return new ReferencePrice()
            {
                AuctionId = auction.UId,
                Day = GetDay(auction.End),
                Price = basePrice - valueSubstract - extraValue,
                Seller = GetSellerId(auction),
                Buyer = buyer?.Bidder == null ? (short)0 : Convert.ToInt16(buyer.Bidder.Substring(0, 4), 16)
            };
        }

        private static short GetSellerId(SaveAuction auction)
        {
            return auction.AuctioneerId == null ? (short)(auction.SellerId % (2 << 14)) : Convert.ToInt16(auction.AuctioneerId.Substring(0, 4), 16);
        }

        public static short GetDay(DateTime date = default)
        {
            if (date == default)
                if (CurrentDayCache != 0)
                    return CurrentDayCache;
                else
                    date = DateTime.UtcNow;
            return (short)(date - StartTime).TotalDays;
        }

        private bool TryGetReferenceAuctions(SaveAuction auction, out ReferenceAuctions bucket)
        {
            bucket = null;
            if (!Lookups.TryGetValue(GetAuctionGroupTag(auction.Tag).tag, out PriceLookup lookup))
                return false;
            var l = lookup.Lookup;
            var basekey = DetailedKeyFromSaveAuction(auction);
            if (l.TryGetValue(basekey, out bucket))
                return true;
            if (l.TryGetValue(basekey.GetReduced(1), out bucket))
                return true;
            if (l.TryGetValue(basekey.GetReduced(2), out bucket))
                return true;
            return l.TryGetValue(basekey.GetReduced(3), out bucket);
        }

        private static readonly System.Collections.ObjectModel.ReadOnlyCollection<KeyValuePair<string, string>> EmptyModifiers = new(new List<KeyValuePair<string, string>>());
        private static readonly System.Collections.ObjectModel.ReadOnlyCollection<KeyValuePair<string, string>> EmptyPetModifiers = new(new List<KeyValuePair<string, string>>() { new("candyUsed", "0"), new("exp", "0") });
        private static readonly DateTime UnlockedIntroduction = new DateTime(2021, 9, 11);
        private static readonly List<string> GemPurities = new() { "PERFECT", "FLAWLESS", "FINE", "ROUGH" };
        public long EstStarCost(string item, int tier)
        {
            var items = itemService.GetStarIngredients(item, tier);
            var sum = 0;
            foreach (var ingred in items)
            {
                if (BazaarPrices.TryGetValue(ingred.itemId, out var cost))
                    sum += (int)cost * ingred.amount;
                else
                {
                    sum += 1_000_000;
                }
            }
            return sum;
        }
        public AuctionKeyWithValue KeyFromSaveAuction(SaveAuction auction, int dropLevel = 0)
        {
            return DetailedKeyFromSaveAuction(auction).GetReduced(dropLevel);
        }
        public KeyWithValueBreakdown ValueKeyForTest(SaveAuction auction)
        {
            return DetailedKeyFromSaveAuction(auction);
        }

        /// <summary>WS-A test hook: parse <paramref name="auction"/> a FRESH way (memo bypassed) and then via the parse
        /// memo at the SAME pricing epoch (forcing a store-then-serve), and return whether the two outputs are equal by
        /// the parse contract (<see cref="ParseResultsEqual"/>). Used by the parse-memo bit-exactness unit test so the
        /// guard's equality contract is covered without needing the env flag / a full replay. Returns true iff bit-exact.</summary>
        internal bool ParseMemoRoundtripEqualsForTest(SaveAuction auction)
        {
            var fresh = ParseFresh(auction, false, 5);
            ulong hash = BatchContentHash(auction); // always-computed standalone hash (independent of ParseMemoActive)
            long epoch = Interlocked.Read(ref pricingEpoch);
            // store + serve at the same epoch (mirrors the memo's store-then-hit roundtrip)
            var (enchants, modifiers) = SelectValuable(auction);
            var built = ParseBodyAfterSelect(auction, enchants, modifiers, Constants.RelevantReforges.Contains(auction.Reforge), 5);
            parseMemo.Values[hash] = new ParseMemo.Entry(built, epoch);
            var served = parseMemo.Values.TryGetValue(hash, out var e) && e.Epoch == epoch ? e.Value : built;
            return ParseResultsEqual(fresh, served) && ParseResultsEqual(served, fresh);
        }
        private KeyWithValueBreakdown DetailedKeyFromSaveAuction(SaveAuction auction, bool fastMode = false, int limit = 5)
        {
            var shouldIncludeReforge = Constants.RelevantReforges.Contains(auction.Reforge);
            long valueSubstracted = 0;
            bool removedRarity = false;
            List<RankElem> rankElems = [];
            List<Enchant> enchants;
            List<KeyValuePair<string, string>> modifiers;
            // SelectValuable is the unavoidable one-time content traversal (the irreducible floor); it also folds the
            // flatNbt portion of the content hash into _flatNbtContentHash as it walks (WS-A "fuse, no second pass").
            // We always run it: it produces the modifiers AND the enchants the rest of the hash needs, and it is the
            // cheap part of the parse (<0.1% de-inlined). The memo skips the HEAVIER tail (CapKeyLength + ComparisonValue
            // + Constructkey) on a hit.
            (enchants, modifiers) = SelectValuable(auction, fastMode);

            // WS-A: the (contentHash, pricingEpoch) probe. Only the non-fastMode parse is memoized — fastMode is a
            // distinct output shape (and a rare bulk path), so it is never served from / stored into the memo. The
            // counting probe runs regardless (so production self-measures the cross-auction dup at any setting).
            ulong contentHash = 0;
            long epoch = 0;
            // The memo machinery runs when the cross-auction memo is ON, when only counting, OR when the parity guard is
            // on. VerifyParseMemo forces serve+store behaviour (memoServes) even if SNIPER_PARSE_MEMO is off, so the
            // guard validates the actual hit path on every duplicate. Only the standard non-fastMode/limit==5 parse is
            // memoized (fastMode is a distinct, rare output shape).
            bool wantMemo = !fastMode && limit == 5 && ParseMemoActive;
            bool memoServes = ParseMemoEnabled || VerifyParseMemo;
            if (wantMemo)
            {
                contentHash = ComputeContentHash(auction, enchants);
                epoch = Interlocked.Read(ref pricingEpoch);
                if (ParseMemoCount)
                    Interlocked.Increment(ref ParseMemoCalls);
                bool epochHit = parseMemo.Values.TryGetValue(contentHash, out var memoEntry) && memoEntry.Epoch == epoch;
                if (ParseMemoCount)
                {
                    if (epochHit) Interlocked.Increment(ref ParseMemoHits);
                    else Interlocked.Increment(ref ParseMemoMisses);
                }
                if (memoServes && epochHit)
                {
                    if (VerifyParseMemo)
                        AssertParseMemoParity(auction, fastMode, limit, memoEntry.Value);
                    // `enchants`/`modifiers` are discarded (the heavy CapKeyLength tail that would have consumed them is
                    // skipped); the cached breakdown is read-only downstream (GetReduced allocates fresh reduced keys).
                    return memoEntry.Value;
                }
            }

            var built = ParseBodyAfterSelect(auction, enchants, modifiers, shouldIncludeReforge, limit);
            // WS-A: store the fresh parse under (contentHash, epoch). Overwrite in place (no dict realloc — the warm
            // fix): within one epoch the parse is deterministic for fixed content, so a concurrent same-(hash,epoch)
            // writer stores an equivalent value (last-writer-wins is harmless). A stamp captured a hair before the parse
            // can only make a future read re-parse (a conservative miss), never serve a value the live state wouldn't.
            if (wantMemo && memoServes)
                parseMemo.Values[contentHash] = new ParseMemo.Entry(built, epoch);
            return built;
        }

        // The parse tail (everything after SelectValuable): cap the key by the live market threshold, apply the tier
        // adjustments / no-effect-enchant + modifier cleanups, and construct the key+breakdown. Extracted verbatim from
        // DetailedKeyFromSaveAuction so the parity guard's fresh re-parse (ParseFresh) and the main path share ONE body
        // — no divergence risk. `enchants`/`modifiers` are mutated in place here (as before), so callers must pass
        // freshly-SelectValuable'd lists (the main path and ParseFresh both do).
        private KeyWithValueBreakdown ParseBodyAfterSelect(SaveAuction auction, List<Enchant> enchants,
            List<KeyValuePair<string, string>> modifiers, bool shouldIncludeReforge, int limit)
        {
            var (valueSubstracted, removedRarity, includeReforge, rankElems) = CapKeyLength(enchants, modifiers, auction, limit);
            shouldIncludeReforge = includeReforge;

            if (enchants == null)
                enchants = new List<Enchant>();
            var tier = auction.Tier;
            if (auction.Tag == "ENCHANTED_BOOK")
            {
                // rarities don't matter for enchanted books and often used for scamming
                tier = Tier.UNCOMMON;
            }
            if (auction.Tag == "PANDORAS_BOX")
                // pandoras box tier gets set based on the player
                tier = Tier.COMMON;
            // Reduce tier if PET_ITEM_TIER_BOOST was present (it's now removed from key)
            if (auction.FlatenedNBT?.TryGetValue("heldItem", out var heldItemValue) == true && heldItemValue == "PET_ITEM_TIER_BOOST")
            {
                tier = ReduceRarity(tier);
            }
            if (removedRarity)
            {
                tier = ReduceRarity(tier);
            }
            var reducedEnchants = RemoveNoEffectEnchants(auction, enchants);
            if (reducedEnchants.Count < enchants.Count)
            {
                // Cold path (an enchant was dropped): keep breakdown entries for non-enchant items or surviving enchants.
                var kept = new List<RankElem>(rankElems.Count);
                for (int i = 0; i < rankElems.Count; i++)
                {
                    var r = rankElems[i];
                    if (r.Enchant.Type == default || ContainsEnchantType(reducedEnchants, r.Enchant.Type))
                        kept.Add(r);
                }
                rankElems = kept;
                enchants = reducedEnchants;
            }
            if (auction.Tag != null && AttributeToIgnoreOnLookup.TryGetValue(auction.Tag, out var ignore))
            {
                modifiers.RemoveAll(m => ignore.Contains(m.Key));
            }
            if (AnyModifierKey(modifiers, "rarity_upgrades") && !Constants.DoesRecombMatter(auction.Category, auction.Tag))
            {
                RemoveModifiersByKey(modifiers, "rarity_upgrades");
                if (!IsRune(auction.Tag))
                    tier = ReduceRarity(tier);
            }
            // Remove PET_ITEM_TIER_BOOST if it somehow got into modifiers (it shouldn't be in the key)
            RemoveModifiersByValue(modifiers, "TIER_BOOST");

            return Constructkey(auction, enchants, modifiers, shouldIncludeReforge, valueSubstracted, rankElems, tier);
        }

        // WS-A parity guard: a FULL fresh parse with the memo bypassed (re-runs SelectValuable + the shared body), so the
        // guard compares the memoized value against an independent recompute. Used only under SNIPER_VERIFY_PARSE_MEMO.
        private KeyWithValueBreakdown ParseFresh(SaveAuction auction, bool fastMode, int limit)
        {
            var shouldIncludeReforge = Constants.RelevantReforges.Contains(auction.Reforge);
            var (enchants, modifiers) = SelectValuable(auction, fastMode);
            return ParseBodyAfterSelect(auction, enchants, modifiers, shouldIncludeReforge, limit);
        }

        // Equality of two parse outputs by the type's OWN contract (what every downstream consumer relies on): the
        // AuctionKey's Equals + GetHashCode are deliberately ORDER-INDEPENDENT over Enchants/Modifiers (bucket selection,
        // the reduced-key dispatch's membership compaction, and the dictionary hash/equals all treat the key as an
        // unordered set — see AuctionKey.Equals/GetHashCode). Modifier/enchant ORDER (from the pooled-dict iteration) and
        // value-tie order in the breakdown (the unstable Sort) are PRE-EXISTING non-determinism — two FRESH parses of the
        // same auction can already produce different orders, and nothing correctness-bearing reads them positionally — so
        // the parity guard compares as an unordered multiset, exactly matching the contract a memo hit must preserve.
        private static bool ParseResultsEqual(KeyWithValueBreakdown a, KeyWithValueBreakdown b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            if (a.SubstractedValue != b.SubstractedValue) return false;
            if (!Equals(a.Key, b.Key)) return false;          // AuctionKey.Equals (containment, order-independent)
            if (!Equals(b.Key, a.Key)) return false;          // symmetric containment (proves count+membership both ways)
            // SubstractedValue (compared above) is the parse's substracted total; KeyWithValueBreakdown.Key is the base
            // AuctionKey (no per-instance ValueSubstract field — that lives on AuctionKeyWithValue, produced by GetReduced).
            return BreakdownMultisetEqual(a.ValueBreakdown, b.ValueBreakdown);
        }

        // Order-independent multiset comparison of two value breakdowns (the Sort is unstable, so value-tie order is
        // non-deterministic). Greedy match by RankElem.Equals (Enchant + Modifier + Reforge + Value) plus IsEstimate.
        private static bool BreakdownMultisetEqual(List<RankElem> ab, List<RankElem> bb)
        {
            if (ab == null || bb == null) return ab == bb;
            if (ab.Count != bb.Count) return false;
            var matched = new bool[bb.Count];
            for (int i = 0; i < ab.Count; i++)
            {
                var x = ab[i];
                bool found = false;
                for (int j = 0; j < bb.Count; j++)
                {
                    if (matched[j]) continue;
                    var y = bb[j];
                    if (x.Value == y.Value && x.IsEstimate == y.IsEstimate && x.Reforge == y.Reforge
                        && x.Enchant.Equals(y.Enchant)
                        && x.Modifier.Key == y.Modifier.Key && x.Modifier.Value == y.Modifier.Value)
                    { matched[j] = true; found = true; break; }
                }
                if (!found) return false;
            }
            return true;
        }

        // WS-A: the immutable-content FNV hash. Combines the flatNbt portion (folded into _flatNbtContentHash by the
        // SelectValuable traversal we just ran) with the FILTERED enchants (Type+Lvl, the parse's actual enchant input),
        // Tier, Reforge, Count, Tag, AND HighestBidAmount. HighestBidAmount feeds CapKeyLength's percentDiff and so the
        // substracted value/breakdown — a per-instance value that genuinely affects the parse output, so it is included
        // for correctness (over-miss safe; the parity guard would fire if it were wrongly excluded). The enchant list is
        // order-deterministic here (SelectValuable stable-sorts by Type), so a positional fold is content-stable.
        private static ulong ComputeContentHash(SaveAuction auction, List<Enchant> enchants)
        {
            ulong h = FnvOffset ^ _flatNbtContentHash;
            h = FnvString(h, auction.Tag);
            h = FnvLong(h, (long)auction.Tier);
            h = FnvLong(h, (long)auction.Reforge);
            h = FnvLong(h, auction.Count);
            h = FnvLong(h, auction.HighestBidAmount);
            if (enchants != null)
                for (int i = 0; i < enchants.Count; i++)
                {
                    var e = enchants[i];
                    h = FnvLong(h, (long)e.Type);
                    h = FnvLong(h, e.Lvl);
                }
            return h;
        }

        // WS-A parity guard: recompute a FRESH parse (memo bypassed) and assert byte-equal AuctionKey + breakdown vs the
        // memoized value. Throws loudly on divergence — catches a wrong NonContentKeys exclusion / a stale-epoch serve.
        private void AssertParseMemoParity(SaveAuction auction, bool fastMode, int limit, KeyWithValueBreakdown memoized)
        {
            var fresh = ParseFresh(auction, fastMode, limit);
            if (!ParseResultsEqual(memoized, fresh))
            {
                var nbt = auction.FlatenedNBT == null ? "(null)" : string.Join(",", auction.FlatenedNBT.Select(m => m.Key + "=" + m.Value));
                throw new InvalidOperationException(
                    $"PARSE MEMO DIVERGENCE tag={auction.Tag} uuid={auction.Uuid} hb={auction.HighestBidAmount} nbt=[{nbt}] memo=[{memoized}] fresh=[{fresh}]");
            }
        }

        // FNV-1a 64-bit constants — the content hash is FNV over the immutable item content (cheap, well-distributed,
        // no allocation). Folded into SelectValuable's existing single flatNbt traversal (the WS-A "fuse, no second
        // pass" rail) and combined with the enchant/scalar content in DetailedKeyFromSaveAuction.
        private const ulong FnvOffset = 14695981039346656037UL;
        private const ulong FnvPrime = 1099511628211UL;
        // [ThreadStatic] accumulator: SelectValuable folds the (non-per-instance) flatNbt entries it traverses into this
        // per-worker slot; DetailedKeyFromSaveAuction reads it immediately after the call (same thread, no escape). Used
        // only when the caller wants the content hash; GetFullKey/GetBreakdownKey ignore it.
        [ThreadStatic] private static ulong _flatNbtContentHash;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong FnvString(ulong h, string s)
        {
            // Hash the UTF-16 chars directly (the strings are interned-ish small NBT keys/values); deterministic and
            // allocation-free. A null is folded as a single distinguishing byte so null vs "" never collide.
            if (s == null)
                return (h ^ 0xFF) * FnvPrime;
            for (int i = 0; i < s.Length; i++)
                h = (h ^ (byte)s[i]) * FnvPrime ^ ((byte)(s[i] >> 8)) * FnvPrime;
            return h;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong FnvLong(ulong h, long v)
        {
            for (int i = 0; i < 8; i++)
            {
                h = (h ^ (byte)v) * FnvPrime;
                v >>= 8;
            }
            return h;
        }

        // splitmix64 finalizer — a strong avalanche mixer. Applied to each per-flatNbt-entry FNV hash BEFORE the
        // order-independent commutative SUM, so the entries' contributions have no linear structure that lets two
        // different (key,value) sets sum to the same total (a raw-FNV commutative SUM collided two distinct
        // {scroll_count,upgrade_level} contents — finalizing each addend kills that). Order-independence is preserved
        // (addition is commutative); collision-resistance is restored to ~2^-64 per pair.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Mix(ulong z)
        {
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            return z ^ (z >> 31);
        }

        // WS-D: a STANDALONE content hash over the RAW immutable SaveAuction content — the same content definition the
        // parse memo keys on (flatNbt minus per-instance "uid" keys + raw Enchantments Type+Lvl + Tier + Reforge + Count
        // + Tag + HighestBidAmount), but computed directly from the SaveAuction WITHOUT running SelectValuable. Two
        // auctions with the same hash have byte-identical parse inputs, so within one pricing epoch they parse to the
        // identical key — i.e. this is a sufficient condition for a parse-memo hit. Used to measure intra-Kafka-batch
        // parse duplication (the first REAL dup number — a property of the batch, measurable without production
        // telemetry). It hashes the RAW (unfiltered) enchants rather than the SelectValuable-filtered set; that is a
        // conservative dup measure (raw-equal ⟹ filtered-equal), so it can only UNDER-count duplicates, never over-count.
        public static ulong BatchContentHash(SaveAuction auction)
        {
            ulong flatHash = 0;
            var flatNbt = auction.FlatenedNBT;
            if (flatNbt != null)
                foreach (var item in flatNbt)
                {
                    if (IsNonContentKey(item.Key))
                        continue;
                    flatHash += Mix(FnvString(FnvString(FnvOffset, item.Key), item.Value));
                }
            ulong h = FnvOffset ^ flatHash;
            h = FnvString(h, auction.Tag);
            h = FnvLong(h, (long)auction.Tier);
            h = FnvLong(h, (long)auction.Reforge);
            h = FnvLong(h, auction.Count);
            h = FnvLong(h, auction.HighestBidAmount);
            var ench = auction.Enchantments;
            if (ench != null)
                for (int i = 0; i < ench.Count; i++)
                {
                    // ORDER-independent over enchants (raw Enchantments order is not guaranteed): finalized commutative fold.
                    h += Mix(FnvLong(FnvLong(FnvOffset, (long)ench[i].Type), ench[i].Level));
                }
            return h;
        }

        // WS-D: count distinct-content vs total over a batch of auctions (the kept Kafka batch, or a replay dispatch
        // batch). Returns (distinct, total). Accumulates into the rolling overall counters when track=true.
        public static (int distinct, int total) MeasureBatchDup(IReadOnlyList<SaveAuction> batch, bool track = true)
        {
            if (batch == null || batch.Count == 0)
                return (0, 0);
            var seen = new HashSet<ulong>(batch.Count);
            for (int i = 0; i < batch.Count; i++)
                seen.Add(BatchContentHash(batch[i]));
            if (track)
            {
                Interlocked.Add(ref BatchDupTotal, batch.Count);
                Interlocked.Add(ref BatchDupDistinct, seen.Count);
                Interlocked.Increment(ref BatchDupBatches);
            }
            return (seen.Count, batch.Count);
        }

        // WS-D rolling counters (overall across all measured batches). Reported by the replay / production probe.
        internal static long BatchDupTotal;
        internal static long BatchDupDistinct;
        internal static long BatchDupBatches;
        // WS-D production batch-dup probe: when on, ConsumeNewAuctions measures the kept Kafka batch's distinct-vs-total.
        internal static readonly bool BatchDupCount
            = Environment.GetEnvironmentVariable("SNIPER_BATCH_DUP_COUNT") is "1" or "true" or "TRUE";

        // Conservative per-instance exclusion: only keys containing "uid"/"uuid" (the codebase's own per-instance
        // marker — see e.g. `!f.Key.Contains("uid")` in IsValuableEnoughForClosest). Everything else is hashed →
        // over-miss is safe, under-miss is a correctness bug. "uuid" contains "uid", so the single Contains covers both.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsNonContentKey(string key) => key != null && key.Contains("uid");

        internal (List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers) SelectValuable(SaveAuction auction, bool fastMode = false)
        {
            // De-LINQ of `Enchantments?.Where(...).OrderBy(e => e.Type).Select(new Enchant).ToList()`. Filter into the
            // result list directly (no Where/Select iterators), then stable-sort by Type with an insertion sort —
            // OrderBy is a stable sort and the enchant counts are tiny, so insertion sort is both bit-identical (ties
            // keep their original relative order) and allocation-free.
            List<Enchant> enchants = null;
            var sourceEnchants = auction.Enchantments;
            if (sourceEnchants != null)
            {
                enchants = new List<Enchant>(sourceEnchants.Count);
                for (int i = 0; i < sourceEnchants.Count; i++)
                {
                    var e = sourceEnchants[i];
                    if (MinEnchantMap.TryGetValue(e.Type, out byte value) && e.Level >= value)
                        enchants.Add(new Models.Enchant() { Lvl = e.Level, Type = e.Type });
                }
                // stable insertion sort by Type (== Comparer<EnchantmentType>.Default, the OrderBy key comparer)
                for (int i = 1; i < enchants.Count; i++)
                {
                    var cur = enchants[i];
                    int j = i - 1;
                    while (j >= 0 && enchants[j].Type > cur.Type)
                    {
                        enchants[j + 1] = enchants[j];
                        j--;
                    }
                    enchants[j + 1] = cur;
                }
            }
            var flatNbt = auction.FlatenedNBT;

            // WS-A: when the parse memo is active, fold each non-per-instance flatNbt entry's FNV into an
            // ORDER-INDEPENDENT, splitmix-finalized accumulator (sum of finalized per-entry hashes) — Dictionary
            // iteration order is not guaranteed stable across two distinct dicts holding the same content, so a
            // commutative combine makes the content hash insertion-order-invariant; the finalizer makes the sum
            // collision-resistant. Hashed BEFORE the include filter, so the hash covers the full immutable content, not
            // just the SelectValuable-kept subset (a hit guarantees byte-identical inputs to the whole parse). When the
            // memo is OFF (the shipped default) this hashing is skipped — the path is byte-for-byte the pre-R7 parse.
            bool hashContent = ParseMemoActive;
            ulong flatHash = 0;
            if (!AllocatedDicts.TryDequeue(out var modifiers))
                modifiers = new Dictionary<string, string>(5);
            if (flatNbt != null)
                foreach (var item in flatNbt)
                {
                    if (hashContent && !IsNonContentKey(item.Key))
                        flatHash += Mix(FnvString(FnvString(FnvOffset, item.Key), item.Value));
                    if (!IncludeKeys.Contains(item.Key) && item.Value != "PERFECT" && !IsRune(item.Key) && !IsSoul(item))
                    {
                        continue;
                    }
                    var normalized = NormalizeData(item, auction.Tag, flatNbt);
                    if (normalized.Key != Ignore.Key)
                        modifiers.Add(normalized.Key, normalized.Value);
                }
            if (hashContent)
                _flatNbtContentHash = flatHash;
            if (auction.ItemCreatedAt < UnlockedIntroduction
                // safe guard for when the creation date is wrong 
                && flatNbt != null
                && !flatNbt.ContainsKey("unlocked_slots"))
            {
                var allUnlockable = itemService?.GetUnlockableSlots(auction.Tag).ToList();
                if (flatNbt.TryGetValue("gemstone_slots", out var countString) && int.TryParse(countString, out var count))
                {
                    allUnlockable = allUnlockable.Take(count).ToList();
                    modifiers.Remove("gemstone_slots");
                }
                if (allUnlockable?.Count > 0)
                    modifiers.Add("unlocked_slots", string.Join(",", allUnlockable.OrderBy(s => s)));
            }
            var result = modifiers.ToList();
            // Recycle the working dictionary back into the pool (it was only ever dequeued, never returned, so this path
            // allocated a fresh dict every auction). The contents have been copied into `result`; clear and re-enqueue.
            modifiers.Clear();
            if (AllocatedDicts.Count < 256)
                AllocatedDicts.Enqueue(modifiers);
            return (enchants, result);
        }

        // Allocation-free replacements for the per-auction `modifiers.Any(...)` / `modifiers.RemoveAll(...)` lambdas in
        // DetailedKeyFromSaveAuction (each ran every call and allocated a delegate). RemoveAll-equivalent: removes every
        // match while preserving the relative order of the rest (in-place compaction).
        private static bool AnyModifierKey(List<KeyValuePair<string, string>> modifiers, string key)
        {
            for (int i = 0; i < modifiers.Count; i++)
                if (modifiers[i].Key == key) return true;
            return false;
        }

        private static void RemoveModifiersByKey(List<KeyValuePair<string, string>> modifiers, string key)
        {
            int write = 0;
            for (int read = 0; read < modifiers.Count; read++)
            {
                if (modifiers[read].Key == key)
                    continue;
                if (write != read)
                    modifiers[write] = modifiers[read];
                write++;
            }
            if (write < modifiers.Count)
                modifiers.RemoveRange(write, modifiers.Count - write);
        }

        private static void RemoveModifiersByValue(List<KeyValuePair<string, string>> modifiers, string value)
        {
            int write = 0;
            for (int read = 0; read < modifiers.Count; read++)
            {
                if (modifiers[read].Value == value)
                    continue;
                if (write != read)
                    modifiers[write] = modifiers[read];
                write++;
            }
            if (write < modifiers.Count)
                modifiers.RemoveRange(write, modifiers.Count - write);
        }

        private static bool ContainsEnchantType(List<Enchant> enchants, Enchantment.EnchantmentType type)
        {
            for (int i = 0; i < enchants.Count; i++)
                if (enchants[i].Type == type) return true;
            return false;
        }

        private static KeyWithValueBreakdown Constructkey(SaveAuction auction, List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers, bool shouldIncludeReforge, long valueSubstracted, List<RankElem> rankElems, Tier tier)
        {
            var key = new AuctionKeyWithValue()
            {
                // order attributes
                Modifiers = modifiers.AsReadOnly(),
                Enchants = enchants.AsReadOnly(),
                Tier = tier,
                Reforge = shouldIncludeReforge ? auction.Reforge : ItemReferences.Reforge.Any,
                Count = (byte)auction.Count,
                ValueSubstract = valueSubstracted
            };
            var fullKey = new KeyWithValueBreakdown()
            {
                Key = key,
                SubstractedValue = valueSubstracted,
                ValueBreakdown = rankElems
            };
            return fullKey;
        }

        public KeyWithValueBreakdown GetBreakdownKey(AuctionKey key, string tag)
        {
            var virtualAuction = new SaveAuction() { Reforge = key.Reforge, Tier = key.Tier, Tag = tag, Count = key.Count, FlatenedNBT = key.Modifiers.ToDictionary() };
            var capped = CapKeyLength(key.Enchants.ToList(), key.Modifiers.ToList(), virtualAuction, 0);
            return Constructkey(virtualAuction, [.. key.Enchants], key.Modifiers.ToList(), capped.includeReforge, capped.valueSubstracted, capped.ranked, key.Tier);
        }

        public static Tier ReduceRarity(Tier tier)
        {
            if (tier < Tier.ULTIMATE)
            {
                if (tier == Tier.MYTHIC)
                    tier = Tier.LEGENDARY;
                else if (tier == Tier.DIVINE)
                    tier = Tier.MYTHIC;
                else
                    tier--;
            }

            return tier;
        }


        private static HashSet<string> SoulKeys = new()
        {
            "MASTER_CRYPT_TANK_ZOMBIE_70",
            "MASTER_CRYPT_TANK_ZOMBIE_80",
            "MASTER_CRYPT_TANK_ZOMBIE_60",
            "MINOS_INQUISITOR_750",
            "MINOS_CHAMPION_310",
            "MINOS_CHAMPION_175",
            "MASTER_CRYPT_UNDEAD_25",
            "MASTER_CRYPT_UNDEAD_HYPIXEL_25",
            "MASTER_CRYPT_UNDEAD_VALENTIN_40",
            "MASTER_CRYPT_UNDEAD_CONNORLINFOOT_25",
            "MASTER_CRYPT_UNDEAD_PIETER_40",
            "MASTER_CRYPT_UNDEAD_NICHOLAS_40",
            "MASTER_CRYPT_UNDEAD_MINIKLOON_25",
            "MASTER_CRYPT_UNDEAD_LIKAOS_25",
            "MASTER_CRYPT_UNDEAD_JAMIETHEGEEK_25",
            "MASTER_CRYPT_UNDEAD_FRIEDRICH_40",
            "MASTER_CRYPT_UNDEAD_PLANCKE_25",
            "MASTER_CRYPT_UNDEAD_BLOOZING_25",
            "MASTER_CRYPT_UNDEAD_SYLENT_25",
            "MASTER_CRYPT_UNDEAD_CODENAME_B_25",
            "MASTER_CRYPT_UNDEAD_JAYAVARMEN_25",
            "MASTER_CRYPT_UNDEAD_ALEXANDER_40",
            "MASTER_CRYPT_UNDEAD_ORANGEMARSHALL_25",
            "MASTER_CRYPT_UNDEAD_BERNHARD_40",
            "MASTER_CRYPT_UNDEAD_CECER_25",
            "MASTER_CRYPT_UNDEAD_APUNCH_25",
            "MASTER_CRYPT_UNDEAD_DCTR_25",
            "MASTER_CRYPT_UNDEAD_RELENTER_25",
            "MASTER_CRYPT_UNDEAD_NITROHOLIC__25",
            "MASTER_CRYPT_UNDEAD_DONPIRESO_25",
            "MASTER_CRYPT_UNDEAD_LADYBLEU_25",
            "MASTER_CRYPT_UNDEAD_DUECES_25",
            "MASTER_CRYPT_UNDEAD_JUDG3_25",
            "MASTER_CRYPT_UNDEAD_BEMBO_25",
            "MASTER_CRYPT_UNDEAD_REZZUS_25",
            "MASTER_CRYPT_UNDEAD_SKYERZZ_25",
            "MASTER_CRYPT_UNDEAD_THEMGRF_25",
            "MASTER_CRYPT_UNDEAD__ONAH_25",
            "MASTER_CRYPT_UNDEAD_EXTERNALIZABLE_25",
            "MASTER_CRYPT_UNDEAD_REVENGEEE_25",
            "MASTER_CRYPT_UNDEAD_MAGICBOYS_25",
            "MASTER_CRYPT_UNDEAD_AGENTK_25",
            "MASTER_CRYPT_UNDEAD_FLAMEBOY101_25",
            "MASTER_CRYPT_UNDEAD_SFARNHAM_25",
            "MASTER_CRYPT_UNDEAD_THORLON_25",
            "MASTER_CRYPT_UNDEAD_CHILYNN_25",
            "MASTER_CRYPT_UNDEAD_MARIUS_40",
            "MASTER_CRYPT_UNDEAD_WILLIAMTIGER_25",
            "MASTER_CRYPT_UNDEAD_CHRISTIAN_40"
        };
        private static bool IsSoul(KeyValuePair<string, string> n)
        {
            // captured souls
            return SoulKeys.Contains(n.Key);
        }

        // [ThreadStatic] scratch for CapKeyLength's two genuinely-transient working lists. CapKeyLength runs on the
        // ingest worker threads (per-worker [ThreadStatic] is race-free, mirroring _cmbRelevant / the PotentialSnipe
        // scratch). `_capCombined` is the SortCombined output (filtered+sorted), `_capToRemove` the below-threshold set;
        // both are fully consumed inside this method and never escape (their RankElem references are copied — by value —
        // into the returned `ordered`/`ranked` list, so reusing the list container is safe). The returned `ranked`
        // (GetOrdered) is retained on the breakdown, so it stays a fresh allocation. `raw` (ComparisonValue's return)
        // is left fresh: ComparisonValue is a shared/benchmark-exposed API whose return contract must not become pooled.
        [ThreadStatic] private static List<RankElem> _capCombined;
        [ThreadStatic] private static List<RankElem> _capToRemove;

        /// <summary>
        /// To find more matches the key length is capped.
        /// This is done by removing the lowest value enchantment or modifier
        /// </summary>
        /// <param name="enchants"></param>
        /// <param name="modifiers"></param>
        /// <returns>The coin amount substracted</returns>
        public (long valueSubstracted, bool removedRarity, bool includeReforge, List<RankElem> ranked) CapKeyLength(
            List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers, SaveAuction auction, long threshold = 500000, int elements = 5)
        {
            long underlyingItemValue = GetCleanItemValue(auction, ref threshold);
            long valueSubstracted = HandleGems(modifiers, auction);
            // ComparisonValue now returns a concrete List<RankElem> (enchants then modifiers, in order). Append the
            // reforge straight onto it (no Append iterator), then filter+sort in place — bit-identical to the old
            // SortCombined (same Value!=0 filter, same unstable List.Sort by descending Value over the same input order).
            var raw = (List<RankElem>)ComparisonValue(enchants, modifiers, auction.Tag, auction.FlatenedNBT) ?? new List<RankElem>();

            bool includeReforge = AddReforgeValue(auction.Reforge, raw);
            List<RankElem> combined = SortCombined(raw);

            // modifierSum: underlyingItemValue + Σ (IsEstimate ? Value/20 : Value), 0 when empty (DefaultIfEmpty(0)).
            long sum = 0;
            for (int i = 0; i < combined.Count; i++)
            {
                var m = combined[i];
                sum += m.IsEstimate ? m.Value / 20 : m.Value;
            }
            var modifierSum = underlyingItemValue + sum;
            threshold = Math.Max(threshold, modifierSum / 22);
            var percentDiff = (double)auction.HighestBidAmount / modifierSum;
            if (auction.HighestBidAmount == 0 || percentDiff > 1)
                percentDiff = 1;
            // remove all but the top 5
            List<RankElem> toRemove = GetItemsToRemove(threshold, combined);
            bool removedRarity = false;
            foreach (var item in toRemove)
            {
                // use percentage of full value
                var adjustedRemoveValue = (long)(item.Value * percentDiff);
                // remove all but the top 5
                if (item.Enchant.Type != 0)
                {
                    if (enchants.Remove(item.Enchant))
                        valueSubstracted += adjustedRemoveValue;
                }
                else if (item.Reforge != ItemReferences.Reforge.None)
                {
                    includeReforge = false;
                }
                else
                {
                    if (item.Modifier.Key == "exp")
                        continue; // even if its valued at very little this needs to stay
                    if (adjustedRemoveValue > 50_000_000 && Constants.AttributeKeys.Contains(item.Modifier.Key))
                        adjustedRemoveValue -= 50_000_000;
                    if (item.IsEstimate)
                        adjustedRemoveValue /= 10;
                    if (modifiers.Remove(item.Modifier))
                        valueSubstracted += adjustedRemoveValue;
                    if (item.Modifier.Key == "skin")
                        modifiers.RemoveAll(m => m.Key == "candyUsed");
                    if (item.Modifier.Key == "rarity_upgrades")
                        removedRarity = true;
                }
            }
            List<RankElem> ordered = GetOrdered(elements, combined);
            return (valueSubstracted, removedRarity, includeReforge, ordered);

            // Clear-and-reuse the [ThreadStatic] _capCombined buffer instead of allocating per call. Bit-identical to the
            // old `new List(...)` form: same Value!=0 filter over the same input order, same unstable List.Sort by
            // descending Value. The buffer is never `list` itself (list==raw, fresh), so the clear is safe.
            static List<RankElem> SortCombined(List<RankElem> list)
            {
                var filtered = _capCombined ??= new List<RankElem>();
                filtered.Clear();
                foreach (var c in list)
                {
                    if (c.Value != 0)
                        filtered.Add(c);
                }
                filtered.Sort((a, b) => b.Value.CompareTo(a.Value));
                return filtered;
            }

            // Bit-identical to the old `combined.Where(c => c.Value == 0).Concat(combined.Take(elements)).ToList()`:
            // `combined` here is the SortCombined output (all Value != 0), so the Where(==0) prefix is always empty and
            // this is simply the first `elements` entries — but the zero-prefix is kept explicit to match exactly.
            static List<RankElem> GetOrdered(int elements, List<RankElem> combined)
            {
                var ordered = new List<RankElem>(Math.Min(combined.Count, elements));
                foreach (var c in combined)
                    if (c.Value == 0)
                        ordered.Add(c);
                int take = Math.Min(elements, combined.Count);
                for (int i = 0; i < take; i++)
                    ordered.Add(combined[i]);
                return ordered;
            }
        }

        private static List<RankElem> GetItemsToRemove(long threshold, List<RankElem> combined)
        {
            // Clear-and-reuse the [ThreadStatic] _capToRemove buffer (distinct from _capCombined, which `combined`
            // aliases — they never share). Bit-identical to the old `new List<RankElem>(5)` form: same predicate, same
            // order; only the list container is reused (its RankElem references are consumed in CapKeyLength's removal
            // loop and never escape).
            var toRemove = _capToRemove ??= new List<RankElem>(5);
            toRemove.Clear();
            for (int i = 0; i < combined.Count; i++)
            {
                var c = combined[i];
                // keep top 1 even if below threshold
                // always remove below 500k or ~1.6%
                if ((i >= 5 && c.Value > 0)
                    || (i >= 1 && i < 5 && c.Value > 0 && c.Value < threshold)
                    || (i < 1 && c.Value > 0 && (c.Value < 500_000 || c.Value < threshold / 4)))
                {
                    toRemove.Add(c);
                }
            }

            return toRemove;
        }

        private long GetCleanItemValue(SaveAuction auction, ref long threshold)
        {
            var underlyingItemValue = 0L;
            if (auction.Tag != null && Lookups.TryGetValue(auction.Tag, out var lookups))
            {
                var price = lookups.CleanPricePerTier.GetValueOrDefault(auction.Tier);
                if (price > 0)
                {
                    underlyingItemValue = price;
                    threshold = Math.Max(price / 20, threshold);
                }
            }

            return underlyingItemValue;
        }

        private long HandleGems(List<KeyValuePair<string, string>> modifiers, SaveAuction auction)
        {
            // Snapshot the PERFECT gems first (the loop mutates `modifiers` via Remove, so we cannot iterate it live).
            // De-LINQ of `modifiers.Where(m => m.Value == "PERFECT").ToList()` — same order, same elements; the common
            // no-gem case avoids the list allocation entirely.
            int gemCount = 0;
            List<KeyValuePair<string, string>> gems = null;
            for (int i = 0; i < modifiers.Count; i++)
            {
                if (modifiers[i].Value == "PERFECT")
                {
                    (gems ??= new List<KeyValuePair<string, string>>()).Add(modifiers[i]);
                    gemCount++;
                }
            }
            long valueSubstracted = 0;
            if (gems != null)
                foreach (var item in gems)
                {
                    var gemKey = mapper.GetItemKeyForGem(item, auction.FlatenedNBT);
                    if (BazaarPrices.TryGetValue(gemKey, out var price))
                    {
                        valueSubstracted += (long)price; // no removal cost because this is just add
                        modifiers.Remove(item);
                    }
                }
            if (gemCount == 5)
            {
                modifiers.Add(new("pgems", "5"));
            }

            return valueSubstracted;
        }

        bool AddReforgeValue(ItemReferences.Reforge reforge, ref IEnumerable<RankElem> combined)
        {
            bool includeReforge = Constants.RelevantReforges.Contains(reforge);
            if (includeReforge)
            {
                if (ReforgeValueLookup.TryGetValue(reforge, out var value))
                {
                    combined = combined.Append(value.Item1);
                    return includeReforge;
                }
                long reforgeValue = GetReforgeValue(reforge);
                var element = new RankElem(reforge, reforgeValue);
                combined = combined.Append(element);
                if (reforgeValue > 0)
                    ReforgeValueLookup[reforge] = (element, DateTime.UtcNow);
            }

            return includeReforge;
        }

        /// <summary>
        /// Allocation-free variant of <see cref="AddReforgeValue"/> for the hot key-extraction path: appends the reforge
        /// <see cref="RankElem"/> straight onto the working list instead of wrapping it in an <c>Append</c> iterator. The
        /// LINQ overload is kept for the candidate-scoring callers that pass a deferred sequence.
        /// </summary>
        private bool AddReforgeValue(ItemReferences.Reforge reforge, List<RankElem> combined)
        {
            bool includeReforge = Constants.RelevantReforges.Contains(reforge);
            if (includeReforge)
            {
                if (ReforgeValueLookup.TryGetValue(reforge, out var value))
                {
                    combined.Add(value.Item1);
                    return includeReforge;
                }
                long reforgeValue = GetReforgeValue(reforge);
                var element = new RankElem(reforge, reforgeValue);
                combined.Add(element);
                if (reforgeValue > 0)
                    ReforgeValueLookup[reforge] = (element, DateTime.UtcNow);
            }

            return includeReforge;
        }

        internal IEnumerable<RankElem> ComparisonValue(IEnumerable<Enchant> enchants, IList<KeyValuePair<string, string>> modifiers, string tag, Dictionary<string, string> flatNbt)
        {
            // De-LINQ + de-alloc: the original built a lazy enchant Select, an eager modifier list, then Concat'd them
            // into a deferred IEnumerable the caller re-materialized. Build the single combined list directly instead —
            // enchants first (in order), then modifiers (index-based, see the mutation note) — which is bit-identical to
            // `valuePerEnchant.Concat(valuePerModifier)` once enumerated, with no Select/Concat iterators. The enchant
            // values are independent of modifier processing, so evaluating them eagerly here yields the same RankElems.
            if (enchants == null && modifiers == null)
                return null;

            int enchCount = enchants is ICollection<Enchant> ec ? ec.Count : 0;
            var combined = new List<RankElem>(enchCount + (modifiers?.Count ?? 0));

            if (enchants != null)
                foreach (var item in enchants)
                    combined.Add(new RankElem(item, mapper.EnchantValue(new Core.Enchantment(item.Type, item.Lvl), null, BazaarPrices, tag)));

            if (modifiers != null)
            {
                // Build the deduped relevant-modifier set once per call (first occurrence per key wins) rather than
                // rebuilding it for every modifier — it is the ModifierValueLookup cache key and was a per-modifier
                // GroupBy/ToDictionary allocation on the hottest inner loop.
                // R10 #2: pool the dict (it was a fresh alloc every call). R10 #1: compute its order-independent hash
                // ONCE here (the sum is identical for all M probes) and stamp it on each ModifierLookupKey, turning the
                // cache-key hashing from O(M²) → O(M)/auction. The stamped hash == what GetHashCode would iterate, so
                // it is bit-exact (identical cache placement / hit-miss set).
                if (!AllocatedDicts.TryDequeue(out var relevant))
                    relevant = new Dictionary<string, string>(modifiers.Count);
                int relevantHash = 0;
                foreach (var rm in modifiers)
                    if (relevant.TryAdd(rm.Key, rm.Value))
                        relevantHash += System.HashCode.Combine(rm.Key, rm.Value);
                bool relevantEscaped = false;

                // Index-based iteration (not foreach): a modifier estimate can mutate `modifiers` in place (the
                // unlocked_slots converter filters inaccessible gemstone slots), and indexing tolerates that the same
                // way the previous `modifiers.Select(...).ToList()` did. A foreach enumerator would throw.
                for (int mi = 0; mi < modifiers.Count; mi++)
                {
                    var m = modifiers[mi];
                    try
                    {
                        var lookupKey = new ModifierLookupKey() { ItemTag = tag, Modifier = m, RelevantModifiers = relevant, RelevantHash = relevantHash };
                        if (ModifierValueLookup.TryGetValue(lookupKey, out var value))
                        {
                            combined.Add(value.Item1);
                            continue;
                        }
                        var calculated = ModifierEstimate(modifiers, tag, flatNbt, m);
                        if (calculated.Value > 0)
                        {
                            ModifierValueLookup[lookupKey] = (calculated, DateTime.UtcNow);
                            relevantEscaped = true; // the stored key retains `relevant`, so it must not be pool-recycled
                        }
                        combined.Add(calculated);
                    }
                    catch (Exception e)
                    {
                        logger.LogInformation($"Error when calculating value for {m.Key} {m.Value} {tag}\n" + e);
                        combined.Add(new RankElem(m, 0));
                    }
                }
                // R10 #2: recycle the dict only when it did NOT escape into the cache (the all-hit warm common case);
                // an escaped dict is now owned by ModifierValueLookup and is left for it (same as the old fresh alloc).
                if (!relevantEscaped)
                {
                    relevant.Clear();
                    if (AllocatedDicts.Count < 256)
                        AllocatedDicts.Enqueue(relevant);
                }
            }
            return combined;
        }

        private RankElem ModifierEstimate(IList<KeyValuePair<string, string>> modifiers, string tag, Dictionary<string, string> flatNbt, KeyValuePair<string, string> mod)
        {
            var items = GetItemKeysForModifier(tag, mod);
            var sum = 0L;
            foreach (var item in items)
            {
                if (!Lookups.TryGetValue(item.tag, out var lookup))
                {
                    var itemPrice = BazaarPrices.TryGetValue(item.tag, out var price) ? (long)price * item.amount : 0;
                    sum += itemPrice;
                    continue;
                }
                if (IsRune(mod.Key))
                {
                    var fromlevel1 = lookup.Lookup.Where(f => f.Value.Price != 0)
                        .OrderBy(v => (v.Key.Count + 1) * (v.Key.Modifiers.Count == 0 ? 1 : int.Parse(v.Key.Modifiers.First().Value)))
                        .FirstOrDefault().Value?.Price * item.amount ?? 0;
                    var matchingLevel = lookup.Lookup.Where(f => f.Value.Price != 0 && f.Key.Modifiers.FirstOrDefault().Value == mod.Value)
                        .Select(f => f.Value.Price).OrderBy(p => p).FirstOrDefault();
                    if (matchingLevel != 0 && matchingLevel < fromlevel1)
                        sum += matchingLevel;
                    else
                        sum += fromlevel1;
                    if (tag.Contains("RUNE") && sum < 500_000)
                    {
                        // do not remove rune levels from runes
                        sum = 0;
                    }
                    continue;
                }
                var lookupPrice = (lookup.Lookup.Values.OrderBy(v => v.Price).FirstOrDefault(f => f.Price != 0)?.Price ?? 0) * item.amount;
                sum += lookupPrice;
            }

            // early return if we have a value before estimates
            if (sum > 0 || mod.Key == null)
                return new RankElem(mod, sum);

            if (Converters.TryGetValue(mod.Key, out var converter))
            {
                return converter(new()
                {
                    ItemTag = tag,
                    Modifier = mod,
                    RelevantModifiers = modifiers
                });
            }
            if (KillKeys.Contains(mod.Key))
            {
                sum += 300_000 * (int)Math.Pow(2, int.Parse(mod.Value)) + 300_000;
            }

            if (Constants.AttributeKeys.Contains(mod.Key))
            {
                sum += 200_000 * (long)Math.Pow(2, int.Parse(mod.Value)) + 600_000;
                if (modifiers.Any(m => m.Key != mod.Key && Constants.AttributeKeys.Contains(m.Key)))
                    sum += 50_000_000; // godroll
            }
            if (IsSoul(mod))
            {
                sum += 3_000_000 * (int)Math.Pow(1.5, int.Parse(mod.Value));
            }
            return new RankElem(mod, sum)
            {
                IsEstimate = true
            };
        }

        private long GetGemstoneSlotWorth(IList<KeyValuePair<string, string>> modifiers, string tag, KeyValuePair<string, string> mod)
        {
            var valueAdd = 0L;
            var present = mod.Value.Split(',').ToList();
            var costs = itemService.GetSlotCostSync(tag, new(), present);
            foreach (var cost in costs.Item1)
            {
                if (cost.Type.ToLower() == "item")
                    valueAdd += GetPriceForItem(cost.ItemId) * cost.Amount ?? 1;
                else
                    valueAdd += cost.Coins;
            }
            if (costs.unavailable.Count() > 0 && modifiers is List<KeyValuePair<string, string>> modList)
            {
                modList.RemoveAll(m => m.Key == "unlocked_slots");
                var remaining = present.Except(costs.unavailable);
                if (remaining.Count() > 0)
                    modList.Add(new(mod.Key, string.Join(",", remaining.OrderBy(s => s))));
            }

            return valueAdd;
        }

        private long GetCandyPrice(string tag, Dictionary<string, string> flatNbt)
        {
            return Math.Max(GetPriceForItem(tag) / 6, 10_000_000) * (flatNbt == null ? 2 : flatNbt.ContainsKey("skin") ? 10 : 1);// for skined pets important
        }

        private int GetExpValue(string tag, KeyValuePair<string, string> mod)
        {
            (var maxExp, var second) = HighExp(tag) ? ("7", GoldenDragonMaxExp) : ("6", PetExpMaxlevel);
            var lvl1Key = new AuctionKey(new(), ItemReferences.Reforge.Any, EmptyPetModifiers.ToList(), Tier.LEGENDARY, 1);
            var maxLevel = new AuctionKey(new(), ItemReferences.Reforge.Any, new List<KeyValuePair<string, string>>() { new("exp", maxExp) }, Tier.LEGENDARY, 1);
            if (Lookups.TryGetValue(tag, out var lookup) && lookup.Lookup.TryGetValue(lvl1Key, out var baseLevel)
                && lookup.Lookup.TryGetValue(maxLevel, out var maxLevelValue) && maxLevelValue.Price > 100)
            {
                var precise = Math.Max((maxLevelValue.Price - baseLevel.Price) / int.Parse(maxExp), 200_000);
                return (int)(precise * Math.Max(float.Parse(mod.Value, CultureInfo.InvariantCulture), 0.5));
            }
            var factor = Math.Max(GetPriceForItem(tag) / 6, 10_000_000);
            var value = (int)(factor * (float.Parse(mod.Value) + 1));
            return value;
        }

        private static bool HighExp(string tag)
        {
            return tag == "PET_GOLDEN_DRAGON" || tag == "PET_JADE_DRAGON" || tag == "PET_ROSE_DRAGON";
        }

        private class ModifierMetadata
        {
            public string ItemTag;
            public IList<KeyValuePair<string, string>> RelevantModifiers;
            public KeyValuePair<string, string> Modifier;
        }

        private long GetReforgeValue(ItemReferences.Reforge reforge)
        {
            var reforgeCost = mapper.GetReforgeCost(reforge);
            var itemCost = GetPriceForItem(reforgeCost.Item1);
            var reforgeValue = itemCost + reforgeCost.Item2;
            return reforgeValue;
        }

        private List<Models.Enchant> RemoveNoEffectEnchants(SaveAuction auction, List<Models.Enchant> ench)
        {
            if (auction.Tag == null)
                return ench;
            if (itemService?.IsDungeonItemSync(auction.Tag) ?? false)
                ench = RemoveEnchantFromKey(ench, Enchantment.EnchantmentType.scavenger);
            foreach (var item in mapper.IrrelevantOn(auction.Tag))
            {
                ench = RemoveEnchantFromKey(ench, item.Item1, item.level);
            }
            if (auction.ItemName == "Enchanted Book Bundle")
                ench = new(); // book bundles themselfs are equivilient to the enchants
            return ench;
        }

        private static List<Models.Enchant> RemoveEnchantFromKey(List<Models.Enchant> enchList, Core.Enchantment.EnchantmentType ench, int maxLevel = 10)
        {
            if (enchList.Any(e => e.Type == ench))
                return enchList.Where(e => e.Type != ench || e.Lvl > maxLevel).ToList();
            return enchList;
        }

        private static List<KeyValuePair<string, string>> AssignEmptyModifiers(SaveAuction auction)
        {
            if (NBT.IsPet(auction.Tag))
                if (auction.FlatenedNBT.TryGetValue("heldItem", out var val) && val == "PET_ITEM_TIER_BOOST")
                    return new List<KeyValuePair<string, string>>(EmptyPetModifiers) { new(PetItemKey, TierBoostShorthand) };
                else
                    return EmptyPetModifiers.ToList();
            if (IsRune(auction.Tag))
                return auction.FlatenedNBT.ToList();
            if (auction.FlatenedNBT.Any(n => NeverDrop.Contains(n.Key)))
                return auction.FlatenedNBT.Where(n => NeverDrop.Contains(n.Key)).ToList();
            else
                return EmptyModifiers.ToList();
        }


        private KeyValuePair<string, string> NormalizeData(KeyValuePair<string, string> s, string tag, Dictionary<string, string> flattenedNbt)
        {
            if (tag == "ATTRIBUTE_SHARD")
                return s; // don't normalize attribute shards only one attribute on them
            if (s.Key == "exp")
            {
                var expMulti = HighExp(tag) ? GoldenDragonMaxExp / PetExpMaxlevel : 1;
                var exp = GetNumeric(s);
                if (exp >= 1_000_000 * expMulti && exp <= 2_500_000 * expMulti)
                    return new KeyValuePair<string, string>(s.Key, "0.3");
                else if (exp > 2_500_000 * expMulti && exp < PetExpMaxlevel * expMulti / 6)
                    return new KeyValuePair<string, string>(s.Key, "0.6");
                if (HighExp(tag))
                    return NormalizeNumberTo(s, GoldenDragonMaxExp / 7, 7);
                else
                    return NormalizeNumberTo(s, PetExpMaxlevel / 6, 6);
            }
            var generalNormalizations = NormalizeGeneral(s, IsMidas(tag),
                flattenedNbt,
                HighExp(tag)
                );
            if (generalNormalizations.Value != "continue")
                return generalNormalizations;
            if (s.Key == "hpc")
                return GetNumeric(s) switch
                {
                    15 => new("hotpc", "1"),
                    /// this is mirrored in <see cref="PropertyMapper"/>
                    > 10 => new("hotpc", "0.1"),
                    10 => new("hotpc", "0"),
                    _ => Ignore
                };
            if (s.Key == "ability_scroll")
                return new("scroll_count", (s.Value.Where(c => c == ',' || c == ' ').Count() + 1).ToString());
            // Special handling for tier boost - it should remain in the key since it changes pet tier
            if (s.Key == "heldItem" && s.Value == "PET_ITEM_TIER_BOOST")
                return new KeyValuePair<string, string>(PetItemKey, TierBoostShorthand);
            if (s.Key == "is_shiny" && !(tag.EndsWith("_CHESTPLATE") || tag == "HYPERION" || tag == "NECRON_HANDLE"))
            {
                return Ignore;
            }
            if (s.Key == "dungeon_item_level" && flattenedNbt.TryGetValue("upgrade_level", out _))
                return Ignore; // upgrade level is always higher (newer)
            if (s.Key == "dungeon_item_level")
                return new KeyValuePair<string, string>("upgrade_level", s.Value);

            if (s.Key == "tuned_transmission")
                if (s.Value == "4")
                    return new KeyValuePair<string, string>(s.Key, "4");
                else
                    return Ignore;
            if (s.Key == "seconds_held")
            {
                return NormalizeNumberTo(s, 180000, 6); // 50 hours per level max 6 levels
            }
            if (ShardAttributes.TryGetValue(s.Key, out var minLvl))
            {
                if (int.Parse(s.Value) >= minLvl)
                    return s;
                if (HasAttributeCombo(s, flattenedNbt, tag))
                    return s;
                return Ignore;
            }
            if (s.Key == "baseStatBoostPercentage")
            {
                var val = int.Parse(s.Value);
                if (val < 50)
                    return Ignore;
                if (val == 50) // max level found
                    return new KeyValuePair<string, string>("baseStatBoost", "50");
                if (val > 50)
                    return new KeyValuePair<string, string>("baseStatBoost", ">50");
            }
            if (s.Key == "item_tier")
            {
                if (s.Value == "10" && tag == "SKELETON_MASTER_CHESTPLATE")
                    return new KeyValuePair<string, string>("item_tier", "10");
                return Ignore;
            }
            if (IsRune(s.Key) && !IncludeKeys.Contains(s.Key) && !IsRune(tag))
            {
                return Ignore;
            }
            if (s.Key == "color" && (flattenedNbt.ContainsKey("dye_item")
                || s.Value == "::" // no actual different color
                || itemService.GetDefaultColorAndCategory(tag).color == s.Value.Replace(':', ',')))
                return Ignore;
            return s;
        }

        private static bool IsMidas(string tag)
        {
            return tag != null && MidasTags.Contains(tag);
        }

        public static KeyValuePair<string, string> NormalizeGeneral(KeyValuePair<string, string> s, bool isMiddas, Dictionary<string, string> flatten, bool isGDrag)
        {
            if (s.Key == "winning_bid")
                if (isMiddas)
                    if (flatten.ContainsKey("additional_coins") || s.Value.Length > 8)
                    {
                        // combine with additional coins
                        var additionalCoins = GetNumeric(flatten.FirstOrDefault(f => f.Key == "additional_coins"));
                        var fullValue = (int)(double.Parse(s.Value) + additionalCoins + 1) / 50_000_000;
                        return new KeyValuePair<string, string>("full_bid", fullValue.ToString());
                    }
                    else
                        return NormalizeNumberTo(s, 25_000_000, 5);
                else
                    return Ignore;
            if (s.Key == "eman_kills")
                return NormalizeGroupNumber(s, 10_000, 25_000, 50_000, 75_000, 100_000, 125_000, 150_000, 200_000);
            if (s.Key == "blood_god_kills")
                return NormalizeGroupNumber(s, 1_000_000, 10_000_000, 20_000_000, 100_000_000);
            if (s.Key == "expertise_kills")
                return NormalizeGroupNumber(s, 5_500, 15_000);
            if (s.Key == "raider_kills")
                return NormalizeGroupNumber(s, 10_000, 17500);
            if (KillKeys.Contains(s.Key))
                return NormalizeNumberTo(s, 10_000);
            if (s.Key == "yogsKilled")
                return NormalizeNumberTo(s, 5_000, 2);
            if (s.Key == "thunder_charge")
                return NormalizeGroupNumber(s, 150_000, 1_000_000, 2_500_000, 5_000_000);
            if (s.Key == "mined_crops")
                return NormalizeNumberTo(s, 500_000_000);
            if (s.Key == "blocksBroken")
                return NormalizeNumberTo(s, 20_000, 2);
            if (s.Key == "collected_coins")
                return NormalizeGroupNumber(s, 100_000_000, 1_000_000_000);
            if (s.Key == "bass_weight")
                return NormalizeGroupNumber(s, 2, 5, 10, 20, 50, 100, 200);
            if (s.Key == "logs_cut")
                return s.Value == "200000" ? new("logs_cut", "200k") : Ignore;
            if (s.Key == "candyUsed")
            {
                var expAmount = GetNumeric(flatten.FirstOrDefault(f => f.Key == "exp"));
                var hasSkin = flatten.ContainsKey("skin");
                if ((expAmount >= PetExpMaxlevel && !isGDrag || expAmount >= GoldenDragonMaxExp) && !hasSkin)
                    return Ignore; // not displayed on max exp items
                else
                    // all candied are the same
                    return new KeyValuePair<string, string>(s.Key, (double.Parse(s.Value) > 0 ? 1 : 0).ToString());
            }
            if (s.Key == "edition")
            {
                return NormalizeEdition(s);
            }
            if (s.Key == "intelligence_earned")
                if (s.Value.Length >= 3)
                    return new KeyValuePair<string, string>(s.Key, "100");
                else
                    return new KeyValuePair<string, string>(s.Key, "1");
            if (s.Key == "talisman_enrichment")
                return new KeyValuePair<string, string>("talisman_enrichment", "yes");
            return new KeyValuePair<string, string>(Ignore.Key, "continue");
        }

        public static KeyValuePair<string, string> NormalizeEdition(KeyValuePair<string, string> s)
        {
            var val = int.Parse(s.Value);
            if (val < 100)
                return new KeyValuePair<string, string>(s.Key, "verylow");
            if (val < 1000)
                return new KeyValuePair<string, string>(s.Key, "low");
            if (val < 10000)
                return new KeyValuePair<string, string>(s.Key, "10k");
            return new KeyValuePair<string, string>(s.Key, "100k");
        }

        /// <summary>
        /// Matches valuable attribute combinations
        /// </summary>
        /// <param name="s"></param>
        /// <param name="auction"></param>
        /// <returns></returns>
        private bool HasAttributeCombo(KeyValuePair<string, string> s, Dictionary<string, string> flattenedNbt, string tag)
        {
            return AttributeComboLookup.TryGetValue(s.Key, out var otherKeys) && otherKeys.Any(otherKey => flattenedNbt.TryGetValue(otherKey, out _))
                || ItemSpecificAttributeComboLookup.TryGetValue(tag, out var combos)
                    && combos.Any(combo => (s.Key == combo.Key && flattenedNbt.TryGetValue(combo.Value, out _) || s.Key == combo.Value && flattenedNbt.TryGetValue(combo.Key, out _)));
        }

        /// <summary>
        /// Returns keys that are higher value and have to be checked before something is declared to be a snipe
        /// </summary>
        /// <param name="baseKey">The actual auction key</param>
        /// <returns></returns>
        private IEnumerable<AuctionKey> HigherValueKeys(AuctionKey baseKey, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, double lbinPrice)
        {
            var exp = baseKey.Modifiers.Where(m => m.Key == "exp").FirstOrDefault();
            if (exp.Key != default && exp.Value != "6")
            {
                for (int i = (int)float.Parse(exp.Value) + 1; i < 7; i++)
                {
                    yield return new AuctionKey(baseKey)
                    {
                        Modifiers = baseKey.Modifiers.Where(m => m.Key != "exp").Append(new("exp", i.ToString())).OrderBy(m => m.Key).ToList().AsReadOnly()
                    };
                }
            }
            foreach (var item in baseKey.Modifiers.Where(m => ShardAttributes.ContainsKey(m.Key)))
            {
                for (int i = int.Parse(item.Value) + 1; i < 10; i++)
                {
                    yield return new AuctionKey(baseKey)
                    {
                        Modifiers = baseKey.Modifiers.Where(m => m.Key != item.Key).Append(new(item.Key, i.ToString())).OrderBy(m => m.Key).ToList().AsReadOnly()
                    };
                }
            }
            if (baseKey.Count <= 1 && lbinPrice > MIN_TARGET * 5)
            {
                for (int i = (int)baseKey.Tier; i < (int)Tier.VERY_SPECIAL + 1; i++)
                {
                    yield return new AuctionKey(baseKey)
                    {
                        Modifiers = baseKey.Modifiers.Append(new("rarity_upgrades", "1")).OrderBy(m => m.Key).ToList().AsReadOnly(),
                        Tier = (Tier)(i + 1)
                    };
                    yield return new AuctionKey(baseKey)
                    {
                        Tier = (Tier)(i + 1)
                    };
                }
                // check against every other item with the same enchants and modifiers (and more) - which should be higher value
                foreach (var item in l.Where(x => x.Value.Price > 0).Select(x => x.Key).Where(k => k != baseKey && baseKey.Modifiers
                    .All(m => k.Modifiers != null && k.Modifiers.Any(km => km.Key == m.Key && km.Value == m.Value))
                        && (baseKey.Reforge == k.Reforge || baseKey.Reforge == ItemReferences.Reforge.Any)
                            && baseKey.Enchants
                    .All(e => k.Enchants != null && k.Enchants.Any(ek => e.Type == ek.Type && ek.Lvl == e.Lvl)) && k.Tier == baseKey.Tier))
                {
                    if (item.Modifiers.Any(m => m.Value == TierBoostShorthand))
                        continue;
                    if (l.GetValueOrDefault(item)?.Price == 0)
                        continue;
                    yield return item;
                }
            }
            var compactEnch = baseKey.Enchants.FirstOrDefault(e => e.Type == Enchantment.EnchantmentType.compact && e.Lvl >= 5);
            if (compactEnch.Lvl != default)
            {
                for (int i = compactEnch.Lvl + 1; i < 10; i++)
                {
                    yield return new AuctionKey(baseKey)
                    {
                        Enchants = baseKey.Enchants.Where(e => e.Type != Enchantment.EnchantmentType.compact).Append(new()
                        {
                            Type = Enchantment.EnchantmentType.compact,
                            Lvl = (byte)i
                        }).OrderBy(e => e.Type).ToList().AsReadOnly()
                    };
                }
            }

            if (baseKey.Count > 1 && baseKey.Count < 64)
                yield return new AuctionKey(baseKey) { Count = 64 };
            if (baseKey.Count > 1 && baseKey.Count < 16)
                yield return new AuctionKey(baseKey) { Count = 16 };
        }

        public static KeyValuePair<string, string> NormalizeNumberTo(KeyValuePair<string, string> s, int groupingSize, int highestGroup = int.MaxValue)
        {
            var value = GetNumeric(s);
            if (s.Key != "exp" && value < groupingSize / 4)
                return Ignore;
            var group = value / groupingSize;
            return new KeyValuePair<string, string>(s.Key, Math.Min(group, highestGroup).ToString());
        }

        /// <summary>
        /// Determine group by  making less than comparison, smallest group is first
        /// </summary>
        /// <param name="s"></param>
        /// <param name="groups"></param>
        /// <returns></returns>
        public static KeyValuePair<string, string> NormalizeGroupNumber(KeyValuePair<string, string> s, params int[] groups)
        {
            var val = GetNumeric(s);
            if (val < 800)
                return Ignore;
            for (int i = 0; i < groups.Length; i++)
            {
                if (val < groups[i])
                {
                    return new KeyValuePair<string, string>(s.Key, i.ToString());
                }
            }
            var highestGroup = groups.Length;
            return new KeyValuePair<string, string>(s.Key, highestGroup.ToString());
        }

        public static long GetNumeric(KeyValuePair<string, string> s)
        {
            if (s.Value == null)
                return 0;
            try
            {
                return ((long)double.Parse(s.Value));
            }
            catch (Exception)
            {
                deferred.Log($"could not parse {s.Key} {s.Value}");
                throw;
            }
        }

        public void TestNewAuction(SaveAuction auction, bool triggerEvents = true, bool fastMode = false)
        {
            var startTimestamp = Stopwatch.GetTimestamp();
            try
            {
                TestNewAuctionInternal(auction, triggerEvents, fastMode);
            }
            finally
            {
                // Only the event-triggering path is the snipe-finding hot path; the silent-store path (triggerEvents=false)
                // is bulk loading and would skew the SLI.
                if (triggerEvents)
                {
                    var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
                    testNewAuctionDuration.Observe(elapsed.TotalSeconds);
                    if (VerboseProfiling)
                        logger?.LogInformation("[profile] TestNewAuction tag={Tag} took {Micros:F1}µs", auction?.Tag, elapsed.TotalMicroseconds);
                }
            }
        }

        private void TestNewAuctionInternal(SaveAuction auction, bool triggerEvents = true, bool fastMode = false)
        {
            using var activity = !triggerEvents ? null : activitySource?.StartActivity("TestNewAuction", ActivityKind.Internal);
            activity?.SetTag("uuid", auction.Uuid);
            activity?.SetTag("server", ServerDnsName);
            var itemGroupTag = GetAuctionGroupTag(auction.Tag);
            var lookup = Lookups.GetOrAdd(itemGroupTag.Item1, key => new PriceLookup());
            var l = lookup.Lookup;
            var cost = auction.StartingBid;
            var lbinPrice = auction.StartingBid * 1.03 + itemGroupTag.Item2;
            var medPrice = auction.StartingBid * 1.05 + itemGroupTag.Item2;
            var lastKey = new AuctionKey();
            var shouldTryToFindClosest = false;
            var basekey = DetailedKeyFromSaveAuction(auction, fastMode);

            for (int i = 0; i < 4; i++)
            {
                var key = basekey.GetReduced(i);
                if (i > 0 && key == lastKey)
                {
                    if (i < 4)
                        shouldTryToFindClosest = true;
                    continue; // already checked that
                }
                lastKey = key;

                if (!l.TryGetValue(key, out ReferenceAuctions bucket))
                {
                    if (triggerEvents && i == 4 && !fastMode)
                    {
                        logger.LogInformation($"could not find bucket {key} for {auction.Tag} {l.Count} {auction.Uuid}");
                        if (this.State < SniperState.Ready)
                        {
                            if (auction.UId % 10 == 2)
                                logger.LogInformation($"closest is not available yet, state is {this.State}");
                            return;
                        }
                        var closests = FindClosest(l, key, itemGroupTag.tag).Take(5).ToList();
                        foreach (var item in closests)
                        {
                            logger.LogInformation($"Closest bucket clean: {item.Key}");
                        }
                        if (!closests.Any())
                        {
                            logger.LogInformation($"No closest bucket found for {key} {auction.Uuid}");
                            break;
                        }
                        if (ShouldIgnoreMostSimilar(auction))
                        {
                            return;
                        }
                    }
                    else if (i != 0)
                        continue;
                    else
                        bucket = CreateAndAddBucket(auction, key);
                }
                if (i == 0)
                    UpdateLbin(auction, bucket, key);
                if (triggerEvents && IsFlipFindingPausedFor(itemGroupTag.tag))
                {
                    activity.Log("Flip finding paused during state rebuild");
                    this.LogNonFlip(auction, bucket, key, 0, 1, 0, "Flip finding paused during state rebuild");
                    return;
                }
                if (triggerEvents)
                {
                    using var tryFind = !triggerEvents ? null : activitySource?.StartActivity("TryFind", ActivityKind.Internal);
                    long extraValue = GetExtraValue(auction, key) - itemGroupTag.Item2;
                    var findFlipStart = Stopwatch.GetTimestamp();
                    var foundFlip = FindFlip(auction, lbinPrice, medPrice, bucket, key, lookup, basekey, extraValue, props =>
                    {
                        props["breakdown"] = JsonConvert.SerializeObject(basekey.ValueBreakdown);
                    }, fastMode);
                    findFlipDuration.Observe(Stopwatch.GetElapsedTime(findFlipStart).TotalSeconds);
                    if (foundFlip)
                        shouldTryToFindClosest = false; // found a snipe, no need to check other lower value buckets
                }
                else
                    return;
                if (IsRune(itemGroupTag.tag))
                    return; // no special handling for runes
            }
            if (!triggerEvents || fastMode)
                return; // no need to check for closest, just storing

            using var alternateFinders = !triggerEvents ? null : activitySource?.StartActivity("AlternateFinders", ActivityKind.Internal);
            var topKey = basekey.GetReduced(0);
            var topAttrib = basekey.ValueBreakdown.FirstOrDefault();
            if (topAttrib != default)
            {
                medPrice = auction.StartingBid * 1.06 + itemGroupTag.Item2;
                // R7 WS-C2: both finders re-parse the same auction via GetFullKey and build BuildDomKey(fullKey); share
                // the full key + its query DomKey across the two calls (built lazily once, after each finder's gates).
                var fullKeyQuery = new FullKeyQuery();
                CheckCombined(auction, lookup, lbinPrice, medPrice, basekey, topAttrib, ref fullKeyQuery);
                CheckLowerKeyFull(auction, lookup, lbinPrice, medPrice, basekey, l, ref fullKeyQuery);
            }
            if (shouldTryToFindClosest && triggerEvents && this.State >= SniperState.Ready)
            {
                var riskyFind = System.Threading.Tasks.Task.Run(() =>
                {
                    using var risky = !triggerEvents ? null : activitySource?.StartActivity("Risky", ActivityKind.Internal);
                    try
                    {
                        TryFindClosestRisky(auction, lookup, basekey, ref medPrice);
                    }
                    catch (System.Exception e)
                    {
                        risky?.SetTag("error", "true");
                        risky?.Log(e.ToString());
                    }
                });
                if (MIN_TARGET == 0)
                    riskyFind.Wait(); // test
            }

            CraftCostFinder(auction, itemGroupTag, lookup, medPrice, basekey);
            // R7 WS-TELEM: only build the (JSON-heavy) breakdown string when the activity is actually recording all
            // data — `activity != null` is true even for a propagation-only (non-recording) listener, for which the
            // event is discarded but the SerializeObject would still run unconditionally. IsAllDataRequested mirrors
            // the internal AddEvent gate, so the emitted event is byte-identical when recording.
            if (activity != null && activity.IsAllDataRequested)
                activity.Log($"BaseKey value {JsonConvert.SerializeObject(basekey.ValueBreakdown)}");
        }


        private static Dictionary<string, string[]> SharedAttributeGroup = new()
        {
            {"VANQUISHED_GLOWSTONE_GAUNTLET", ["GLOWSTONE_GAUNTLET"]},
            {"VANQUISHED_GHAST_CLOAK",[ "GHAST_CLOAK"]},
            {"VANQUISHED_BLAZE_BELT", ["BLAZE_BELT"]},
            {"VANQUISHED_MAGMA_NECKLACE", ["MAGMA_NECKLACE"]},
            {"VANQUISHED_INFERNO_ROD", ["INFERNO_ROD"]},
            { "HELLFIRE_ROD",["MAGMA_ROD", "INFERNO_ROD"]},
            {"INFERNO_ROD", ["MAGMA_ROD"]}
        };

        private void CraftCostFinder(SaveAuction auction, (string tag, long costSubstract) itemGroupTag, PriceLookup lookup, double medPrice, KeyWithValueBreakdown basekey)
        {
            var componentGuess = basekey.ValueBreakdown.Sum(c => c.IsEstimate ? GetValueEstimate(c) : c.Value);
            if (componentGuess <= medPrice / 8) // no need to check if sum is too low
            {
                return;
            }
            if (IsRune(itemGroupTag.tag))
            {
                return; // runes are not crafted so makes not sense to report them
            }
            var valueLookup = basekey.ValueBreakdown.ToDictionary(v =>
            {
                if (v.Modifier.Key != default)
                    return v.Modifier.Key;
                if (v.Reforge != default)
                    return v.Reforge.ToString();
                return v.Enchant.Type.ToString();
            }, c => c.IsEstimate ? GetValueEstimate(c) : AdjustFixedValue(c));
            var cleanCost = GetCleanItemPrice(itemGroupTag.tag, basekey, lookup);
            if (BreakDownIncludesItem(itemGroupTag, basekey))
            {
                cleanCost = 0;
            }
            cleanCost = Math.Min(cleanCost, auction.Tag switch
            {
                "STARRED_MIDAS_STAFF" => 60_000_000,
                "MIDAS_STAFF" => 10_000_000,
                "MIDAS_SWORD" => 5_000_000,
                "STARRED_MIDAS_SWORD" => 20_000_000,
                _ => cleanCost
            });
            var componentSum = valueLookup.Sum(v => (long)(v.Key switch
            {
                "skin" => auction.Tag.StartsWith("PET") ? 0.5 : 0.4,
                "ultimate_fatal_tempo" => 0.65,
                "rarity_upgrades" => 0.5,
                "upgrade_level" => 0.84,
                "talisman_enrichment" => 0.10,
                var s when IsRune(s) => 0.55,
                var s when Constants.AttributeKeys.Contains(s) => 0.01,
                _ => 0.85
            } * v.Value));
            if (cleanCost == componentGuess)
            {
                componentSum = 0; // unique runes shouldn't be counted twice
            }
            var combined = componentSum + cleanCost;
            if (combined / 1.1 > medPrice || combined - auction.StartingBid > 10_000_000)
            {
                var props = new Dictionary<string, string>
                    {
                        { "cleanCost", cleanCost.ToString() },
                        { "componentsSum", componentGuess.ToString() },
                        { "breakdown", JsonConvert.SerializeObject(valueLookup) }
                    };
                FoundAFlip(auction, new(), LowPricedAuction.FinderType.CraftCost, combined, props);
            }

            static bool BreakDownIncludesItem((string tag, long costSubstract) itemGroupTag, KeyWithValueBreakdown basekey)
            {
                // breakdown already includes cheapest item (rune probably)
                return basekey.ValueBreakdown.Count == 1 && basekey.Key.Modifiers.FirstOrDefault(m => m.Key == itemGroupTag.tag).Key != default;
            }

            long GetValueEstimate(RankElem c)
            {
                if (c.Modifier.Key == "candyUsed")
                    return 0;

                if (Constants.AttributeKeys.Contains(c.Modifier.Key))
                {
                    var key = VirtualAttributeKey(c.Modifier);
                    //var virtualList = lookup.Lookup.Where(e => e.Key.Modifiers.Any(m => m.Key == "virtual")).ToList();
                    if (!lookup.Lookup.TryGetValue(key, out var references) || references.Price == 0)
                        return 0;
                    if (Lookups.TryGetValue("ATTRIBUTE_SHARD", out var shardLookup) && shardLookup.Lookup.TryGetValue(key, out var shardBucket))
                    {
                        if (shardBucket.References.Count > 5 && shardBucket.Price > 0 && shardBucket.Price < references.Price)
                            references = shardBucket; // use shard price if it is cheaper as it can be used on all items
                    }
                    if (SharedAttributeGroup.TryGetValue(auction.Tag, out var sharedGroup))
                    {
                        foreach (var item in sharedGroup)
                        {
                            if (Lookups.TryGetValue(item, out var sharedLookup)
                                && sharedLookup.Lookup.TryGetValue(key, out var sharedBucket)
                                && sharedBucket.References.Count > 5 && sharedBucket.Price > 0 && sharedBucket.Price < references.Price)
                            {
                                references = sharedBucket; // use the shared group price if available
                            }
                        }
                    }
                    if (!int.TryParse(c.Modifier.Value, out var level))
                        return 0;
                    return (long)(references.Price * Math.Pow(2, level - 1));
                }
                if (KillKeys.Contains(c.Modifier.Key))
                    return c.Value / 50;
                return c.Value / 20;
            }

            static long AdjustFixedValue(RankElem c)
            {
                if (c.Modifier.Key == PetItemKey)
                    return c.Value / 2;
                return c.Value;
            }
        }

        // R6 WS-CMB2 — per-call relevant entry (de-LINQ'd CheckCombined). One per qualifying bucket; carries the
        // value weight, the captured FIFO reference snapshot (read once → race-free + matches LINQ's lazy
        // References enumeration order) and the bucket's Lbin captured at the same moment.
        private struct CmbRelevant
        {
            public AuctionKey Key;
            public ReferenceAuctions Bucket;
            public double Value;
            public ReferencePrice[] Refs; // FIFO snapshot (ReferenceSnapshot()), length == References.Count at capture
            public ReferencePrice Lbin;   // bucket.Lbin captured with Refs
        }

        // R6 WS-CMB2 — combined reference candidate (ref + its relevancy weight + capture order index for the
        // STABLE relevancy-descending selection; LINQ OrderByDescending is stable, Array.Sort is not).
        private struct CmbCombinedEntry
        {
            public ReferencePrice Ref;
            public double Relevancy;
            public int Idx; // source-enumeration order (relevant order × References order), the stable tiebreak
        }

        // [ThreadStatic] scratch — CheckCombined runs per-auction across worker threads (incl. the background risky
        // Task), so the buffers must be thread-local; cleared/grown per call, never shared across threads.
        [ThreadStatic] private static List<CmbRelevant> _cmbRelevant;
        [ThreadStatic] private static List<CmbCombinedEntry> _cmbCombined;

        // R7 WS-C2 — per-auction lazy cache of the FULL key + its query DomKey. CheckCombined and CheckLowerKeyFull both
        // call GetFullKey(auction) (a SelectValuable re-parse) and BuildDomKey(fullKey) on the SAME auction, so the full
        // key and its DomKey are byte-identical between the two finders. This holder builds each ONCE (on first use, after
        // each finder's own reject gates) and the second finder reuses it — collapsing 2 SelectValuable re-parses → 1 and
        // 2 BuildDomKey(query) DomMod[]/DomEnch[] builds → 1, with zero allocation on the path where neither finder needs
        // it (the holder is a by-ref struct; lazily built so a finder that early-returns before GetFullKey pays nothing).
        // Bit-exact: identical bytes, just built once.
        private struct FullKeyQuery
        {
            private bool built;
            private AuctionKeyWithValue key;
            private DomKey dom;
            public void Ensure(SniperService svc, SaveAuction auction)
            {
                if (built)
                    return;
                key = svc.GetFullKey(auction);
                dom = DominatorIndex.BuildDomKey(key, svc.scoreInterner);
                built = true;
            }
            public AuctionKeyWithValue Key => key;
            public DomKey Dom => dom;
        }

        private void CheckCombined(SaveAuction auction, PriceLookup lookup, double lbinPrice, double medPrice, KeyWithValueBreakdown longKey, RankElem topAttrib, ref FullKeyQuery fullKeyQuery)
        {
            if (UseCombinedReference) // R6 WS-CMB2 A/B seam (default false in production)
            {
                CheckCombinedReference(auction, lookup, lbinPrice, medPrice, longKey, topAttrib);
                return;
            }
            var topKey = longKey.GetReduced(0);
            var targetVolume = 11;
            if (lookup.Lookup.TryGetValue(topKey, out var topBucket) && topBucket.References.Count >= targetVolume)
            {
                return; // enough references in previous check
            }
            var groupTag = GetAuctionGroupTag(auction.Tag);
            var l = lookup.Lookup;
            // R7 WS-C2: full key + its query DomKey built once per auction, shared with CheckLowerKeyFull (same auction →
            // same GetFullKey/BuildDomKey bytes). Lazily materialised here, after the early-return gates above.
            fullKeyQuery.Ensure(this, auction);
            // R4 WS-CMB: the dominance filter (direction B: Dominates(cand=e.Key, query=fullKey)) runs the interned
            // kernel against the bucket's cached DomKey + the sound mask prefilter, instead of the per-candidate string
            // IsHigherValue. Bit-exact (40k fuzz + Combined/LowerKeyAndCombined snipe-set oracles).
            var queryDomCmb = fullKeyQuery.Dom;
            ulong qProvCmb = queryDomCmb.ProvidedMask;
            bool petSpiritCmb = auction.Tag == "PET_SPIRIT";
            var topReforge = topKey.Reforge;
            bool topReforgeAny = topReforge == ItemReferences.Reforge.Any;
            bool hasAttribMod = topAttrib.Modifier.Key != default;
            var topEnchant = topAttrib.Enchant;

            // R6 WS-CMB2 — de-LINQ. Single pass over the dict (same enumeration order as the old
            // `l.Where(...).ToList()`): apply the `similar` predicate, then the reforge + dominance filter, building the
            // `relevant` entries directly. The two-stage filter is fused: an element is in `relevant` iff it was in
            // `similar` AND passed reforge+dominance. The `similar.Count==1 → similar=l.ToList()` fallback only changes
            // the candidate SET when exactly one element matches the similar predicate; that element is unconditionally
            // re-evaluated under reforge+dominance in the fallback, so we recompute `relevant` over ALL entries.
            var relevant = _cmbRelevant ??= new List<CmbRelevant>();
            relevant.Clear();
            int similarCount = 0;
            foreach (var e in l)
            {
                // `similar` predicate: (hasAttribMod && !modifiers.contains("virtual")) || enchants.Contains(topEnchant)
                bool inSimilar = (hasAttribMod && !ModifiersContainVirtual(e.Key.Modifiers)) || e.Key.Enchants.Contains(topEnchant);
                if (!inSimilar)
                    continue;
                similarCount++;
                if (CmbTryBuild(e.Key, e.Value, out var built))
                    relevant.Add(built);
            }
            if (similarCount == 1)
            {
                // include all if no match otherwise — rebuild `relevant` over the WHOLE dict.
                relevant.Clear();
                foreach (var e in l)
                    if (CmbTryBuild(e.Key, e.Value, out var built))
                        relevant.Add(built);
            }

            bool CmbTryBuild(AuctionKey candKey, ReferenceAuctions candBucket, out CmbRelevant built)
            {
                built = default;
                if (!(candKey.Reforge == topReforge || topReforgeAny))
                    return false;
                if (!CmbDominates(candBucket, candKey))
                    return false;
                long compSum = 0;
                var comp = ComparisonValueForKey(groupTag.tag, candKey);
                for (int ci = 0; ci < comp.Count; ci++)
                    compSum += comp[ci].Value;
                var refs = candBucket.ReferenceSnapshot();
                built = new CmbRelevant
                {
                    Key = candKey,
                    Bucket = candBucket,
                    Value = Math.Max(candBucket.Volume, 0.5) * Math.Pow(compSum, 1.8),
                    Refs = refs,
                    Lbin = candBucket.Lbin
                };
                return true;
            }

            bool CmbDominates(ReferenceAuctions candBucket, AuctionKey candKey)
            {
                var candDom = GetBucketDomKey(candBucket, candKey);
                if ((candDom.RequiredMask & qProvCmb) != candDom.RequiredMask)
                    return false; // sound presence prefilter (candidate is the base side)
                return DominatorIndex.Dominates(in candDom, in queryDomCmb, petSpiritCmb);
            }
            if (relevant.Count < 2)
            {
                return; // makes only sense if there is something combined
            }
            // STABLE descending sort by value (OrderByDescending(e=>e.value)). LINQ keeps Comparer<double>.Default
            // (NaN sorts smallest) and is stable → ties keep source (dict-enumeration) order. relevant is consumed in
            // full order in several places below, so this is a real sort (not a partial-select).
            CmbStableSortByValueDesc(relevant);

            short today = GetDay();
            // combined = relevant.SelectMany(refs).Select(relevancy).OrderByDescending(relevancy).Take(targetVolume)
            // Source order for the stable sort = (relevant order) × (each bucket's FIFO References order).
            var combinedEntries = _cmbCombined ??= new List<CmbCombinedEntry>();
            combinedEntries.Clear();
            int srcIdx = 0;
            for (int ri = 0; ri < relevant.Count; ri++)
            {
                var r = relevant[ri];
                var refs = r.Refs;
                for (int j = 0; j < refs.Length; j++)
                {
                    var rp = refs[j];
                    combinedEntries.Add(new CmbCombinedEntry
                    {
                        Ref = rp,
                        Relevancy = r.Value * (rp.Day - today + 12) * Math.Log10(rp.Price + 1),
                        Idx = srcIdx++
                    });
                }
            }
            if (combinedEntries.Count == 0)
            {
                return;
            }
            // STABLE descending sort by relevancy, then take the top targetVolume.
            CmbStableSortByRelevancyDesc(combinedEntries);
            int combinedCount = Math.Min(combinedEntries.Count, targetVolume);
            var combined = new List<ReferencePrice>(combinedCount);
            for (int i = 0; i < combinedCount; i++)
                combined.Add(combinedEntries[i].Ref);

            // lbinBucket = relevant.Select(Lbin).Where(Price!=0).DefaultIfEmpty().MinBy(Price)
            // MinBy returns the FIRST element achieving the minimum (first-wins) over `relevant`'s sorted order; if no
            // Lbin has Price!=0 the result is default(ReferencePrice).
            ReferencePrice lbinBucket = default;
            bool anyLbin = false;
            for (int ri = 0; ri < relevant.Count; ri++)
            {
                var cand = relevant[ri].Lbin;
                if (cand.Price == default)
                    continue;
                if (!anyLbin || cand.Price < lbinBucket.Price)
                {
                    lbinBucket = cand;
                    anyLbin = true;
                }
            }
            // newestRef = combined.OrderByDescending(Day).Skip(1).FirstOrDefault().Day — the 2nd-highest Day (stable),
            // i.e. the element at sorted position 1; default(.Day)==0 if fewer than 2 entries.
            short newestRef = CmbSecondHighestDay(combined);
            var age = today - newestRef;
            var virtualBucket = new ReferenceAuctions()
            {
                Lbins = [lbinBucket],
                References = new(combined),
                Price = (combined.Count < 4 ? 0 : GetCappedMedian(auction, longKey, combined) * 98 / 100) * (age > 10 ? (10 - age / 9) : 10) / 10, // older items may have dropped in value
                OldestRef = (short)(newestRef - 2),
                Volatility = 123// mark as risky
            };
            // Capture the (key, ref-count) pairs the props lambda needs, in relevant's sorted order, with the snapshot
            // length used at build time (race-free; identical to the live Count in the single-threaded oracle). The
            // lambda is only invoked by FindFlip when it builds props for a found flip.
            int relevantCount = relevant.Count;
            var propKeys = new AuctionKey[relevantCount];
            var propRefCounts = new int[relevantCount];
            for (int ri = 0; ri < relevantCount; ri++)
            {
                propKeys[ri] = relevant[ri].Key;
                propRefCounts[ri] = relevant[ri].Refs.Length;
            }
            // mark with extra value -3
            var foundAndAbort = FindFlip(auction, lbinPrice, medPrice, virtualBucket, topKey, lookup, longKey, MIN_TARGET == 0 ? 0 : -3, props =>
            {
                // combined = relevant.TakeWhile(running total of RefCount < targetVolume).Select(key:count)
                var sb = new System.Text.StringBuilder();
                int total = 0;
                bool first = true;
                for (int ri = 0; ri < relevantCount; ri++)
                {
                    total += propRefCounts[ri];
                    if (!(total < targetVolume))
                        break; // TakeWhile stops at (and excludes) the first element failing the predicate
                    if (!first)
                        sb.Append(',');
                    sb.Append(propKeys[ri].ToString());
                    sb.Append(':');
                    sb.Append(propRefCounts[ri]);
                    first = false;
                }
                props.Add("combined", sb.ToString());
                props.Add("breakdown", JsonConvert.SerializeObject(longKey.ValueBreakdown));
                if (logger?.IsEnabled(LogLevel.Information) ?? false)
                {
                    var keysSb = new System.Text.StringBuilder();
                    for (int ri = 0; ri < relevantCount; ri++)
                    {
                        if (ri > 0)
                            keysSb.Append(',');
                        keysSb.Append(propKeys[ri].ToString());
                    }
                    logger.LogInformation($"Combined {longKey} {auction.Uuid} {virtualBucket.Price} {virtualBucket.Lbin.Price} keys: {keysSb}");
                }
            });

            long GetCappedMedian(SaveAuction auction, KeyWithValueBreakdown fullKey, List<ReferencePrice> combined)
            {
                var median = GetMedian(combined, []);
                var shortTerm = GetMedian(combined.Take(5).ToList(), new());
                median = CapAtCraftCost(groupTag.tag, Math.Min(median, shortTerm), fullKey, 0);
                return median;
            }
        }

        /// <summary>
        /// STABLE descending sort by <see cref="CmbRelevant.Value"/>, matching LINQ <c>OrderByDescending(e=>e.value)</c>
        /// (Comparer&lt;double&gt;.Default — NaN sorts smallest — with ties preserving original list order). Uses an index
        /// array so the underlying <see cref="List{T}"/> contents move only once at the end; Array.Sort is unstable so the
        /// index is the explicit tiebreak.
        /// </summary>
        private static void CmbStableSortByValueDesc(List<CmbRelevant> items)
        {
            int n = items.Count;
            if (n < 2)
                return;
            int[] order = ArrayPool<int>.Shared.Rent(n);
            CmbRelevant[] tmp = ArrayPool<CmbRelevant>.Shared.Rent(n);
            try
            {
                for (int i = 0; i < n; i++)
                {
                    order[i] = i;
                    tmp[i] = items[i];
                }
                Array.Sort(order, 0, n, new CmbValueDescComparer(tmp));
                for (int i = 0; i < n; i++)
                    items[i] = tmp[order[i]];
            }
            finally
            {
                ArrayPool<int>.Shared.Return(order);
                // R8 WS-CHURN-B: clear only the written prefix [0,n) instead of the full pooled capacity, then
                // Return with clearArray:false (avoids the pool zeroing ~2n on next-power-of-two oversized buffers).
                // GC-safe: only [0,n) was populated, so nulling [0,n) drops every managed ref we introduced
                // (CmbRelevant carries Key/Bucket/Refs); the untouched tail [n,capacity) holds no refs from us.
                Array.Clear(tmp, 0, n);
                ArrayPool<CmbRelevant>.Shared.Return(tmp, clearArray: false);
            }
        }

        private sealed class CmbValueDescComparer : IComparer<int>
        {
            private readonly CmbRelevant[] items;
            public CmbValueDescComparer(CmbRelevant[] items) => this.items = items;
            public int Compare(int a, int b)
            {
                int c = Comparer<double>.Default.Compare(items[a].Value, items[b].Value);
                if (c != 0)
                    return -c; // descending
                return a.CompareTo(b); // stable: original order
            }
        }

        /// <summary>
        /// STABLE descending sort by <see cref="CmbCombinedEntry.Relevancy"/>, matching LINQ
        /// <c>OrderByDescending(r=>r.relevancy)</c> (Comparer&lt;double&gt;.Default, stable). The carried
        /// <see cref="CmbCombinedEntry.Idx"/> is the source-enumeration order used as the tiebreak.
        /// </summary>
        private static void CmbStableSortByRelevancyDesc(List<CmbCombinedEntry> items)
        {
            int n = items.Count;
            if (n < 2)
                return;
            CmbCombinedEntry[] tmp = ArrayPool<CmbCombinedEntry>.Shared.Rent(n);
            try
            {
                for (int i = 0; i < n; i++)
                    tmp[i] = items[i];
                Array.Sort(tmp, 0, n, CmbRelevancyDescComparer.Instance);
                for (int i = 0; i < n; i++)
                    items[i] = tmp[i];
            }
            finally
            {
                // R8 WS-CHURN-B: CmbCombinedEntry is all-blittable (ReferencePrice = primitives, double, int — no
                // managed refs), so the pool buffer holds nothing for the GC to retain. Skip the clear entirely and
                // Return with clearArray:false — the old clearArray:true was pure wasted ZeroMemory of the (oversized)
                // capacity. (The CmbRelevant sort below DOES carry refs, so it clears its written prefix.)
                ArrayPool<CmbCombinedEntry>.Shared.Return(tmp, clearArray: false);
            }
        }

        private sealed class CmbRelevancyDescComparer : IComparer<CmbCombinedEntry>
        {
            public static readonly CmbRelevancyDescComparer Instance = new();
            public int Compare(CmbCombinedEntry a, CmbCombinedEntry b)
            {
                int c = Comparer<double>.Default.Compare(a.Relevancy, b.Relevancy);
                if (c != 0)
                    return -c; // descending
                return a.Idx.CompareTo(b.Idx); // stable: source-enumeration order
            }
        }

        /// <summary>
        /// Bit-exact replacement for <c>combined.OrderByDescending(c=>c.Day).Skip(1).FirstOrDefault().Day</c>: the Day of
        /// the element at stable-descending-by-Day position 1, or 0 if fewer than 2 elements. Stable: among equal Days the
        /// original (relevancy) order is preserved, so position 1 = the second element to reach the running max Day.
        /// </summary>
        private static short CmbSecondHighestDay(List<ReferencePrice> combined)
        {
            int n = combined.Count;
            if (n < 2)
                return 0;
            // Find rank-0 (stable first-of-max) then rank-1 (stable first-of-max among the rest, excluding the rank-0
            // POSITION). Stable OrderByDescending ⇒ for equal Days the earlier list index ranks first.
            int first = 0;
            for (int i = 1; i < n; i++)
                if (combined[i].Day > combined[first].Day) // strict: ties keep the earlier index (stable)
                    first = i;
            int second = -1;
            for (int i = 0; i < n; i++)
            {
                if (i == first)
                    continue;
                if (second == -1 || combined[i].Day > combined[second].Day)
                    second = i;
            }
            return combined[second].Day;
        }

        private void CheckLowerKeyFull(SaveAuction auction, PriceLookup lookup, double lbinPrice, double medPrice, KeyWithValueBreakdown fullKey, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, ref FullKeyQuery fullKeyQuery)
        {
            // check if complicated item
            if (fullKey.ValueBreakdown.Count < 3)
                return; // not complicated
            if (auction.Tag.StartsWith("PET_"))
                return; // eg Enderman gets cheaper at mythic for some reason
            // R7 WS-C2: full key + query DomKey built once per auction, shared with CheckCombined (same auction → same
            // GetFullKey/BuildDomKey bytes). Materialised here, after the two early-return gates above.
            fullKeyQuery.Ensure(this, auction);
            AuctionKeyWithValue key = fullKeyQuery.Key;
            var today = GetDay();
            // R4 WS-SHARE: single-pass max over the shared DominatorIndex. Direction B: candidates DOMINATED BY the full
            // query key (Dominates(cand, query) == IsHigherValue(tag, cand, key)). Equivalent to Where(...)
            // .OrderByDescending(Price).FirstOrDefault() — strict '>' first-wins over the dict-enumeration order
            // reproduces the stable First-of-max. Same AND-predicate set (Price>0, RefCount>5, reforge-or-Any, recent,
            // dominance); ordering of the ANDed conditions doesn't change the qualifying set. Price / RefCount /
            // recency read LIVE off the bucket; reforge is the immutable cached column; the kernel runs only on
            // mask-prefilter survivors (cheap filters first, as before).
            var index = GetOrBuildDominatorIndex(lookup);
            AuctionKey queryKey = key;
            var query = fullKeyQuery.Dom; // R7 WS-C2: shared query DomKey (== BuildDomKey(queryKey, scoreInterner))
            ulong qProv = query.ProvidedMask;
            bool petSpirit = auction.Tag == "PET_SPIRIT";
            int keyReforge = (int)key.Reforge;
            int anyReforge = (int)ItemReferences.Reforge.Any;
            KeyValuePair<AuctionKey, ReferenceAuctions> containing = default;
            long bestContainingPrice = long.MinValue;
            for (int i = 0; i < index.Count; i++)
            {
                if (!(index.Reforge[i] == keyReforge || index.Reforge[i] == anyReforge))
                    continue;
                var v = index.Buckets[i];
                if (!(v.Price > 0) || v.References.Count <= 5) // LIVE
                    continue;
                bool recent = false;
                foreach (var r in v.ReferenceSnapshot()) // LIVE
                    if (r.Day >= today - 2) { recent = true; break; }
                if (!recent)
                    continue;
                if ((index.RequiredMask[i] & qProv) != index.RequiredMask[i])
                    continue; // sound presence prefilter (candidate is the base side)
                if (!DominatorIndex.Dominates(in index.Doms[i], in query, petSpirit))
                    continue;
                if (v.Price > bestContainingPrice)
                {
                    bestContainingPrice = v.Price;
                    containing = new KeyValuePair<AuctionKey, ReferenceAuctions>(index.Keys[i], v);
                }
            }
            if (VerifyDominatorIndex)
                AssertDominatorParity(l, queryKey, auction.Tag, index, baseIsQuery: false);
            if (containing.Value == default)
                return;
            var extraValue = GetExtraValue(auction, key);
            FindFlip(auction, lbinPrice, medPrice, containing.Value, key, lookup, fullKey, extraValue, props =>
            {
                props.Add("fullKey", key.ToString());
                props.Add("usedKey", containing.Key.ToString());
                props.Add("by", "lowerfullkey");
            });
        }

        internal AuctionKeyWithValue GetFullKey(SaveAuction auction)
        {
            (var enchant, var modifiers) = SelectValuable(auction);
            var key = new AuctionKeyWithValue()
            {
                Count = 1,
                Enchants = new(enchant ?? new()),
                Modifiers = new(modifiers ?? new()),
                Tier = auction.Tier,
                Reforge = auction.Reforge
            };
            return key;
        }

        public static readonly HashSet<string> HyperionGroup = new() { "SCYLLA", "VALKYRIE", "NECRON_BLADE", "ASTRAEA" };
        public static readonly HashSet<string> WinterFragmentGroup = new() { "STARRED_GLACIAL_SCYTHE", "STARRED_ICE_SPRAY_WAND", "STARRED_YETI_SWORD" };
        private static readonly HashSet<string> MidasTags = new HashSet<string>
        {
            "MIDAS_STAFF",
            "MIDAS_SWORD",
            "STARRED_MIDAS_STAFF",
            "STARRED_MIDAS_SWORD"
        };
        private static readonly HashSet<string> CombinableStarred = new();
        private static readonly HashSet<string> RuneLookup = new();
        /// <summary>
        /// Remaps item tags into one item if they are easily switchable
        /// </summary>
        /// <param name="auction"></param>
        /// <returns></returns>
        public (string tag, long costSubstract) GetAuctionGroupTag(string itemGroupTag)
        {
            if (HyperionGroup.Contains(itemGroupTag))
                return ("HYPERION", GetPriceForItem("GIANT_FRAGMENT_LASER") * 8); // easily craftable from one into the other
            if (CombinableStarred.Contains(itemGroupTag))
            {
                // technically neds 8 for crafting but looses the value on craft so using 7
                var isFrozen = WinterFragmentGroup.Contains(itemGroupTag);
                var cost = GetPriceForItem(isFrozen ? "WINTER_FRAGMENT" : "LIVID_FRAGMENT") * 7;
                return (itemGroupTag[8..], cost);
            }
            return (itemGroupTag, 0);
        }

        private static bool ShouldIgnoreMostSimilar(SaveAuction auction)
        {
            return IsRune(auction.Tag);
        }

        private void TryFindClosestRisky(SaveAuction auction, PriceLookup l, KeyWithValueBreakdown keyWithBreakdown, ref double medPrice)
        {
            if (IsRune(auction.Tag)) // TODO: compare levels
                return;
            if (auction.Tag == "NEW_YEAR_CAKE")
                return; // can't use closest for years
            if (auction.FlatenedNBT.TryGetValue("exp", out var exp) && double.Parse(exp) < 1234567)
                return; // don't use closest for low exp pets

            // special case for items that have no reference bucket, search using most similar.
            // R11: reuse the basekey already parsed by TestNewAuctionInternal (passed in as keyWithBreakdown) instead of
            // re-parsing the SAME auction here. TryFindClosestRisky is only reached on the non-fastMode/triggerEvents
            // path, where basekey == DetailedKeyFromSaveAuction(auction) (limit=5, same epoch within the call) — so this
            // is byte-identical, and it removes the single biggest intra-auction redundant parse (it ran on ~84% of warm
            // auctions, on the awaited background closest task). basekey is immutable, safe to read from this Task thread.
            var detailedKey = keyWithBreakdown;
            var key = detailedKey.GetReduced(0);
            var closest = FindClosestTo(l, key, auction.Tag);
            if (NBT.IsPet(auction.Tag) && closest.Key?.Modifiers != null && !closest.Key.Modifiers.Any(m => m.Key == "exp"))
            {
                logger.LogWarning($"Pet without exp {auction.Uuid}");
                return; // don't use closest for pets without exp
            }
            medPrice *= 1.10; // increase price a bit to account for the fact that we are not using the exact same item
            if (closest.Value == null)
            {
                Logs.Enqueue(new LogEntry()
                {
                    Key = key,
                    LBin = -1,
                    Median = -1,
                    Uuid = auction.Uuid,
                    Volume = -1
                });
                return;
            }
            if (auction.FlatenedNBT.Any(f => f.Value == "PET_ITEM_TIER_BOOST") && !closest.Key.Modifiers.Any(m => m.Key == PetItemKey))
                return; // probably overvalues tier boost on enderdragon
            if (closest.Key == key)
                return; // already found - or rather not - by median
            if (closest.Value.Price <= medPrice)
                return;
            var props = new Dictionary<string, string>() { { "closest", closest.Key.ToString() } };
            var missingModifiers = closest.Key.Modifiers.Where(m => !key.Modifiers.Contains(m)).ToList();
            long toSubstract = 0;
            if (closest.Value.StonksHits > 0)
            {
                if (closest.Value.StonksHits > 10)
                    return;
                // risk increases with more hits
                toSubstract += (long)(closest.Value.Price * 0.1 * Math.Pow(1.05, closest.Value.StonksHits));
            }
            if (key.Modifiers.Any(m => m.Value == TierBoostShorthand) && !closest.Key.Modifiers.Any(m => m.Value == TierBoostShorthand))
            {
                toSubstract += GetCostForItem("PET_ITEM_TIER_BOOST");
            }
            if (missingModifiers.Count > 0)
            {
                toSubstract = GetPriceSumForModifiers(missingModifiers, key.Modifiers, auction);
                toSubstract += AdjustForAttributes(closest.Value.Price, key, missingModifiers, auction);
                if (toSubstract < 0)
                {
                    deferred.Log($"Negative value to substract for {string.Join(",", missingModifiers.Select(m => $"{m.Key}:{m.Value}"))} {auction.Uuid}");
                    toSubstract = Math.Abs(toSubstract);
                }
                var fromExp = GetValueDifferenceForExp(auction, closest.Key, l.Lookup);
                if (fromExp != 0)
                {
                    props.Add("fromExp", fromExp.ToString());
                }
                toSubstract -= fromExp;
                if (missingModifiers.Any(m => m.Key == "candyUsed" && m.Value == "1"))
                    toSubstract += (long)(closest.Value.Price * 0.1 + 400_000); // 10% for pet candy
                var killModifier = missingModifiers.FirstOrDefault(m => KillKeys.Contains(m.Key));
                if (killModifier.Key != default)
                {
                    var killCount = int.Parse(killModifier.Value);
                    var present = key.Modifiers.FirstOrDefault(n => n.Key == killModifier.Key);
                    var difference = killCount - int.Parse(present.Value ?? "0");
                    var killPrice = difference * 1_000_000;
                    if (difference < 0)
                        killPrice /= 2; // only half for adding kills
                    toSubstract += killPrice;
                }
                var formatted = string.Join(",", missingModifiers.Select(m => $"{m.Key}:{m.Value}"));
                if (toSubstract == 0)
                {
                    deferred.Log($"Could not find value to substract for {formatted} {auction.Uuid}");
                }
                props.Add("missingModifiers", formatted + $" ({toSubstract})");
            }
            var missingEnchants = closest.Key.Enchants.Where(m => !key.Enchants.Contains(m)).ToList();
            if (missingEnchants.Count > 0)
            {
                var enchVal = GetPriceSumForEnchants(missingEnchants, auction.Tag);
                toSubstract += enchVal;
                props.Add("missingEnchants", string.Join(",", missingEnchants.Select(e => $"{e.Type}_{e.Lvl}")) + $" ({enchVal})");
            }
            var additionalEnchants = key.Enchants.Where(e => !closest.Key.Enchants.Contains(e)).ToList();
            if (additionalEnchants.Count > 0)
            {
                var valEst = GetPriceSumForEnchants(additionalEnchants, auction.Tag) / 2;
                var enchantVal = Math.Min(valEst, closest.Value.Price / 4);
                toSubstract -= enchantVal;
                props.Add("enchValueAdded", string.Join(",", additionalEnchants.Select(e => $"{e.Type}_{e.Lvl}")) + $" ({enchantVal}-{valEst})");
            }
            var targetPrice = (long)((closest.Value.Price - toSubstract) * 0.9);
            // adjust due to count
            if (closest.Key.Count != auction.Count)
            {
                var countDiff = closest.Key.Count - auction.Count;
                var countDiffPrice = countDiff * targetPrice / closest.Key.Count;
                if (countDiff < 0)
                    countDiffPrice /= 2; // only half for adding items
                targetPrice -= countDiffPrice;
                props.Add("countDiff", $"{countDiff} ({countDiffPrice})");
            }
            // adjust price of reforge 
            if (closest.Key.Reforge != auction.Reforge)
            {
                var closestDetails = mapper.GetReforgeCost(closest.Key.Reforge, auction.Tier);
                var auctionDetails = mapper.GetReforgeCost(auction.Reforge, auction.Tier);
                var closestItemCost = GetCostForItem(closestDetails.Item1);
                if (closestItemCost == 0 && !string.IsNullOrEmpty(closestDetails.Item1))
                {
                    closestItemCost = 2_000_000; // estimated cost for missing items
                }
                var reforgeDifference = closestItemCost + closestDetails.Item2 - (GetCostForItem(auctionDetails.Item1) - auctionDetails.Item2) / 2;
                targetPrice -= reforgeDifference;
                props.Add("reforge", $"{closest.Key.Reforge} -> {auction.Reforge} ({reforgeDifference})");
            }
            if (closest.Key.Tier > auction.Tier && auction.FlatenedNBT.Any(f => f.Key == "exp"))
            {
                var stepsDiff = closest.Key.Tier - auction.Tier;
                var tierDifference = (long)(closest.Value.Price * 0.9 - closest.Value.Price / Math.Pow(5, Math.Abs(stepsDiff)));
                targetPrice -= tierDifference;
                props.Add("tierVal", $"{closest.Key.Tier} -> {auction.Tier} ({tierDifference})");
            }
            props.Add("breakdown", JsonConvert.SerializeObject(detailedKey.ValueBreakdown));
            var cleanPrice = GetCleanItemPrice(auction.Tag, detailedKey, l);
            props.Add("cleanPrice", cleanPrice.ToString());
            var modifierValue = (detailedKey.ValueBreakdown.Where(v => v?.Modifier.Key != "candyUsed").Sum(v => v.Value) + cleanPrice) * 1.1;
            targetPrice = Math.Min(targetPrice, (long)modifierValue);
            closest.Value.StonksHits++;
            FoundAFlip(auction, closest.Value, LowPricedAuction.FinderType.STONKS, targetPrice, props);
        }

        private long AdjustForAttributes(double medPrice, AuctionKey key, List<KeyValuePair<string, string>> missingModifiers, SaveAuction auction)
        {
            var missingAttributes = missingModifiers.Where(m => Constants.AttributeKeys.Contains(m.Key)).ToList();
            if (missingAttributes.Count > 0)
            {
                var biggestDifference = missingAttributes.Select(m => int.Parse(m.Value) - int.Parse(key.Modifiers.Where(km => km.Key == m.Key)?.FirstOrDefault().Value ?? "0")).Max();
                if (biggestDifference < 0)
                // conservatively adjust upwards
                {
                    var baseFactor = 1.5;
                    if (missingModifiers.Count > 1)
                        baseFactor = 1.35;
                    var factor = Math.Pow(baseFactor, Math.Abs(biggestDifference)) - 1;
                    return -(long)(medPrice * factor);
                }
                var keyhasCombo = missingModifiers.Any(m => HasAttributeCombo(m, auction.FlatenedNBT, auction.Tag));
                var defaultDifference = (medPrice - Math.Pow(0.4, biggestDifference) * medPrice);
                if (keyhasCombo)
                {
                    defaultDifference *= 1.6;
                }
                return (long)defaultDifference;

            }

            return 0;
        }

        private long GetCostForItem(string tag)
        {
            if (Lookups.TryGetValue(tag.ToUpper(), out var itemLookup))
            {
                var prices = itemLookup.Lookup.Values.First();
                return prices.Price;
            }
            return 0;
        }

        private long GetPriceSumForEnchants(IEnumerable<Models.Enchant> missingEnchants, string itemTag)
        {
            long toSubstract = 0;
            foreach (var item in missingEnchants)
            {
                toSubstract += mapper.EnchantValue(new Core.Enchantment(item.Type, item.Lvl), null, BazaarPrices, itemTag);
            }
            return toSubstract;
        }

        private long GetExtraValue(SaveAuction auction, AuctionKey key)
        {
            long extraValue = 0;
            var flatNbt = auction.FlatenedNBT;

            foreach (var item in RemovableItems)
            {
                if (TryGetItemKeyValue(flatNbt, item, out var value))
                {
                    var itemTag = value.ToUpperInvariant();
                    long itemPrice = 0;

                    if (item.IsPetItem)
                    {
                        var isTierBoost = itemTag == "PET_ITEM_TIER_BOOST";
                        if (BazaarPrices.TryGetValue(itemTag, out var bazaarPrice))
                        {
                            itemPrice = (long)bazaarPrice;
                            if (!isTierBoost)
                                itemPrice -= itemService.GetPetItemRemovalCost(itemTag);
                        }
                        else if (TryGetReferencePrice(itemTag, out var price))
                        {
                            itemPrice = price;
                            if (!isTierBoost)
                                itemPrice -= itemService.GetPetItemRemovalCost(itemTag);
                        }
                    }
                    else if (TryGetReferencePrice(itemTag, out var price))
                    {
                        itemPrice = price * 97 / 100 - DrillPartRemovalCost;
                    }

                    if (itemPrice > 0)
                        extraValue += itemPrice;
                }
            }

            return extraValue + GetGemValue(auction, key);
        }

        /// <summary>
        /// Gets the full bazaar price of removable items for storing references.
        /// Unlike GetExtraValue which returns (price - removal cost) for retrieval,
        /// this returns the full bazaar price for subtracting when storing.
        /// </summary>
        private long GetFullRemovableValue(SaveAuction auction, AuctionKey key)
        {
            long totalValue = 0;
            var flatNbt = auction.FlatenedNBT;

            foreach (var item in RemovableItems)
            {
                if (TryGetItemKeyValue(flatNbt, item, out var value))
                {
                    var itemTag = value.ToUpperInvariant();

                    if (item.IsPetItem && BazaarPrices.TryGetValue(itemTag, out var bazaarPrice))
                    {
                        totalValue += (long)bazaarPrice;
                        continue;
                    }

                    if (TryGetReferencePrice(itemTag, out var price))
                        totalValue += price;
                }
            }

            return totalValue + GetGemValue(auction, key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryGetItemKeyValue(Dictionary<string, string> flatNbt, in RemovableItemKey item, out string value)
        {
            value = null;
            if (flatNbt == null)
                return false;
            if (flatNbt.TryGetValue(item.Key, out value))
                return true;
            return item.AliasKey != null && flatNbt.TryGetValue(item.AliasKey, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ContainsModifierKey(IReadOnlyList<KeyValuePair<string, string>> modifiers, string key)
        {
            if (modifiers == null)
                return false;

            for (var index = 0; index < modifiers.Count; index++)
            {
                if (modifiers[index].Key == key)
                    return true;
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryGetReferencePrice(string itemTag, out long price)
        {
            price = 0;
            if (!Lookups.TryGetValue(itemTag, out var itemLookup))
                return false;

            if (!TryGetLookupReferencePrices(itemLookup, out var prices))
                return false;

            price = GetReferencePrice(prices);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long GetReferencePrice(ReferenceAuctions prices)
        {
            var lbinPrice = prices.Lbin.Price;
            return lbinPrice == 0 ? prices.Price : Math.Min(prices.Price, lbinPrice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryGetLookupReferencePrices(PriceLookup itemLookup, out ReferenceAuctions prices)
        {
            prices = null;
            if (itemLookup?.Lookup == null || itemLookup.Lookup.Count == 0)
                return false;

            var lookup = itemLookup.Lookup;
            if (itemLookup.CleanKey is not null && lookup.TryGetValue(itemLookup.CleanKey, out prices) && prices is not null)
                return true;

            foreach (var entry in lookup)
            {
                var candidate = entry.Value;
                if (entry.Key is null || candidate is null)
                    continue;

                if (candidate.Price > 0 || candidate.Lbin.Price > 0)
                {
                    prices = candidate;
                    return true;
                }
            }

            return false;
        }

        public long GetGemValue(SaveAuction auction, AuctionKey key)
        {
            var gemValue = 0L;
            var flatNbt = auction.FlatenedNBT;
            if (flatNbt == null || flatNbt.Count == 0)
                return 0;

            var modifiers = key.Modifiers;
            foreach (var item in flatNbt)
            {
                if (item.Value != "PERFECT" && item.Value != "FLAWLESS")
                    continue;

                if (ContainsModifierKey(modifiers, item.Key))
                    continue;

                var gemkey = mapper.GetItemKeyForGem(item, flatNbt);
                if (!BazaarPrices.TryGetValue(gemkey, out var gemLookup))
                    continue;

                gemValue += (long)gemLookup - (item.Value == "PERFECT" ? 500_000 : 100_000);
            }

            return gemValue;
        }

        private bool FindFlip(SaveAuction auction,
                              double lbinPrice,
                              double minMedPrice,
                              ReferenceAuctions bucket,
                              AuctionKeyWithValue key,
                              PriceLookup lookup,
                              KeyWithValueBreakdown breakdown,
                              long extraValue = 0,
                              Action<Dictionary<string, string>> addProps = null,
                              bool fastMode = false)
        {
            var l = lookup.Lookup;
            var expValue = GetValueDifferenceForExp(auction, key, l);
            var volume = bucket.Volume;
            var medianPrice = bucket.Price + extraValue;
            var groupTag = GetAuctionGroupTag(auction.Tag);
            var foundSnipe = false;
            if ((bucket.Lbin.Price > lbinPrice || bucket.Price == 0) && (MaxMedianPriceForSnipe(bucket, breakdown) > lbinPrice)
                && (!fastMode || bucket.Volume > 5)
               )
            {
                foundSnipe = PotentialSnipe(auction, groupTag, lbinPrice, bucket, key, lookup, extraValue, breakdown);
            }
            if (medianPrice > minMedPrice && BucketHasEnoughReferencesForPrice(bucket, lookup))
            {
                long adjustedMedianPrice = bucket.Price;
                if (key.Count > 1)
                    adjustedMedianPrice = CheckHigherValueKeyForLowerPrice(bucket, key, l, medianPrice);
                if (Activity.Current is { IsAllDataRequested: true })
                    Activity.Current.Log($"Bucket has enough references {bucket.References.Count} and medianPrice > minMedPrice {medianPrice} > {minMedPrice} adjusted {adjustedMedianPrice} {extraValue} {expValue}");
                if (adjustedMedianPrice + extraValue < minMedPrice)
                {
                    if (WouldLogNonFlip(volume, bucket))
                        LogNonFlip(auction, bucket, key, extraValue, volume, medianPrice, $"Adjusted median {adjustedMedianPrice} lower than min price {minMedPrice} {extraValue}");
                    return false;
                }
                var referenceAuctionId = bucket.References.LastOrDefault().AuctionId;
                var props = CreateReference(referenceAuctionId, key, extraValue, bucket);
                if (key.ValueSubstract != 0)
                {
                    props["valuedropped"] = key.ValueSubstract.ToString();
                }
                if (expValue != 0)
                {
                    props["expvalue"] = expValue.ToString();
                }
                addProps?.Invoke(props);
                if (lookup.Volume < 1 && lookup.Volume > 0 && bucket.Price > 100_000_000 && bucket.Lbin.Price > 0 && bucket.Price > bucket.Lbin.Price)
                {
                    props.Add("lbin", JsonConvert.SerializeObject(bucket.Lbin));
                    adjustedMedianPrice = Math.Min(adjustedMedianPrice, bucket.Lbin.Price);
                }
                var keyMissing = key.ValueSubstract > 5_000_000 ? (key.ValueSubstract - extraValue - 1_000_000) / 2 + MoreIfExpensive(bucket.Price, key) : 0;
                if (keyMissing > 0 && lookup.CleanPricePerTier.TryGetValue(key.Tier, out var cleanPricePerTier))
                {
                    var basePrice = Math.Min(auction.StartingBid, cleanPricePerTier * 2);
                    // on very cheap items expensive modifiers don't add much value
                    var adjusted = Math.Min(keyMissing, Math.Max(basePrice * 3 - 100_000, basePrice));
                    if (adjusted > 0)
                        keyMissing = adjusted;
                }
                props.Add("keyMissing", keyMissing.ToString());
                FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER_MEDIAN, adjustedMedianPrice + extraValue + expValue + keyMissing, props, key, extraValue);
            }
            if (medianPrice - auction.StartingBid < 2_500_000 && bucket.RiskyEstimate > minMedPrice
                && (bucket.Lbin.AuctionId == default || bucket.Lbin.Price * 1.04 > lbinPrice))
            {
                var referenceAuctionId = bucket.References.LastOrDefault().AuctionId;
                var props = CreateReference(referenceAuctionId, key, extraValue, bucket);
                addProps?.Invoke(props);
                props.Add("riskyEst", bucket.RiskyEstimate.ToString());
                var target = bucket.RiskyEstimate + extraValue + expValue;
                if (bucket.Lbin.Price != 0)
                    target = (long)Math.Min(target, bucket.Lbin.Price * 1.05);
                if (bucket.Price != 0)
                    target = (long)Math.Min(target, bucket.Price * 1.10 + 1_000_000);
                if ((GetDay() - bucket.OldestRef) > 10 || bucket.DeduplicatedReferenceCount < 8)
                    target = target * 11 / 12;
                FoundAFlip(auction, bucket, LowPricedAuction.FinderType.STONKS, target, props, key, extraValue);
            }
            else
            {
                if (Activity.Current is { IsAllDataRequested: true })
                    Activity.Current.Log($"Bucket has too few references {bucket.References.Count} or medianPrice > minMedPrice {medianPrice} > {minMedPrice}");
                if (WouldLogNonFlip(volume, bucket))
                    LogNonFlip(auction, bucket, key, extraValue, volume, medianPrice, $"Median {medianPrice} lower than min price {minMedPrice} {bucket.References.Count}");
            }
            return foundSnipe;

            long MoreIfExpensive(long Median, AuctionKeyWithValue key)
            {
                // attributes keep more of their value on expensive items
                if (Median < 500_000_000)
                    return 0;
                return (key.ValueSubstract - extraValue - 2_000_000) / 4;
            }
        }
        // The condition under which LogNonFlip actually enqueues a diagnostic entry. Checked at the call site too so the
        // interpolated message string isn't built for the (common, in prod) sub-MIN_TARGET buckets it would discard.
        private static bool WouldLogNonFlip(float volume, ReferenceAuctions bucket)
            => volume == 0 || bucket.Lbin.Price == 0 || bucket.Price == 0 || bucket.Price > MIN_TARGET;

        void LogNonFlip(SaveAuction auction, ReferenceAuctions bucket, AuctionKey key, long extraValue, float volume, long medianPrice, string v = null)
        {
            if (WouldLogNonFlip(volume, bucket))
                Logs.Enqueue(new LogEntry()
                {
                    Key = key,
                    ExtraContext = $"+{extraValue} {v}",
                    LBin = bucket.Lbin.Price,
                    Median = medianPrice,
                    Uuid = auction.Uuid,
                    Volume = bucket.Volume
                });
            if (Logs.Count > 2000)
                PrintLogQueue();
        }

        private static long GetValueDifferenceForExp(SaveAuction auction, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l)
        {
            // determine extra expvalue
            if (auction.FlatenedNBT.TryGetValue("exp", out var expString))
            {
                var maxExp = HighExp(auction.Tag) ? ("7", GoldenDragonMaxExp) : ("6", PetExpMaxlevel);
                var exp = Math.Min((long)double.Parse(expString), maxExp.Item2);
                var lvl1Key = new AuctionKey(new(), ItemReferences.Reforge.Any, EmptyPetModifiers.ToList(), Tier.LEGENDARY, 1);
                var lvl100Key = new AuctionKey(new(), ItemReferences.Reforge.Any, new List<KeyValuePair<string, string>>() { new("exp", maxExp.Item1) }, Tier.LEGENDARY, 1);
                if (l.TryGetValue(lvl1Key, out var lvl1Bucket) && l.TryGetValue(lvl100Key, out var lvl100Bucket))
                {
                    var lvl1Price = lvl1Bucket.Price;
                    var lvl100Price = lvl100Bucket.Price;
                    var accountedFor = double.Parse(key.Modifiers.Where(m => m.Key == "exp").Select(v => v.Value).FirstOrDefault("0"));
                    if (auction.Tier == Tier.EPIC)
                        accountedFor += 1;
                    var accountedMiddle = accountedFor + Math.Min(0.5, accountedFor / 2);
                    var accountedExp = maxExp.Item2 / 7 * accountedMiddle;
                    var perExp = (double)((lvl100Price - lvl1Price) / (double)(maxExp.Item2 - 1));
                    var expValue = (long)(perExp * (exp - 1 - accountedExp));
                    if (exp > 11_600_000 && expValue > 0) // only block upwards price changes
                        return 0; // bad effect with so many exp
                    if (exp > 4_000_000 && expValue > 0)
                        return expValue / 2; // graceful reduce
                    if (expValue < 0 && accountedFor <= 1)
                    {
                        var matchingKey = new AuctionKey(new(), ItemReferences.Reforge.Any, EmptyPetModifiers.ToList(), auction.Tier, 1);
                        var matchingTier = l.GetValueOrDefault(matchingKey)?.Price ?? 0;
                        return Math.Max(expValue, -matchingTier / 4); // reduce target by max 25% the starting bid
                    }
                    return expValue;
                }

            }

            return 0;
        }

        /// <summary>
        /// Checks higher value keys for a lower median price
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="l"></param>
        /// <param name="medianPrice"></param>
        /// <returns></returns>
        private long CheckHigherValueKeyForLowerPrice(ReferenceAuctions bucket, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, long medianPrice)
        {
            if (key.Count > 1)
            {
                var lowerCountKey = new AuctionKey(key)
                {
                    Count = 1
                };
                if (l.TryGetValue(lowerCountKey, out ReferenceAuctions lowerCountBucket))
                {
                    if (lowerCountBucket.Price != 0)
                        return Math.Min(bucket.Price, lowerCountBucket.Price * key.Count);
                }
            }
            return bucket.Price;
        }

        private static bool BucketHasEnoughReferencesForPrice(ReferenceAuctions bucket, PriceLookup lookup)
        {
            // high value items need more volume to pop up
            return bucket.Price < 280_000_000 || bucket.References.Count > 5 || bucket.Volume > lookup.Volume / 3;
        }

        public void UpdateBazaar(dev.BazaarPull bazaar)
        {
            var today = GetDay(bazaar.Timestamp);
            foreach (var item in bazaar.Products.OrderBy(p => p.Id))
            {
                if (item.SellSummary.Count() == 0 && item.BuySummery.Count() == 0)
                    continue;
                if (!Lookups.TryGetValue(item.ProductId, out var lookup))
                {
                    lookup = new();
                    Lookups[item.ProductId] = lookup;
                    //logger.LogInformation($"Added {item.ProductId} to lookup");
                }
                var bucket = lookup.Lookup.GetOrAdd(defaultKey, _ => new());
                var itemPrice = 0D;
                if (item.SellSummary.Any() && item.BuySummery?.Count > 0 && item.QuickStatus?.BuyOrders >= 20)
                {
                    // some items have undervalued buy orders so use the center of the spread as estimate
                    // only for items with at least 20 buy orders to avoid manipulation
                    var sellPrice = item.SellSummary.First().PricePerUnit;
                    var buyPrice = item.BuySummery.OrderBy(s => s.PricePerUnit).First().PricePerUnit;
                    itemPrice = (long)(sellPrice + buyPrice) / 2;

                    if (item.ProductId.StartsWith("ENCHANTMENT"))
                    {
                        itemPrice = MakePriceAtMost40PercentLowerthanLowerLevel(item.ProductId, (long)itemPrice);
                    }
                }
                else if (item.SellSummary.Any())
                {
                    itemPrice = (long)item.SellSummary.First().PricePerUnit;
                }
                else if (item.BuySummery.Any())
                {
                    itemPrice = (long)item.BuySummery.OrderBy(s => s.PricePerUnit).First().PricePerUnit;
                }
                if (!BazaarPrices.ContainsKey(item.ProductId))
                    BazaarPrices[item.ProductId] = itemPrice;
                if (bucket.References.Count >= 5 && NotEnoughTimePassed(bazaar, bucket))
                    continue; // only sample prices every 10 minutes
                bucket.EnqueueReference(new()
                {
                    Day = today,
                    Price = (long)itemPrice,
                    AuctionId = bazaar.Timestamp.Ticks,
                    Seller = (short)DateTime.Now.Ticks,
                    Buyer = (short)(DateTime.Now.Ticks + 1)
                });
                if (bucket.Price == 0)
                    bucket.Price = (long)itemPrice;
                UpdateMedian(bucket, default);
                CapBucketSize(bucket);

                // make sure higher enchants are higher value
                if (item.ProductId.StartsWith("ENCHANTMENT"))
                {
                    var cheapestBuy = item.BuySummery.OrderBy(s => s.PricePerUnit).FirstOrDefault()?.PricePerUnit;
                    MakePriceAtLeast90PercentHigherthanLowerLevel(item, bucket, cheapestBuy);
                }

                if (bucket.Price > 0)
                    BazaarPrices[item.ProductId] = bucket.Price;
            }
            // make sure higher enchants are higher value
            foreach (var item in Lookups)
            {
                var lookup = item.Value;
                var key = item.Key;
                var refernces = lookup.Lookup.GetOrAdd(defaultKey, _ => new());
                if (refernces.Price == 0)
                    continue;
                var higherEnchants = HigherValueKeys(defaultKey, lookup.Lookup, refernces.Price);
                foreach (var higherEnchant in higherEnchants)
                {
                    if (lookup.Lookup.TryGetValue(higherEnchant, out var higherEnchantReference))
                    {
                        if (higherEnchantReference.Price == 0)
                        {
                            higherEnchantReference.Price = refernces.Price + 1;
                            Interlocked.Increment(ref pricingEpoch); // WS-A: out-of-band price write
                        }
                    }
                }
            }
            logger.LogInformation($"Updated bazaar {Lookups.Count} items {bazaar.Timestamp}");

            void MakePriceAtLeast90PercentHigherthanLowerLevel(dev.ProductInfo item, ReferenceAuctions refernces, double? cheapestBuy)
            {
                var currentLevel = int.Parse(item.ProductId.Split("_").Last());
                if (currentLevel <= 1 || item.ProductId.Contains("_MANA_") && currentLevel <= 5)
                { // mana enchants can drop up to lvl 5 and is worth almost the same at all levels
                    return;
                }
                var lowerLevelId = item.ProductId.Replace($"_{currentLevel}", $"_{currentLevel - 1}");
                if (BazaarPrices.TryGetValue(lowerLevelId, out var lowerValue))
                {
                    refernces.Price = (long)Math.Min(Math.Max(refernces.Price, lowerValue * 1.9), cheapestBuy ?? long.MaxValue);
                    Interlocked.Increment(ref pricingEpoch); // WS-A: out-of-band price write
                }
            }
            long MakePriceAtMost40PercentLowerthanLowerLevel(string id, long estimate)
            {
                var currentLevel = int.Parse(id.Split("_").Last());
                if (currentLevel <= 1 || id.Contains("_MANA_") && currentLevel <= 5)
                { // mana enchants can drop up to lvl 5 and is worth almost the same at all levels
                    return estimate;
                }
                var higherLevelId = id.Replace($"_{currentLevel}", $"_{currentLevel + 1}");
                if (BazaarPrices.TryGetValue(higherLevelId, out var higherValue))
                {
                    return (long)Math.Min(estimate, higherValue * 0.6);
                }
                return estimate;
            }

            static bool NotEnoughTimePassed(dev.BazaarPull bazaar, ReferenceAuctions bucket)
            {
                var lastAdded = new DateTime(bucket.References.Last().AuctionId);
                return lastAdded.AddMinutes(10) > bazaar.Timestamp;
            }
        }

        private bool PotentialSnipe(SaveAuction auction, (string tag, long costSubstract) groupTag, double lbinPrice, ReferenceAuctions bucket, AuctionKey key, PriceLookup lookup, long extraValue, KeyWithValueBreakdown breakdown)
        {
            var l = lookup.Lookup;
            var lowestHigherBin = GetLbinCap(groupTag.tag, lookup, breakdown);
            var higherValueLowerBin = bucket.Lbin.Price;
            if (lowestHigherBin.AuctionId != default)
                if (lowestHigherBin.Price < lbinPrice)
                    return false;
                else if (higherValueLowerBin > lowestHigherBin.Price || higherValueLowerBin == 0)
                    higherValueLowerBin = lowestHigherBin.Price;

            if (IsStacksize1Cheaper(lbinPrice, key, l))
            {
                Activity.Current.Log("Stacksize 1 is cheaper");
                return false;
            }
            var targetPrice = Math.Min(higherValueLowerBin - 1, MaxMedianPriceForSnipe(bucket, breakdown)) + extraValue - MIN_TARGET / 200;
            if (bucket.Price != 0)
                if (bucket.Price > 500_000_000)
                    targetPrice = Math.Min(targetPrice, (long)(bucket.Price * 1.1));
                else
                    targetPrice = Math.Min(targetPrice, bucket.Price * 2);
            if (targetPrice < auction.StartingBid * 1.03)
                return false;
            var percentile = long.MaxValue;

            // R7 WS-C2: the props dict (CreateReference + the mVal/hvlbin/sellerMatch/noHigherLbin entries) is allocated
            // only AFTER the `percentile < lbinPrice` reject gate below — a candidate rejected there never pays for the
            // dict or its string conversions. The pre-gate value-affecting side effects (the sellerMatch `percentile /= 2`)
            // still run before the gate; only the props WRITES are deferred. The emitted dict is byte-identical: same keys
            // in the same insertion order, same values (snapshotted at the original write point). `props` is null until the
            // gate passes; nothing reads it before then.
            Dictionary<string, string> props = null;
            higherValueLowerBin = Math.Min(higherValueLowerBin, bucket.Price * 2);
            // snapshots of the deferred pre-gate prop values, captured at the exact point the original code wrote them
            string hvlbinProp = higherValueLowerBin.ToString();
            string sellerMatchProp = null;
            string noHigherLbinProp = null;

            if (bucket.Price == 0 || bucket.Volume < 10)
            {
                // check for 80th percentile from references
                var subsetSize = 20;
                var refCount = PotentialSnipeEightiethPercentile(bucket.ReferenceSnapshot(), subsetSize, out var eightieth);
                if (refCount >= 1)
                    percentile = eightieth;
                else if (refCount == 0)
                    // undercut only similar lbin drasticly
                    percentile = targetPrice * 9 / 11;

                // no references, check against all lbins
                var lowestLbin = lowestHigherBin.Price;
                if (lowestHigherBin.AuctionId == default)
                    lowestLbin = long.MaxValue;
                var referencePrice = bucket.Price;
                if (bucket.References.Count < 5)
                {
                    // all key modifiers and enchants need to be in the reference bucket or higher
                    // R4 WS-SHARE higher-value scan: flat scan of the shared DominatorIndex (direction A: candidates that
                    // dominate the query key) accumulating the lowest qualifying higher-value lbin and the divided
                    // references for the 25th percentile (bit-exact). Lbin/refs read LIVE off each bucket.
                    var domIndex = GetOrBuildDominatorIndex(lookup);
                    var higherValueKeyCount = PotentialSnipeHigherValueScan(
                        domIndex, groupTag.tag, key, bucket.Lbin.Price, out lowestLbin, out int allReferencesCount);
                    // 25th percentile of all references
                    referencePrice = PotentialSnipeQuarterPercentile(allReferencesCount, targetPrice / 2);

                    if (bucket.Price == 0 && bucket.References.Count > 2 && higherValueKeyCount <= 2) // manip indicator
                    {
                        percentile /= 5;
                    }
                    else if (bucket.References.Count < 4 && allReferencesCount < 5)
                    {
                        percentile = Math.Min(percentile, referencePrice / 2);
                        if (allReferencesCount == 0)
                            percentile /= 2;
                    }
                    else if (bucket.References.Count <= 3)
                        percentile = percentile / 3; // not enough statistical evidence
                }
                percentile = Math.Min(percentile, referencePrice);
                if (bucket.Price == 0 && bucket.Lbin.Seller == GetSellerId(auction))
                {
                    sellerMatchProp = percentile.ToString(); // deferred props["sellerMatch"], captured before the /= 2
                    // seller matching is sus
                    percentile /= 2;
                }
                percentile = Math.Min(percentile, lowestLbin);
                if (lowestLbin > 10_000_000_000)
                {
                    Activity.Current.Log($"Reduced because no higher value lbin");
                    percentile = Math.Min(percentile, Math.Min(targetPrice * (60 + (int)(bucket.Volume * 5)) / 100, (long)(referencePrice * 1.2)));
                    noHigherLbinProp = percentile.ToString(); // deferred props["noHigherLbin"]
                }
                if (percentile < lbinPrice)
                    return false; // to low already don't waste time
                // gate passed -> now allocate the props dict and replay the deferred writes in the ORIGINAL insertion order
                props = CreateReference(bucket.Lbin.AuctionId, key, extraValue, bucket);
                props["mVal"] = bucket.Price.ToString();
                props["hvlbin"] = hvlbinProp;
                if (sellerMatchProp != null)
                    props["sellerMatch"] = sellerMatchProp;
                if (noHigherLbinProp != null)
                    props["noHigherLbin"] = noHigherLbinProp;
                var reduced = CapAtCraftCost(groupTag.tag, percentile, breakdown, 0);
                if (reduced > 0)
                {
                    if (percentile != reduced)
                        if (lowestLbin > 10_000_000_000 && higherValueLowerBin > 10_000_000_000)
                            reduced = reduced * 80 / 100; // 80% for no higher lbin
                        else
                            reduced = reduced * 21 / 20; // 5% extra for snipe
                    percentile = Math.Min(reduced, percentile);
                    if (Activity.Current is { IsAllDataRequested: true })
                        Activity.Current.Log($"Reduced to craft cost {reduced}");
                    props["craftCost"] = reduced.ToString();
                }
                if (Activity.Current is { IsAllDataRequested: true })
                    Activity.Current.Log($"No references, checking all lbins {percentile} {lowestLbin} {referencePrice}");
                props["referencePrice"] = referencePrice.ToString();
                props["lowestLbin"] = lowestLbin.ToString();
            }
            else
            {
                // the else branch has no reject gate before FoundAFlip, so allocate + replay mVal/hvlbin here (same order)
                props = CreateReference(bucket.Lbin.AuctionId, key, extraValue, bucket);
                props["mVal"] = bucket.Price.ToString();
                props["hvlbin"] = hvlbinProp;
                CapHighValue(groupTag, bucket, key, breakdown, higherValueLowerBin, ref targetPrice, ref percentile, props);
            }
            props["percentile"] = percentile.ToString();
            targetPrice = Math.Min(targetPrice, percentile);
            return FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER, targetPrice, props, key, extraValue);
        }

        // ===== R2-B: de-LINQ'd PotentialSnipe percentile / higher-value-scan helpers =====
        // Thread-static scratch buffers so the hot snipe path reuses storage instead of allocating
        // Int64[]/ReferencePrice[]/Func/SelectIterator per call. PotentialSnipe runs serially per group-tag but across
        // worker threads (per-core dispatcher), so the buffers are per-thread to stay race-free.
        [ThreadStatic] private static (long price, short day, int idx)[] _potentialSnipeRefScratch;
        [ThreadStatic] private static long[] _potentialSnipePriceScratch;
        [ThreadStatic] private static long[] _potentialSnipeDividedScratch;

        private static T[] PotentialSnipeEnsure<T>(ref T[] buffer, int needed)
        {
            if (buffer == null)
                buffer = new T[Math.Max(needed, 16)];
            else if (buffer.Length < needed)
                Array.Resize(ref buffer, Math.Max(needed, buffer.Length * 2)); // preserves existing contents
            return buffer;
        }

        /// <summary>
        /// Bit-exact replacement for
        /// <c>References.OrderByDescending(r=>r.Day).Take(subsetSize).Select(r=>r.Price).OrderBy(p=>p).ElementAt(min(Count,subsetSize)*8/10)</c>.
        /// Returns the reference count (snapshot) and emits the 80th-percentile price in <paramref name="eightieth"/>.
        /// </summary>
        private static int PotentialSnipeEightiethPercentile(
            ReferencePrice[] references, int subsetSize, out long eightieth)
        {
            // Snapshot in enumeration order, tagging the original index so the day-descending sort is *stable*
            // (LINQ OrderByDescending is stable; Array.Sort is not — break ties by original index to match).
            var scratch = PotentialSnipeEnsure(ref _potentialSnipeRefScratch, 0);
            int count = 0;
            foreach (var r in references)
            {
                if (count >= scratch.Length)
                    scratch = PotentialSnipeEnsure(ref _potentialSnipeRefScratch, count + 1);
                scratch[count] = (r.Price, r.Day, count);
                count++;
            }
            if (count == 0)
            {
                eightieth = 0;
                return 0;
            }
            int take = Math.Min(count, subsetSize);
            // stable sort by Day descending: partial-select the top `take` by (day desc, idx asc).
            // count is bounded (<= references in a bucket); an insertion-style partial selection keeps it allocation-free.
            Array.Sort(scratch, 0, count, PotentialSnipeDayDescComparer.Instance);

            var prices = PotentialSnipeEnsure(ref _potentialSnipePriceScratch, take);
            for (int i = 0; i < take; i++)
                prices[i] = scratch[i].price;
            Array.Sort(prices, 0, take); // ascending, matches OrderBy(p=>p) for the indexed value
            eightieth = prices[take * 8 / 10];
            return count;
        }

        private sealed class PotentialSnipeDayDescComparer : IComparer<(long price, short day, int idx)>
        {
            public static readonly PotentialSnipeDayDescComparer Instance = new();
            public int Compare((long price, short day, int idx) a, (long price, short day, int idx) b)
            {
                if (a.day != b.day)
                    return b.day.CompareTo(a.day); // Day descending
                return a.idx.CompareTo(b.idx);     // stable: preserve original enumeration order
            }
        }

        /// <summary>
        /// Bit-exact single-pass replacement for the higher-value LINQ scan. Computes
        /// <c>lowestLbin = higherValueKeys.Where(Lbin.Price>0 &amp;&amp; Lbin.Price&lt;bucketLbinPrice).Select(Lbin.Price).DefaultIfEmpty(long.MaxValue).Min()</c>
        /// and fills the thread-static divided-price buffer with
        /// <c>higherValueKeys.SelectMany(x=>x.Value.References.Select(r=>r.Price/(x.Key.Count==0?1:x.Key.Count)))</c>.
        /// Returns the higher-value key count. Iteration order is irrelevant: lowestLbin is a Min and the divided
        /// prices are later sorted, so the emitted values are independent of dictionary enumeration order.
        /// </summary>
        private int PotentialSnipeHigherValueScan(
            DominatorIndex index, string tag, AuctionKey key, long bucketLbinPrice,
            out long lowestLbin, out int dividedCount)
        {
            long lowest = long.MaxValue;
            int keyCount = 0;
            int divided = 0;
            var buffer = PotentialSnipeEnsure(ref _potentialSnipeDividedScratch, 0);
            // Direction A: candidates that dominate the query `key` (Dominates(key, cand)), over the BROAD index (all
            // non-null buckets — incl Price==0/virtual, exactly the old `foreach (var x in l)`). keyCount/lowest are
            // order-independent; the divided buffer is sorted by PotentialSnipeQuarterPercentile so its order is moot.
            var query = DominatorIndex.BuildDomKey(key, scoreInterner);
            ulong qReq = query.RequiredMask;
            bool petSpirit = tag == "PET_SPIRIT";
            for (int i = 0; i < index.Count; i++)
            {
                if ((qReq & index.ProvidedMask[i]) != qReq)
                    continue; // sound presence prefilter
                if (!DominatorIndex.Dominates(in query, in index.Doms[i], petSpirit))
                    continue;
                keyCount++;
                var bucket = index.Buckets[i];
                var lbinPrice = bucket.Lbin.Price; // LIVE
                if (lbinPrice > 0 && lbinPrice < bucketLbinPrice && lbinPrice < lowest)
                    lowest = lbinPrice;
                int countDivisor = index.Keys[i].Count == 0 ? 1 : index.Keys[i].Count;
                var refs = bucket.ReferenceSnapshot(); // LIVE
                for (int r = 0; r < refs.Length; r++)
                {
                    if (divided >= buffer.Length)
                        buffer = PotentialSnipeEnsure(ref _potentialSnipeDividedScratch, divided + 1);
                    buffer[divided++] = refs[r].Price / countDivisor;
                }
            }
            lowestLbin = lowest; // DefaultIfEmpty(long.MaxValue).Min(): long.MaxValue when nothing qualified
            dividedCount = divided;
            return keyCount;
        }

        /// <summary>
        /// Bit-exact replacement for
        /// <c>allReferences.OrderBy(p=>p).Skip(allReferences.Count/4).DefaultIfEmpty(fallback).Min()</c> over the
        /// divided-price buffer filled by <see cref="PotentialSnipeHigherValueScan"/>: the value at sorted index
        /// <c>count/4</c>, or <paramref name="fallback"/> when empty.
        /// <para>
        /// WS-CHURN-A: bit-exact partial-selection replacement (in-place quickselect / nth_element instead of a full
        /// <see cref="Array.Sort"/>), gated against <see cref="PotentialSnipeQuarterPercentileReference"/>. The result
        /// is a scalar order statistic over a <c>long[]</c>, so the k-th smallest VALUE is uniquely defined regardless
        /// of duplicates; any correct selection returns the identical long. The buffer is refilled fresh every auction
        /// by <see cref="PotentialSnipeHigherValueScan"/> and is not read after this call, so leaving it only
        /// partitioned (not fully sorted) is safe.
        /// </para>
        /// </summary>
        private static long PotentialSnipeQuarterPercentile(int count, long fallback)
            => PotentialSnipeQuarterPercentile(_potentialSnipeDividedScratch, count, fallback);

        /// <summary>
        /// Core of <see cref="PotentialSnipeQuarterPercentile(int,long)"/> operating on an explicit buffer so it is
        /// directly unit-testable without the <c>[ThreadStatic]</c> field. Selects the (<paramref name="count"/>/4)-th
        /// smallest of <c>buffer[0, count)</c> via in-place quickselect; returns <paramref name="fallback"/> when empty.
        /// Mutates <c>buffer[0, count)</c> (partition only).
        /// </summary>
        internal static long PotentialSnipeQuarterPercentile(long[] buffer, int count, long fallback)
        {
            if (count == 0)
                return fallback;
            int k = count / 4; // OrderBy then Skip(count/4) then Min == element at index count/4
            QuickSelectLong(buffer, 0, count - 1, k);
            return buffer[k];
        }

        /// <summary>
        /// Reference oracle for <see cref="PotentialSnipeQuarterPercentile(long[],int,long)"/>: the ORIGINAL
        /// full-sort body verbatim. Kept as the bit-exactness gate (mirrors the
        /// <see cref="CleanItemPriceSelectTarget"/> / <see cref="CleanItemPriceSelectTargetReference"/> pattern).
        /// </summary>
        internal static long PotentialSnipeQuarterPercentileReference(long[] buffer, int count, long fallback)
        {
            if (count == 0)
                return fallback;
            Array.Sort(buffer, 0, count); // ascending
            return buffer[count / 4]; // OrderBy then Skip(count/4) then Min == element at index count/4
        }

        /// <summary>
        /// In-place quickselect (nth_element) on <c>a[lo..hi]</c> for the k-th smallest VALUE: on return,
        /// <c>a[k]</c> holds the element that a full ascending sort would place at index <paramref name="k"/>, with
        /// everything left of it &lt;= and everything right of it &gt;=. Median-of-three pivot guards against the
        /// O(n^2) behaviour on already-sorted / heavy-tie inputs that a full <see cref="Array.Sort"/> tolerated.
        /// </summary>
        private static void QuickSelectLong(long[] a, int lo, int hi, int k)
        {
            while (lo < hi)
            {
                int p = QuickSelectLongPartition(a, lo, hi);
                if (p == k) return;
                if (p < k) lo = p + 1; else hi = p - 1;
            }
        }

        private static int QuickSelectLongPartition(long[] a, int lo, int hi)
        {
            // median-of-three pivot selection, parked at hi-1 (Lomuto over [lo, hi-1) with a[hi] already >= pivot).
            int mid = lo + ((hi - lo) >> 1);
            if (a[mid] < a[lo]) (a[lo], a[mid]) = (a[mid], a[lo]);
            if (a[hi] < a[lo]) (a[lo], a[hi]) = (a[hi], a[lo]);
            if (a[hi] < a[mid]) (a[mid], a[hi]) = (a[hi], a[mid]);
            // a[lo] <= a[mid] <= a[hi]; pivot = a[mid]. With only two elements (hi == lo+1) skip the parking dance.
            if (hi - lo < 2)
                return lo; // a[lo] <= a[hi]; lo is already the partition boundary for the smaller element
            long pivot = a[mid];
            (a[mid], a[hi - 1]) = (a[hi - 1], a[mid]);
            int store = lo;
            for (int j = lo; j < hi - 1; j++)
            {
                if (a[j] < pivot)
                {
                    (a[store], a[j]) = (a[j], a[store]);
                    store++;
                }
            }
            (a[store], a[hi - 1]) = (a[hi - 1], a[store]); // restore pivot to its sorted position
            return store;
        }

        private void CapHighValue((string tag, long costSubstract) groupTag, ReferenceAuctions bucket, AuctionKey key, KeyWithValueBreakdown breakdown, long higherValueLowerBin, ref long targetPrice, ref long percentile, Dictionary<string, string> props)
        {
            long capped = 0;
            if ((craftCostService?.TryGetCost(groupTag.tag, out var craftCost) ?? false) || key.Modifiers.Count > 0 || key.Enchants.Count > 0)
                capped = CapAtCraftCost(groupTag.tag, higherValueLowerBin, breakdown, 0);
            else
            {
                var nonCraftCap = Math.Min(higherValueLowerBin * 99 / 100, bucket.Price * 5 / 4 + 1_000_000);
                // High-supply, fast-turnover items are unlikely to rise toward the much higher LBIN wall.
                // Keep the stricter MaxMedianPriceForSnipe cap for those, but retain uplift for normal non-craftable clean items with slow sells.
                var highSupplyFastTurnover = bucket.Volume >= 25
                    && bucket.TimeToSell > 0
                    && bucket.TimeToSell <= 60
                    && bucket.Volatility <= 10
                    && bucket.Price < 15_000_000;
                targetPrice = highSupplyFastTurnover ? Math.Min(targetPrice, nonCraftCap) : nonCraftCap;
                if (bucket.References.Count < WorkingSize && bucket.References.All(r => r.Day >= GetDay() - 1)) // no full context window (~80 sales) indicates new item that is probably dorpping in price
                    targetPrice = Math.Min(targetPrice, bucket.Price); // limit at median (which may also still drop)
            }
            if (capped > 0)
            {
                percentile = Math.Min(percentile, capped * 12 / 11) + 500_000; // 500k extra since this is high volume
                if (Activity.Current is { IsAllDataRequested: true })
                    Activity.Current.Log($"Capped at craft cost {capped}");
                props["breakdown"] = JsonConvert.SerializeObject(breakdown.ValueBreakdown);
                props["craftCost"] = capped.ToString();
            }
            else
                props["nocapped"] = capped.ToString();
        }

        /// <summary>
        /// True if <paramref name="toCheck"/> dominates <paramref name="baseKey"/> (same-or-higher tier/count, and every
        /// base modifier/enchant is covered with an equal-or-greater value). Hot: called per candidate in GetLbinCap and
        /// the low-reference snipe scan. This is an allocation-free, bit-identical de-LINQ of the original predicate —
        /// the LINQ version (<see cref="IsHigherValueReference"/>) is retained as the oracle and the two are fuzzed for
        /// boolean parity in IsHigherValueTests. Modifier keys are unique within a key, so the inner key-match is at most
        /// one hit; the loop still scans on a value-miss to mirror the original Any's all-candidates semantics.
        /// </summary>
        internal bool IsHigherValue(string tag, AuctionKey baseKey, AuctionKey toCheck)
        {
            if (baseKey.Tier > toCheck.Tier)
                return false;
            if (toCheck.Tier == Tier.LEGENDARY && tag == "PET_SPIRIT")
                return false;
            if (baseKey.Count > toCheck.Count)
                return false;

            var baseMods = baseKey.Modifiers;
            var checkMods = toCheck.Modifiers;
            var firstBaseModKey = baseMods.Count > 0 ? baseMods[0].Key : null; // == baseKey.Modifiers.FirstOrDefault().Key
            for (int i = 0; i < baseMods.Count; i++)
            {
                var m = baseMods[i];
                bool covered = false;
                for (int j = 0; j < checkMods.Count; j++)
                {
                    var other = checkMods[j];
                    if (other.Key != m.Key)
                        continue;
                    bool valueOk =
                        other.Value == m.Value
                        || (float.TryParse(other.Value, out var otherVal)
                            && (firstBaseModKey != "new_years_cake" || !ImportantCakeYears.Contains(other.Value))
                            && float.TryParse(m.Value, out var ownVal)
                            && (InvertedValueKey.Contains(other.Key) ? otherVal < ownVal : otherVal > ownVal))
                        || (other.Value.Contains(m.Value)
                            && !float.TryParse(other.Value, out _)
                            && (other.Value.Contains(' ') || other.Value.Contains(',')));
                    if (valueOk && MatchesTierBoostOrLowerTier(baseKey, toCheck, m))
                    {
                        covered = true;
                        break;
                    }
                }
                // original: ... || InvertedValueKey.Contains(m.Key) && !toCheck.Modifiers.Any(o => o.Key == m.Key)
                if (!covered && !(InvertedValueKey.Contains(m.Key) && !AnyModWithKey(checkMods, m.Key)))
                    return false;
            }

            var baseEnch = baseKey.Enchants;
            var checkEnch = toCheck.Enchants;
            for (int i = 0; i < baseEnch.Count; i++)
            {
                var e = baseEnch[i];
                bool found = false;
                for (int j = 0; j < checkEnch.Count; j++)
                {
                    if (checkEnch[j].Type == e.Type && checkEnch[j].Lvl >= e.Lvl) { found = true; break; }
                }
                if (!found)
                    return false;
            }
            return true;

            static bool AnyModWithKey(System.Collections.ObjectModel.ReadOnlyCollection<KeyValuePair<string, string>> mods, string key)
            {
                for (int j = 0; j < mods.Count; j++)
                    if (mods[j].Key == key) return true;
                return false;
            }

            static bool MatchesTierBoostOrLowerTier(AuctionKey baseKey, AuctionKey toCheck, KeyValuePair<string, string> m)
            {
                if (m.Key != "exp")
                    return true;
                // De-LINQ'd: the old .Where(...).FirstOrDefault() + .Any(...) allocated Func/enumerator closures per
                // exp-modifier per candidate (a per-bucket read-path & lower-key allocator). Same semantics (a petItem
                // =TIER_BOOST modifier present?), bit-exact (IsHigherValueTests fuzz parity gate).
                if (!HasTierBoostPetItem(toCheck.Modifiers))
                    return true;
                return HasTierBoostPetItem(baseKey.Modifiers) || baseKey.Tier < toCheck.Tier;
            }

            static bool HasTierBoostPetItem(System.Collections.ObjectModel.ReadOnlyCollection<KeyValuePair<string, string>> mods)
            {
                if (mods == null)
                    return false;
                for (int j = 0; j < mods.Count; j++)
                    if (mods[j].Key == "petItem" && mods[j].Value == "TIER_BOOST")
                        return true;
                return false;
            }
        }

        /// <summary>
        /// Original LINQ implementation of <see cref="IsHigherValue"/>, retained only as the bit-exact oracle for
        /// IsHigherValueTests' fuzz parity (do not call from production — use <see cref="IsHigherValue"/>).
        /// </summary>
        internal bool IsHigherValueReference(string tag, AuctionKey baseKey, AuctionKey toCheck)
        {
            return baseKey.Tier <= toCheck.Tier
                    && (toCheck.Tier != Tier.LEGENDARY || tag != "PET_SPIRIT")
                    && baseKey.Count <= toCheck.Count
                    && baseKey.Modifiers.All(m => toCheck.Modifiers.Any(other => other.Key == m.Key
                                            && (other.Value == m.Value ||
                                                float.TryParse(other.Value, out var otherVal)
                                            && (baseKey.Modifiers.FirstOrDefault().Key != "new_years_cake" || !ImportantCakeYears.Contains(other.Value))
                                            && float.TryParse(m.Value, out var ownVal) && (InvertedValueKey.Contains(other.Key) ? otherVal < ownVal : otherVal > ownVal)
                                            || other.Value.Contains(m.Value) && !float.TryParse(other.Value, out _)
                                                && (other.Value.Contains(' ') || other.Value.Contains(','))
                                            )
                                            && MatchesTierBoostOrLowerTier(baseKey, toCheck, m))
                                                || InvertedValueKey.Contains(m.Key) && !toCheck.Modifiers.Any(other => other.Key == m.Key)
                                            )
                    && baseKey.Enchants.All(e => toCheck.Enchants.Any(other => other.Type == e.Type && other.Lvl >= e.Lvl));

            static bool MatchesTierBoostOrLowerTier(AuctionKey baseKey, AuctionKey toCheck, KeyValuePair<string, string> m)
            {
                if (m.Key != "exp")
                    return true;
                var toCheckModifiers = toCheck.Modifiers.Where(other => other.Key == "petItem" && other.Value == "TIER_BOOST").FirstOrDefault();
                if (toCheckModifiers.Key == default)
                    return true;
                var res = baseKey.Modifiers.Any(other => other.Key == "petItem" && other.Value == "TIER_BOOST");
                return res || baseKey.Tier < toCheck.Tier;
            }
        }

        private static bool IsStacksize1Cheaper(double lbinPrice, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l)
        {
            if (key.Count <= 1)
            {
                return false;
            }
            var lowerCountKey = new AuctionKey(key)
            {
                Count = 1
            };
            if (l.TryGetValue(lowerCountKey, out ReferenceAuctions lowerCountBucket))
            {
                if (lowerCountBucket.Price != 0 && lowerCountBucket.Price * key.Count < lbinPrice)
                    return true;
            }
            return false;
        }

        private static void AddMedianSample(ReferencePrice[] references, Dictionary<string, string> props)
        {
            // Newest-first (reverse FIFO) take 5, bit-exact with the old `bucket.Reverse().Take(5)`. Iterate the cached
            // snapshot array backwards instead of allocating a ConcurrentQueue enumerator + a LINQ Reverse buffer.
            int count = Math.Min(references.Length, 5);
            if (count > 0)
            {
                var sb = new System.Text.StringBuilder(180);
                for (int i = 0; i < count; i++)
                {
                    if (i > 0) sb.Append(',');
                    sb.Append(references[references.Length - 1 - i].AuctionId);
                }
                props["med"] = sb.ToString();
            }
        }

        private static long MaxMedianPriceForSnipe(ReferenceAuctions bucket, KeyWithValueBreakdown breakdown)
        {
            var price = bucket.RiskyEstimate == 0 ? bucket.Price : Math.Min(bucket.RiskyEstimate, bucket.Price * 11 / 10 + 1_000_000);
            var reducedCraftValue = breakdown.ValueBreakdown.Sum(v => v.Value) * 0.7;
            if (price == 0)
                return long.MaxValue; // disabled with 0 volume
            var fromPrice = price * 21 / 20;
            if (price < 15_000_000)
                fromPrice = price * 13 / 10;
            else if (price < 100_000_000)
                fromPrice = price * 14 / 12;
            if (bucket.Volume > 10 && bucket.DeduplicatedReferenceCount > 32)
                fromPrice = fromPrice * (reducedCraftValue == 0 ? 11 : 12) / 10; // stable unbiased price, allow higher snipes
            return Math.Max((long)reducedCraftValue, fromPrice);
        }

        public void PrintLogQueue()
        {
            // The per-entry "Info:" line (and the AuctionKey.ToString() in result.Key) is only built when the logger
            // would actually emit it; otherwise the queue is just drained (it must still empty to stay bounded). The
            // produced log text is byte-identical when Information logging is enabled.
            bool emit = logger != null && logger.IsEnabled(LogLevel.Information);
            while (Logs.TryDequeue(out LogEntry result))
            {
                if (!emit)
                    continue;
                var finderName = result.Finder == LowPricedAuction.FinderType.UNKOWN ? "NF" : result.Finder.ToString();
                logger.LogInformation($"Info: {finderName} {result.Uuid} m:{result.Median} \t{result.LBin} {result.Volume} {result.Key}{result.ExtraContext}");
            }
            deferred.PrintQueue();
        }

        private void UpdateLbin(SaveAuction auction, ReferenceAuctions bucket, AuctionKeyWithValue key)
        {
            LbinUpdates.Enqueue((auction, bucket, key));
        }

        // <paramref name="keyForProps"/> (when non-null) is the deferred "key" props entry — CreateReference no longer
        // builds it eagerly; it is materialised here after the reject gates pass, so a rejected flip never pays for
        // AuctionKey.ToString(). The emitted props are byte-identical (same value, present on every emitted flip).
        private bool FoundAFlip(SaveAuction auction, ReferenceAuctions bucket, LowPricedAuction.FinderType type, long targetPrice, Dictionary<string, string> props, AuctionKey keyForProps = null, long extraValueForProps = 0)
        {
            if (targetPrice < MIN_TARGET || targetPrice < auction.StartingBid * 1.03)
            {
                LogNonFlip(auction, bucket, defaultKey, 0, bucket.Volume, targetPrice, "Target price too low " + targetPrice);
                return false; // to low
            }
            var refAge = (GetDay() - bucket.OldestRef);
            if (bucket.OldestRef != 0 && (refAge > 60 && IsNotClean(auction) || State < SniperState.FullyLoaded && refAge > 10))
            {
                if (Activity.Current is { IsAllDataRequested: true })
                    Activity.Current.Log($"References too old {refAge} {State}");
                LogNonFlip(auction, bucket, defaultKey, 0, bucket.Volume, targetPrice, $"References too old for {State} ({refAge})");
                return false; // too old
            }
            if (keyForProps != null)
                MaterializeKeyProp(props, keyForProps, extraValueForProps);
            AddMedianSample(bucket.ReferenceSnapshot(), props);
            props["refAge"] = refAge.ToString();
            props["server"] = ServerDnsName;
            props["refCount"] = bucket.DeduplicatedReferenceCount.ToString();
            props["oldRef"] = (GetDay() - (bucket.References?.Select(r => r.Day).FirstOrDefault(GetDay()) ?? GetDay())).ToString();
            props["volat"] = bucket.Volatility.ToString();
            props["minToSell"] = (bucket.TimeToSell * (refAge / 10 + 1)).ToString();

            if (type == LowPricedAuction.FinderType.SNIPER_MEDIAN && bucket.HitsSinceCalculating < 10
                && IsProbablyNotBait(auction, targetPrice))
            {
                if (bucket.HitsSinceCalculating > bucket.Volume / 10)
                    targetPrice = (long)(targetPrice / Math.Pow(1.05, bucket.HitsSinceCalculating));
                props["hits"] = bucket.HitsSinceCalculating.ToString();
                bucket.HitsSinceCalculating++;
            }
            using var found = activitySource?.StartActivity("FoundFlip", ActivityKind.Internal);
            found?.AddTag("uuid", auction.Uuid);
            if (found is { IsAllDataRequested: true })
                found.Log($"Found flip {auction.Uuid} {targetPrice} {type} {bucket.Volume}");

            // Apply Diana adjustment to Diana-related items when Diana's term is ending or just ended
            if (mayorService != null
                && MayorService.DianaRelatedItems.Contains(auction.Tag)
                && mayorService.IsDianaItemsAdjustmentActive(DateTime.UtcNow))
            {
                targetPrice = (long)(targetPrice * 0.9);
                props["diana-adj"] = "true";
            }

            snipesFoundCounter.WithLabels(type.ToString()).Inc();
            FoundSnipe?.Invoke(new LowPricedAuction()
            {
                Auction = auction,
                Finder = type,
                TargetPrice = targetPrice,
                DailyVolume = bucket.Volume,
                AdditionalProps = props
            });
            Logs.Enqueue(new LogEntry()
            {
                ExtraContext = props.GetValueOrDefault("key") ?? (props.GetValueOrDefault("breakdown") + "+" + props.GetValueOrDefault("cleanCost")),
                LBin = bucket.Lbin.Price,
                Median = bucket.Price,
                Uuid = auction.Uuid,
                Volume = bucket.Volume,
                Finder = type
            });
            return true;

            static bool IsProbablyNotBait(SaveAuction auction, long targetPrice)
            {
                return targetPrice < auction.StartingBid * 10;
            }
        }

        private static bool IsNotClean(SaveAuction auction)
        {
            return auction.Enchantments.Count > 0 || auction.FlatenedNBT.Any(f => !f.Key.Contains("uid"));
        }

        // The "key" entry (which calls the costly AuctionKey.ToString()) is NOT built here — it is materialised by
        // FoundAFlip after its reject gates pass (see MaterializeKeyProp), so a flip that is rejected (target too low /
        // refs too old) never pays for the ToString(). The emitted dict is byte-identical: "key" still ends up present
        // for CreateReference-sourced props (the only consumer reads it at emit time, after the gates).
        private static Dictionary<string, string> CreateReference(long reference, AuctionKey key, long extraValue, ReferenceAuctions bucket)
        {
            var dict = new Dictionary<string, string>() {
                { "reference", AuctionService.Instance.GetUuid(reference) }
            };
            if (extraValue != 0)
                dict["extraValue"] = extraValue.ToString();
            return dict;
        }

        // Builds the deferred "key" props entry exactly as CreateReference used to, only when a flip is actually emitted.
        private static void MaterializeKeyProp(Dictionary<string, string> props, AuctionKey key, long extraValue)
        {
            props["key"] = key.ToString() + (extraValue == 0 ? "" : $" +{extraValue}");
        }

        public async System.Threading.Tasks.Task Init()
        {
            await itemService.GetItemsAsync();
            await mapper.LoadNeuConstants();
            UpdateToday();
            AddRunes();
        }


        private void AddRunes()
        {
            foreach (var item in new string[]{
                "RUNE",
                "RUNEBOOK",
                "RUNE_BITE",
                "RUNE_BLOOD_2",
                "RUNE_CLOUDS",
                "RUNE_COUTURE",
                "RUNE_DRAGON",
                "RUNE_ENCHANT",
                "RUNE_ENDERSNAKE",
                "RUNE_FIERY_BURST",
                "RUNE_FIRE_SPIRAL",
                "RUNE_GEM",
                "RUNE_GOLDEN",
                "RUNE_GRAND_SEARING",
                "RUNE_HEARTS",
                "RUNE_HOT",
                "RUNE_ICE",
                "RUNE_JERRY",
                "RUNE_LAVA",
                "RUNE_LAVATEARS",
                "RUNE_LIGHTNING",
                "RUNE_MAGIC",
                "RUNE_MUSIC",
                "RUNE_RAINBOW",
                "RUNE_REDSTONE",
                "RUNE_SACK",
                "RUNE_SLIMY",
                "RUNE_SMOKEY",
                "RUNE_SNAKE",
                "RUNE_SNOW",
                "RUNE_SOULTWIST",
                "RUNE_SPARKLING",
                "RUNE_SPELLBOUND",
                "RUNE_SPIRIT",
                "RUNE_TIDAL",
                "RUNE_WAKE",
                "RUNE_WHITE_SPIRAL",
                "RUNE_ZAP",
                "RUNE_ZOMBIE_SLAYER",
                "SMALL_RUNES_SACK",
                "UNIQUE_RUNE",
                "UNIQUE_RUNE_BARK_TUNES",
                "UNIQUE_RUNE_GOLDEN_CARPET",
                "UNIQUE_RUNE_GRAND_FREEZING",
                "UNIQUE_RUNE_HEARTSPLOSION",
                "UNIQUE_RUNE_ICE_SKATES",
                "UNIQUE_RUNE_MEOW_MUSIC",
                "UNIQUE_RUNE_ORNAMENTAL",
                "UNIQUE_RUNE_PRIMAL_FEAR",
                "UNIQUE_RUNE_RAINY_DAY",
                "UNIQUE_RUNE_SMITTEN",
                "UNIQUE_RUNE_SPELLBOUND",
                "UNIQUE_RUNE_SUPER_PUMPKIN"
            })
            {
                RuneLookup.Add(item);
            }
        }
    }
}
