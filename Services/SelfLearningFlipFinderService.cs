using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.FlipTracker.Client.Model;
using MessagePack;
using Microsoft.Extensions.Logging;
using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.ML.Trainers;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

/// <summary>
/// Service for self-learning auction price prediction using ML.NET FastTree regression.
/// Trains per-item models to estimate auction values based on attributes (enchantments, upgrades, etc.)
/// Uses FastTree (gradient boosted decision trees) which handles large attribute values well.
/// </summary>
public interface ISelfLearningFlipFinderService
{
    Task TrainAsync(ComplicatedFlip flip, CancellationToken cancellationToken = default);
    Task TrainBatchAsync(IEnumerable<ComplicatedFlip> flips, CancellationToken cancellationToken = default);
    Task<SelfLearningFlipEstimate?> EstimateAsync(ComplicatedFlip flip, CancellationToken cancellationToken = default);
    SelfLearningFlipModelSnapshot GetSnapshot();
    IReadOnlyDictionary<string, SelfLearningFlipFinderService.ModelStats> GetModelStats();
    Task PersistModelAsync(string? tag = null);
    bool IsRelevantItem(string tag);
}

/// <summary>
/// Model evaluation metrics for regression tasks.
/// Rmse and RSquared are measured on the training samples in the unit the model is fit in (log price).
/// The held-out figures are relative price errors on sales the serving model had not been trained on yet.
/// </summary>
public sealed record ModelMetrics(double Rmse, double RSquared, double? HeldOutMedianError = null, double? HeldOutP90Error = null, int HeldOutSales = 0);

/// <summary>
/// Self-learning auction price predictor using ML.NET FastTree regression.
/// FastTree uses gradient boosted decision trees which handle large feature values and non-linear relationships well.
/// </summary>
public sealed class SelfLearningFlipFinderService : ISelfLearningFlipFinderService, IDisposable
{
    public sealed record ModelStats(string Tag, IReadOnlyCollection<string> FeatureNames, int SampleCount, bool ModelLoaded, ModelMetrics? Metrics);

    private readonly int minSamplesForTraining;
    private readonly ILogger<SelfLearningFlipFinderService> logger;
    private readonly MLContext mlContext;
    private readonly IPersitanceManager persitance;
    private readonly ReaderWriterLockSlim gate = new(LockRecursionPolicy.NoRecursion);
    private readonly Dictionary<string, List<FlipData>> trainingDataByItem = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Dictionary<string, int>> featureIndexByItem = new(StringComparer.OrdinalIgnoreCase);
    private readonly object predictionSync = new();
    private readonly Dictionary<string, ITransformer?> models = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, PredictionEngine<FlipData, FlipPrediction>?> predictionEngines = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ModelMetrics?> lastMetricsByItem = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, DateTime> lastPersistedByTag = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> loadedTags = new(StringComparer.OrdinalIgnoreCase);
    // Track last time a model refit was performed per tag to avoid excessive retraining
    private readonly Dictionary<string, DateTime> lastRefitByTag = new(StringComparer.OrdinalIgnoreCase);
    // Track the expected feature vector size for each model's prediction engine
    private readonly Dictionary<string, int> modelVectorSizeByTag = new(StringComparer.OrdinalIgnoreCase);
    // Track the maximum sold price observed in training data per tag to cap unrealistic predictions
    private readonly Dictionary<string, float> maxTrainingLabelByTag = new(StringComparer.OrdinalIgnoreCase);
    // Offset added to the score of a model that predicts the logarithm of the price; a tag without one holds a legacy raw-coin model
    private readonly Dictionary<string, double> logLabelCenterByTag = new(StringComparer.OrdinalIgnoreCase);
    // End of the newest sale seen per tag: only a later sale can be new to the serving model, a replay never is
    private readonly Dictionary<string, DateTime> newestSaleByTag = new(StringComparer.OrdinalIgnoreCase);
    // Tags whose serving model was fit in this process. What a loaded model was trained on is unknown
    private readonly HashSet<string> tagsFitHere = new(StringComparer.OrdinalIgnoreCase);
    // Tags whose model was fit on prices without the removable parts, only their estimates get the parts added
    private readonly HashSet<string> tagsFitWithoutRemovables = new(StringComparer.OrdinalIgnoreCase);
    // Relative errors of the serving model on sales it was not trained on yet, newest last
    private readonly Dictionary<string, Queue<float>> heldOutErrorsByTag = new(StringComparer.OrdinalIgnoreCase);
    private const int HeldOutWindow = 200;
    // Extra column holding the sum of all attribute values, the same sum the AI finder caps its price with
    private const string AttributeSumFeature = "attributesum";
    // A sale priced above this multiple of the tag's usual price to attribute sum ratio is a coin transfer, not a price
    private const float TransferAboveUsualRatio = 2f;
    // A sale below this fraction of the tag's median price is junk; on a log scale a single one moves a whole leaf
    private const float JunkBelowMedianFraction = 1f / 20;
    private const string LogModelBlobPrefix = "selflearning/logmodel/";
    private const string LegacyModelBlobPrefix = "selflearning/model/";
    // Only keep/train models for these complicated / relevant items (mirror of AIFormattingService.RelevantItems)
    private static readonly HashSet<string> RelevantItems = new(StringComparer.OrdinalIgnoreCase)
    {
        "HYPERION",
        "PET_ROSE_DRAGON",
        "PET_GOLDEN_DRAGON",
        "DARK_CLAYMORE",
        "GIANTS_SWORD",
        "PET_ENDER_DRAGON",
        "TERMINATOR",
        "PET_SCATHA",
        "WARDEN_HELMET",
        "TITANIUM_DRILL_4",
        "PET_MOSQUITO",
        "CROWN_OF_AVARICE",
        "POWER_WITHER_LEGGINGS",
        "POWER_WITHER_CHESTPLATE",
        "PET_ENDERMAN",
        "WISE_WITHER_CHESTPLATE",
        "ATOMSPLIT_KATANA",
        "JUJU_SHORTBOW",
        "SPEED_WITHER_BOOTS",
        "SHADOW_FURY",
        "WISE_WITHER_LEGGINGS",
        "DIVAN_HELMET",
        "PET_BLACK_CAT",
        "DIVAN_BOOTS",
        "SKELETON_MASTER_CHESTPLATE",
        "WISE_WITHER_HELMET",
        "DIVAN_CHESTPLATE",
        "TITANIUM_DRILL_3",
        "DIVAN_LEGGINGS",
        "SHADOW_ASSASSIN_CHESTPLATE",
        "LIVID_DAGGER",
        "AXE_OF_THE_SHREDDED",
        "POWER_WITHER_BOOTS",
        "WISE_WITHER_BOOTS",
        "FERMENTO_HELMET",
        "STARRED_MIDAS_SWORD",
        "WITHER_GOGGLES",
        "PET_GRIFFIN",
        "FINAL_DESTINATION_HELMET",
        "PET_WITCH",
        "FINAL_DESTINATION_CHESTPLATE",
        "POWER_WITHER_HELMET",
        "FERMENTO_CHESTPLATE",
        "PET_JELLYFISH",
        "FERMENTO_LEGGINGS",
        "STARRED_DAEDALUS_AXE",
        "PET_FLYING_FISH",
        "REAPER_MASK",
        "MIDAS_STAFF",
        "GEMSTONE_DRILL_4",
        "MOSQUITO_BOW",
        "FINAL_DESTINATION_BOOTS",
        "FINAL_DESTINATION_LEGGINGS",
        "FIGSTONE_AXE",
        "BOUQUET_OF_LIES",
        "SPIRIT_MASK",
        "HELIANTHUS_BOOTS",
        "BAT_WAND",
        "PRIMORDIAL_HELMET",
        "RAGNAROCK_AXE",
        "PET_LION",
        "PET_SLUG",
        "FELTHORN_REAPER",
        "REAPER_SCYTHE",
        "FERMENTO_BOOTS",
        "PET_PHOENIX",
        "MIDAS_SWORD",
        "PET_HEDGEHOG",
        "PET_BABY_YETI",
        "PET_ELEPHANT",
        "ASPECT_OF_THE_VOID",
        "TANK_WITHER_CHESTPLATE",
        "PET_TIGER",
        "STING",
        "GEMSTONE_GAUNTLET",
        "GLOSSY_MINERAL_LEGGINGS",
        "PET_GLACITE_GOLEM",
        "GLOSSY_MINERAL_CHESTPLATE",
        "PET_BLUE_WHALE",
        "PET_SQUID",
        "SHADOW_ASSASSIN_LEGGINGS",
        "GLOSSY_MINERAL_BOOTS",
        "PET_BLAZE",
        "GLACIAL_SCYTHE",
        "PET_RABBIT",
        "GLOSSY_MINERAL_HELMET",
        "HELLFIRE_ROD",
        "RANCHERS_BOOTS",
        "PET_MOOSHROOM_COW",
        "BONE_NECKLACE",
        "MITHRIL_DRILL_2",
        "PET_SKELETON",
        "SHADOW_ASSASSIN_HELMET",
        "SUPERIOR_DRAGON_CHESTPLATE",
        "TITANIUM_DRILL_2",
        "SHADOW_ASSASSIN_CLOAK",
        "HEARTFIRE_DAGGER",
        "PULSE_RING",
        "VORPAL_KATANA",
        "PET_PARROT",
        "ROD_OF_THE_SEA",
        "HEARTMAW_DAGGER",
        "PET_CROW",
        "DAEDALUS_AXE",
        "TANK_WITHER_LEGGINGS",
        "BURNING_CRIMSON_CHESTPLATE",
        "BURNING_CRIMSON_LEGGINGS",
        "FIERY_CRIMSON_LEGGINGS",
        "BURNING_CRIMSON_BOOTS",
        "SUPERIOR_DRAGON_LEGGINGS",
        "BLOSSOM_CLOAK",
        "TANK_WITHER_BOOTS",
        "FIERY_CRIMSON_BOOTS",
        "SOULWEAVER_GLOVES",
        "SCORPION_FOIL",
        "YETI_SWORD",
        "TITANIUM_DRILL_1",
        "PET_WOLF",
        "ASPECT_OF_THE_DRAGON",
        "BLOSSOM_BRACELET",
        "FLAMING_FLAY",
        "SORROW_CHESTPLATE",
        "BLOSSOM_NECKLACE",
        "SORROW_BOOTS",
        "PET_FROG",
        "FIERY_CRIMSON_CHESTPLATE",
        "MAGMA_LORD_CHESTPLATE",
        "POOCH_SWORD",
        "ADAPTIVE_BELT",
        "BONZO_STAFF",
        "PET_BAL",
        "PET_MONKEY",
        "SHADOW_ASSASSIN_BOOTS",
        "PET_TARANTULA",
        "PET_SHEEP",
        "BLOSSOM_BELT",
        "GILLSPLASH_BELT",
        "SORROW_HELMET",
        "NEW_YEAR_CAKE",
        "SORROW_LEGGINGS",
        "PET_GUARDIAN",
        "MAGMA_LORD_LEGGINGS",
        "MAGMA_LORD_HELMET",
        "SUPERIOR_DRAGON_BOOTS",
        "WITHER_CHESTPLATE",
        "MAGMA_LORD_BOOTS",
        "NECROMANCER_LORD_CHESTPLATE",
        "SUPERIOR_DRAGON_HELMET",
        "PET_HOUND",
        "PET_SNAIL",
        "ITEM_SPIRIT_BOW",
        "ICE_SPRAY_WAND",
        "SUMMONING_RING",
        "TARANTULA_HELMET",
        "BURNING_CRIMSON_HELMET",
        "REAPER_SWORD",
        "PET_WITHER_SKELETON",
        "FROZEN_BLAZE_CHESTPLATE",
        "FROZEN_BLAZE_LEGGINGS",
        "PET_GOBLIN",
        "NECROMANCER_SWORD",
        "BURNING_AURORA_BOOTS",
        "PET_AMMONITE",
        "FIERY_AURORA_LEGGINGS",
        "BURNING_AURORA_CHESTPLATE",
        "BURNING_AURORA_LEGGINGS",
        "GEMSTONE_DRILL_2",
        "PET_SPIRIT",
        "FROZEN_BLAZE_BOOTS",
        "SQUASH_CHESTPLATE",
        "SQUASH_LEGGINGS",
        "PET_TYRANNOSAURUS",
        "PET_GHOUL",
        "FROZEN_BLAZE_HELMET",
        "FROZEN_SCYTHE",
        "PET_MOLE",
        "HOT_CRIMSON_BOOTS",
        "FLOWER_OF_TRUTH",
        "GEMSTONE_DRILL_3",
        "INFERNO_ROD",
        "PET_ARMADILLO",
        "MYTHOS_LEGGINGS",
        "WISE_DRAGON_CHESTPLATE",
        "WISE_DRAGON_LEGGINGS",
        "PET_DOLPHIN",
        "PET_ZOMBIE",
        "PET_HERMIT_CRAB",
        "SPEED_WITHER_LEGGINGS",
        "HOT_CRIMSON_LEGGINGS",
        "FIERY_CRIMSON_HELMET",
        "PET_TURTLE",
        "PESTHUNTERS_GLOVES",
        "PESTHUNTERS_BELT",
        "MYTHOS_CHESTPLATE",
        "PESTHUNTERS_NECKLACE",
        "FIG_CHESTPLATE",
        "PET_MEGALODON",
        "PRIMORDIAL_LEGGINGS",
        "PET_BEE",
        "PRIMORDIAL_CHESTPLATE",
        "SOUL_WHIP",
        "PET_GIRAFFE",
        "NECROMANCER_LORD_LEGGINGS",
        "PET_PIG",
        "LOTUS_CLOAK",
        "CROPIE_LEGGINGS",
        "WISE_DRAGON_BOOTS",
        "ADVANCED_GARDENING_HOE",
        "BONE_BOOMERANG",
        "REAPER_LEGGINGS",
        "HOT_CRIMSON_CHESTPLATE",
        "GAUNTLET_OF_CONTAGION",
        "FIG_LEGGINGS",
        "PARTY_HAT_CRAB_ANIMATED",
        "REAPER_CHESTPLATE",
        "REAPER_BOOTS",
        "MENDER_CROWN",
        "PRIMORDIAL_BOOTS",
        "LOTUS_NECKLACE",
        "PET_ENDERMITE",
        "FIG_BOOTS",
        "MITHRIL_DRILL_1",
        "PET_BAT",
        "FIG_HELMET",
        "HOT_CRIMSON_HELMET",
        "PET_MAGMA_CUBE",
        "LOTUS_BELT",
        "PET_CHICKEN",
        "LOTUS_BRACELET",
        "MASTIFF_CHESTPLATE",
        "PESTHUNTERS_CLOAK",
        "GEMSTONE_DRILL_1",
        "IMPLOSION_BELT",
        "THUNDER_CHESTPLATE",
        "VOIDEDGE_KATANA",
        "THUNDER_LEGGINGS",
        "GIANT_CLEAVER",
        "CROPIE_HELMET",
        "LAST_BREATH",
        "SPEED_WITHER_CHESTPLATE",
        "BONZO_MASK",
        "PET_RAT",
        "MYTHOS_BOOTS",
        "VANQUISHED_GHAST_CLOAK",
        "PIGMAN_SWORD",
        "SHADOW_GOGGLES",
        "PET_SPIDER",
        "PET_ROCK",
        "ASPECT_OF_THE_END",
        "THUNDER_BOOTS",
        "THUNDER_HELMET",
        "MYTHOS_HELMET",
        "LEGEND_ROD",
        "JUNGLE_PICKAXE",
        "MOLTEN_NECKLACE",
        "NECROMANCER_LORD_BOOTS",
        "SHARK_SCALE_CHESTPLATE",
        "POLISHED_TOPAZ_ROD",
        "SHARK_SCALE_HELMET",
        "HOT_AURORA_BOOTS",
        "MOLTEN_BRACELET",
        "SHARK_SCALE_LEGGINGS",
        "SHARK_SCALE_BOOTS",
        "BURSTFIRE_DAGGER",
        "VANQUISHED_GLOWSTONE_GAUNTLET",
        "CRIMSON_HELMET",
        "RUNAANS_BOW",
        "MAGMA_ROD",
        "TANK_WITHER_HELMET",
        "BURSTMAW_DAGGER",
        "VANQUISHED_MAGMA_NECKLACE"
    };

    private bool disposed;

    public SelfLearningFlipFinderService(ILogger<SelfLearningFlipFinderService> logger, IPersitanceManager persitance, int minSamplesForTraining = 120)
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.persitance = persitance ?? throw new ArgumentNullException(nameof(persitance));
        mlContext = new MLContext(seed: Environment.TickCount);
        this.minSamplesForTraining = minSamplesForTraining;

        // no eager restore; models are loaded on demand per item when training or estimating
    }

    /// <summary>
    /// Forces persistence of trained models to storage.
    /// </summary>
    /// <param name="tag">Item tag to persist, or null to persist all models</param>
    public Task PersistModelAsync(string? tag = null)
    {
        if (disposed)
            throw new ObjectDisposedException(nameof(SelfLearningFlipFinderService));

        gate.EnterWriteLock();
        try
        {
            var tagsToProcess = tag is null
                ? trainingDataByItem.Keys.Union(featureIndexByItem.Keys).Distinct(StringComparer.OrdinalIgnoreCase).ToArray()
                : new[] { tag };

            foreach (var itemTag in tagsToProcess)
            {
                try
                {
                    RefitModel(itemTag, forcePersist: true);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to persist model for {Tag}", itemTag);
                }
            }
        }
        finally
        {
            gate.ExitWriteLock();
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Trains models with a batch of flips. More efficient than individual TrainAsync calls.
    /// Groups flips by item tag and trains/refits models for each tag.
    /// </summary>
    /// <param name="flips">Completed auction flips with attributes and sale prices</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public Task TrainBatchAsync(IEnumerable<ComplicatedFlip> flips, CancellationToken cancellationToken = default)
    {
        if (flips is null)
            throw new ArgumentNullException(nameof(flips));
        if (disposed)
            throw new ObjectDisposedException(nameof(SelfLearningFlipFinderService));

        var flipsByTag = GroupFlipsByTag(flips);
        if (flipsByTag.Count == 0)
            return Task.CompletedTask;

        // Only keep/train for relevant items
        var relevant = flipsByTag.Where(kv => RelevantItems.Contains(kv.Key)).ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
        if (relevant.Count == 0)
            return Task.CompletedTask;

        gate.EnterWriteLock();
        try
        {
            AddTrainingSamples(relevant);
            RefitModelsForTags(relevant.Keys);
        }
        finally
        {
            gate.ExitWriteLock();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Groups flips by item tag, filtering out invalid entries.
    /// </summary>
    private Dictionary<string, List<ComplicatedFlip>> GroupFlipsByTag(IEnumerable<ComplicatedFlip> flips)
    {
        var flipsByTag = new Dictionary<string, List<ComplicatedFlip>>(StringComparer.OrdinalIgnoreCase);
        foreach (var flip in flips)
        {
            if (flip is null || flip.AttributeValues is null || flip.AttributeValues.Count == 0 || flip.SoldFor <= 0)
                continue;

            var tag = flip.ItemTag ?? "_global";
            if (!RelevantItems.Contains(tag))
            {
                logger.LogDebug("Skipping training for non-relevant tag {Tag}", tag);
                continue;
            }
            if (!flipsByTag.TryGetValue(tag, out var list))
            {
                list = new List<ComplicatedFlip>();
                flipsByTag[tag] = list;
            }
            list.Add(flip);
        }
        return flipsByTag;
    }

    /// <summary>
    /// Adds training samples for multiple tags.
    /// </summary>
    private void AddTrainingSamples(Dictionary<string, List<ComplicatedFlip>> flipsByTag)
    {
        foreach (var (tag, flips) in flipsByTag)
        {
            if (!trainingDataByItem.TryGetValue(tag, out var sampleList))
            {
                sampleList = new List<FlipData>();
                trainingDataByItem[tag] = sampleList;
            }
            if (!featureIndexByItem.TryGetValue(tag, out var featureIndex))
            {
                featureIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                featureIndexByItem[tag] = featureIndex;
            }

            foreach (var flip in flips)
            {
                sampleList.Add(CreateSample(tag, flip, featureIndex, sampleList));
            }
        }
    }

    /// <summary>
    /// Refits models for the specified tags.
    /// </summary>
    private void RefitModelsForTags(IEnumerable<string> tags)
    {
        var now = DateTime.UtcNow;

        foreach (var tag in tags)
        {
            try
            {
                if (lastRefitByTag.TryGetValue(tag, out var last) && (now - last) < TimeSpan.FromMinutes(5))
                {
                    logger.LogDebug("Skipping refit for {Tag} (last refit {Elapsed} ago)", tag, now - last);
                    continue;
                }
                RefitModel(tag, forcePersist: true);
                lastRefitByTag[tag] = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to refit model for {Tag} during batch training", tag);
            }
        }
    }

    public IReadOnlyDictionary<string, ModelStats> GetModelStats()
    {
        gate.EnterReadLock();
        try
        {
            var result = new Dictionary<string, ModelStats>(StringComparer.OrdinalIgnoreCase);
            foreach (var tag in trainingDataByItem.Keys.Union(featureIndexByItem.Keys).Distinct(StringComparer.OrdinalIgnoreCase))
            {
                trainingDataByItem.TryGetValue(tag, out var list);
                featureIndexByItem.TryGetValue(tag, out var featureIndex);
                models.TryGetValue(tag, out var model);
                lastMetricsByItem.TryGetValue(tag, out var metrics);
                var stat = new ModelStats(tag, featureIndex?.Keys.ToArray() ?? Array.Empty<string>(), list?.Count ?? 0, model is not null, metrics);
                result[tag] = stat;
            }
            return result;
        }
        finally
        {
            gate.ExitReadLock();
        }
    }

    /// <summary>
    /// Trains a single flip. For bulk training, use TrainBatchAsync instead.
    /// </summary>
    public Task TrainAsync(ComplicatedFlip flip, CancellationToken cancellationToken = default)
    {
        if (flip is null)
            throw new ArgumentNullException(nameof(flip));
        if (disposed)
            throw new ObjectDisposedException(nameof(SelfLearningFlipFinderService));

        cancellationToken.ThrowIfCancellationRequested();

        if (flip.AttributeValues is null || flip.AttributeValues.Count == 0)
        {
            logger.LogDebug("Skipping training for {Tag} because it has no attribute values", flip.ItemTag);
            return Task.CompletedTask;
        }

        if (flip.SoldFor <= 0)
        {
            logger.LogDebug("Skipping training for {Tag} because SoldFor is {SoldFor}", flip.ItemTag, flip.SoldFor);
            return Task.CompletedTask;
        }

        var tagCheck = flip.ItemTag ?? "_global";
        if (!RelevantItems.Contains(tagCheck))
        {
            logger.LogDebug("Skipping training for non-relevant tag {Tag}", tagCheck);
            return Task.CompletedTask;
        }

        gate.EnterWriteLock();
        try
        {
            var tag = tagCheck;
            // create per-item structures if missing
            if (!trainingDataByItem.TryGetValue(tag, out var list))
            {
                list = new List<FlipData>();
                trainingDataByItem[tag] = list;
            }
            if (!featureIndexByItem.TryGetValue(tag, out var fIndex))
            {
                fIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                featureIndexByItem[tag] = fIndex;
            }

            list.Add(CreateSample(tag, flip, fIndex, list));

            if (list.Count >= minSamplesForTraining && fIndex.Count > 0)
            {
                RefitModel(tag);
            }
        }
        finally
        {
            gate.ExitWriteLock();
        }

        return Task.CompletedTask;
    }

    public Task<SelfLearningFlipEstimate?> EstimateAsync(ComplicatedFlip flip, CancellationToken cancellationToken = default)
    {
        if (flip is null)
            throw new ArgumentNullException(nameof(flip));
        if (disposed)
            throw new ObjectDisposedException(nameof(SelfLearningFlipFinderService));

        cancellationToken.ThrowIfCancellationRequested();

        gate.EnterReadLock();
        try
        {
            var baseline = ComputeBaseline(flip);
            var tag = flip.ItemTag ?? "_global";
            if (!RelevantItems.Contains(tag) || !trainingDataByItem.TryGetValue(tag, out var list) || !featureIndexByItem.TryGetValue(tag, out var fIndex))
            {
                return Task.FromResult<SelfLearningFlipEstimate?>(null);
            }

            // try to lazy-load persisted model/meta if available
            if (!models.TryGetValue(tag, out var tagModel) || tagModel is null)
            {
                // release read lock and try to load
                gate.ExitReadLock();
                try
                {
                    LoadPersistedModelIfExists(tag).Wait();
                }
                catch { }
                finally
                {
                }
                gate.EnterReadLock();
                models.TryGetValue(tag, out tagModel);
            }

            // if still no model but we have enough in-memory samples, train on-demand
            if ((tagModel is null) && trainingDataByItem.TryGetValue(tag, out var inMemList) && inMemList.Count >= minSamplesForTraining && fIndex.Count > 0)
            {
                // upgrade to write lock to train safely
                gate.ExitReadLock();
                gate.EnterWriteLock();
                try
                {
                    RefitModel(tag);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "On-demand RefitModel failed for {Tag}", tag);
                }
                finally
                {
                    gate.ExitWriteLock();
                }
                gate.EnterReadLock();
                models.TryGetValue(tag, out tagModel);
            }

            if (tagModel is null || !predictionEngines.TryGetValue(tag, out var tagEngine) || fIndex.Count == 0 || list.Count < minSamplesForTraining)
            {
                return Task.FromResult<SelfLearningFlipEstimate?>(new SelfLearningFlipEstimate(baseline, baseline, false, list.Count, lastMetricsByItem.GetValueOrDefault(tag)));
            }

            var attrs = WithAttributeSum(WithoutRemovable(flip.AttributeValues ?? new Dictionary<string, long>(), out var removableValue, out _), out _);

            // Use the exact vector size the model expects (from when it was trained/loaded)
            // This prevents errors when new features appear that weren't in the training data
            var expectedVectorSize = modelVectorSizeByTag.GetValueOrDefault(tag, fIndex.Count);
            var features = CreateFeatureVectorForPrediction(attrs, fIndex, expectedVectorSize);

            if (features.Length != expectedVectorSize)
            {
                logger.LogWarning("Feature vector size mismatch for {Tag}: created {ActualSize}, expected {ExpectedSize}",
                    tag, features.Length, expectedVectorSize);
                return Task.FromResult<SelfLearningFlipEstimate?>(new SelfLearningFlipEstimate(baseline, baseline, false, list.Count, lastMetricsByItem.GetValueOrDefault(tag)));
            }

            double predictedPrice;
            lock (predictionSync)
            {
                predictedPrice = ScoreToPrice(tag, tagEngine!.Predict(new FlipData { Features = features }).Score);
                logger.LogInformation("Prediction for {Tag}: {Score} (baseline {Baseline})", tag, predictedPrice, baseline);
            }
            var score = !double.IsFinite(predictedPrice) || predictedPrice <= 0 ? baseline : predictedPrice;

            // Cap prediction to 1.5x the maximum sold price seen in training data.
            // Prevents items like SKELETON_MASTER_CHESTPLATE from being valued at billions
            // when no training sample supports such a price (attributes defaulting to high estimates).
            if (maxTrainingLabelByTag.TryGetValue(tag, out var maxLabel) && maxLabel > 0 && score > maxLabel * 1.5f)
            {
                logger.LogWarning("AI prediction for {Tag} capped from {OriginalScore:F0} to {CappedScore:F0} (max training label: {MaxLabel:F0}, attrs: {Attrs})",
                    tag, score, maxLabel * 1.5f, maxLabel,
                    string.Join(", ", (flip.AttributeValues ?? new Dictionary<string, long>()).Select(kv => $"{kv.Key}={kv.Value}")));
                score = maxLabel * 1.5f;
            }

            // what can be taken off and sold is worth its price on any item, the model only estimates the rest
            return Task.FromResult<SelfLearningFlipEstimate?>(new SelfLearningFlipEstimate(score + RemovableValueToAdd(tag, removableValue), baseline, true, list.Count, lastMetricsByItem.GetValueOrDefault(tag)));
        }
        finally
        {
            gate.ExitReadLock();
        }
    }

    public SelfLearningFlipModelSnapshot GetSnapshot()
    {
        if (disposed)
            throw new ObjectDisposedException(nameof(SelfLearningFlipFinderService));
        gate.EnterReadLock();
        try
        {
            // aggregate feature names across items
            var allFeatures = featureIndexByItem.Values.SelectMany(d => d.Keys).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            var sampleCount = trainingDataByItem.Values.Sum(l => l.Count);
            // no aggregate metrics; return null
            return new SelfLearningFlipModelSnapshot(allFeatures, sampleCount, null);
        }
        finally
        {
            gate.ExitReadLock();
        }
    }

    // test helper: force (re)build the model for a given tag from in-memory samples
    public Task<bool> EnsureTrainedModelAsync(string tag)
    {
        if (disposed)
            throw new ObjectDisposedException(nameof(SelfLearningFlipFinderService));

        gate.EnterWriteLock();
        try
        {
            RefitModel(tag);
            var hasModel = models.TryGetValue(tag, out var m) && m is not null;
            var hasEngine = predictionEngines.TryGetValue(tag, out var e) && e is not null;
            // diagnostics removed
            return Task.FromResult(hasModel && hasEngine);
        }
        finally
        {
            gate.ExitWriteLock();
        }
    }

    private float[] CreateFeatureVector(IDictionary<string, long> attributes, Dictionary<string, int> featureIndex, bool expandFeatureSpace, List<FlipData> trainingList)
    {
        if (expandFeatureSpace)
        {
            foreach (var key in attributes.Keys)
            {
                EnsureFeatureExists(key, featureIndex, trainingList);
            }
        }

        var vector = new float[featureIndex.Count];
        if (attributes.Count == 0)
        {
            return vector;
        }

        foreach (var (key, value) in attributes)
        {
            if (!featureIndex.TryGetValue(key, out var index))
            {
                continue;
            }

            vector[index] = ToSplitSafe(SafeToFloat(value));
        }

        return vector;
    }

    /// <summary>
    /// Creates a feature vector for prediction with the exact size the model expects.
    /// This prevents errors when new features appear that weren't in the training data.
    /// Unknown features are simply ignored (set to 0).
    /// </summary>
    private float[] CreateFeatureVectorForPrediction(IDictionary<string, long> attributes, Dictionary<string, int> featureIndex, int expectedVectorSize)
    {
        var vector = new float[expectedVectorSize];

        if (attributes.Count == 0)
        {
            return vector;
        }

        foreach (var (key, value) in attributes)
        {
            if (!featureIndex.TryGetValue(key, out var index))
            {
                // Feature not in model - ignore it (stays 0)
                continue;
            }

            // Only set the value if the index is within the expected vector size
            if (index < expectedVectorSize)
            {
                vector[index] = ToSplitSafe(SafeToFloat(value));
            }
        }

        return vector;
    }

    /// <summary>
    /// Drops the two lowest mantissa bits. FastTree stores a split threshold as the float nearest to the midpoint of
    /// two neighbouring values; when those are adjacent floats (coin values one coin apart) the threshold lands on
    /// the upper one and prediction sends it down the other branch than training did.
    /// </summary>
    private static float ToSplitSafe(float value)
    {
        return BitConverter.Int32BitsToSingle(BitConverter.SingleToInt32Bits(value) & ~3);
    }

    /// <summary>
    /// Copies the attributes and adds their sum as one more feature.
    /// The sum leaves out the candy flag like the cap in the AI finder does.
    /// </summary>
    private static Dictionary<string, long> WithAttributeSum(IDictionary<string, long> attributes, out long attributeSum)
    {
        var result = new Dictionary<string, long>(attributes);
        attributeSum = 0;
        foreach (var (key, value) in attributes)
        {
            if (!key.StartsWith("candyUsed:", StringComparison.OrdinalIgnoreCase))
                attributeSum += value;
        }
        result[AttributeSumFeature] = attributeSum;
        return result;
    }

    /// <summary>
    /// Splits off the parts and gems that can be taken off the item and sold on their own.
    /// Their price is known, so they are neither features nor part of the price the model learns.
    /// </summary>
    private static Dictionary<string, long> WithoutRemovable(IDictionary<string, long> attributes, out long removableValue, out bool removablesListed)
    {
        var result = new Dictionary<string, long>();
        removableValue = 0;
        removablesListed = attributes.ContainsKey(SaveAuctionExtensions.RemovablesListedMarker);
        foreach (var (key, value) in attributes)
        {
            if (key.StartsWith(SaveAuctionExtensions.RemovablePrefix, StringComparison.Ordinal))
                removableValue += value;
            else
                result[key] = value;
        }
        return result;
    }

    /// <summary>
    /// The removable value an estimate of this tag has to add: nothing for a model that learned full prices,
    /// it already contains what the parts were worth on the sales it was fit on.
    /// </summary>
    private long RemovableValueToAdd(string tag, long removableValue)
    {
        return tagsFitWithoutRemovables.Contains(tag) ? removableValue : 0;
    }

    /// <summary>
    /// Sets the price every sample is trained on and returns whether it is the price without the removable parts.
    /// That needs every sample to list its parts. A record from before they were features has them inside its
    /// price at an unknown value, so with one of those the tag is trained on full prices like before.
    /// </summary>
    private static bool PrepareLabels(List<FlipData> samples)
    {
        var withoutRemovables = samples.TrueForAll(s => s.RemovablesListed);
        foreach (var sample in samples)
        {
            // at least one coin so the logarithm exists
            sample.Label = withoutRemovables ? Math.Max(sample.SoldFor - sample.RemovableValue, 1) : sample.SoldFor;
        }
        return withoutRemovables;
    }

    /// <summary>
    /// Turns a sold auction into a training sample. The serving model is scored on it first.
    /// </summary>
    private FlipData CreateSample(string tag, ComplicatedFlip flip, Dictionary<string, int> featureIndex, List<FlipData> samples)
    {
        var attributes = WithAttributeSum(WithoutRemovable(flip.AttributeValues, out var removableValue, out var removablesListed), out var attributeSum);
        RecordHeldOutError(tag, flip, attributes, featureIndex, RemovableValueToAdd(tag, removableValue));
        return new FlipData
        {
            Features = CreateFeatureVector(attributes, featureIndex, expandFeatureSpace: true, samples),
            SoldFor = SafeToFloat(flip.SoldFor),
            RemovableValue = SafeToFloat(removableValue),
            RemovablesListed = removablesListed,
            AttributeSum = SafeToFloat(attributeSum),
            HasCleanCost = flip.AttributeValues.ContainsKey("cleancost")
        };
    }

    /// <summary>
    /// Converts a model score into coins. Log models predict the offset from the centre their labels were shifted by.
    /// </summary>
    private double ScoreToPrice(string tag, float score)
    {
        return logLabelCenterByTag.TryGetValue(tag, out var center) ? Math.Exp(score + center) : score;
    }

    /// <summary>
    /// Scores a sale with the serving model before it becomes a training sample and remembers the relative error.
    /// Only a sale that ended after everything seen so far counts, and only against a model fit in this process:
    /// a replayed sale is already part of the training data, and a loaded model may have been trained on anything.
    /// </summary>
    private void RecordHeldOutError(string tag, ComplicatedFlip flip, Dictionary<string, long> attributes, Dictionary<string, int> featureIndex, long removableValue)
    {
        if (newestSaleByTag.TryGetValue(tag, out var newest) && flip.EndedAt <= newest)
            return;
        newestSaleByTag[tag] = flip.EndedAt;
        if (!tagsFitHere.Contains(tag) || !predictionEngines.TryGetValue(tag, out var engine) || engine is null || !modelVectorSizeByTag.TryGetValue(tag, out var vectorSize))
            return;

        var features = CreateFeatureVectorForPrediction(attributes, featureIndex, vectorSize);
        double price;
        lock (predictionSync)
        {
            price = ScoreToPrice(tag, engine.Predict(new FlipData { Features = features }).Score) + removableValue;
        }
        if (!double.IsFinite(price))
            return;

        if (!heldOutErrorsByTag.TryGetValue(tag, out var errors))
        {
            errors = new Queue<float>();
            heldOutErrorsByTag[tag] = errors;
        }
        errors.Enqueue((float)(Math.Abs(price - flip.SoldFor) / flip.SoldFor));
        if (errors.Count > HeldOutWindow)
            errors.Dequeue();
    }

    /// <summary>
    /// Builds the metrics of a freshly fit model: its in-sample fit and the held-out errors collected so far.
    /// </summary>
    private ModelMetrics BuildMetrics(string tag, double rmse, double rSquared)
    {
        if (!heldOutErrorsByTag.TryGetValue(tag, out var errors) || errors.Count == 0)
            return new ModelMetrics(rmse, rSquared);
        var sorted = errors.ToArray();
        Array.Sort(sorted);
        return new ModelMetrics(rmse, rSquared, sorted[sorted.Length / 2], sorted[(int)((sorted.Length - 1) * 0.9)], sorted.Length);
    }

    /// <summary>
    /// Sets the label every sample is trained on: the logarithm of its price, shifted by the logarithm of the
    /// median price, and returns that shift. An overpriced sale then weighs like a few sales instead of hundreds,
    /// and because boosting starts at zero a model that finds no split predicts the median instead of nothing.
    /// </summary>
    private static double PrepareTrainLabels(List<FlipData> samples)
    {
        var center = Math.Log(Median(samples.Select(s => s.Label)));
        foreach (var sample in samples)
            sample.TrainLabel = (float)(Math.Log(sample.Label) - center);
        return center;
    }

    /// <summary>
    /// Leaves out the sales no model should learn from: coin transfers through an item (far above what its
    /// attributes are usually worth) and junk prices (far below what the tag sells for). A price far below the
    /// attribute sum stays, it is what teaches the model that a sum overstates.
    /// </summary>
    private static List<FlipData> SelectTrainingSamples(List<FlipData> samples)
    {
        var junkBelow = Median(samples.Select(s => s.Label)) * JunkBelowMedianFraction;
        // only with a clean cost is the sum a valuation of the whole item
        var transferAbove = Median(samples.Where(s => s.HasCleanCost && s.AttributeSum > 0).Select(s => s.Label / s.AttributeSum)) * TransferAboveUsualRatio;
        var kept = samples.FindAll(s => s.Label >= junkBelow && !(s.HasCleanCost && s.Label > s.AttributeSum * transferAbove));
        return kept.Count > 0 ? kept : samples;
    }

    private static float Median(IEnumerable<float> values)
    {
        var sorted = values.ToArray();
        if (sorted.Length == 0)
            return 0;
        Array.Sort(sorted);
        return sorted[sorted.Length / 2];
    }

    private void EnsureFeatureExists(string key, Dictionary<string, int> featureIndex, List<FlipData> trainingList)
    {
        if (featureIndex.ContainsKey(key))
        {
            return;
        }

        featureIndex[key] = featureIndex.Count;

        foreach (var sample in trainingList)
        {
            if (sample.Features.Length == featureIndex.Count)
            {
                continue;
            }

            var resized = new float[featureIndex.Count];
            Array.Copy(sample.Features, resized, Math.Min(sample.Features.Length, resized.Length));
            sample.Features = resized;
        }
    }

    private void RefitModel(string tag, bool forcePersist = false)
    {
        if (!RelevantItems.Contains(tag))
            return; // irrelevant
        var now = DateTime.UtcNow;
        if (!forcePersist && lastRefitByTag.TryGetValue(tag, out var last) && (now - last) < TimeSpan.FromMinutes(5))
        {
            logger.LogDebug("Skipping on-demand refit for {Tag} (last refit {Elapsed} ago)", tag, now - last);
            return;
        }

        var hasFIndex = featureIndexByItem.TryGetValue(tag, out var fIndex);
        var hasList = trainingDataByItem.TryGetValue(tag, out var list);

        if (!hasFIndex || !hasList)
        {
            // No feature index or training data available yet
            return;
        }

        var featureCount = fIndex!.Count;
        if (featureCount == 0 || list!.Count < minSamplesForTraining)
        {
            models[tag] = null;
            lock (predictionSync)
            {
                predictionEngines.TryGetValue(tag, out var eng);
                eng?.Dispose();
                predictionEngines[tag] = null;
            }
            lastMetricsByItem[tag] = null;
            modelVectorSizeByTag.Remove(tag);
            logLabelCenterByTag.Remove(tag);
            tagsFitHere.Remove(tag);
            tagsFitWithoutRemovables.Remove(tag);
            return;
        }
        // mark that we're about to refit this tag to avoid concurrent/rapid re-fits
        lastRefitByTag[tag] = DateTime.UtcNow;

        var withoutRemovables = PrepareLabels(list);
        var training = SelectTrainingSamples(list);

        // Track the maximum sold price (label) in training data to cap unrealistic predictions
        var maxLabel = training.Max(s => s.Label);
        if (maxLabel > 0)
            maxTrainingLabelByTag[tag] = maxLabel;

        var schema = SchemaDefinition.Create(typeof(FlipData));
        schema[nameof(FlipData.Features)].ColumnType = new VectorDataViewType(NumberDataViewType.Single, featureCount);

        var labelCenter = PrepareTrainLabels(training);
        var dataView = mlContext.Data.LoadFromEnumerable(training, schema);

        // Use linear regression (SDCA) instead of FastForest since attributes have additive/linear effects
        // FastTree is a gradient boosted decision tree trainer that handles large feature values well
        // and can learn non-linear relationships between attributes and price
        // Configuration: balanced between accuracy and overfitting prevention
        // Adjust parameters based on sample size for better small-sample performance
        var minLeafSize = training.Count < 100 ? 1 : Math.Max(5, training.Count / 100);
        var numTrees = training.Count < 100 ? 50 : 100;

        var pipeline = mlContext.Regression.Trainers.FastTree(
            featureColumnName: nameof(FlipData.Features),
            labelColumnName: nameof(FlipData.TrainLabel),
            numberOfLeaves: 20,            // Moderate tree complexity
            minimumExampleCountPerLeaf: minLeafSize, // Adaptive: allow smaller leaves for small datasets
            numberOfTrees: numTrees,       // Adaptive: fewer trees for small datasets
            learningRate: 0.2              // Moderate learning rate
        );

        ITransformer? tagModel = null;
        double rmse = double.NaN, r2 = double.NaN;
        try
        {
            var fitted = pipeline.Fit(dataView);
            tagModel = fitted;
            lock (predictionSync)
            {
                predictionEngines.TryGetValue(tag, out var existing);
                existing?.Dispose();
                // use the same schema definition we used to create the IDataView so the Features vector has a fixed size
                predictionEngines[tag] = mlContext.Model.CreatePredictionEngine<FlipData, FlipPrediction>(tagModel, ignoreMissingColumns: false, schema, null);
            }

            models[tag] = tagModel;

            // Store the expected vector size for this model
            modelVectorSizeByTag[tag] = featureCount;
            logLabelCenterByTag[tag] = labelCenter;
            tagsFitHere.Add(tag);
            if (withoutRemovables)
                tagsFitWithoutRemovables.Add(tag);
            else
                tagsFitWithoutRemovables.Remove(tag);

            logger.LogDebug("Stored model vector size for {Tag}: {VectorSize} features", tag, featureCount);

            var metrics = mlContext.Regression.Evaluate(tagModel!.Transform(dataView), labelColumnName: nameof(FlipData.TrainLabel));
            rmse = metrics?.RootMeanSquaredError ?? double.NaN;
            r2 = metrics?.RSquared ?? double.NaN;
            var modelMetrics = BuildMetrics(tag, rmse, r2);
            lastMetricsByItem[tag] = modelMetrics;

            logger.LogInformation("Trained FastTree model for {Tag}: {SampleCount} samples, {FeatureCount} features, RMSE={Rmse:F2}, R²={R2:F3}, trees={Trees}, dropped={Dropped}, heldOutMedianError={HeldOutMedianError:F3}, heldOutP90Error={HeldOutP90Error:F3}, heldOutSales={HeldOutSales}",
                tag, list.Count, featureCount, rmse, r2, fitted.Model.TrainedTreeEnsemble.Trees.Count, list.Count - training.Count,
                modelMetrics.HeldOutMedianError, modelMetrics.HeldOutP90Error, modelMetrics.HeldOutSales);
            // mark last refit timestamp
            lastRefitByTag[tag] = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to train model for {Tag}", tag);
            ClearModelState(tag);
            return;
        }

        PersistModelAndMetadata(tag, tagModel, dataView, fIndex!, list!, rmse, r2, forcePersist, labelCenter);
    }

    /// <summary>
    /// Clears all state for a specific tag's model.
    /// </summary>
    private void ClearModelState(string tag)
    {
        models[tag] = null;
        lock (predictionSync)
        {
            predictionEngines.TryGetValue(tag, out var engine);
            engine?.Dispose();
            predictionEngines[tag] = null;
        }
        lastMetricsByItem[tag] = null;
        modelVectorSizeByTag.Remove(tag);
        logLabelCenterByTag.Remove(tag);
        tagsFitHere.Remove(tag);
        tagsFitWithoutRemovables.Remove(tag);
    }

    /// <summary>
    /// Persists the trained model to storage. Its metadata travels in front of it in the same blob: the feature
    /// order and the label centre describe this one model, and a shared metadata object could be overwritten by
    /// another instance (or another version of the service) between the two writes.
    /// </summary>
    private void PersistModelAndMetadata(string tag, ITransformer model, IDataView dataView,
        Dictionary<string, int> featureIndex, List<FlipData> trainingData,
        double rmse, double rSquared, bool forcePersist, double labelCenter)
    {
        try
        {
            var shouldPersist = forcePersist ||
                !lastPersistedByTag.TryGetValue(tag, out var lastPersist) ||
                (DateTime.UtcNow - lastPersist) > TimeSpan.FromDays(1);
            if (!shouldPersist)
                return;

            var meta = MessagePack.MessagePackSerializer.Serialize(new PersistMeta
            {
                FeatureNames = featureIndex.OrderBy(kv => kv.Value).Select(kv => kv.Key).ToArray(),
                SampleCount = trainingData.Count,
                Rmse = double.IsNaN(rmse) ? null : rmse,
                RSquared = double.IsNaN(rSquared) ? null : rSquared,
                LabelCenter = labelCenter,
                WithoutRemovables = tagsFitWithoutRemovables.Contains(tag)
            });
            using var ms = new System.IO.MemoryStream();
            ms.Write(BitConverter.GetBytes(meta.Length));
            ms.Write(meta);
            using (var modelStream = new System.IO.MemoryStream())
            {
                mlContext.Model.Save(model, dataView.Schema, modelStream);
                modelStream.WriteTo(ms);
            }
            ms.Position = 0;

            _ = persitance.SaveBlob(LogModelBlobPrefix + tag, ms);
            lastPersistedByTag[tag] = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to persist model for {Tag}", tag);
        }
    }

    private async Task LoadPersistedModelIfExists(string tag)
    {
        // Ensure we only try to load once per tag during service lifetime
        if (!loadedTags.Add(tag))
            return;

        try
        {
            var bundle = await LoadLogModelBundle(tag);
            var meta = bundle?.meta ?? await LoadLegacyMetadata(tag);
            if (meta is not null)
                ApplyPersistedMetadata(tag, meta);

            var modelStream = bundle?.model ?? await persitance.LoadBlob(LegacyModelBlobPrefix + tag);
            // a legacy model scores in coins and has no centre
            var labelCenter = bundle?.meta.LabelCenter;
            if (modelStream is not null)
            {
                var tagModel = mlContext.Model.Load(modelStream, out var schema);
                models[tag] = tagModel;
                tagsFitHere.Remove(tag);
                if (bundle?.meta.WithoutRemovables == true)
                    tagsFitWithoutRemovables.Add(tag);
                else
                    tagsFitWithoutRemovables.Remove(tag);
                if (labelCenter.HasValue)
                    logLabelCenterByTag[tag] = labelCenter.Value;
                else
                    logLabelCenterByTag.Remove(tag);
                lock (predictionSync)
                {
                    var inputSchema = SchemaDefinition.Create(typeof(FlipData));

                    // Try to read the feature vector size from the loaded model schema. If unavailable,
                    // fall back to the stored feature index size for this tag (if present) or 0.
                    int vectorSize = -1;
                    var col = schema.GetColumnOrNull(nameof(FlipData.Features));
                    if (col.HasValue && col.Value.Type is VectorDataViewType v)
                    {
                        vectorSize = v.Size;
                    }

                    if (vectorSize <= 0)
                    {
                        // fallback to feature index count if we have it
                        if (!featureIndexByItem.TryGetValue(tag, out var fIndex))
                        {
                            fIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                            featureIndexByItem[tag] = fIndex;
                        }
                        vectorSize = fIndex.Count;
                    }

                    inputSchema[nameof(FlipData.Features)].ColumnType = new VectorDataViewType(NumberDataViewType.Single, Math.Max(0, vectorSize));
                    predictionEngines[tag] = mlContext.Model.CreatePredictionEngine<FlipData, FlipPrediction>(tagModel, ignoreMissingColumns: false, inputSchema, null);

                    // Store the expected vector size for this loaded model
                    modelVectorSizeByTag[tag] = vectorSize;

                    logger.LogInformation("Loaded persisted model for {Tag} with {VectorSize} features", tag, vectorSize);
                }
            }
            else
            {
                // if there's no model on disk but we have enough in-memory data, refit and persist once
                if (featureIndexByItem.TryGetValue(tag, out var fIndex) && trainingDataByItem.TryGetValue(tag, out var list) && fIndex.Count > 0 && list.Count >= minSamplesForTraining)
                {
                    // upgrade to write lock to refit and persist
                    gate.EnterWriteLock();
                    try
                    {
                        RefitModel(tag, forcePersist: true);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Refit during load failed for {Tag}", tag);
                    }
                    finally
                    {
                        gate.ExitWriteLock();
                    }
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "No persisted model/meta for {Tag}", tag);
        }
    }

    /// <summary>
    /// Loads the model of a tag in the current format together with the metadata stored in front of it.
    /// </summary>
    private async Task<(PersistMeta meta, System.IO.Stream model)?> LoadLogModelBundle(string tag)
    {
        System.IO.Stream? bundle;
        try
        {
            bundle = await persitance.LoadBlob(LogModelBlobPrefix + tag);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "No persisted log model for {Tag}, trying the legacy one", tag);
            return null;
        }
        if (bundle is null)
            return null;

        using (bundle)
        {
            var length = new byte[sizeof(int)];
            bundle.ReadExactly(length);
            var meta = new byte[BitConverter.ToInt32(length)];
            bundle.ReadExactly(meta);
            var model = new System.IO.MemoryStream();
            await bundle.CopyToAsync(model);
            model.Position = 0;
            return (MessagePack.MessagePackSerializer.Deserialize<PersistMeta>(meta), model);
        }
    }

    /// <summary>
    /// Loads the metadata of a model stored before metadata travelled with the model. That shared object is only
    /// read: instances still running the previous version keep writing it together with their raw-coin models.
    /// </summary>
    private async Task<PersistMeta?> LoadLegacyMetadata(string tag)
    {
        try
        {
            var combinedStream = await persitance.LoadBlob("selflearning/meta/all");
            if (combinedStream is null)
                return null;
            var combined = MessagePack.MessagePackSerializer.Deserialize<Dictionary<string, PersistMeta>>(combinedStream);
            return combined != null && combined.TryGetValue(tag, out var meta) ? meta : null;
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "No persisted combined model/meta available");
            return null;
        }
    }

    private void ApplyPersistedMetadata(string tag, PersistMeta meta)
    {
        var fIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < meta.FeatureNames.Length; i++)
            fIndex[meta.FeatureNames[i]] = i;
        featureIndexByItem[tag] = fIndex;
        lastMetricsByItem[tag] = new ModelMetrics(meta.Rmse ?? double.NaN, meta.RSquared ?? double.NaN);
    }

    private static float ComputeBaseline(ComplicatedFlip flip)
    {
        if (flip.AttributeValues is null || flip.AttributeValues.Count == 0)
        {
            return flip.SoldFor > 0 ? SafeToFloat(flip.SoldFor) : 0f;
        }

        if (flip.AttributeValues.TryGetValue("cleancost", out var cleanCost) && cleanCost > 0)
        {
            return SafeToFloat(cleanCost);
        }

        var positiveValues = flip.AttributeValues.Values.Where(v => v > 0).ToArray();
        if (positiveValues.Length == 0)
        {
            return flip.SoldFor > 0 ? SafeToFloat(flip.SoldFor) : 0f;
        }

        return SafeToFloat(positiveValues.Average());
    }

    private static float SafeToFloat(double value)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            return 0f;
        }

        if (value >= float.MaxValue)
        {
            return float.MaxValue;
        }

        if (value <= float.MinValue)
        {
            return float.MinValue;
        }

        return (float)value;
    }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;
        lock (predictionSync)
        {
            foreach (var eng in predictionEngines.Values)
            {
                eng?.Dispose();
            }
            predictionEngines.Clear();
        }
        gate.Dispose();
    }

    public bool IsRelevantItem(string tag)
    {
        if (disposed)
            throw new ObjectDisposedException(nameof(SelfLearningFlipFinderService));
        if (string.IsNullOrWhiteSpace(tag))
            return false;
        return RelevantItems.Contains(tag);
    }

    [MessagePackObject]
    public sealed class PersistMeta
    {
        [Key(0)]
        public string[] FeatureNames { get; set; } = Array.Empty<string>();
        [Key(1)]
        public int SampleCount { get; set; }
        [Key(2)]
        public double? Rmse { get; set; }
        [Key(3)]
        public double? RSquared { get; set; }
        /// <summary>Offset of a log model's label, absent for a model that scores in coins</summary>
        [Key(4)]
        public double? LabelCenter { get; set; }
        /// <summary>Whether the labels were prices without the removable parts, absent for a model that learned full prices</summary>
        [Key(5)]
        public bool WithoutRemovables { get; set; }
    }

    private sealed class FlipData
    {
        public float[] Features { get; set; } = Array.Empty<float>();
        /// <summary>Price in coins the sample stands for, set on every refit by <see cref="PrepareLabels"/></summary>
        public float Label { get; set; }
        /// <summary>What the model is fit on, set on every refit by <see cref="PrepareTrainLabels"/></summary>
        public float TrainLabel { get; set; }
        [NoColumn]
        public float AttributeSum { get; set; }
        [NoColumn]
        public bool HasCleanCost { get; set; }
        [NoColumn]
        public float SoldFor { get; set; }
        [NoColumn]
        public float RemovableValue { get; set; }
        /// <summary>False for a record from before removable parts were features</summary>
        [NoColumn]
        public bool RemovablesListed { get; set; }
    }

    private sealed class FlipPrediction
    {
        public float Score { get; set; }
    }
}

public sealed record SelfLearningFlipEstimate(double EstimatedValue, double BaselineValue, bool ModelReady, int SampleCount, ModelMetrics? Metrics);

public sealed record SelfLearningFlipModelSnapshot(IReadOnlyCollection<string> FeatureNames, int SampleCount, ModelMetrics? Metrics);
