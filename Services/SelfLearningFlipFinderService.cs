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
/// </summary>
public sealed record ModelMetrics(double Rmse, double RSquared);

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
    // Serializes access to the combined metadata blob to prevent concurrent S3 writes
    private readonly object metaSaveLock = new();
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
                var attributes = new Dictionary<string, long>(flip.AttributeValues);
                var featureVector = CreateFeatureVector(attributes, featureIndex, expandFeatureSpace: true, sampleList);
                sampleList.Add(new FlipData
                {
                    Features = featureVector,
                    Label = SafeToFloat(flip.SoldFor)
                });
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

            var attrs = new Dictionary<string, long>(flip.AttributeValues);
            var featureVector = CreateFeatureVector(attrs, fIndex, expandFeatureSpace: true, list);
            list.Add(new FlipData
            {
                Features = featureVector,
                Label = SafeToFloat(flip.SoldFor)
            });

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

            var attrs = new Dictionary<string, long>(flip.AttributeValues ?? new Dictionary<string, long>());

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

            FlipPrediction prediction;
            lock (predictionSync)
            {
                prediction = tagEngine!.Predict(new FlipData { Features = features });
                logger.LogInformation("Prediction for {Tag}: {Score} (baseline {Baseline})", tag, prediction.Score, baseline);
            }
            var score = double.IsNaN(prediction.Score) || prediction.Score <= 0 ? baseline : prediction.Score;

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

            return Task.FromResult<SelfLearningFlipEstimate?>(new SelfLearningFlipEstimate(score, baseline, true, list.Count, lastMetricsByItem.GetValueOrDefault(tag)));
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

            vector[index] = SafeToFloat(value);
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
                vector[index] = SafeToFloat(value);
            }
        }

        return vector;
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
            return;
        }
        // mark that we're about to refit this tag to avoid concurrent/rapid re-fits
        lastRefitByTag[tag] = DateTime.UtcNow;

        // Track the maximum sold price (label) in training data to cap unrealistic predictions
        var maxLabel = list.Max(s => s.Label);
        if (maxLabel > 0)
            maxTrainingLabelByTag[tag] = maxLabel;

        var schema = SchemaDefinition.Create(typeof(FlipData));
        schema[nameof(FlipData.Features)].ColumnType = new VectorDataViewType(NumberDataViewType.Single, featureCount);

        var dataView = mlContext.Data.LoadFromEnumerable(list, schema);

        // Use linear regression (SDCA) instead of FastForest since attributes have additive/linear effects
        // FastTree is a gradient boosted decision tree trainer that handles large feature values well
        // and can learn non-linear relationships between attributes and price
        // Configuration: balanced between accuracy and overfitting prevention
        // Adjust parameters based on sample size for better small-sample performance
        var minLeafSize = list.Count < 100 ? 1 : Math.Max(5, list.Count / 100);
        var numTrees = list.Count < 100 ? 50 : 100;

        var pipeline = mlContext.Regression.Trainers.FastTree(
            featureColumnName: nameof(FlipData.Features),
            labelColumnName: nameof(FlipData.Label),
            numberOfLeaves: 20,            // Moderate tree complexity
            minimumExampleCountPerLeaf: minLeafSize, // Adaptive: allow smaller leaves for small datasets
            numberOfTrees: numTrees,       // Adaptive: fewer trees for small datasets
            learningRate: 0.2              // Moderate learning rate
        );

        ITransformer? tagModel = null;
        double rmse = double.NaN, r2 = double.NaN;
        try
        {
            tagModel = pipeline.Fit(dataView);
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

            logger.LogDebug("Stored model vector size for {Tag}: {VectorSize} features", tag, featureCount);

            var metrics = mlContext.Regression.Evaluate(tagModel!.Transform(dataView), labelColumnName: nameof(FlipData.Label));
            rmse = metrics?.RootMeanSquaredError ?? double.NaN;
            r2 = metrics?.RSquared ?? double.NaN;
            lastMetricsByItem[tag] = new ModelMetrics(rmse, r2);

            logger.LogInformation("Trained FastTree model for {Tag}: {SampleCount} samples, {FeatureCount} features, RMSE={Rmse:F2}, R²={R2:F3}",
                tag, list.Count, featureCount, rmse, r2);
            // mark last refit timestamp
            lastRefitByTag[tag] = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to train model for {Tag}", tag);
            ClearModelState(tag);
            return;
        }

        PersistModelAndMetadata(tag, tagModel, dataView, fIndex!, list!, rmse, r2, forcePersist);
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
    }

    /// <summary>
    /// Persists the trained model and metadata to storage.
    /// Combined metadata save is serialized with a dedicated lock to prevent
    /// concurrent S3 writes to the same object.
    /// </summary>
    private void PersistModelAndMetadata(string tag, ITransformer model, IDataView dataView,
        Dictionary<string, int> featureIndex, List<FlipData> trainingData,
        double rmse, double rSquared, bool forcePersist)
    {
        try
        {
            using var ms = new System.IO.MemoryStream();
            mlContext.Model.Save(model, dataView.Schema, ms);
            ms.Position = 0;

            var shouldPersist = forcePersist ||
                !lastPersistedByTag.TryGetValue(tag, out var lastPersist) ||
                (DateTime.UtcNow - lastPersist) > TimeSpan.FromDays(1);

            if (shouldPersist)
            {
                _ = persitance.SaveBlob($"selflearning/model/{tag}", ms);
                lastPersistedByTag[tag] = DateTime.UtcNow;
            }

            var meta = new PersistMeta
            {
                FeatureNames = featureIndex.OrderBy(kv => kv.Value).Select(kv => kv.Key).ToArray(),
                SampleCount = trainingData.Count,
                Rmse = double.IsNaN(rmse) ? null : rmse,
                RSquared = double.IsNaN(rSquared) ? null : rSquared
            };

            lock (metaSaveLock)
            {
                try
                {
                    var combinedMeta = LoadCombinedMetadata();
                    combinedMeta[tag] = meta;
                    if (shouldPersist)
                        SaveCombinedMetadata(combinedMeta);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to persist combined metadata for {Tag}", tag);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to persist model for {Tag}", tag);
        }
    }

    /// <summary>
    /// Loads all persisted metadata from storage.
    /// </summary>
    private Dictionary<string, PersistMeta> LoadCombinedMetadata()
    {
        try
        {
            var existing = persitance.LoadBlob("selflearning/meta/all").Result;
            if (existing is not null)
            {
                return MessagePack.MessagePackSerializer.Deserialize<Dictionary<string, PersistMeta>>(existing);
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Could not load combined metadata, starting fresh");
        }

        return new Dictionary<string, PersistMeta>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Saves all metadata to storage synchronously.
    /// Callers should hold <see cref="metaSaveLock"/> to prevent concurrent
    /// writes to the same S3 object (which would trigger
    /// "Reduce your concurrent request rate" errors).
    /// </summary>
    private void SaveCombinedMetadata(Dictionary<string, PersistMeta> metadata)
    {
        using var outStream = new System.IO.MemoryStream();
        MessagePack.MessagePackSerializer.Serialize(outStream, metadata);
        outStream.Position = 0;
        persitance.SaveBlob("selflearning/meta/all", outStream).GetAwaiter().GetResult();
    }

    private async Task LoadPersistedModelIfExists(string tag)
    {
        // Ensure we only try to load once per tag during service lifetime
        if (!loadedTags.Add(tag))
            return;

        try
        {
            try
            {
                var combinedStream = await persitance.LoadBlob("selflearning/meta/all");
                if (combinedStream is not null)
                {
                    var combined = MessagePack.MessagePackSerializer.Deserialize<Dictionary<string, PersistMeta>>(combinedStream);
                    if (combined != null && combined.TryGetValue(tag, out var meta))
                    {
                        var fIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                        for (int i = 0; i < meta.FeatureNames.Length; i++)
                            fIndex[meta.FeatureNames[i]] = i;
                        featureIndexByItem[tag] = fIndex;
                        trainingDataByItem.TryGetValue(tag, out var list);
                        if (meta.Rmse.HasValue || meta.RSquared.HasValue)
                        {
                            lastMetricsByItem[tag] = new ModelMetrics(meta.Rmse ?? double.NaN, meta.RSquared ?? double.NaN);
                        }
                        else
                        {
                            lastMetricsByItem[tag] = new ModelMetrics(double.NaN, double.NaN);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "No persisted combined model/meta available");
            }

            var modelStream = await persitance.LoadBlob($"selflearning/model/{tag}");
            if (modelStream is not null)
            {
                var tagModel = mlContext.Model.Load(modelStream, out var schema);
                models[tag] = tagModel;
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
    }

    private sealed class FlipData
    {
        public float[] Features { get; set; } = Array.Empty<float>();
        public float Label { get; set; }
    }

    private sealed class FlipPrediction
    {
        public float Score { get; set; }
    }
}

public sealed record SelfLearningFlipEstimate(double EstimatedValue, double BaselineValue, bool ModelReady, int SampleCount, ModelMetrics? Metrics);

public sealed record SelfLearningFlipModelSnapshot(IReadOnlyCollection<string> FeatureNames, int SampleCount, ModelMetrics? Metrics);
