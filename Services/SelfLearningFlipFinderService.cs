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
using Microsoft.ML.Trainers.FastTree;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

public interface ISelfLearningFlipFinderService
{
    Task TrainAsync(ComplicatedFlip flip, CancellationToken cancellationToken = default);
    Task<SelfLearningFlipEstimate> EstimateAsync(ComplicatedFlip flip, CancellationToken cancellationToken = default);
    SelfLearningFlipModelSnapshot GetSnapshot();
    IReadOnlyDictionary<string, SelfLearningFlipFinderService.ModelStats> GetModelStats();
}

public sealed record ModelMetrics(double Rmse, double RSquared);

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
    private bool disposed;

    public SelfLearningFlipFinderService(ILogger<SelfLearningFlipFinderService> logger, IPersitanceManager persitance, int minSamplesForTraining = 120)
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.persitance = persitance ?? throw new ArgumentNullException(nameof(persitance));
        mlContext = new MLContext(seed: Environment.TickCount);
        this.minSamplesForTraining = minSamplesForTraining;

        // no eager restore; models are loaded on demand per item when training or estimating
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
                featureIndexByItem.TryGetValue(tag, out var fIndex);
                models.TryGetValue(tag, out var model);
                lastMetricsByItem.TryGetValue(tag, out var metrics);
                var stat = new ModelStats(tag, fIndex?.Keys.ToArray() ?? Array.Empty<string>(), list?.Count ?? 0, model is not null, metrics);
                result[tag] = stat;
            }
            return result;
        }
        finally
        {
            gate.ExitReadLock();
        }
    }

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

        gate.EnterWriteLock();
        try
        {
            var tag = flip.ItemTag ?? "_global";
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

    public Task<SelfLearningFlipEstimate> EstimateAsync(ComplicatedFlip flip, CancellationToken cancellationToken = default)
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
            if (!trainingDataByItem.TryGetValue(tag, out var list) || !featureIndexByItem.TryGetValue(tag, out var fIndex))
            {
                return Task.FromResult(new SelfLearningFlipEstimate(baseline, baseline, false, 0, null));
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
                return Task.FromResult(new SelfLearningFlipEstimate(baseline, baseline, false, list.Count, lastMetricsByItem.GetValueOrDefault(tag)));
            }

            var attrs = new Dictionary<string, long>(flip.AttributeValues ?? new Dictionary<string, long>());
            var features = CreateFeatureVector(attrs, fIndex, expandFeatureSpace: false, list);
            FlipPrediction prediction;
            lock (predictionSync)
            {
                prediction = tagEngine!.Predict(new FlipData { Features = features });
            }
            var score = double.IsNaN(prediction.Score) || prediction.Score <= 0 ? baseline : prediction.Score;

            return Task.FromResult(new SelfLearningFlipEstimate(score, baseline, true, list.Count, lastMetricsByItem.GetValueOrDefault(tag)));
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

    private void RefitModel(string tag)
    {
        var hasFIndex = featureIndexByItem.TryGetValue(tag, out var fIndex);
        var hasList = trainingDataByItem.TryGetValue(tag, out var list);
        // debug diagnostics removed

        if (!hasFIndex || !hasList)
        {
            // nothing to do
            try { Console.WriteLine($"RefitModel early exit for '{tag}': missing fIndex or list"); } catch { }
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
            return;
        }
        var schema = SchemaDefinition.Create(typeof(FlipData));
        schema[nameof(FlipData.Features)].ColumnType = new VectorDataViewType(NumberDataViewType.Single, featureCount);

        var dataView = mlContext.Data.LoadFromEnumerable(list, schema);

        var pipeline = mlContext.Transforms.NormalizeMinMax(nameof(FlipData.Features))
            .Append(mlContext.Regression.Trainers.FastForest(new FastForestRegressionTrainer.Options
            {
                FeatureColumnName = nameof(FlipData.Features),
                LabelColumnName = nameof(FlipData.Label),
                NumberOfLeaves = Math.Clamp(featureCount * 2, 8, 256),
                NumberOfTrees = 200,
                MinimumExampleCountPerLeaf = 4,
                FeatureFraction = 0.8
            }));

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

            // model created

            var metrics = mlContext.Regression.Evaluate(tagModel!.Transform(dataView), labelColumnName: nameof(FlipData.Label));
            rmse = metrics?.RootMeanSquaredError ?? double.NaN;
            r2 = metrics?.RSquared ?? double.NaN;
            // store lightweight, serializable metrics
            lastMetricsByItem[tag] = new ModelMetrics(rmse, r2);
            logger.LogInformation("Self-learning flip finder retrained for {Tag} with {SampleCount} samples, RMSE {Rmse:F2}, R2 {R2:F3}", tag, list.Count, rmse, r2);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Training failed for {Tag}", tag);
            // ensure we clear any partial state
            models[tag] = null;
            lock (predictionSync)
            {
                predictionEngines.TryGetValue(tag, out var eng);
                eng?.Dispose();
                predictionEngines[tag] = null;
            }
            lastMetricsByItem[tag] = null;
            return;
        }

        // persist model and metadata asynchronously per-tag
        try
        {
            // persisting model
            using var ms = new System.IO.MemoryStream();
            mlContext.Model.Save(tagModel, dataView.Schema, ms);
            ms.Position = 0;
            _ = persitance.SaveBlob($"selflearning/model/{tag}", ms);

            var meta = new PersistMeta
            {
                FeatureNames = fIndex.OrderBy(kv => kv.Value).Select(kv => kv.Key).ToArray(),
                SampleCount = list.Count,
                Rmse = double.IsNaN(rmse) ? null : rmse,
                RSquared = double.IsNaN(r2) ? null : r2
            };

            // Persist all metas in a single blob to reduce IO operations
            try
            {
                // load existing combined meta blob
                var combinedMeta = new Dictionary<string, PersistMeta>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    var existing = persitance.LoadBlob("selflearning/meta/all").Result;
                    if (existing is not null)
                    {
                        combinedMeta = MessagePack.MessagePackSerializer.Deserialize<Dictionary<string, PersistMeta>>(existing);
                    }
                }
                catch { /* ignore, we'll overwrite */ }

                combinedMeta[tag] = meta;
                var outStream = new System.IO.MemoryStream();
                MessagePack.MessagePackSerializer.Serialize(outStream, combinedMeta);
                outStream.Position = 0;
                _ = persitance.SaveBlob("selflearning/meta/all", outStream);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to persist combined self-learning meta for {Tag}", tag);
            }
            // metrics already populated from evaluation above
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to persist self-learning model for {Tag}", tag);
        }
    }

    private async Task LoadPersistedModelIfExists(string tag)
    {
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
                    inputSchema[nameof(FlipData.Features)].ColumnType = new VectorDataViewType(NumberDataViewType.Single, inputSchema[nameof(FlipData.Features)] is not null ? ((VectorDataViewType)inputSchema[nameof(FlipData.Features)].ColumnType).Size : schema.GetColumnOrNull(nameof(FlipData.Features))?.Type is VectorDataViewType v ? v.Size : -1);
                    predictionEngines[tag] = mlContext.Model.CreatePredictionEngine<FlipData, FlipPrediction>(tagModel, ignoreMissingColumns: false, inputSchema, null);
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
