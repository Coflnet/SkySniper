using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.FlipTracker.Client.Model;
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
}

public sealed record ModelMetrics(double Rmse, double RSquared);

public sealed class SelfLearningFlipFinderService : ISelfLearningFlipFinderService, IDisposable
{
    private const int MinSamplesForTraining = 120;

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

    public SelfLearningFlipFinderService(ILogger<SelfLearningFlipFinderService> logger, IPersitanceManager persitance)
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.persitance = persitance ?? throw new ArgumentNullException(nameof(persitance));
        mlContext = new MLContext(seed: Environment.TickCount);

        // no eager restore; models are loaded on demand per item when training or estimating
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

            if (list.Count >= MinSamplesForTraining && fIndex.Count > 0)
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

            if (tagModel is null || !predictionEngines.TryGetValue(tag, out var tagEngine) || fIndex.Count == 0 || list.Count < MinSamplesForTraining)
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
        if (!featureIndexByItem.TryGetValue(tag, out var fIndex) || !trainingDataByItem.TryGetValue(tag, out var list))
        {
            // nothing to do
            return;
        }

        var featureCount = fIndex.Count;
        if (featureCount == 0 || list.Count < MinSamplesForTraining)
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

        var tagModel = pipeline.Fit(dataView);
        lock (predictionSync)
        {
            predictionEngines.TryGetValue(tag, out var existing);
            existing?.Dispose();
            predictionEngines[tag] = mlContext.Model.CreatePredictionEngine<FlipData, FlipPrediction>(tagModel);
        }

        models[tag] = tagModel;

    var metrics = mlContext.Regression.Evaluate(tagModel.Transform(dataView), labelColumnName: nameof(FlipData.Label));
    var rmse = metrics?.RootMeanSquaredError ?? double.NaN;
    var r2 = metrics?.RSquared ?? double.NaN;
    // store lightweight, serializable metrics
    lastMetricsByItem[tag] = new ModelMetrics(rmse, r2);
        logger.LogInformation("Self-learning flip finder retrained for {Tag} with {SampleCount} samples, RMSE {Rmse:F2}, R2 {R2:F3}", tag, list.Count, rmse, r2);

        // persist model and metadata asynchronously per-tag
        try
        {
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
            var metaStream = new System.IO.MemoryStream();
            MessagePack.MessagePackSerializer.Serialize(metaStream, meta);
            metaStream.Position = 0;
            _ = persitance.SaveBlob($"selflearning/meta/{tag}", metaStream);
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
            var metaStream = await persitance.LoadBlob($"selflearning/meta/{tag}");
            if (metaStream is not null)
            {
                var meta = MessagePack.MessagePackSerializer.Deserialize<PersistMeta>(metaStream);
                // restore feature index
                var fIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < meta.FeatureNames.Length; i++)
                    fIndex[meta.FeatureNames[i]] = i;
                featureIndexByItem[tag] = fIndex;
                // set sample count placeholder
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
            var modelStream = await persitance.LoadBlob($"selflearning/model/{tag}");
            if (modelStream is not null)
            {
                var tagModel = mlContext.Model.Load(modelStream, out var schema);
                models[tag] = tagModel;
                lock (predictionSync)
                {
                    predictionEngines[tag] = mlContext.Model.CreatePredictionEngine<FlipData, FlipPrediction>(tagModel);
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

    [System.Serializable]
    private sealed class PersistMeta
    {
        public string[] FeatureNames { get; set; } = Array.Empty<string>();
        public int SampleCount { get; set; }
        public double? Rmse { get; set; }
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
