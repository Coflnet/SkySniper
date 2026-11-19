using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.AIFinder;
using Coflnet.Sky.AIFinder.Features;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Logging;
using TrackerFlip = Coflnet.Sky.FlipTracker.Client.Model.ComplicatedFlip;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

/// <summary>The AI finder of SkyAIFinder, fed with this sniper's values. Only compiled when that repository is next to this one.</summary>
public sealed class SkyAiFinderAdapter : IAiFinder, IDisposable
{
    private static readonly TimeSpan SummaryInterval = TimeSpan.FromHours(1);
    private static readonly TimeSpan ContextInterval = TimeSpan.FromMinutes(10);
    private readonly ISelfLearningFlipFinderService model;
    private readonly AiFeatureExtractor extractor;
    private readonly SniperService sniper;
    private readonly ItemDetails? itemDetails;
    private readonly ILogger<SkyAiFinderAdapter> logger;
    private readonly AiFinderSettings settings;
    private readonly Action<string> report;
    private readonly Func<string?> currentMayor;
    private readonly AiFlipWorker worker;
    private readonly List<Timer> timers = new();
    private int started;

    /// <param name="report">Where the summary lines go; the logger when null</param>
    /// <param name="currentMayor">The mayor the training instance shares with the instances that only load models</param>
    public SkyAiFinderAdapter(ISelfLearningFlipFinderService model, AiFlipFinder flipFinder, AiFeatureExtractor extractor, SniperService sniper,
        ItemDetails? itemDetails, ILogger<SkyAiFinderAdapter> logger, AiFinderSettings settings, Action<string>? report = null, Func<string?>? currentMayor = null)
    {
        this.model = model;
        this.extractor = extractor;
        this.sniper = sniper;
        this.itemDetails = itemDetails;
        this.logger = logger;
        this.settings = settings;
        this.report = report ?? (line => logger.LogInformation("{Line}", line));
        this.currentMayor = currentMayor ?? (() => null);
        worker = new AiFlipWorker(flipFinder.Check, model.IsRelevantItem, logger, settings.Worker);
    }

    public static string VariantName => "SkyAIFinder " + typeof(AiFlipFinder).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

    public string Variant => VariantName;

    public bool Active => true;

    public IEnumerable<string> ModelTags => model.GetModelStats().Keys;

    public void Start(Action<LowPricedAuction> publish, Func<bool>? pause = null)
    {
        if (Interlocked.Exchange(ref started, 1) == 1)
            return;
        report($"AI finder settings: {settings.Describe()}; sniper values asked: {extractor.AsksSniper}");
        if (FeatureWriter.NumberFormatProblem() is { } problem)
            logger.LogWarning("AI finder: {Problem}", problem);
        worker.Start(publish, pause);
        sniper.AuctionTested += auction => worker.Enqueue(auction);
        model.StartLoading(pause, took => { report($"AI finder read the stored models in {took.TotalSeconds:0.0} s"); Summarize(); });
        timers.Add(new Timer(_ => Summarize(), null, SummaryInterval, SummaryInterval));
        if (model.Trains)
            timers.Add(new Timer(_ => ShareContext(), null, TimeSpan.Zero, ContextInterval));
    }

    private void Summarize() => report(AiFinderCounters.Summary(Variant, model.GetModelStats().Values.Count(s => s.ModelLoaded)));

    private void ShareContext() => ShareContext(model, currentMayor, logger);

    /// <summary>Shares the mayor with the loading instances; without a known mayor the last shared context stays</summary>
    internal static void ShareContext(ISelfLearningFlipFinderService model, Func<string?> currentMayor, ILogger logger)
    {
        try
        {
            if (currentMayor() is not { } mayor)
                return;
            model.ShareContextAsync(mayor).GetAwaiter().GetResult();
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "AI finder could not share its context");
        }
    }

    public Task AddSale(SaveAuction auction)
    {
        if (!model.Trains || !model.IsRelevantItem(auction.Tag))
            return Task.CompletedTask;
        return model.TrainAsync(extractor.Extract(auction));
    }

    public async Task<AiEstimate?> Estimate(SaveAuction auction)
    {
        if (!model.IsRelevantItem(auction.Tag))
            return null;
        var sample = extractor.Extract(auction);
        var estimate = await model.EstimateAsync(sample);
        if (estimate == null)
            return null;
        return new AiEstimate(estimate.EstimatedValue, estimate.ModelReady, estimate.SampleCount, estimate.HeldOut, sample);
    }

    public Task Retrain(string tag, CancellationToken cancellationToken, DateTime totalStart, Func<SaveAuction, Task> addSale)
    {
        if (!model.Trains || itemDetails == null)
            return Task.CompletedTask;
        var source = new HypixelSoldAuctionSource(new HypixelContext(), itemDetails.GetItemIdForTag(tag));
        return new TrainingReplay(source, addSale, logger).Run(tag, cancellationToken, totalStart);
    }

    public Task TrainFromTracker(IEnumerable<TrackerFlip>? flips) => model.TrainBatchAsync(flips?.Select(f => f.ToSample()) ?? []);

    public IEnumerable<object> GetModelStats() => model.GetModelStats().Values.Select(s => new
    {
        s.Tag,
        FeatureNames = s.FeatureNames,
        s.SampleCount,
        s.ModelLoaded,
        s.HeldOut,
        s.TrainedAt
    });

    public Task PersistModel(string? tag) => model.PersistModelAsync(tag);

    public void Dispose()
    {
        foreach (var timer in timers)
            timer.Dispose();
        worker.Dispose();
    }
}
