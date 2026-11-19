using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Prometheus;
using TrackerFlip = Coflnet.Sky.FlipTracker.Client.Model.ComplicatedFlip;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

/// <summary>
/// The AI finder as the sniper uses it. It lives in the separate SkyAIFinder repository; a build without that
/// repository uses <see cref="NoAiFinder"/> and finds no AI flips.
/// </summary>
public interface IAiFinder
{
    /// <summary>Which finder this build contains, logged once at startup</summary>
    string Variant { get; }

    /// <summary>Whether this instance finds AI flips at all</summary>
    bool Active { get; }

    /// <summary>Tags the finder has models for</summary>
    IEnumerable<string> ModelTags { get; }

    /// <summary>
    /// Starts valuing new auctions in the background: the sniper hands each tested auction over without waiting, flips go
    /// to <paramref name="publish"/> on the finder's own thread
    /// </summary>
    /// <param name="pause">While it returns true the finder does not value anything, for hosts that want every core while they process new auctions</param>
    void Start(Action<LowPricedAuction> publish, Func<bool>? pause = null);

    /// <summary>Learns from a sold auction</summary>
    Task AddSale(SaveAuction auction);

    /// <returns>The estimate, null when the finder has no model for the item (yet)</returns>
    Task<AiEstimate?> Estimate(SaveAuction auction);

    /// <summary>Retrains the model of one tag by replaying its sales since <paramref name="totalStart"/> through <paramref name="addSale"/></summary>
    Task Retrain(string tag, CancellationToken cancellationToken, DateTime totalStart, Func<SaveAuction, Task> addSale);

    /// <summary>Trains on flips the tracker stored</summary>
    Task TrainFromTracker(IEnumerable<TrackerFlip>? flips);

    /// <summary>One entry per model, for the stats endpoint</summary>
    IEnumerable<object> GetModelStats();

    Task PersistModel(string? tag);
}

/// <param name="Quality">How well the model priced recent sales it was not trained on</param>
/// <param name="Sample">What the estimate was made from, for logging</param>
public sealed record AiEstimate(double EstimatedValue, bool ModelReady, int SampleCount, object? Quality, object Sample);

/// <summary>The AI finder of a build without the SkyAIFinder repository: no flips, no models, no training</summary>
public sealed class NoAiFinder(string variant = NoAiFinder.VariantName) : IAiFinder
{
    public const string VariantName = "none (built without SkyAIFinder)";
    public const string DisabledName = "none (disabled by AI_FINDER:ENABLED)";
    public const string InvalidName = "none (invalid AI_FINDER configuration, see the error logged at start)";
    public string Variant => variant;
    public bool Active => false;
    public IEnumerable<string> ModelTags => [];
    public void Start(Action<LowPricedAuction> publish, Func<bool>? pause = null) { }
    public Task AddSale(SaveAuction auction) => Task.CompletedTask;
    public Task<AiEstimate?> Estimate(SaveAuction auction) => Task.FromResult<AiEstimate?>(null);
    public Task Retrain(string tag, CancellationToken cancellationToken, DateTime totalStart, Func<SaveAuction, Task> addSale) => Task.CompletedTask;
    public Task TrainFromTracker(IEnumerable<TrackerFlip>? flips) => Task.CompletedTask;
    public IEnumerable<object> GetModelStats() => [];
    public Task PersistModel(string? tag) => Task.CompletedTask;
}

/// <summary>Registers the AI finder of this build: SkyAIFinder when it was built in and is enabled, <see cref="NoAiFinder"/> otherwise</summary>
public static partial class AiFinderRegistration
{
    private static readonly Gauge VariantGauge = Metrics.CreateGauge("sky_ai_finder_variant",
        "Always 1, labelled with the AI finder variant of this instance and whether it finds AI flips (active 1) or not (0)", "variant", "active");

    /// <param name="trains">Whether this instance trains and stores models (the manager); others only load them</param>
    /// <param name="instance">Name carried on every flip of an edge instance, null centrally</param>
    /// <param name="report">Where the finder's summary lines go; the logger when null</param>
    /// <remarks>An invalid setting never stops the host: the reason is written once at error level and the finder stays off</remarks>
    public static IServiceCollection AddAiFinder(this IServiceCollection services, IConfiguration config, bool trains, string? instance = null,
        Action<string>? report = null)
    {
        var problem = ConfigurationProblem(config);
        if (problem != null)
            Console.Error.WriteLine($"Error: AI finder is off because its configuration is invalid: {problem}");
        var (active, variant) = Describe(config);
        VariantGauge.WithLabels(variant, active ? "1" : "0").Set(1);
        if (problem != null)
            return services.AddSingleton<IAiFinder>(new NoAiFinder(NoAiFinder.InvalidName));
        if (!IsEnabled(config))
            return services.AddSingleton<IAiFinder>(new NoAiFinder(NoAiFinder.DisabledName));
        return AddBuiltIn(services, config, trains, instance, report);
    }

    /// <summary>Whether <see cref="AddAiFinder"/> registers a finder that finds flips, and its name, for the startup log line</summary>
    public static (bool active, string variant) Describe(IConfiguration config)
    {
        if (ConfigurationProblem(config) != null)
            return (false, NoAiFinder.InvalidName);
        if (!IsEnabled(config))
            return (false, NoAiFinder.DisabledName);
        return (BuiltIn, BuiltInVariant);
    }

    /// <returns>Why the configuration cannot be used, null when it can (or the finder is switched off anyway)</returns>
    internal static string? ConfigurationProblem(IConfiguration config)
    {
        try
        {
            return IsEnabled(config) ? SettingsProblem(config) : null;
        }
        catch (Exception e) when (e is ArgumentException or FormatException or OverflowException)
        {
            return e.Message;
        }
    }

    /// <exception cref="ArgumentException">AI_FINDER:ENABLED is set to something else than true/1/yes/on or false/0/no/off</exception>
    internal static bool IsEnabled(IConfiguration config)
    {
        var value = config["AI_FINDER:ENABLED"];
        if (string.IsNullOrWhiteSpace(value))
            return true;
        return value.Trim().ToLowerInvariant() switch
        {
            "true" or "1" or "yes" or "on" => true,
            "false" or "0" or "no" or "off" => false,
            _ => throw new ArgumentException($"AI_FINDER:ENABLED has the invalid value '{value}', expected true or false")
        };
    }

#if !AI_FINDER
    private const bool BuiltIn = false;
    private static string BuiltInVariant => NoAiFinder.VariantName;

    private static string? SettingsProblem(IConfiguration config) => null;

    private static IServiceCollection AddBuiltIn(IServiceCollection services, IConfiguration config, bool trains, string? instance, Action<string>? report)
        => services.AddSingleton<IAiFinder>(new NoAiFinder());
#endif
}
