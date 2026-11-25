using System;
using Coflnet.Sky.AIFinder;
using Coflnet.Sky.AIFinder.Model;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

/// <summary>The SkyAIFinder registration, compiled only when that repository is next to this one</summary>
public static partial class AiFinderRegistration
{
    private const bool BuiltIn = true;
    private static string BuiltInVariant => SkyAiFinderAdapter.VariantName;

    /// <summary>Reads the settings the way <see cref="AddBuiltIn"/> does; the caller turns a throw into "finder off"</summary>
    private static string? SettingsProblem(IConfiguration config)
    {
        var settings = AiFinderSettings.From(config.GetSection(AiFinderSettings.Section), trains: false);
        settings.RequireValuesFor(hostHasValues: true);
        return null;
    }

    private static IServiceCollection AddBuiltIn(IServiceCollection services, IConfiguration config, bool trains, string? instance, Action<string>? report)
    {
        // already validated by ConfigurationProblem, so this does not throw
        var settings = AiFinderSettings.From(config.GetSection(AiFinderSettings.Section), trains);
        settings.RequireValuesFor(hostHasValues: true);
        // the manager trains and stores the models, every other instance (workers, edge instances) only loads them
        services.AddSingleton<SniperRareCostTable>();
        services.AddHostedService(sp => sp.GetRequiredService<SniperRareCostTable>());
        services.AddSingleton<IModelBlobStore>(sp => new PersistenceModelBlobStore(sp.GetRequiredService<IPersitanceManager>()));
        services.AddSingleton<ISelfLearningFlipFinderService>(sp => new SelfLearningFlipFinderService(
            sp.GetRequiredService<ILogger<SelfLearningFlipFinderService>>(), sp.GetRequiredService<IModelBlobStore>(), settings.Model with { RareCosts = sp.GetRequiredService<SniperRareCostTable>() }, settings.Features().Signature));
        services.AddSingleton(sp => CreateExtractor(sp, settings));
        services.AddSingleton(sp => new AiFlipFinder(sp.GetRequiredService<ISelfLearningFlipFinderService>(), sp.GetRequiredService<AiFeatureExtractor>(),
            sp.GetRequiredService<ILogger<AiFlipFinder>>(), settings.Rule, instance));
        return services.AddSingleton<IAiFinder>(sp => new SkyAiFinderAdapter(sp.GetRequiredService<ISelfLearningFlipFinderService>(),
            sp.GetRequiredService<AiFlipFinder>(), sp.GetRequiredService<AiFeatureExtractor>(), sp.GetRequiredService<SniperService>(),
            sp.GetService<ItemDetails>(), sp.GetRequiredService<ILogger<SkyAiFinderAdapter>>(), settings, report,
            () => sp.GetService<IMayorService>() is { } mayors ? new MayorServiceLookup(mayors).GetMayor(DateTime.UtcNow) : null,
            costsReady: CostsReady(settings, sp)));
    }

    /// <summary>With the premium cap on, the finder waits for the first cost snapshot; without it nothing reads the costs</summary>
    private static Func<bool> CostsReady(AiFinderSettings settings, IServiceProvider services)
        => settings.Model.RareCostCap ? () => services.GetRequiredService<SniperRareCostTable>().HasSnapshot : () => true;

    /// <summary>
    /// The finder's feature extraction, reading this sniper's values. The mayor comes from the mayor service where the host has
    /// one, otherwise from what the training instance shared next to the models.
    /// </summary>
    private static AiFeatureExtractor CreateExtractor(IServiceProvider services, AiFinderSettings settings)
    {
        var model = services.GetRequiredService<ISelfLearningFlipFinderService>();
        IMayorLookup mayors = services.GetService<IMayorService>() is { } mayorService
            ? new MayorServiceLookup(mayorService)
            : new SharedContextMayors(() => model.Context);
        var values = new SniperItemValueSource(services.GetRequiredService<SniperService>(), services.GetService<ICraftCostService>());
        return new AiFeatureExtractor(values, mayors, settings.Features(), settings.Rule.NeedsPartsValue);
    }
}
