using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.FlipTracker.Client.Api;
using ComplicatedFlip = Coflnet.Sky.FlipTracker.Client.Model.ComplicatedFlip;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper.Services;
#nullable enable

public class AIFormattingService
{
    private readonly SniperService sniper;
    private readonly ILogger<AIFormattingService> logger;
    private readonly IMayorService mayorService = null!;
    private readonly ITrackerApi trackerApi = null!;
    private readonly ICraftCostService craftCostService;
    private readonly ISelfLearningFlipFinderService flipFinder;

    public AIFormattingService(SniperService sniper, ILogger<AIFormattingService> logger, IMayorService mayorService, ITrackerApi trackerApi, ICraftCostService craftCostService, ISelfLearningFlipFinderService flipFinder)
    {
        this.sniper = sniper;
        this.logger = logger;
        this.mayorService = mayorService;
        this.trackerApi = trackerApi;
        this.craftCostService = craftCostService;
        this.flipFinder = flipFinder;
    }
    // only some mayors have an effect on relevant item prices
    private static readonly HashSet<string> RelevantMayors = new() { "scorpius", "derpy", "jerry", "diana", "aatrox", "marina" };
    private static readonly HashSet<string> RelevantItems = [
        "HYPERION",
        "PET_GOLDEN_DRAGON",
        "PET_ENDER_DRAGON",
        "TERMINATOR",
        "GIANTS_SWORD",
        "DIVAN_DRILL",
        "TITANIUM_DRILL_4",
        "DARK_CLAYMORE",
        "PET_SCATHA",
        "HELLFIRE_ROD",
        "ATOMSPLIT_KATANA",
        "WARDEN_HELMET",
        "POWER_WITHER_CHESTPLATE",
        "STARRED_MIDAS_SWORD",
        "POWER_WITHER_LEGGINGS",
        "SHADOW_FURY",
        "JUJU_SHORTBOW",
        "MIDAS_STAFF",
        "PET_ENDERMAN",
        "PET_BLACK_CAT",
        "STARRED_MIDAS_STAFF",
        "SPEED_WITHER_BOOTS",
        "ENDER_ARTIFACT",
        "WISE_WITHER_CHESTPLATE",
        "AXE_OF_THE_SHREDDED",
        "DIVAN_HELMET",
        "STARRED_DAEDALUS_AXE",
        "ENDER_RELIC",
        "WITHER_GOGGLES",
        "WISE_WITHER_LEGGINGS",
        "POWER_WITHER_BOOTS",
        "DIVAN_CHESTPLATE",
        "FERMENTO_CHESTPLATE",
        "CRIMSON_CHESTPLATE",
        "MELON_DICER_3",
        "CRIMSON_LEGGINGS",
        "DIVAN_BOOTS",
        "PET_FLYING_FISH",
        "DIVAN_LEGGINGS",
        "FERMENTO_HELMET",
        "FERMENTO_LEGGINGS",
        "PET_GRIFFIN",
        "LIVID_DAGGER",
        "CRIMSON_BOOTS"];
    public async Task AddSample(SaveAuction auction)
    {
        try
        {
            await InternalAddSample(auction);
        }
        catch (Exception e)
        {
            logger.LogError(e, "Failed to add sample");
        }
    }
    private async Task InternalAddSample(SaveAuction auction)
    {
        if (!RelevantItems.Contains(auction.Tag))
            return;
        // include full breakdown, mayor and cleancost when creating training sample
        var fullFlip = SaveAuctionExtensions.ToComplicatedFlip(auction, includeBreakdown: true, sniper: sniper, mayorService: mayorService, craftCostService: craftCostService);
        var attributeList = fullFlip.AttributeValues;
        var mayor = mayorService.GetMayor(auction.End);
        logger.LogInformation("Adding sample for {tag} with {mayor} mayor", auction.Tag, mayor);

        var complicatedFlip = new ComplicatedFlip
        {
            AuctionId = Guid.Parse(auction.Uuid),
            ItemTag = auction.Tag,
            EndedAt = auction.End,
            SoldFor = auction.HighestBidAmount,
            AttributeValues = new Dictionary<string, long>(attributeList)
        };

        SelfLearningFlipEstimate? estimate = null;
        try
        {
            var estimationInput = new ComplicatedFlip
            {
                AuctionId = complicatedFlip.AuctionId,
                ItemTag = complicatedFlip.ItemTag,
                EndedAt = complicatedFlip.EndedAt,
                SoldFor = 0,
                AttributeValues = new Dictionary<string, long>(attributeList)
            };
            estimate = await flipFinder.EstimateAsync(estimationInput);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to estimate flip value for {tag}", auction.Tag);
        }

        await trackerApi.SaveComplicatedFlipAsync(complicatedFlip);

        if (estimate is not null)
        {
            logger.LogInformation(
                "Flip estimator prediction for {tag}: {estimateValue:F0} baseline {baseline:F0} (ready: {ready}, samples: {sampleCount})",
                auction.Tag,
                estimate.EstimatedValue,
                estimate.BaselineValue,
                estimate.ModelReady,
                estimate.SampleCount);
        }

        try
        {
            await flipFinder.TrainAsync(complicatedFlip);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to train flip estimator for {tag}", auction.Tag);
        }
    }
}
#nullable disable
