using System;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using ComplicatedFlip = Coflnet.Sky.FlipTracker.Client.Model.ComplicatedFlip;

namespace Coflnet.Sky.Sniper.Services;
#nullable enable

/// <summary>
/// The reference based estimate of one item next to the self-learning estimate, and which of the two is displayed
/// </summary>
public sealed record DisplayEstimateComparison(
    string Tag,
    PriceEstimate Reference,
    int ValuableModifiers,
    bool EasilyComparable,
    double? SelfLearningEstimate,
    double? SelfLearningUncapped,
    int SampleCount,
    ModelMetrics? Metrics,
    bool SelfLearningDisplayed)
{
    public long DisplayedMedian => SelfLearningDisplayed ? (long)SelfLearningEstimate!.Value : Reference.Median;
}

/// <summary>
/// Chooses the median shown to a player (<c>/price</c>): the reference based price for easily comparable items,
/// the self-learning estimate for the others. Flip finders do not use this.
/// </summary>
public class DisplayPriceEstimator
{
    /// <summary>Sales per day the matched reference bucket needs for its price to be trusted</summary>
    public const float MinComparableVolume = 1;
    /// <summary>More valuable modifiers than this have no comparable reference</summary>
    public const int MaxComparableValuableModifiers = 5;
    /// <summary>Coins a modifier or enchant has to be worth to count as valuable</summary>
    public const long ValuableModifierMinValue = 1_000_000;
    /// <summary>A model that was off by more than this on half of its held-out sales is not displayed</summary>
    public const double MaxHeldOutMedianError = 0.2;
    /// <summary>Held-out sales needed before the error says something about the model</summary>
    public const int MinHeldOutSales = 20;
    private const string CleanCostFeature = "cleancost";

    private readonly SniperService sniper;
    private readonly ISelfLearningFlipFinderService flipFinder;
    private readonly IMayorService? mayorService;
    private readonly ICraftCostService? craftCostService;

    public DisplayPriceEstimator(SniperService sniper, ISelfLearningFlipFinderService flipFinder, IMayorService? mayorService = null, ICraftCostService? craftCostService = null)
    {
        this.sniper = sniper;
        this.flipFinder = flipFinder;
        this.mayorService = mayorService;
        this.craftCostService = craftCostService;
    }

    /// <summary>
    /// The estimate to display: the reference based one, with the median replaced by the self-learning estimate
    /// when the item is not easily comparable and a trustworthy model is loaded.
    /// </summary>
    public PriceEstimate GetPrice(SaveAuction auction, bool includeSelfLearning = false)
    {
        var comparison = Compare(auction, estimateComparable: includeSelfLearning);
        if (!comparison.SelfLearningDisplayed && !(includeSelfLearning && comparison.SelfLearningEstimate.HasValue))
            return comparison.Reference;
        // the reference estimate can be a cached instance and is not to be modified
        var displayed = Copy(comparison.Reference);
        if (includeSelfLearning)
            displayed.SelfLearningEstimatedValue = comparison.SelfLearningEstimate ?? 0;
        if (comparison.SelfLearningDisplayed)
        {
            displayed.Median = comparison.DisplayedMedian;
            displayed.MedianKey += "+AI";
        }
        return displayed;
    }

    /// <param name="estimateComparable">also ask the model for an easily comparable item, whose estimate is not displayed</param>
    public DisplayEstimateComparison Compare(SaveAuction auction, bool estimateComparable = true)
    {
        var reference = sniper.GetPrice(auction);
        if (reference == null || !flipFinder.IsRelevantItem(auction.Tag))
            return new(auction?.Tag ?? string.Empty, reference!, 0, true, null, null, 0, null, false);

        var valuable = CountValuableModifiers(auction);
        // without a bucket for exactly this item the volume is that of another item
        var hasOwnBucket = reference.Median > 0 && reference.MedianKey != null && reference.MedianKey.StartsWith(reference.ItemKey ?? "-");
        var comparable = hasOwnBucket && reference.Volume >= MinComparableVolume && valuable <= MaxComparableValuableModifiers;
        if (comparable && !estimateComparable)
            return new(auction.Tag, reference, valuable, comparable, null, null, 0, null, false);

        var flip = auction.ToComplicatedFlip(includeBreakdown: true, sniper: sniper, mayorService: mayorService, craftCostService: craftCostService);
        var estimate = flipFinder.EstimateWithLoadedModel(flip);
        if (estimate == null || estimate.EstimatedValue <= 0)
            return new(auction.Tag, reference, valuable, comparable, null, null, estimate?.SampleCount ?? 0, estimate?.Metrics, false);

        var capped = CapAtAttributeSum(flip, estimate.EstimatedValue);
        return new(auction.Tag, reference, valuable, comparable, capped, estimate.EstimatedValue, estimate.SampleCount, estimate.Metrics,
            !comparable && IsTrustworthy(estimate.Metrics));
    }

    /// <summary>
    /// A pricing key lists the five most valuable upgrades. What did not fit is only known as a sum,
    /// so it counts as one more valuable modifier when it is worth as much as one.
    /// </summary>
    private int CountValuableModifiers(SaveAuction auction)
    {
        // what is taken off a key is scaled down by a low bid, the count must not depend on the listing price
        var key = sniper.ValueKeyForTest(new SaveAuction(auction) { HighestBidAmount = 0, FlatenedNBT = auction.FlatenedNBT, Enchantments = auction.Enchantments });
        var listed = key.ValueBreakdown.Where(e => !e.IsEstimate && e.Value >= ValuableModifierMinValue).ToList();
        // a listed upgrade that was taken off the key is part of the subtracted sum as well
        var listedOffKey = listed.Where(e => !key.Key.Enchants.Contains(e.Enchant) && !key.Key.Modifiers.Contains(e.Modifier)).Sum(e => e.Value);
        var unlisted = key.SubstractedValue - listedOffKey >= ValuableModifierMinValue;
        return listed.Count + (unlisted ? 1 : 0);
    }

    /// <summary>
    /// The same bound the AI flip finder applies: an item is not worth more than its clean cost plus everything on it.
    /// Without a clean cost the sum does not value the whole item and can not bound it.
    /// </summary>
    private static double CapAtAttributeSum(ComplicatedFlip flip, double estimate)
    {
        if (!flip.AttributeValues.ContainsKey(CleanCostFeature))
            return estimate;
        var attributeSum = flip.AttributeValues
            .Where(a => !a.Key.StartsWith("candyUsed:", StringComparison.OrdinalIgnoreCase))
            .Sum(a => a.Value);
        return Math.Min(estimate, attributeSum);
    }

    private static bool IsTrustworthy(ModelMetrics? metrics)
    {
        return metrics == null || metrics.HeldOutSales < MinHeldOutSales || !(metrics.HeldOutMedianError > MaxHeldOutMedianError);
    }

    private static PriceEstimate Copy(PriceEstimate source) => new()
    {
        Lbin = source.Lbin,
        SLbin = source.SLbin,
        Median = source.Median,
        Volume = source.Volume,
        LbinKey = source.LbinKey,
        MedianKey = source.MedianKey,
        ItemKey = source.ItemKey,
        Volatility = source.Volatility,
        AvgSellTime = source.AvgSellTime,
        LastSale = source.LastSale,
        SelfLearningEstimatedValue = source.SelfLearningEstimatedValue
    };
}
