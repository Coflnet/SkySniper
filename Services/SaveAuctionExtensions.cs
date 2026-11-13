using System;
using System.Collections.Generic;
using ComplicatedFlip = Coflnet.Sky.FlipTracker.Client.Model.ComplicatedFlip;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

public static class SaveAuctionExtensions
{
    // Small set of mayors relevant for pricing (kept in sync with AIFormattingService)
    private static readonly HashSet<string> RelevantMayors = new() { "scorpius", "derpy", "jerry", "diana", "aatrox", "marina" };
    // Modifiers the sniper values with a weight for ranking instead of coins, they only get a presence flag
    private static readonly HashSet<string> WeightOnlyModifiers = new() { "candyUsed", "pgems" };
    private const string TierBoost = "PET_ITEM_TIER_BOOST";
    /// <summary>
    /// Marks what can be taken off the item and sold on its own. Its value is known, so it is added to an estimate instead of learned.
    /// </summary>
    public const string RemovablePrefix = "removable:";
    /// <summary>
    /// Present on every flip that lists its removable parts, so one without parts can be told from an older record that never listed any.
    /// </summary>
    public const string RemovablesListedMarker = RemovablePrefix + "listed";

    /// <summary>
    /// Convert a SaveAuction to a ComplicatedFlip used by the self-learning service.
    /// - includeBreakdown: when true, uses the provided SniperService to compute a ValueBreakdown and include estimate flags and modifiers.
    /// - when false, a lightweight conversion is performed: numeric entries from FlatenedNBT and enchantments are added.
    /// </summary>
    public static ComplicatedFlip ToComplicatedFlip(this Coflnet.Sky.Core.SaveAuction auction, bool includeBreakdown = false, SniperService? sniper = null, IMayorService? mayorService = null, ICraftCostService? craftCostService = null, bool includeEnchantments = true)
    {
        var attrs = new Dictionary<string, long>();

        if (includeBreakdown)
        {
            if (sniper == null) throw new ArgumentNullException(nameof(sniper), "sniper is required when includeBreakdown is true");

            // the lookup key keeps only its five most valuable elements, the features need all of them
            var withBreakdown = sniper.FullValueBreakdown(auction);
            AddValuesOutsideOfKey(attrs, auction, sniper, withBreakdown.Key);
            long petBasePrice = 0;
            KeyValuePair<string, string>? tierPricedExp = null;
            // Use 1 as a presence indicator for categorical features (pet tier, mayor)
            // that don't have a meaningful numeric value.
            const long presenceFlag = 1L;

            foreach (var x in withBreakdown.ValueBreakdown)
            {
                string key;
                if (x.Enchant.Type != default)
                    key = $"{x.Enchant.Type}:{x.Enchant.Lvl}";
                else if (KillCounterValue.Keys.Contains(x.Modifier.Key))
                    key = x.Modifier.Key; // one feature per counter, its value rises with the bucket so a trend can be learned
                else if (!string.IsNullOrEmpty(x.Modifier.Key))
                    key = $"{x.Modifier.Key}:{x.Modifier.Value}";
                else
                    key = x.Reforge.ToString();

                // Use the actual estimated value instead of a sentinel flag.
                // Previously all estimates were set to 10B which caused the ML model
                // to overvalue items whose attributes were always estimated (no lookup data).
                // For candyUsed: the value from GetCandyPrice is a weight (min 10M) intended
                // as a pricing signal, not an actual coin value. Exclude it from the ML
                // feature set to prevent inflating attribute sums and predictions.
                // The same holds for pgems (flat 100M), the gems themselves are valued in "gems".
                if (WeightOnlyModifiers.Contains(x.Modifier.Key))
                {
                    // Use a small presence flag instead of the large weight so the ML model
                    // can still learn from the candy state without the inflated value.
                    attrs[key] = 1L;
                }
                else if (x.Modifier.Key == "exp" && sniper.TryGetTierExpValue(auction.Tag, withBreakdown.Key.Tier, x.Modifier, out var tierExpValue, out petBasePrice))
                {
                    // The breakdown prices exp from legendary buckets, which inflates the value
                    // (and the AI finder's attribute sum cap) of lower tier pets.
                    attrs[key] = tierExpValue;
                    tierPricedExp = x.Modifier;
                }
                else
                {
                    attrs[key] = x.Value;
                }
            }
            if(auction.Tag.StartsWith("PET_"))
                AddPetTierFlags(auction, attrs, presenceFlag, GetTierBoostValue(auction, sniper, tierPricedExp, craftCostService, presenceFlag));

            var mayor = mayorService?.GetMayor(TimeOfSale(auction))?.ToLowerInvariant();
            if (mayor != null && RelevantMayors.Contains(mayor))
                attrs["m:" + mayor] = presenceFlag;

            if (craftCostService != null && craftCostService.TryGetCost(auction.Tag, out var cost))
                attrs["cleancost"] = (long)cost;
            else if (petBasePrice > 0)
                attrs["cleancost"] = petBasePrice; // the level 1 pet of this tier is the clean item
        }
        else
        {
            throw new Exception("Lightweight conversion is not supported anymore. Always include breakdown.");
        }

        var id = Guid.TryParse(auction.Uuid, out var g) ? g : Guid.Empty;

        return new ComplicatedFlip
        {
            AuctionId = id,
            ItemTag = auction.Tag,
            EndedAt = auction.End,
            // include actual sold price so trainers receive labels
            SoldFor = auction.HighestBidAmount,
            AttributeValues = attrs
        };
    }

    /// <summary>
    /// Adds what the sniper values outside of the lookup key and its breakdown: removable parts and gems.
    /// </summary>
    private static void AddValuesOutsideOfKey(Dictionary<string, long> attrs, SaveAuction auction, SniperService sniper, Models.AuctionKey key)
    {
        attrs[RemovablesListedMarker] = 0;
        foreach (var part in sniper.RemovableItemBreakdown(auction))
        {
            // the tier boost changes the tier of the pet and is not a part with a value of its own
            if (part.Modifier.Value != TierBoost)
                attrs[$"{RemovablePrefix}{part.Modifier.Key}:{part.Modifier.Value}"] = part.Value;
        }
        var gemValue = sniper.GetGemValue(auction, key);
        if (gemValue > 0)
            attrs[RemovablePrefix + "gems"] = gemValue;
    }

    /// <summary>
    /// A sold auction ended when it was bought, a running one ends in the future and is valued as of now.
    /// </summary>
    private static DateTime TimeOfSale(SaveAuction auction)
    {
        var now = DateTime.UtcNow;
        return auction.End > now ? now : auction.End;
    }

    /// <summary>
    /// Flags the tier the pet has without its tier boost and adds the boost itself as separate feature,
    /// so a boosted pet is not learned (or estimated) as a pet of the higher tier.
    /// </summary>
    private static void AddPetTierFlags(SaveAuction auction, Dictionary<string, long> attrs, long presenceFlag, long tierBoostValue)
    {
        var tier = auction.Tier;
        if (HasTierBoost(auction))
        {
            tier = SniperService.ReduceRarity(tier);
            attrs[$"{SniperService.PetItemKey}:{SniperService.TierBoostShorthand}"] = tierBoostValue;
        }
        attrs["tier:" + tier] = presenceFlag;
    }

    private static bool HasTierBoost(SaveAuction auction)
    {
        return auction.FlatenedNBT?.TryGetValue("heldItem", out var heldItem) == true && heldItem == TierBoost;
    }

    /// <summary>
    /// The price of the tier boost item, at most what a real pet of the displayed tier is worth more
    /// than the pet without its boost so the attribute sum does not exceed that pet's.
    /// Only the presence flag if no price is known or the displayed tier is not worth more.
    /// </summary>
    private static long GetTierBoostValue(SaveAuction auction, SniperService sniper, KeyValuePair<string, string>? tierPricedExp, ICraftCostService? craftCostService, long presenceFlag)
    {
        if (!HasTierBoost(auction) || !sniper.TryGetPetItemPrice(TierBoost, out var value) || value <= 0)
            return presenceFlag;
        if (tierPricedExp == null
            || !sniper.TryGetTierExpValue(auction.Tag, SniperService.ReduceRarity(auction.Tier), tierPricedExp.Value, out var realExp, out var realBase)
            || !sniper.TryGetTierExpValue(auction.Tag, auction.Tier, tierPricedExp.Value, out var displayedExp, out var displayedBase))
            return value;
        long spread = displayedExp - realExp;
        if (craftCostService?.TryGetCost(auction.Tag, out _) != true)
            spread += displayedBase - realBase; // cleancost is the level 1 pet of the tier
        return Math.Max(Math.Min(value, spread), presenceFlag); // 0 would read as no boost
    }
}
