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

            var withBreakdown = sniper.ValueKeyForTest(auction);
            long petBasePrice = 0;
            // Use 1 as a presence indicator for categorical features (pet tier, mayor)
            // that don't have a meaningful numeric value.
            const long presenceFlag = 1L;

            foreach (var x in withBreakdown.ValueBreakdown)
            {
                string key;
                if (x.Enchant.Type != default)
                    key = $"{x.Enchant.Type}:{x.Enchant.Lvl}";
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
                if (x.Modifier.Key == "candyUsed")
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
                }
                else
                {
                    attrs[key] = x.Value;
                }
            }
            if(auction.Tag.StartsWith("PET_"))
                AddPetTierFlags(auction, attrs, presenceFlag);

            var mayor = mayorService?.GetMayor(auction.End);
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
    /// Flags the tier the pet has without its tier boost and the boost itself as separate features,
    /// so a boosted pet is not learned (or estimated) as a pet of the higher tier.
    /// </summary>
    private static void AddPetTierFlags(SaveAuction auction, Dictionary<string, long> attrs, long presenceFlag)
    {
        var tier = auction.Tier;
        if (auction.FlatenedNBT?.TryGetValue("heldItem", out var heldItem) == true && heldItem == "PET_ITEM_TIER_BOOST")
        {
            tier = SniperService.ReduceRarity(tier);
            attrs[$"{SniperService.PetItemKey}:{SniperService.TierBoostShorthand}"] = presenceFlag;
        }
        attrs["tier:" + tier] = presenceFlag;
    }
}
