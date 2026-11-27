using System;
using System.Collections.Generic;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>
/// A pet holding the tier boost is priced as its own item: it is keyed at its displayed tier with the
/// <c>petItem=TIER_BOOST</c> modifier, the boost is neither subtracted when storing nor added back when valuing,
/// keys with and without the boost never value each other, and a boosted pet has no clean price.
/// </summary>
public partial class SniperService
{
    public const string TierBoostItemTag = "PET_ITEM_TIER_BOOST";

    /// <summary>True when the auction is a pet holding the tier boost.</summary>
    internal static bool HoldsTierBoost(SaveAuction auction)
    {
        var nbt = auction.FlatenedNBT;
        return nbt != null && nbt.TryGetValue("heldItem", out var held) && held == TierBoostItemTag;
    }

    internal static bool IsTierBoostModifier(KeyValuePair<string, string> modifier)
        => modifier.Key == PetItemKey && modifier.Value == TierBoostShorthand;

    /// <summary>True when the modifiers contain <c>petItem=TIER_BOOST</c>. Allocation free.</summary>
    internal static bool HasTierBoost(IReadOnlyList<KeyValuePair<string, string>> modifiers)
    {
        if (modifiers == null)
            return false;
        for (int i = 0; i < modifiers.Count; i++)
            if (IsTierBoostModifier(modifiers[i]))
                return true;
        return false;
    }

    /// <summary>True when exactly one of the keys carries the tier boost, so neither may value the other.</summary>
    internal static bool TierBoostDiffers(AuctionKey a, AuctionKey b)
        => HasTierBoost(a.Modifiers) != HasTierBoost(b.Modifiers);

    /// <summary>True for the held tier boost of a removable-item entry, which stays part of the key instead.</summary>
    private static bool IsHeldTierBoost(in RemovableItemKey item, string itemTag)
        => item.IsPetItem && itemTag == TierBoostItemTag;

    /// <summary>
    /// Makes <paramref name="modifiers"/> carry exactly one <c>petItem=TIER_BOOST</c> entry when the auction holds
    /// the boost and none otherwise. Runs after the key length cap so the boost is never priced into the breakdown
    /// and never dropped from the key.
    /// </summary>
    private static void SetTierBoostModifier(List<KeyValuePair<string, string>> modifiers, SaveAuction auction)
    {
        RemoveModifiersByValue(modifiers, TierBoostShorthand);
        if (HoldsTierBoost(auction))
            modifiers.Add(new(PetItemKey, TierBoostShorthand));
    }

    /// <summary>A pet reference key for exp pricing, carrying the tier boost when the priced pet does.</summary>
    private static AuctionKey ExpReferenceKey(List<KeyValuePair<string, string>> modifiers, Tier tier, bool tierBoost)
    {
        if (tierBoost)
            modifiers.Add(new(PetItemKey, TierBoostShorthand));
        return new AuctionKey(new(), ItemReferences.Reforge.Any, modifiers, tier, 1);
    }

    /// <summary>
    /// The stored clean price of the key's tier. A tier boosted pet has none (see <see cref="GetCleanItemPrice"/>), so
    /// the plain clean price of its tier never floors, caps or scales it.
    /// </summary>
    private static bool TryGetStoredCleanPrice(PriceLookup lookup, AuctionKey key, Tier tier, out long price)
    {
        price = 0;
        if (lookup?.CleanPricePerTier == null || key is null || HasTierBoost(key.Modifiers))
            return false;
        return lookup.CleanPricePerTier.TryGetValue(tier, out price);
    }

    /// <summary>
    /// Caps the value added for modifiers dropped from the key: on cheap items expensive modifiers add little. The
    /// base is the starting bid, lowered to twice the stored clean price when there is one. A tier boosted pet has
    /// no clean price, so its starting bid alone is the base (never uncapped). Other keys without a stored clean
    /// price keep their previous, uncapped behaviour.
    /// </summary>
    internal static long CapKeyMissing(long keyMissing, long startingBid, PriceLookup lookup, AuctionKey key)
    {
        if (keyMissing <= 0)
            return keyMissing;
        if (TryGetStoredCleanPrice(lookup, key, key.Tier, out var cleanPrice))
            return CapByBasePrice(keyMissing, Math.Min(startingBid, cleanPrice * 2));
        if (HasTierBoost(key.Modifiers))
            return CapByBasePrice(keyMissing, startingBid);
        return keyMissing;
    }

    private static long CapByBasePrice(long keyMissing, long basePrice)
    {
        var adjusted = Math.Min(keyMissing, Math.Max(basePrice * 3 - 100_000, basePrice));
        return adjusted > 0 ? adjusted : keyMissing;
    }

    private static long StoredCleanPriceOrZero(PriceLookup lookup, AuctionKey key)
        => key is not null && TryGetStoredCleanPrice(lookup, key, key.Tier, out var price) ? price : 0;
}
