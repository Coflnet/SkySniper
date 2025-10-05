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
            const long fullFlag = 10_000_000_000L;

            foreach (var x in withBreakdown.ValueBreakdown)
            {
                string key;
                if (x.Enchant.Type != default)
                    key = $"{x.Enchant.Type}:{x.Enchant.Lvl}";
                else if (!string.IsNullOrEmpty(x.Modifier.Key))
                    key = $"{x.Modifier.Key}:{x.Modifier.Value}";
                else
                    key = x.Reforge.ToString();

                long val = x.IsEstimate ? fullFlag : x.Value;
                attrs[key] = val;
            }

            var mayor = mayorService?.GetMayor(auction.End);
            if (mayor != null && RelevantMayors.Contains(mayor))
                attrs["m:" + mayor] = fullFlag;

            if (craftCostService != null && craftCostService.TryGetCost(auction.Tag, out var cost))
                attrs["cleancost"] = (long)cost;
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
            SoldFor = 0,
            AttributeValues = attrs
        };
    }

    // Fallback overload that accepts any object with SaveAuction-like properties. Uses reflection to extract the needed fields.
    public static ComplicatedFlip ToComplicatedFlip(object auctionObj, bool includeBreakdown = false, SniperService? sniper = null, IMayorService? mayorService = null, ICraftCostService? craftCostService = null, bool includeEnchantments = true)
    {
        if (auctionObj == null) throw new ArgumentNullException(nameof(auctionObj));
        // If it's the expected SaveAuction type, call the typed overload
        if (auctionObj is Coflnet.Sky.FlipTracker.Client.Model.SaveAuction sa)
            return ToComplicatedFlip(sa, includeBreakdown, sniper, mayorService, craftCostService, includeEnchantments);

        // For other types, perform a best-effort reflection-based extraction
        var attrs = new Dictionary<string, long>();

        try
        {
            var t = auctionObj.GetType();
            var flatProp = t.GetProperty("FlatenedNBT");
            if (flatProp != null)
            {
                var flat = flatProp.GetValue(auctionObj) as System.Collections.IDictionary;
                if (flat != null)
                {
                    foreach (System.Collections.DictionaryEntry kv in flat)
                    {
                        if (kv.Value is string s && long.TryParse(s, out var v))
                            attrs[kv.Key?.ToString() ?? string.Empty] = v;
                    }
                }
            }

            if (includeEnchantments)
            {
                var enchProp = t.GetProperty("Enchantments");
                if (enchProp != null)
                {
                    const long fullFlag = 10_000_000_000L;
                    var enchObj = enchProp.GetValue(auctionObj) as System.Collections.IEnumerable;
                    if (enchObj != null)
                    {
                        foreach (var e in enchObj)
                        {
                            if (e == null) continue;
                            var et = e.GetType();
                            var typeProp = et.GetProperty("Type");
                            var lvlProp = et.GetProperty("Lvl") ?? et.GetProperty("Level") ?? et.GetProperty("Lvl");
                            var typeVal = typeProp?.GetValue(e)?.ToString();
                            var lvlVal = lvlProp != null && int.TryParse(lvlProp.GetValue(e)?.ToString(), out var lv) ? lv : 0;
                            if (typeVal != null)
                            {
                                var key = $"{typeVal}:{lvlVal}";
                                if (!attrs.ContainsKey(key)) attrs[key] = fullFlag;
                            }
                        }
                    }
                }
            }
        }
        catch { }

        // extract common fields
        Guid id = Guid.Empty;
        DateTime end = default;
        string tag = string.Empty;
        try
        {
            var t = auctionObj.GetType();
            var uuidProp = t.GetProperty("Uuid");
            if (uuidProp != null)
            {
                var u = uuidProp.GetValue(auctionObj)?.ToString();
                if (!string.IsNullOrEmpty(u) && Guid.TryParse(u, out var g)) id = g;
            }
            var endProp = t.GetProperty("End") ?? t.GetProperty("EndedAt") ?? t.GetProperty("end");
            if (endProp != null && endProp.GetValue(auctionObj) is DateTime dt) end = dt;
            var tagProp = t.GetProperty("Tag");
            if (tagProp != null) tag = tagProp.GetValue(auctionObj)?.ToString() ?? string.Empty;
        }
        catch { }

        return new ComplicatedFlip
        {
            AuctionId = id,
            ItemTag = tag,
            EndedAt = end,
            SoldFor = 0,
            AttributeValues = attrs
        };
    }
}
