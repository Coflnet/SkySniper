using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

/// <summary>
/// AI finder features for conditions that lower what an item sells for.
/// They are read from the auction itself, so they do not depend on what the lookup key kept.
/// The values are a scale for the model and no coins, they are stored negative and never count towards the attribute sum cap.
/// </summary>
public static class ValueLoweringFeatures
{
    /// <summary>
    /// Price of the skin on a pet that had candy used. Collectors pay less of the skin premium for a candied pet,
    /// the more the skin is worth the more is lost.
    /// </summary>
    public const string CandiedSkin = "candiedskin";
    /// <summary>
    /// Year of a new year cake. Every year is its own <c>new_years_cake:YEAR</c> feature,
    /// the plain number lets the model learn that later years sell for less.
    /// </summary>
    public const string CakeYear = "cakeyear";

    private const string SkinFeaturePrefix = "skin:";

    public static bool IsValueLowering(string feature)
    {
        return feature == CandiedSkin || feature == CakeYear;
    }

    public static void AddTo(Dictionary<string, long> attrs, SaveAuction auction)
    {
        var nbt = auction.FlatenedNBT;
        if (nbt == null)
            return;
        if (nbt.ContainsKey("skin") && Number(nbt, "candyUsed") > 0)
            attrs[CandiedSkin] = -Math.Max(SkinValue(attrs), 1); // an unpriced skin still marks the candy
        var cakeYear = (long)Number(nbt, "new_years_cake");
        if (cakeYear > 0)
            attrs[CakeYear] = -cakeYear;
    }

    private static long SkinValue(Dictionary<string, long> attrs)
    {
        return attrs.FirstOrDefault(a => a.Key.StartsWith(SkinFeaturePrefix, StringComparison.Ordinal)).Value;
    }

    private static double Number(Dictionary<string, string> nbt, string key)
    {
        if (nbt.TryGetValue(key, out var raw) && double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
            return value;
        return 0;
    }
}
