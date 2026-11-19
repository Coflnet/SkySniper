using System.Collections.Generic;
using Coflnet.Sky.Sniper.Services;

namespace Coflnet.Sky.Sniper.Models;

/// <summary>
/// The sniper's values for one auction beyond its lookup key, for consumers that price items themselves
/// </summary>
/// <param name="AllModifiers">Every valued modifier of the item, highest value first, not only the ones the lookup key keeps</param>
/// <param name="ValueSubstracted">Value of the modifiers the lookup key leaves out</param>
/// <param name="RemovableValue">Removable items (pet items, drill and rod parts) and gems at their full price</param>
/// <param name="CleanPrice">Price of the item of this tier without modifiers, 0 if unknown</param>
/// <param name="Median">Median of the lookup key, 0 if unknown</param>
/// <param name="LowestBin">Lowest BIN of the lookup key, 0 if none</param>
public sealed record ItemValueDetails(
    IReadOnlyList<SniperService.RankElem> AllModifiers,
    long ValueSubstracted,
    long RemovableValue,
    long CleanPrice,
    long Median,
    long LowestBin);
