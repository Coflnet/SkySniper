using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using MessagePack;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Coflnet.Sky.Sniper.Services;
#nullable enable
public class PartialCalcService
{
    private ConcurrentDictionary<string, PriceLookup> Lookups;
    private ConcurrentDictionary<string, AttributeLookup> AttributeLookups = new();
    private PropertyMapper Mapper = new();
    private ICraftCostService CraftCostService = null!;
    private IMayorService mayorService = null!;
    private IPersitanceManager persitanceManager = null!;
    private ILogger<PartialCalcService> logger = null!;
    private double adjustRate = 0.01;
    private SniperService sniper;

    public IEnumerable<string> ItemKeys => Lookups.Keys;

    public PartialCalcService(SniperService sniper, ICraftCostService craftCostService, IMayorService mayorService, IPersitanceManager persitanceManager, ILogger<PartialCalcService> logger)
    {
        Lookups = sniper.Lookups;
        this.sniper = sniper;
        CraftCostService = craftCostService;
        this.mayorService = mayorService;
        this.persitanceManager = persitanceManager;
        this.logger = logger;
    }

    public Dictionary<string, Dictionary<object, double>> GetAttributeCosts(string tag)
    {
        if (!AttributeLookups.TryGetValue(tag, out var attribs))
            return new();
        // return copy
        return attribs.Values.ToDictionary(x => x.Key, x => x.Value.ToDictionary(y => y.Key, y => y.Value));
    }

    public class PartialResult
    {
        public long Price;
        public List<string>? BreakDown;
    }

    public PartialResult GetPrice(Item originalItem, bool includeBreakDown = false)
    {
        var result = new PartialResult();
        if (includeBreakDown)
            result.BreakDown = new();
        var breakDown = result.BreakDown;
        var item = new ItemBreakDown(originalItem);
        if (!Lookups.TryGetValue(item.OriginalItem.Tag, out var cleanItemLookup))
            return result;
        var attribs = AttributeLookups.GetOrAdd(item.OriginalItem.Tag, tag => new());

        result.Price = (long)GetValueOf(item.OriginalItem.Tag, attribs, item.Flatten, breakDown);

        return result;
    }

    public PartialResult GetPrice(SaveAuction auction, bool includeBreakDown = false)
    {
        var result = new PartialResult();
        if (includeBreakDown)
            result.BreakDown = new();
        var breakDown = result.BreakDown;
        var item = new ItemBreakDown(auction, mayorService.GetMayor(DateTime.UtcNow));
        if (!Lookups.TryGetValue(item.OriginalItem.Tag, out var cleanItemLookup))
            return result;
        var attribs = AttributeLookups.GetOrAdd(item.OriginalItem.Tag, tag => new());

        result.Price = (long)GetValueOf(item.OriginalItem.Tag, attribs, item.Flatten, breakDown);

        var gemValue = sniper.GetGemValue(auction, new());
        result.Price += gemValue;
        if (gemValue > 0)
            breakDown?.Add($"Gems: {gemValue}");

        return result;
    }

    private long GetCleanItemPrice(ItemBreakDown item, PriceLookup cleanItemLookup)
    {
        return 0;
        var key = DefaultForTier(item);
        if (!cleanItemLookup.Lookup.TryGetValue(key, out var cleanItem) || cleanItem.Price == 0)
            cleanItem = SniperService.FindClosest(cleanItemLookup.Lookup, key, 200).FirstOrDefault().Value;
        return cleanItem?.Price ?? 0;
    }

    public void AddSell(SaveAuction auction)
    {
        var item = new ItemBreakDown(auction, mayorService.GetMayor(auction.End));
        var attribs = AttributeLookups.GetOrAdd(auction.Tag, tag => new());
        var modifiers = item.Flatten;
        //if (adjustRate > 0.01 && Random.Shared.NextDouble() < 0.05 && modifiers.Count > 2)
        //    modifiers.Remove(modifiers.OrderBy(x => Random.Shared.Next()).First().Key);
        double estimation = GetValueOf(auction.Tag, attribs, modifiers);
        var difference = GetItemSellValue(auction) - sniper.GetGemValue(auction, new()) - estimation;
        var reduction = 4;
        if (modifiers.Count < 3)
            reduction = 2;
        // if(!auction.Tag.StartsWith("PET"))
        //     reduction *= 3;
        var perItemChange = difference / reduction * adjustRate;
        foreach (var mod in modifiers)
        {
            if (Mapper.TryGetDefinition(auction.Tag, mod.Key, out var def))
            {
                if (def.Behaviour == PropertyMapper.Behaviour.Exp)
                {
                    continue;
                }
            }
            var cost = attribs.Values.GetOrAdd(mod.Key, _ => new())
                .GetOrAdd(mod.Value, (k) =>
                {
                    throw new Exception("should not reach");
                });
            var percentOfdifference = Math.Abs(cost / estimation);
            if (mod.Key == "exp_expGroup" && (double)mod.Value == 4 && auction.Tag == "PET_BLUE_WHALE")
            {
                if (Random.Shared.NextDouble() < 0.1)
                    Console.WriteLine($"changing by {perItemChange * percentOfdifference} {percentOfdifference} {cost} est:{estimation} {perItemChange} on {auction.Uuid}");
            }
            var changeAmount = perItemChange * percentOfdifference;
            while (Math.Abs(changeAmount) > Math.Abs(cost) * adjustRate * 2)
            {
                // no one item should change the value by more than it is currently times the adjust factor - anti market manipulation
                changeAmount /= 2;
                if (changeAmount < 0)
                    break; // allow higher decreases
            }
            cost += changeAmount;
            if (cost < 0)
            {
                attribs.Values[mod.Key][mod.Value] = Math.Clamp(cost, -250_000_000, -10);
            }
            else
                attribs.Values[mod.Key][mod.Value] = Math.Clamp(cost, 10, 800_000_000);
        }
    }

    private static long GetItemSellValue(SaveAuction auction)
    {
        if (auction.Count > 1)
            return auction.HighestBidAmount / auction.Count;
        return auction.HighestBidAmount;
    }

    private double GetValueOf(string tag, AttributeLookup attribs, Dictionary<string, object> modifiers, List<string>? breakDown = null)
    {
        double costSum = 0d;
        foreach (var mod in modifiers.ToList())
        {
            if (Mapper.TryGetDefinition(tag, mod.Key, out var def))
            {
                if (def.Behaviour == PropertyMapper.Behaviour.Exp)
                {
                    var exp = Math.Min(def.Max, GetNumeric(mod));
                    if (exp == def.Max)
                    {
                        modifiers.Add(mod.Key + "_max", true);
                        costSum += GetPriceFor(mod.Key + "_max", true);
                        continue;
                    }
                    else
                    {
                        var expGroup = Math.Floor(exp / def.Max * 5);
                        modifiers.Add(mod.Key + "_expGroup", expGroup);
                        costSum += GetPriceFor(mod.Key + "_expGroup", expGroup);
                        continue;
                    }
                    if (exp == 0)
                        continue;
                    if (exp == double.NaN)
                        Console.WriteLine($"NaN {mod.Key} {mod.Value}");
                    var perExpCost = attribs.Values.GetOrAdd(mod.Key, _ => new())
                        .GetOrAdd(String.Empty, (k) => 1);


                    costSum += perExpCost * exp;
                    if (costSum == double.NaN)
                    {
                        Console.WriteLine($"NaN {mod.Key} {mod.Value}");
                        Task.Delay(10000).Wait();
                    }
                    breakDown?.Add($"{mod.Key}: {perExpCost} * {exp}");
                    continue;
                }
            }
            double cost = GetPriceFor(mod.Key, mod.Value);
            costSum += cost;
        }

        return Math.Max(costSum, 1);

        double GetPriceFor(string key, object value)
        {
            var cost = attribs.Values.GetOrAdd(key, _ => new())
                .GetOrAdd(value, (k) =>
                {
                    if (TryGetItemCost(key, value, out var price) && price > 0)
                        return price / 20;
                    if (key == "skin")
                    {
                        return 20_000_000;
                    }
                    if (key == "heldItem")
                    {
                        var client = new RestSharp.RestClient("https://sky.coflnet.com/api");
                        var request = new RestSharp.RestRequest($"item/price/{value}");
                        var response = client.Execute(request);
                        // use median from {"min":0,"median":0,"mean":0.0,"mode":0,"volume":0.0,"max":0}
                        var apiPrice = JsonConvert.DeserializeObject<Dictionary<string, double>>(response.Content)!["median"];
                        if (apiPrice > 0)
                        {
                            Lookups[key] = new PriceLookup() { Lookup = new(new Dictionary<AuctionKey, ReferenceAuctions>() { { new(), new() { Price = (long)apiPrice } } }) };
                            return apiPrice * 0.7;
                        }
                    }
                    if (key == "mayor")
                        return 1;
                    return 12000;
                });
            if (cost == double.NaN || cost < -1000000000 || cost > 1000000000)
            {
                Console.WriteLine($"NaN o {key} {value} {cost} {JsonConvert.SerializeObject(modifiers, Formatting.Indented)}");
                Console.WriteLine(JsonConvert.SerializeObject(attribs.Values.GetOrAdd(key, _ => new()), Formatting.Indented));
                Task.Delay(10000).Wait();
            }
            breakDown?.Add($"{key} {value}: {cost}");
            return cost;
        }
    }

    private static double GetNumeric(KeyValuePair<string, object> mod)
    {
        return GetNumeric(mod.Value);
    }
    private static double GetNumeric(object mod)
    {
        if (mod is string s)
        {
            if (double.TryParse(s.Replace(',', '.'), NumberStyles.Any, CultureInfo.InvariantCulture, out var d))
                return d;
            throw new Exception($"Could not parse string to number {mod}");
        }
        else if (mod is double d)
            return d;
        else if (mod is int i)
            return i;
        else if (mod is long l)
            return l;
        else if (mod is float f)
            return f;
        else if (mod is short sh)
            return sh;
        else if (mod is byte b)
            return b;
        else if (mod is decimal dec)
            return (double)dec;
        else if (mod is bool bo)
            return bo ? 1 : 0;
        else if (mod is Enum e)
            return Convert.ToInt64(e);
        else
            throw new Exception($"Unknown type {mod.GetType()}");
    }

    public void CapAtCraftCost()
    {
        foreach (var item in AttributeLookups)
        {
            foreach (var attrib in item.Value.Values)
            {
                foreach (var val in attrib.Value)
                {
                    var value = attrib.Value[val.Key];
                    if (value == 0)
                        value = 1.1;
                    if (TryGetItemCost(attrib.Key, val.Key, out double price))
                    {
                        if (price < val.Value && price > 200_000 && Random.Shared.NextDouble() < 0.1)
                            Console.WriteLine($"Capping {attrib.Key} {val.Key} at {price} from {val.Value}");
                        value = Math.Min(price * 1.05, val.Value);
                    }
                    else if (attrib.Key.StartsWith("ench.") || Constants.AttributeKeys.Contains(attrib.Key))
                    {
                        value = CapAtExponetialGrowth(attrib, val, value);
                    }
                    else if (attrib.Key == "tier")
                    {
                        // higher tier can't be cheaper
                        if (Enum.TryParse<Tier>(val.Key.ToString(), true, out var tier) && attrib.Value.TryGetValue((tier + 1).ToString(), out var higherTier))
                        {
                            if (higherTier < val.Value)
                            {
                                value = higherTier;
                            }
                        }
                        var minDay = SniperService.GetDay(DateTime.UtcNow) - 5;
                        var allOrdered = Lookups.GetValueOrDefault(item.Key)?.Lookup.Where(l => l.Key.Tier == tier).SelectMany(l => l.Value.References).Where(r => r.Day >= minDay).OrderBy(l => l.Price).ToList();
                        var totalCount = allOrdered?.Count() ?? 0;
                        var target = allOrdered?.Skip(totalCount / 20 + 2).FirstOrDefault();
                        if (target.HasValue && totalCount > 3 && target.Value.Price < val.Value)
                        {
                            logger.LogInformation($"Capping {attrib.Key} {val.Key} at {target.Value.Price} from {val.Value} on {item.Key}");
                            value = target.Value.Price;
                        }
                    }
                    if (value != 0)
                        attrib.Value[val.Key] = value;
                }
            }
        }
    }

    private static double CapAtExponetialGrowth(KeyValuePair<string, ConcurrentDictionary<object, double>> attrib, KeyValuePair<object, double> val, double value)
    {
        // each higher level is at most double the lower level
        var level = GetNumeric(val.Key);
        if (attrib.Value.TryGetValue((byte)(level + 1), out var higherVal))
        {
            if (higherVal < val.Value * 1.9 && higherVal > 100)
            {
                if (higherVal > 1_000_000)
                    Console.WriteLine($"Capping {attrib.Key} {val.Key} at {higherVal / 2} from {val.Value}");
                value = higherVal / 1.9;
            }
        }
        else if (attrib.Value.TryGetValue((byte)(level + 2), out var higherVal2lvl))
        {
            if (higherVal2lvl < val.Value * 4 && higherVal > 1)
            {
                if (higherVal > 1_000_000)
                    Console.WriteLine($"Capping {attrib.Key} {val.Key} at {higherVal / 4} from {val.Value}");
                value = higherVal / 3;
            }
        }
        if (attrib.Key.StartsWith("ench.telekinesis"))
            value = 100;
        return value;
    }

    private bool TryGetItemCost(string key, object val, out double price)
    {
        if (val is string s)
        {
            if (key == "modifier")
            {

                var cost = Mapper.GetReforgeCost(Enum.Parse<ItemReferences.Reforge>(s, true));
                if (cost.Item2 == 0)
                {
                    price = 0;
                    return false;
                }
                if (TryGetItemCost(cost.Item1, out var reforgeStonePrice))
                {
                    price = reforgeStonePrice + cost.Item2;
                    return true;
                }
                else
                {
                    // Console.WriteLine($"Not Found Reforge {key} {s} {cost.Item1}");
                }
            }

            if (TryGetItemCost(s, out var itemPrice))
            {
                // Console.WriteLine($"Found Val {key} {s} {lookup.Lookup.First().Value.Price}");
                price = itemPrice;
                return true;
            }
            else
            {
                //Console.WriteLine($"Not Found Val {key} {s}");
            }
        }

        if (key.StartsWith("ench.") && TryGetItemCost($"ENCHANTMENT_{key.Substring(5).ToUpper()}_{val}", out var enchPrice))
        {
            price = enchPrice;
            return true;
        }

        if (key == "rarity_upgrades")
        {
            price = 7_000_000;
            return true;
        }
        price = 0;
        return false;
    }

    private bool TryGetItemCost(string s, out double value)
    {
        if (Lookups.TryGetValue(s, out var lookup) && lookup?.Lookup != null && lookup.Lookup.Count > 0 && lookup.Lookup.First().Value?.Price > 0)
        {
            value = lookup.Lookup.First().Value.Price;
            return true;
        }
        return CraftCostService.TryGetCost(s, out value) && value > 0 && value < int.MaxValue;
    }

    private AuctionKey DefaultForTier(ItemBreakDown item)
    {
        return new AuctionKey()
        {
            Tier = Enum.Parse<Tier>(item.Flatten?.GetValueOrDefault("tier")?.ToString() ?? "COMMON"),
            Enchants = new(),
            Modifiers = new(),
            Reforge = ItemReferences.Reforge.Any,
            Count = 0 // because default for auctions
        };
    }

    internal void SetLearningRate(double v)
    {
        adjustRate = v;
    }

    internal Task Save()
    {
        return persitanceManager.SaveWeigths(AttributeLookups);
    }

    internal async Task Load()
    {
        try
        {
            AttributeLookups = await persitanceManager.GetWeigths();
            logger.LogInformation($"Loaded {AttributeLookups.Sum(s => s.Value.Values.Count)} partial weigths");
            foreach (var item in AttributeLookups)
            {
                foreach (var attrib in item.Value.Values)
                {
                    foreach (var val in attrib.Value.Keys)
                    {
                        if (val is string x)
                            continue;
                        if (!attrib.Value.TryGetValue(val.ToString() ?? "", out var _))
                            continue;
                        // remove invalid key
                        attrib.Value.Remove(val, out _);
                        logger.LogInformation($"Removed invalid key {item.Key} {attrib.Key} {val}");
                    }
                }
            }
        }
        catch (System.Exception e)
        {
            logger.LogInformation(e, "Could not load weigths");
        }
    }

    internal void Correct(string itemTag, Dictionary<string, Dictionary<object, double>> corrections)
    {
        foreach (var attrib in corrections)
        {
            foreach (var val in attrib.Value)
            {
                if (!AttributeLookups.GetOrAdd(itemTag, _ => new()).Values.TryGetValue(attrib.Key, out var dictionary))
                    continue;
                dictionary[val.Key] = val.Value;
                logger.LogInformation($"Corrected {itemTag} {attrib.Key} {val.Key} to {val.Value}");
            }
        }
    }
}

[MessagePackObject]
public class AttributeLookup
{
    [Key(0)]
    public ConcurrentDictionary<string, ConcurrentDictionary<object, double>> Values = new();


}

public class ItemBreakDown
{
    public Item OriginalItem;
    public Dictionary<string, object> Flatten;

    public ItemBreakDown(Item item)
    {
        this.OriginalItem = item;
        this.Flatten = NBT.FlattenNbtData(item.ExtraAttributes).GroupBy(x => x.Key).Select(x => x.First())
            .ToDictionary(x => x.Key, x => x.Value);
        foreach (var ench in item.Enchantments ?? new())
        {
            this.Flatten[$"ench.{ench.Key.ToLower()}"] = ench.Value;
        }
        RecordSpecialCount(item.Count);
        Preprocess();
    }

    private void RecordSpecialCount(byte count)
    {
        if (count == 64)
            this.Flatten["count"] = 64;
        else if (count >= 32)
            this.Flatten["count"] = 32;
        else if (count >= 16)
            this.Flatten["count"] = 16;
        else if (count >= 8)
            this.Flatten["count"] = 8;
    }

    private void Preprocess()
    {
        Flatten.Remove("uid");
        Flatten.Remove("uuid");
        Flatten.Remove("spawnedFor");
        Flatten.Remove("bossId");
        Flatten.Remove("hideRightClick");
        Flatten.Remove("type");
        Flatten.Remove("active");
        Flatten.Remove("hideInfo");
        Flatten.Remove("stats_book");
        Flatten.Remove("candyUsed");
        Flatten.Remove("dungeon_skill_req");
        Flatten.Remove("item_durability");
        Flatten.Remove("compact_blocks");
        foreach (var attrib in Flatten.OrderBy(x => x.Key).ToList())
        {
            if (!Constants.AttributeKeys.Contains(attrib.Key))
                continue;
            var combo = (string)Flatten.GetValueOrDefault("atCombo", attrib.Key + "_");
            if (combo.EndsWith("_") && !combo.StartsWith(attrib.Key))
                combo += attrib.Key;
            Flatten["atCombo"] = combo;
        }

        Flatten.Remove("boss_tier");

        Flatten.Remove("champion_combat_xp");
        foreach (var item in Flatten.Where(f => f.Key.EndsWith(".uuid") || f.Key.EndsWith("_gem") || f.Key.EndsWith("_0")).ToList())
        {
            Flatten.Remove(item.Key);
        }
        if (Flatten.TryGetValue("ability_scroll", out var f) && f is string flatten)
        {
            foreach (var item in flatten.Split(' '))
            {
                Flatten[$"ability_scroll.{item}"] = 1;
            }
            Flatten.Remove("ability_scroll");
        }
        // normalize
        foreach (var item in Flatten.Keys)
        {
            var kv = new KeyValuePair<string, string>(item, Flatten[item].ToString() ?? "");
            var normalized = SniperService.NormalizeGeneral(kv, true, (long)Convert.ToDouble(Flatten.GetValueOrDefault("exp", 0)));
            if (normalized.Key != SniperService.Ignore.Key)
                Flatten[normalized.Key] = normalized.Value;
        }
    }

    public ItemBreakDown(SaveAuction auction, string mayor)
    {
        this.OriginalItem = new() { Tag = auction.Tag };
        if (auction.FlatenedNBT != null)
        {
            this.Flatten = auction.FlatenedNBT.Select(x =>
            {
                object value = x.Value;
                if (int.TryParse(x.Value.ToString(), out var intValue))
                    value = intValue;
                return new KeyValuePair<string, object>(x.Key, value);
            }).ToDictionary(x => x.Key, x => x.Value);
            if (!this.Flatten.ContainsKey("tier"))
                this.Flatten["tier"] = auction.Tier.ToString();
            if (!this.Flatten.ContainsKey("modifier") && auction.Reforge != ItemReferences.Reforge.None)
                this.Flatten["modifier"] = auction.Reforge.ToString().ToLower();
        }
        else
            this.Flatten = NBT.FlattenNbtData(auction.NbtData.Data).ToDictionary(x => x.Key, x => x.Value);
        foreach (var ench in auction.Enchantments ?? new())
        {
            this.Flatten[$"ench.{ench.Type.ToString().ToLower()}"] = ench.Level;
        }
        RecordSpecialCount((byte)auction.Count);
        Flatten["mayor"] = mayor;
        Preprocess();
    }
}
#nullable disable
