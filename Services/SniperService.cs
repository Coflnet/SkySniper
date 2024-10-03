using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Core;
using Prometheus;
using Coflnet.Sky.Core.Services;
using Amazon.Runtime.Internal.Util;
using Newtonsoft.Json;
using System.Net;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper.Services
{
    public partial class SniperService
    {
        public const string PetItemKey = "petItem";
        public const string TierBoostShorthand = "TIER_BOOST";
        private const int SizeToKeep = 80;
        public const int PetExpMaxlevel = 4_225_538 * 6;
        private const int GoldenDragonMaxExp = 30_036_483 * 7;
        public static int MIN_TARGET = 200_000;
        public static DateTime StartTime = new DateTime(2021, 9, 25);
        private readonly ILogger<SniperService> logger;
        public ConcurrentDictionary<string, PriceLookup> Lookups = new ConcurrentDictionary<string, PriceLookup>(3, 2000);

        private readonly ConcurrentQueue<LogEntry> Logs = new ConcurrentQueue<LogEntry>();
        public static readonly DeferredLogger deferred = new();
        private readonly ConcurrentQueue<(SaveAuction, ReferenceAuctions, AuctionKeyWithValue)> LbinUpdates = new();
        private readonly AuctionKey defaultKey = new AuctionKey();
        public SniperState State { get; set; } = SniperState.LoadingLbin;
        private readonly PropertyMapper mapper = new();
        private readonly (string, int)[] EmptyArray = [];
        private readonly Dictionary<string, double> BazaarPrices = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (PriceEstimate result, DateTime addedAt)> ClosetLbinMapLookup = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (PriceEstimate result, DateTime addedAt)> ClosetMedianMapLookup = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (ReferencePrice result, DateTime addedAt)> HigherValueLbinMapLookup = new();

        private readonly Counter sellClosestSearch = Metrics.CreateCounter("sky_sniper_sell_closest_search", "Number of searches for closest sell");
        private readonly Counter closestMedianBruteCounter = Metrics.CreateCounter("sky_sniper_closest_median_brute", "Number of brute force searches for closest median");
        private readonly Counter closestLbinBruteCounter = Metrics.CreateCounter("sky_sniper_closest_lbin_brute", "Number of brute force searches for closest median");

        public event Action<LowPricedAuction> FoundSnipe;
        public event Action<PotentialCraftFlip> CappedKey;
        public event Action<(SaveAuction, AuctionKeyWithValue)> OnSold;
        public event Action OnSummaryUpdate;
        public readonly string ServerDnsName = Dns.GetHostName();
        public void MockFoundFlip(LowPricedAuction auction)
        {
            FoundSnipe?.Invoke(auction);
        }

        internal readonly string[] CrimsonArmors = new string[] { "CRIMSON_", "TERROR_", "AURORA_", "FERVOR_" };
        private readonly HashSet<string> IncludeKeys = new HashSet<string>()
        {
            "baseStatBoostPercentage", // has an effect on drops from dungeons, is filtered to only max level
            "dye_item",
            // "backpack_color", no difference anymore
            "party_hat_color",
            "party_hat_emoji",
            "color", // armour
            "model", // abicase
            // potion "level", // not engough impact
            // "item_tier", // mostly found on armor, unsure what it does
            "talisman_enrichment", // talismans can be enriched with additional stats
            "drill_part_engine",
            "drill_part_fuel_tank",
            "drill_part_upgrade_module", // low worth of normal omlet but can go up to 180m
            "divan_powder_coating",
            "ability_scroll", // applied to hyperions worth ~250m https://discord.com/channels/267680588666896385/1031668335731019886/1031668607479975976
            // magma armor is to cheap "magmaCubesKilled"
            "captured_player", // cake soul 
            "event", // year+eventtype
            "wood_singularity_count", // self explanatory
            "art_of_war_count", //       ^^
            "artOfPeaceApplied",
            "new_years_cake", // year of the cake
            "heldItem", // pet held item
            "skin", // cosmetic skins
            "candyUsed", // candy count
            "exp", // collected experience of pets
            "rarity_upgrades", // recomb
            "winning_bid", // price paid for midas
            "dungeon_item_level", "upgrade_level", // "stars"
            "farming_for_dummies_count",
            "unlocked_slots", // available gemstone slots
            "gemstone_slots", // old unlocked slots
            "zombie_kills", // slayer kills
            "spider_kills", // slayer kills
            "eman_kills", // slayer kills
            "expertise_kills", // kills for expertise enchant counting sea creatures
            "bow_kills", // huricane bow
            "raider_kills", // raiders axe
            "sword_kills",
            "blood_god_kills",
            "yogsKilled", // yog armor
            "mined_crops", // eg THEORETICAL_HOE_WARTS_3
            "blocksBroken", // promising (pick)axe
            "collected_coins", // crown of avarice
            "ethermerge",
            "edition", // great spook stuff
            "hpc", // hot potato books
            "thunder_charge", // Pulse ring modifer
            //"tuned_transmission", // aotv upgrade
            //"power_ability_scroll", // disabled as suggested by Coyu because comonly not worth 1m (up to 2m at most)
            "captured_player", // cake souls
            "handles_found", // NECRON ladder
            "RUNE_MUSIC",
            "RUNE_MEOW_MUSIC",
            "RUNE_DRAGON",
            "RUNE_TIDAL",
            "RUNE_GRAND_SEARING",
            "RUNE_ENCHANT",
            "RUNE_BARK_TUNES",
            "RUNE_BARK_SMITTEN",
            "RUNE_BARK_ICE_SKATES",
            "RUNE_SPELLBOUND",
            "RUNE_GRAND_FREEZING",
            "RUNE_PRIMAL_FEAR",
            "is_shiny", // cosmetic effect on wither armor ~5% drop chance on Master Mode 7
        };

        public static readonly HashSet<string> InvertedValueKey = new()
        {
            "edition",
            "new_years_cake",
            "candyUsed",
        };

        /// <summary>
        /// Keys containing itemTags that should be added separately
        /// </summary>
        private readonly HashSet<string> ItemKeys = new()
        {
            "drill_part_engine",
            "drill_part_fuel_tank",
            "drill_part_upgrade_module",
        };

        private static readonly Dictionary<string, short> ShardAttributes = new(){
            {"mana_pool", 1},
            {"breeze", 2},
            {"speed", 2},
            {"life_regeneration", 2}, // especially valuable in combination with mana_pool
            {"fishing_experience", 1}, // golem said lvl 1 important
            {"ignition", 2},
            {"blazing_fortune", 2},
            {"double_hook", 3},
            {"mana_regeneration", 2},
            {"mending", 3},
            {"dominance", 3},
            {"trophy_hunter", 5},
            {"fisherman", 5},
            {"magic_find", 2},
            {"veteran", 4},
            {"lifeline", 3} // low volume but valuable
            // life recovery 3
        };

        // combos that are worth more starting at lvl 1 because they are together
        private readonly KeyValuePair<string, string>[] AttributeCombos = [
            new("blazing_fortune", "fishing_experience"),
            new("life_regeneration", "mana_pool"),
            new("veteran", "mending"),
            new("veteran", "magic_find"),
            new("mana_regeneration", "mana_pool"),
            new("dominance", "speed"),
            new("mending", "dominance"),
            new("double_hook", "fishing_speed"),
            new("trophy_hunter", "fishing_speed"),
            new("dominance", "mana_pool"),
            new("breeze", "mana_pool"),
            new("dominance", "magic_find"),
            new("dominance", "veteran"),
            new("mending", "mana_pool"),
        ];

        private readonly KeyValuePair<List<string>, List<KeyValuePair<string, string>>>[] ItemSpecificAttribCombo =
        [
            new(["LAVA_SHELL_NECKLACE", "MOLTEN_BELT"], new (){new("lifeline", "mana_pool"), new("lifeline", "lifeline")}),
            new(new (){"TERROR_BOOTS", "TERROR_LEGGINGS", "TERROR_CHESTPLATE"}, new (){new("lifeline", "mana_pool")}),
            new(new (){"MAGMA_LORD_BOOTS", "MAGMA_LORD_LEGGINGS", "MAGMA_LORD_CHESTPLATE", "MAGMA_LORD_HELMET"},
                new (){new("blazing_fortune", "mana_pool"), new("blazing_fortune", "fishing_experience"),
                       new("blazing_fortune", "magic_find"), new("blazing_fortune","blazing_fortune")}),
            new(new (){"AURORA_BOOTS", "AURORA_LEGGINGS", "AURORA_CHESTPLATE", "AURORA_HELMET", // not high but still noticable
                    "CRIMSON_BOOTS", "CRIMSON_LEGGINGS", "CRIMSON_CHESTPLATE", "CRIMSON_HELMET"}, new (){new("veteran", "mana_regeneration")}),
            new(new (){"CRIMSON_BOOTS", "CRIMSON_LEGGINGS", "CRIMSON_CHESTPLATE", "CRIMSON_HELMET", "MOLTEN_BRACELET"},
                    new (){new("magic_find", "mending")}), // vitality = mending
            new(new(){"MOLTEN_BRACELET"}, new (){new("lifeline", "mana_pool")}),
            new(new(){"GAUNTLET_OF_CONTAGION"}, new (){new("veteran", "mana_regeneration"),new("veteran", "breeze"),new("veteran", "mana_pool")}),
        ];

        private readonly List<KeyValuePair<List<string>, List<string>>> AttributesToIgnoreOn = [
            new(["VELVET_TOP_HAT","CASHMERE_JACKET", "SATIN_TROUSERS", "OXFORD_SHOES"],["color"]) // random https://hypixel-skyblock.fandom.com/wiki/Seymour%27s_Special_Armor
        ];
        private readonly Dictionary<string, string[]> AttributeToIgnoreOnLookup = new();

        public readonly Dictionary<string, List<KeyValuePair<string, string>>> ItemSpecificAttributeComboLookup = new();
        public readonly ConcurrentDictionary<string, HashSet<string>> AttributeComboLookup = new();

        public void FinishedUpdate()
        {
            ProcessLbins();
            var removeBefore = DateTime.UtcNow.AddHours(-3);
            foreach (var item in HigherValueLbinMapLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                HigherValueLbinMapLookup.TryRemove(item.Key, out _);
            }
            removeBefore = DateTime.UtcNow.AddMinutes(-0.10);
            foreach (var item in ClosetLbinMapLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                ClosetLbinMapLookup.TryRemove(item.Key, out _);
            }
            removeBefore = DateTime.UtcNow.AddHours(-0.5);
            foreach (var item in ClosetMedianMapLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                ClosetMedianMapLookup.TryRemove(item.Key, out _);
            }
        }

        public void ProcessLbins()
        {
            var count = LbinUpdates.Count;
            while (LbinUpdates.TryDequeue(out var update))
            {
                var (auction, bucket, key) = update;
                var extraValue = GetGemValue(auction, key);
                var item = CreateReferenceFromAuction(auction, extraValue);
                if (bucket.Lbins == null)
                    bucket.Lbins = new();
                if (!bucket.Lbins.Contains(item))
                {
                    bucket.Lbins.Add(item);
                    bucket.Lbins.Sort(ReferencePrice.Compare);
                    if (bucket.Lbins.First().AuctionId == item.AuctionId)
                    {
                        logger.LogInformation($"New lowest lbin {auction.Uuid} {auction.StartingBid} from {bucket.Lbins.Skip(1).FirstOrDefault().Price}");
                    }
                }
                if (Lookups.TryGetValue(auction.Tag, out var lookup) && lookup.Category == Category.UNKNOWN)
                {
                    lookup.Category = auction.Category;
                }
            }
            logger.LogInformation($"Finished processing {count} lbin updates");
        }

        private readonly Dictionary<string, string> ModifierItemPrefixes = new()
        {
            {"drill_part_engine", String.Empty},
            {"drill_part_fuel_tank", String.Empty},
            {"drill_part_upgrade_module", String.Empty},
            {"skin", "PET_SKIN_"},
            {"dye_item", String.Empty},
            {"petItem", "PET_ITEM_"}
        };
        private readonly HypixelItemService itemService;
        private readonly ActivitySource activitySource;
        private readonly ICraftCostService craftCostService;
        private readonly Dictionary<Core.Enchantment.EnchantmentType, byte> MinEnchantMap = new();

        /** NOTES
yogsKilled - needs further be looked into
skeletorKills - to low volume to include 
farmed_cultivating - tells the state of the cultivating enchant (already taken care of)

*/
        /* select helper
SELECT l.AuctionId,l.KeyId,l.Value,a.StartingBid, a.HighestBidAmount,a.Uuid,a.Tag,a.End FROM `NBTLookups` l, Auctions a
where KeyId = 128
and auctionid > 305000000  
and AuctionId = a.Id  
ORDER BY l.`AuctionId`  DESC;

        */

        // stuff changing value by 10+M
        public static HashSet<string> VeryValuable = new HashSet<string>()
        {
            "upgrade_level", // lvl 8+ are over 10m
            "rarity_upgrades",
            "winning_bid",
            "exp",
            "color",
            "dye_item",
            "ethermerge",
            "unlocked_slots",
            "skin",
            "candyUsed",
            "GRAND_SEARING",
            "new_years_cake" // not that valuable but the only attribute
        };

        // 200m+
        public static HashSet<string> Increadable = new HashSet<string>()
        {
            "ability_scroll",
            "color"
        };

        public static HashSet<string> NeverDrop = new()
        {
            "virtual", // virtual keys for calculations
            "exp", // this helps with closest match 
            "party_hat_color", // closest to clean would mix them up
            "ability_scroll", // most expensive attribues in the game
            "candyUsed", // candy count on pets can't be accounted
            "new_years_cake" // not that valuable but the only attribute
        };

        public static KeyValuePair<string, string> Ignore { get; } = new KeyValuePair<string, string>(string.Empty, string.Empty);


        public SniperService(HypixelItemService itemService, ActivitySource activitySource, ILogger<SniperService> logger, ICraftCostService craftCostService)
        {

            this.FoundSnipe += la =>
            {
                if (la.Finder == LowPricedAuction.FinderType.SNIPER && (float)la.Auction.StartingBid / la.TargetPrice < 0.8 && la.TargetPrice > 1_000_000)
                    logger.LogInformation($"A: {la.Auction.Uuid} {la.Auction.StartingBid} -> {la.TargetPrice}  {KeyFromSaveAuction(la.Auction)}");
            };
            foreach (var item in AttributeCombos.ToList())
            {
                AttributeComboLookup.GetOrAdd(item.Key, a => new()).Add(item.Value);
                AttributeComboLookup.GetOrAdd(item.Value, a => new()).Add(item.Key);
                VeryValuable.Add(item.Key);
                VeryValuable.Add(item.Value);
            }
            foreach (var item in AttributeCombos)
            {
                IncludeKeys.Add(item.Key);
            }
            foreach (var elements in ItemSpecificAttribCombo)
            {
                foreach (var item in elements.Key)
                {
                    var lookup = ItemSpecificAttributeComboLookup.GetValueOrDefault(item, new());
                    lookup.AddRange(elements.Value);
                    ItemSpecificAttributeComboLookup[item] = lookup;
                }
            }
            foreach (var sample in AttributesToIgnoreOn)
            {
                foreach (var item in sample.Key)
                {
                    AttributeToIgnoreOnLookup[item] = sample.Value.ToArray();
                }
            }
            foreach (var item in Constants.AttributeKeys)
            {
                if (!ShardAttributes.ContainsKey(item))
                    ShardAttributes.Add(item, 9);
            }
            foreach (var item in ShardAttributes)
            {
                IncludeKeys.Add(item.Key);
            }

            foreach (var enchant in Enum.GetValues<Core.Enchantment.EnchantmentType>())
            {
                MinEnchantMap[enchant] = 6;
            }

            foreach (var item in Constants.RelevantEnchants)
            {
                MinEnchantMap[item.Type] = item.Level;
            }

            this.itemService = itemService;
            this.activitySource = activitySource;
            this.logger = logger;
            this.craftCostService = craftCostService;
        }

        public void SummaryUpdate()
        {
            OnSummaryUpdate?.Invoke();
        }

        public PriceEstimate GetPrice(SaveAuction auction)
        {
            if (auction == null || auction.Tag == null)
                return null;
            if (BazaarPrices.TryGetValue(auction.Tag, out var bazaar))
                return new() { Median = (long)bazaar };
            var tagGroup = GetAuctionGroupTag(auction.Tag);
            if ((tagGroup.tag.StartsWith("HOT_") || tagGroup.tag.StartsWith("BURNING_") || tagGroup.tag.StartsWith("FIERY_") || tagGroup.tag.StartsWith("INFERNAL_")) && CrimsonArmors.Any(tagGroup.tag.Contains))
            {
                tagGroup.tag = tagGroup.tag.Replace($"{tagGroup.tag.Split('_')[0]}_", "");
            }

            var result = new PriceEstimate();
            if (!Lookups.TryGetValue(tagGroup.Item1, out PriceLookup lookup))
            {
                return result;
            }
            var l = lookup.Lookup;
            if (auction.Category == Category.UNKNOWN)
            {
                auction.Category = lookup.Category;
            }
            var detailedKey = DetailedKeyFromSaveAuction(auction);
            var itemKey = detailedKey.GetReduced(0);
            result.ItemKey = itemKey.ToString();

            // add back gem value
            var gemVal = GetGemValue(auction, itemKey);
            if (l.TryGetValue(itemKey, out ReferenceAuctions bucket))
            {
                if (result.Lbin.AuctionId == default && bucket.Lbin.AuctionId != default)
                {
                    var lbinGemValue = gemVal;
                    if (itemKey.Modifiers.Any(m => m.Key == "pgems" && m.Value == "5"))
                    {// gems are already accounted for
                        lbinGemValue = 0;
                    }
                    result.Lbin = new(bucket.Lbin)
                    {
                        Price = bucket.Lbin.Price + lbinGemValue
                    };
                    result.LbinKey = itemKey.ToString();
                    result.SLbin = bucket.Lbins.Skip(1).FirstOrDefault();
                }
                if (result.Median == default && bucket.Price != default)
                {
                    AssignMedian(result, itemKey, bucket, gemVal);
                }
            }
            if (result.Median == default)
            {
                var now = DateTime.UtcNow;
                var res = ClosetMedianMapLookup
                            .GetOrAdd(((string, AuctionKey))(auction.Tag, itemKey),
                                      _ => GetEstimatedMedian(auction, result, l, detailedKey, gemVal, now));
                if (res.addedAt != now)
                {
                    result.Median = res.result.Median;
                    result.MedianKey = res.result.MedianKey;
                    result.Volume = res.result.Volume;
                }
            }
            if (result.Lbin.Price == default && l.Count > 0)
            {
                var now = DateTime.UtcNow;
                var res = ClosetLbinMapLookup.GetOrAdd(((string, AuctionKey))(auction.Tag, itemKey), a =>
                {
                    return ClosestLbin(auction, result, l, itemKey, now);
                });
                if (res.addedAt != now)
                {
                    result.Lbin = res.result.Lbin;
                    result.LbinKey = res.result.LbinKey;
                }
            }
            var lbinCap = HigherValueLbinMapLookup.GetOrAdd(((string, AuctionKey))(auction.Tag, itemKey), a =>
            {
                var higherValue = l.Where(k => k.Value.Lbin.Price != 0
                                    && IsHigherValue(auction.Tag, itemKey, k.Key) && k.Key.Reforge == itemKey.Reforge);
                var MaxValue = higherValue.OrderBy(b => b.Value.Lbin.Price).FirstOrDefault();
                if (MaxValue.Key == a.Item2)
                    return (default, DateTime.UtcNow); // best match is itself, skip
                return (MaxValue.Value?.Lbin ?? default, DateTime.UtcNow);
            });
            if (lbinCap.result.Price != 0 && result.Lbin.Price > lbinCap.result.Price)
            {
                result.Lbin = lbinCap.result;
                result.LbinKey += $"+HV";
            }
            // correct for combined items
            if (tagGroup.Item2 != 0)
            {
                result.Median += tagGroup.Item2;
                result.MedianKey += $"&comb";
            }
            return result;
        }


        private (PriceEstimate result, DateTime addedAt) GetEstimatedMedian(SaveAuction auction, PriceEstimate result, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, KeyWithValueBreakdown itemKey, long gemVal, DateTime now)
        {
            closestMedianBruteCounter.Inc();
            foreach (var c in FindClosest(l, itemKey, auction.Tag))
            {
                AssignMedian(result, c.Key, c.Value, gemVal);
                GetDifferenceSum(auction, result, itemKey, c, out var diffExp, out var changeAmount);
                if (changeAmount != 0)
                {
                    result.Median -= changeAmount;
                    result.MedianKey += diffExp;
                }
                foreach (var item in itemKey.Key.Modifiers.Where(m => Constants.AttributeKeys.Contains(m.Key)))
                {
                    // "scrap for parts"
                    var key = VirtualAttributeKey(item);
                    if (!l.TryGetValue(key, out var references) || references.Price == 0)
                        continue;
                    var median = references.Price;
                    if (result.Median < median)
                    {
                        result.Median = median;
                        result.MedianKey = key.ToString();
                    }
                }

                if (Random.Shared.NextDouble() < 0.05)
                    logger.LogInformation($"no match found for {auction.Tag} {itemKey.Key} options: {l.Count} {c.Key}");
                if (result.Median > 0)
                    break;
            }
            if (result.Median > 0)
            {
                // check lower value keys
                var lowerValue = l.Where(k => IsHigherValue(auction.Tag, k.Key, itemKey) && k.Key.Reforge == itemKey.Key.Reforge);
                var MaxValue = lowerValue
                    .OrderByDescending(b => b.Value.Price).FirstOrDefault();
                if (MaxValue.Value?.Price > result.Median)
                {
                    result.Median = MaxValue.Value.Price;
                    result.MedianKey += $"+HV-{MaxValue.Key}";
                    result.Median += itemKey.ValueBreakdown
                        .Where(m => !MaxValue.Key.Modifiers.Contains(m.Modifier) && !MaxValue.Key.Enchants.Contains(m.Enchant))
                        .Sum(m => m.IsEstimate ? m.Value / 20 : m.Value) / 9;
                }
            }
            return (result, now);
        }

        private (PriceEstimate result, DateTime addedAt) ClosestLbin(SaveAuction auction, PriceEstimate result, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKeyWithValue itemKey, DateTime now)
        {
            closestLbinBruteCounter.Inc();
            var closest = GetClosestLbins(l, itemKey).FirstOrDefault();
            if (closest.Key != default)
            {
                result.Lbin = closest.Value.Lbin;
                result.LbinKey = closest.Key.ToString();

                GetDifferenceSum(auction, result, itemKey, closest, out var diffExp, out var changeAmount);
                if (changeAmount != 0)
                {
                    var lbinPrice = result.Lbin.Price - changeAmount;
                    if (lbinPrice < 0)
                        lbinPrice = result.Lbin.Price;
                    result.Lbin = new ReferencePrice()
                    {
                        AuctionId = result.Lbin.AuctionId,
                        Day = result.Lbin.Day,
                        Price = lbinPrice,
                        Seller = result.Lbin.Seller
                    };
                    result.LbinKey += diffExp;
                }
            }
            return (result, now);
        }

        public IEnumerable<(AuctionKey, ReferencePrice lbin)> ClosestLbinKeys(string tag, AuctionKey baseKey)
        {
            var tagGroup = GetAuctionGroupTag(tag);

            var result = new PriceEstimate();
            if (!Lookups.TryGetValue(tagGroup.Item1, out PriceLookup lookup))
            {
                return null;
            }
            var l = lookup.Lookup;
            var itemKey = GetBreakdownKey(baseKey, tag);
            return GetClosestLbins(l, itemKey).Select(c => (c.Key, c.Value.Lbin));
        }

        private static IOrderedEnumerable<KeyValuePair<AuctionKey, ReferenceAuctions>> GetClosestLbins(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKeyWithValue itemKey)
        {
            return l.Where(l => l.Key != null && l.Value?.Price > 0 && l.Value?.Lbin.Price > 0)
                            .OrderByDescending(m => itemKey.Similarity(m.Key) + Math.Min(m.Value.Volume, 2));
        }

        private void GetDifferenceSum(SaveAuction auction, PriceEstimate result, AuctionKeyWithValue itemKey, KeyValuePair<AuctionKey, ReferenceAuctions> c, out string diffExp, out long changeAmount)
        {
            (var modVal, var modExp) = AdjustMedianForModifiers(result, itemKey, c, auction);
            (var enchal, var enchExp) = AdjustForMissingEnchants(result, itemKey, c);
            var reforgediff = 0L;
            if (c.Key.Reforge != itemKey.Reforge)
                reforgediff = GetReforgeValue(c.Key.Reforge) - GetReforgeValue(itemKey.Reforge) / 20;
            diffExp = modExp + enchExp;
            changeAmount = modVal + enchal + reforgediff;
        }

        private (long substract, string add) AdjustForMissingEnchants(PriceEstimate result, AuctionKey itemKey, KeyValuePair<AuctionKey, ReferenceAuctions> closest)
        {
            // closest should be bigger 
            var missingEnchants = closest.Key.Enchants.Where(m => !itemKey.Enchants.Any(e => e.Type == m.Type && e.Lvl >= m.Lvl)).ToList();
            if (missingEnchants.Count > 0)
            {
                var enchCost = GetPriceSumForEnchants(missingEnchants);
                if (enchCost > 0)
                {
                    return (enchCost, $"-{string.Join(",", missingEnchants.Select(m => $"{m.Type}{m.Lvl}"))}");
                }
            }
            return (0, string.Empty);
        }

        private (long sub, string exp) AdjustMedianForModifiers(PriceEstimate result, AuctionKey itemKey, KeyValuePair<AuctionKey, ReferenceAuctions> closest, SaveAuction auction)
        {
            var missingModifiers = closest.Key.Modifiers.Where(m => !itemKey.Modifiers.Contains(m)).ToList();
            if (missingModifiers.Count > 0)
            {
                long toSubstract = GetPriceSumForModifiers(missingModifiers, itemKey.Modifiers, auction, true);
                toSubstract += AdjustForAttributes(result.Median, itemKey, missingModifiers, auction);
                if (toSubstract != 0)
                {
                    return (toSubstract, $"- {string.Join(",", missingModifiers.Select(m => m.Value))}");
                }
            }
            return (0, string.Empty);
        }

        private long GetPriceSumForModifiers(List<KeyValuePair<string, string>> missingModifiers, IEnumerable<KeyValuePair<string, string>> modifiers, SaveAuction auction, bool calculate = false)
        {
            if (missingModifiers == null)
                return 0;
            return missingModifiers.Select(m =>
            {
                if (Constants.AttributeKeys.Contains(m.Key)
                    || m.Key == "exp" || m.Key == "candyUsed" || m.Key.EndsWith("kills"))
                {
                    return 0;
                }
                var elem = ModifierEstimate(modifiers.ToList(), auction.Tag, auction.FlatenedNBT, m);
                if (elem.Value == 0)
                {
                    deferred.Log($"Missing modifier value {m.Key} {m.Value} {auction.Uuid}");
                    return 4_000_000_000; // not found potentially very valuable
                }
                if (calculate && elem.IsEstimate)
                    return elem.Value / 20;
                return elem.Value;
            }).Sum();
        }

        private IEnumerable<(string tag, int amount)> GetItemKeysForModifier(IEnumerable<KeyValuePair<string, string>> modifiers, Dictionary<string, string> flatNbt, string tag, KeyValuePair<string, string> m)
        {
            if (m.Key == null)
                return EmptyArray;
            if (ModifierItemPrefixes.TryGetValue(m.Key, out var prefix))
                if (prefix == string.Empty)
                    return new (string, int)[] { (prefix + m.Value.ToUpper(), 1) };
                else
                    // some of the items actually don't have the prefix, skins on pets may but other skins don't
                    return new (string, int)[] { (prefix + m.Value.ToUpper(), 1), (m.Value.ToUpper(), 1) };
            if (tag?.StartsWith("STARRED_SHADOW_ASSASSIN") ?? false && m.Key.StartsWith("JASPER_0"))
            {
                // Jasper0 slot can't be accessed on starred (Fragged) items
                return EmptyArray;
            }

            if (m.Value == "PERFECT" || m.Value == "FLAWLESS")
                return new (string, int)[] { (mapper.GetItemKeyForGem(m, flatNbt ?? new()), 1) };
            if (mapper.TryGetIngredients(m.Key, m.Value, null, out var ingredients))
            {
                return ingredients.Select(i => (i, 1));
            }
            return EmptyArray;
        }

        public long GetPriceForItem(string item)
        {
            if (Lookups.TryGetValue(item, out var lookup))
            {
                return lookup.Lookup.Values.FirstOrDefault(p => p.Price != 0)?.Price ?? 0;
            }
            return 0;
        }

        private KeyValuePair<AuctionKey, ReferenceAuctions> FindClosestTo(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKey itemKey, string itemTag)
        {
            return FindClosest(l, itemKey, itemTag).FirstOrDefault();
        }
        public IEnumerable<KeyValuePair<AuctionKey, ReferenceAuctions>> FindClosest(ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKey itemKey, string itemTag, int maxAge = 8)
        {
            var minDay = GetDay() - maxAge;
            var values = ComparisonValue(itemKey.Enchants, itemKey.Modifiers.ToList(), itemTag, null).ToList();
            return l.Where(l => l.Key != null && l.Value?.References != null && l.Value.Price > 0 && !l.Key.Modifiers.Any(m => m.Key == "virtual"))
                            .OrderByDescending(m => itemKey.Similarity(m.Key, this, ComparisonValue(m.Key.Enchants, m.Key.Modifiers.ToList(), itemTag, null).ToList(), values) + (m.Value.OldestRef > minDay ? 0 : -10));
        }

        void AssignMedian(PriceEstimate result, AuctionKey key, ReferenceAuctions bucket, long gemVal)
        {
            result.Median = bucket.Price + gemVal;
            result.Volume = bucket.Volume;
            result.MedianKey = key.ToString();
            result.Volatility = bucket.Volatility;
            result.LastSale = bucket.References.LastOrDefault();
        }

        internal void Move(string tag, long auctionId, AuctionKey from, string toTag, AuctionKey to)
        {
            var oldBucket = Lookups[tag].Lookup[from];
            var newBucket = GetOrAdd(to, Lookups[toTag]);

            var toChange = oldBucket.References.Where(e => e.AuctionId == auctionId).First();
            var newList = oldBucket.References.Where(e => e.AuctionId != auctionId).ToList();
            oldBucket.References = new ConcurrentQueue<ReferencePrice>(newList);

            if (!newBucket.References.Contains(toChange))
                newBucket.References.Enqueue(toChange);

            UpdateMedian(oldBucket);
            UpdateMedian(newBucket);
        }

        public IEnumerable<long> GetReferenceUids(SaveAuction auction)
        {
            if (TryGetReferenceAuctions(auction, out ReferenceAuctions bucket))
                return bucket.References.Select(r => r.AuctionId);
            return new long[0];
        }

        /// <summary>
        /// Adds persisted lookup data
        /// </summary>
        /// <param name="itemTag"></param>
        /// <param name="loadedVal"></param>
        public void AddLookupData(string itemTag, PriceLookup loadedVal)
        {
            foreach (var item in loadedVal.Lookup.Keys)
            {
                if (item.Modifiers.Any(m => m.Key == "candyUsed" && m.Value == "0")
                        && item.Modifiers.Any(m => m.Key == "exp" && m.Value == (itemTag == "PET_GOLDEN_DRAGON" ? "7" : "6")))
                    loadedVal.Lookup.TryRemove(item, out _); // have been dropped
                var value = loadedVal.Lookup.GetValueOrDefault(item);
                if (value == null)
                    continue;
                if (value.References.Count == 0 && value.Lbins.Count == 0 || value.References.All(r => r.Day == 1047) // lost nbt data that day
                    || value.References.All(r => r.Day < GetDay() - 21) && !item.IsClean())
                    loadedVal.Lookup.TryRemove(item, out _); // unimportant
            }
            if (itemTag.Contains("RUNE_"))
            {
                foreach (var item in loadedVal.Lookup.Keys.ToList())
                {
                    // the rune level should never be removed
                    if (!item.Modifiers.Any(m => m.Key.Contains("RUNE")))
                        loadedVal.Lookup.TryRemove(item, out _);
                }
            }
            var current = Lookups.AddOrUpdate(itemTag, (id) =>
            {
                foreach (var item in loadedVal.Lookup)
                {
                    item.Value.References = new ConcurrentQueue<ReferencePrice>(item.Value.References.Where(r => r.Price > 0).OrderBy(r => r.Day));
                    loadedVal.Lookup[item.Key] = item.Value;
                }
                return loadedVal;
            }, (tag, value) =>
            {
                foreach (var item in loadedVal.Lookup)
                {
                    if (!value.Lookup.TryGetValue(item.Key, out ReferenceAuctions existingBucket))
                    {
                        item.Value.References = new ConcurrentQueue<ReferencePrice>(item.Value.References.Where(r => r.Price > 0).OrderBy(r => r.Day));
                        value.Lookup[item.Key] = item.Value;
                        continue;
                    }
                    CombineBuckets(item, existingBucket);
                    item.Value.Lbins.Sort(ReferencePrice.Compare);
                }
                return value;
            });
            var idLookup = current.Lookup.SelectMany(l => l.Value.References.Select(r => r.AuctionId)).GroupBy(a => a).Where(g => g.Count() > 1).Select(g => g.Key).ToHashSet();
            foreach (var item in current.Lookup.Keys.ToList())
            {
                try
                {
                    // remove all with perfect
                    if (item.Modifiers.Any(m => m.Value == "PERFECT"))
                        current.Lookup.TryRemove(item, out _);
                    // remove all with too many enchants
                    if (item.Enchants.Count > 5)
                        current.Lookup.TryRemove(item, out _);
                    //Deduplicate(itemTag, current, item); probably not needed anymore
                    if (item.Modifiers.Count <= 1 && current.Lookup.TryGetValue(item, out var tocheck) && tocheck.References.Any(r => idLookup.Contains(r.AuctionId)))
                    {
                        tocheck.References = new(tocheck.References.Where(r => !idLookup.Contains(r.AuctionId)));
                    }
                }
                catch (System.Exception e)
                {
                    dev.Logger.Instance.Error(e, $"Could not deduplicate");
                }
            }

            static void CombineBuckets(KeyValuePair<AuctionKey, ReferenceAuctions> item, ReferenceAuctions existingBucket)
            {
                var existingRef = existingBucket.References;
                existingBucket.References = item.Value.References;
                if (existingRef != null)
                {
                    existingBucket.References = new(existingRef.Concat(item.Value.References).ToList()
                        .DistinctBy(d => d.AuctionId)
                        .OrderBy(r => r.Day));

                    var today = GetDay();
                    while (existingBucket.References.Count > 7 && existingBucket.References.TryPeek(out var r) && r.Day < today - 30)
                    {
                        existingBucket.References.TryDequeue(out _);
                    }
                }
                existingBucket.Price = item.Value.Price;
                if (item.Value.Lbins == null)
                    item.Value.Lbins = new();
                // load all non-empty lbins
                foreach (var binAuction in item.Value.Lbins)
                {
                    if (!existingBucket.Lbins.Contains(binAuction) && binAuction.Price > 0)
                        existingBucket.Lbins.Add(binAuction);
                }
                CapBucketSize(existingBucket);
            }
        }

        private static void CapBucketSize(ReferenceAuctions bucket)
        {
            while (bucket.References.Count > SizeToKeep && bucket.References.TryDequeue(out _)) { }
        }

        public void AddSoldItem(SaveAuction auction, bool preventMedianUpdate = false)
        {
            (ReferenceAuctions bucket, var key) = GetBucketForAuction(auction);
            AddAuctionToBucket(auction, preventMedianUpdate, bucket, key.ValueSubstract);
            try
            {
                var attributesOnAuction = auction.FlatenedNBT.Where(a => Constants.AttributeKeys.Contains(a.Key)).ToList();
                if (attributesOnAuction.Count == 0)
                    return;
                if (key.Enchants.Count > 1 || key.Modifiers.Count > 2)
                    return; // only add attributes for (almost) clean items, one allowed for things that drop with extra enchants
                var groupTag = GetAuctionGroupTag(auction.Tag);
                var itemGroupTag = groupTag.Item1;
                foreach (var item in attributesOnAuction)
                {
                    AuctionKey itemKey = VirtualAttributeKey(item);
                    var bucketForAttribute = GetOrAdd(itemKey, Lookups[itemGroupTag]);
                    var level = int.Parse(item.Value);
                    var power = Math.Pow(2, level - 1);
                    var toSubstractForLvl1 = auction.HighestBidAmount - auction.HighestBidAmount / power;
                    AddAuctionToBucket(auction, preventMedianUpdate, bucketForAttribute, (long)toSubstractForLvl1);
                }
                OnSold?.Invoke((auction, key));
            }
            catch (System.Exception e)
            {
                dev.Logger.Instance.Error(e, $"Occured when trying to store attribue value");
            }
        }

        private static AuctionKey VirtualAttributeKey(KeyValuePair<string, string> item)
        {
            return new AuctionKey()
            {
                Modifiers = new(new List<KeyValuePair<string, string>>()
                        {
                            new(item.Key, "1"),
                            new("virtual",string.Empty)
                        })
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="auction"></param>
        /// <param name="preventMedianUpdate"></param>
        /// <param name="bucket"></param>
        /// <param name="valueSubstract">Extra value to substract based on what is not matching in the bucket</param>
        public void AddAuctionToBucket(SaveAuction auction, bool preventMedianUpdate, ReferenceAuctions bucket, long valueSubstract = 0)
        {
            if (bucket.References.Where(r => r.AuctionId == auction.UId).Any())
                return; // duplicate
            var reference = CreateReferenceFromAuction(auction, valueSubstract);
            if (reference.Price < 0 && valueSubstract > 1_000_000)
            {
                logger.LogInformation($"Negative price {JsonConvert.SerializeObject(auction)} {reference.Price} {valueSubstract}");
            }
            // move reference to sold
            bucket.References.Enqueue(reference);
            bucket.Lbins.RemoveAll(l => l.AuctionId == auction.UId);
            CapBucketSize(bucket);
            if (!preventMedianUpdate)
            {
                var key = DetailedKeyFromSaveAuction(auction);
                UpdateMedian(bucket, (GetAuctionGroupTag(auction.Tag).tag, key));
            }
        }

        public void UpdateMedian(ReferenceAuctions bucket, (string tag, KeyWithValueBreakdown key) keyCombo = default)
        {
            var size = bucket.References.Count;
            if (size < 4)
                return; // can't have enough volume
            List<ReferencePrice> deduplicated = ApplyAntiMarketManipulation(bucket);
            DropUnderlistings(deduplicated);

            size = deduplicated.Count();
            if (size <= 3 || deduplicated.Count(d => d.Day >= GetDay() - 20) < 3 && !(keyCombo.Item2?.Key.IsClean() ?? false) && !IsMaxAttrib(keyCombo))
            {
                bucket.Price = 0; // to low vol
                return;
            }
            // short term protects against price drops after updates
            List<ReferencePrice> shortTermList = GetShortTermBatch(deduplicated).OrderByDescending(b => b.Day).ToList();
            PriceLookup lookup;
            Dictionary<short, long> cleanPriceLookup;
            bool isCleanitem;
            GetCleanPriceLookup(keyCombo, out lookup, out cleanPriceLookup, out isCleanitem);
            var shortTermPrice = GetMedian(shortTermList, cleanPriceLookup);
            bucket.OldestRef = shortTermList.Take(4).Min(s => s.Day);
            if (shortTermList.Count >= 3 && bucket.OldestRef - shortTermList.First().Day <= -5
                && shortTermList.First().AuctionId != shortTermList.OrderByDescending(o => o.Price).First().AuctionId
                && bucket.Volume > 0.25) // 5 day gaps are to be expected at ~0.2 volume
            {
                // probably derpy or weird price drop
                var reduced = (shortTermList.OrderBy(s => s.Price).First().Price + shortTermPrice * 2) / 3;
                shortTermPrice = Math.Max(shortTermPrice * 7 / 10, reduced);
            }
            // long term protects against market manipulation
            var monthSpan = deduplicated.Where(d => d.Day >= GetDay() - 30).ToList();
            var longSpanPrice = monthSpan.Count switch
            {
                > 24 => HighReferenceCount(cleanPriceLookup, monthSpan),
                > 5 => GetMedian(monthSpan, cleanPriceLookup),
                _ => GetMedian(deduplicated.Take(29).ToList(), cleanPriceLookup)
            };
            if (deduplicated.All(d => d.Day >= GetDay()))
            {
                // all prices are from today, use 25th percentile instead
                longSpanPrice = deduplicated.OrderBy(d => d.Price).Take((int)Math.Max(deduplicated.Count() * 0.25, 1)).Max(d => d.Price);
            }
            var medianPrice = Math.Min(shortTermPrice, longSpanPrice);
            var lbinMedian = bucket.Lbins.Where(l => l.Price > medianPrice / 2 && l.Day > GetDay() + 5).OrderBy(l => l.Price).Skip(2).FirstOrDefault();
            if (lbinMedian.AuctionId != default)
            {
                medianPrice = Math.Min(medianPrice, lbinMedian.Price);
                shortTermPrice = Math.Min(shortTermPrice, lbinMedian.Price);
            }
            (bucket.Volatility, medianPrice) = GetVolatility(lookup, bucket, shortTermPrice, longSpanPrice);
            bucket.HitsSinceCalculating = 0;
            bucket.Volume = deduplicated.Count() / (GetDay() - deduplicated.OrderBy(d => d.Day).First().Day + 1);
            bucket.DeduplicatedReferenceCount = (short)deduplicated.Count();
            PreCalculateVolume(keyCombo);
            // get price of item without enchants and add enchant value 
            if (keyCombo != default)
            {
                var breakdown = keyCombo.key.ValueBreakdown;
                var volatMedian = medianPrice;
                long limitedPrice = CapAtCraftCost(keyCombo.tag, medianPrice, keyCombo.key, bucket.Price);
                var craftCostCap = limitedPrice;
                if (limitedPrice == 0)
                {
                    limitedPrice = medianPrice;
                }
                // check higher value keys for lower price 
                limitedPrice = CapPriceAtHigherLevelKey(keyCombo, limitedPrice);

                if (size > 40)
                {
                    var riskyLongTerm = GetMedian(monthSpan.Where(d => d.Day >= GetDay() - 10).ToList(), cleanPriceLookup, 3f);
                    var riskyShort = GetMedian(monthSpan.Where(d => d.Day >= GetDay() - 2).ToList(), cleanPriceLookup, 3f);
                    var marketManipLimit = limitedPrice * 4 / 3 + 1_000_000;
                    bucket.RiskyEstimate = Math.Min(Math.Min(riskyShort, riskyLongTerm), marketManipLimit);
                }
                else
                {
                    bucket.RiskyEstimate = limitedPrice;
                }
                if (limitedPrice != bucket.Price)
                {
                    if (limitedPrice == 0)
                    {
                        logger.LogWarning($"Price capped {keyCombo.tag} -> {limitedPrice} ({craftCostCap}) {keyCombo.key.Key} {medianPrice} {bucket.Price} - {volatMedian} {shortTermPrice} {longSpanPrice}");
                        limitedPrice = 11;
                    }
                    medianPrice = limitedPrice;
                    if (medianPrice < 0)
                    {
                        logger.LogWarning($"Negative price {keyCombo.tag} -> {limitedPrice}  {keyCombo.key} {medianPrice} {bucket.Price}");
                    }
                    else
                        bucket.Price = medianPrice;
                    return;
                }

                var keyWithNoEnchants = new AuctionKey(keyCombo.Item2)
                {
                    Enchants = new(new List<Enchant>())
                };

                if (keyCombo.Item2.Key.Count > 1)
                {
                    var lowerCountKey = new AuctionKey(keyCombo.Item2)
                    {
                        Count = 1
                    };
                    if (lookup.Lookup.TryGetValue(lowerCountKey, out var lowerCountBucket)
                        && lowerCountBucket.Price != 0
                        && lowerCountBucket.Price * keyCombo.Item2.Key.Count < medianPrice)
                    {
                        medianPrice = Math.Min(medianPrice, lowerCountBucket.Price * keyWithNoEnchants.Count);

                        logger.LogInformation($"Adjusted for count {keyCombo.tag} -> {medianPrice}  {keyWithNoEnchants} - {keyCombo.Item2.Key}");
                    }
                }
            }
            if (isCleanitem)
            {
                lookup.CleanPricePerDay ??= new();
                if (medianPrice > 0)
                    lookup.CleanPricePerDay[shortTermList.OrderByDescending(s => s.Day).First().Day] = medianPrice;
            }
            if (medianPrice < 0)
            {
                logger.LogWarning($"Negative price {keyCombo.tag} -> {medianPrice}  {keyCombo.key} {shortTermPrice} {longSpanPrice}");
            }
            else
                bucket.Price = medianPrice;

            bool IsMaxAttrib((string tag, KeyWithValueBreakdown key) keyCombo)
            {
                var matchCount = keyCombo.key?.Key?.Modifiers?.Where(m => Constants.AttributeKeys.Contains(m.Key) && m.Value == "10").Count();
                return matchCount == 2;
            }

            static void DropUnderlistings(List<ReferencePrice> deduplicated)
            {
                var bucketSize = deduplicated.Count();
                var toRemove = new List<ReferencePrice>();
                for (int i = 0; i < bucketSize; i++)
                {
                    var batch = deduplicated.Skip(i).Take(5).ToList();
                    if (batch.Count < 3)
                        break;
                    var targetAuction = deduplicated[i];
                    var hit = batch.Where(a => a.Buyer == targetAuction.Seller).FirstOrDefault();
                    if (hit.AuctionId == default)
                        continue;
                    if (i < 3 && batch.Take(3).Where(a => a.AuctionId != hit.AuctionId).Select(a => a.Price).Average() < hit.Price)
                        continue;// skip if median would be pulled down, the point of this is to remove to low value
                    toRemove.Add(hit);
                }
                if (deduplicated.Count - toRemove.Count < 4)
                {
                    return;
                }
                foreach (var item in toRemove)
                {
                    deduplicated.Remove(item);
                }
            }

            static List<ReferencePrice> ApplyAntiMarketManipulation(ReferenceAuctions bucket)
            {
                var buyerCounter = 0;
                // check for back and forth selling
                var buyerSellerCombos = bucket.References.GroupBy(a => a.Buyer > a.Seller ? a.Buyer << 15 + a.Seller : a.Seller << 15 + a.Buyer)
                    .Where(g => g.Count() > 1 && !g.All(gi => gi.Seller == g.First().Seller))
                    .ToLookup(l => l.First().Seller);
                var isPersonManipulating = bucket.References.OrderByDescending(r => r.Price).Take(bucket.References.Count / 2)
                            .GroupBy(r => r.Seller).Where(g => g.Count() >= Math.Max(bucket.References.Count / 3, 3)).OrderByDescending(g => g.Count()).Select(g => g.First().Seller).FirstOrDefault();
                var deduplicated = bucket.References.Reverse()
                    .Where(d => !buyerSellerCombos.Contains(d.Seller) && !buyerSellerCombos.Contains(d.Buyer))
                    .OrderByDescending(b => b.Day)
                    .GroupBy(a => a.Seller)
                    .Select(a => a.OrderBy(ai => ai.Price).First())  // only use one (the cheapest) price from each seller
                    .GroupBy(a => a.Buyer == 0 ? buyerCounter++ : a.Buyer)
                    .Select(a => a.OrderBy(ai => ai.Price).First())  // only use cheapest price from each buyer 
                    .Take(60)
                    .ToList();
                if (isPersonManipulating != default)
                {
                    for (int i = 0; i < deduplicated.Count; i++)
                    {
                        if (deduplicated[i].Seller == isPersonManipulating)
                        {
                            var elem = deduplicated[i];
                            elem.Price /= 2;
                            deduplicated[i] = elem;
                        }
                    }
                }
                return deduplicated;
            }

            long CapPriceAtHigherLevelKey((string tag, KeyWithValueBreakdown key) keyCombo, long limitedPrice)
            {
                var oldestDay = bucket.OldestRef;
                var cheaperHigherValue = Lookups[keyCombo.tag].Lookup
                    .Where(k => k.Value.Price < limitedPrice && k.Value.Price != 0
                            && !k.Key.Modifiers.Any(m => m.Key == "virtual")
                            && k.Value.OldestRef >= oldestDay // only relevant if price dropped recently
                            && k.Value.DeduplicatedReferenceCount > 3 && k.Value.Price > limitedPrice / 20
                            && IsHigherValue(keyCombo.tag, keyCombo.key, k.Key) && k.Key.Reforge == keyCombo.key.Key.Reforge)
                    .OrderBy(b => b.Value.Price).Select(b => b.Value.Price).FirstOrDefault(limitedPrice);
                if (cheaperHigherValue != default && cheaperHigherValue < limitedPrice)
                {
                    limitedPrice = cheaperHigherValue;
                }

                return limitedPrice;
            }

            void GetCleanPriceLookup((string tag, KeyWithValueBreakdown key) keyCombo, out PriceLookup lookup, out Dictionary<short, long> cleanPriceLookup, out bool isCleanitem)
            {
                if (keyCombo == default)
                {
                    lookup = default;
                    cleanPriceLookup = default;
                    isCleanitem = false;
                    return;
                }
                lookup = Lookups.GetOrAdd(keyCombo.tag, new PriceLookup());
                cleanPriceLookup = lookup.CleanPricePerDay;
                if (lookup.CleanKey?.Count == default && lookup.Lookup.Count > 1)
                {
                    lookup.CleanKey = lookup.Lookup.Where(l => !l.Key.Modifiers.Any(m => m.Key == "virtual")).OrderByDescending(l => l.Value.Volume - l.Key.Modifiers.Count * 5).Select(l => l.Key).FirstOrDefault();
                }
                isCleanitem = keyCombo.key?.Key == lookup.CleanKey;
                if (lookup.CleanKey == default || isCleanitem)
                    cleanPriceLookup = new(); // no change to clean price itself
            }

            static long HighReferenceCount(Dictionary<short, long> cleanPriceLookup, List<ReferencePrice> monthSpan)
            {
                var lastTwoWeeks = monthSpan.Where(d => d.Day >= GetDay() - 10).ToList();
                if (lastTwoWeeks.Count < 5)
                {
                    return GetMedian(monthSpan, cleanPriceLookup);
                }
                return GetMedian(lastTwoWeeks, cleanPriceLookup);
            }
        }

        private void PreCalculateVolume((string tag, KeyWithValueBreakdown key) keyCombo)
        {
            if (keyCombo.tag == null || !Lookups.TryGetValue(keyCombo.tag, out var itemLookup))
            {
                return;
            }
            itemLookup.Volume = (float)itemLookup.Lookup.Sum(l => l.Value.References.Count) / 60;
        }

        private static (byte, long) GetVolatility(PriceLookup lookup, ReferenceAuctions bucket, long shortTermPrice, long longTerm)
        {
            var oldMedian = GetMedian(bucket.References.AsEnumerable().Reverse().Take(5).ToList(), lookup?.CleanPricePerDay);
            var secondNewestMedian = 0L;
            if (bucket.References.Count > 8)
                secondNewestMedian = GetMedian(bucket.References.AsEnumerable().Skip(5).Take(5).ToList(), lookup?.CleanPricePerDay);
            var medianList = new float[] { oldMedian, secondNewestMedian, longTerm, shortTermPrice }.OrderByDescending(m => m).ToList();
            var mean = medianList.Average();
            medianList = medianList.Select(m => m / mean).ToList();
            mean = medianList.Average();
            var variance = medianList.Select(m => Math.Pow(m - mean, 2)).Sum() / medianList.Count;
            var volatility = Math.Sqrt(variance);
            var volatilityReduced = (byte)Math.Clamp(volatility * 100, -120, 120);
            var newMedian = Math.Min(shortTermPrice, longTerm);
            if (IsTrendDownwards(shortTermPrice, longTerm, oldMedian, secondNewestMedian))
            {
                var difference = secondNewestMedian - shortTermPrice;
                var inPercent = (float)difference / shortTermPrice;
                if (difference > 0 && newMedian > difference && inPercent < 0.4)
                {
                    newMedian = newMedian - (long)(newMedian * inPercent);
                    deferred.Log($"Trend downwards {bucket.References.First().AuctionId} - {bucket.Price} {shortTermPrice} {longTerm} {secondNewestMedian} diff:{difference} {inPercent}% {newMedian}");
                }
            }

            return (volatilityReduced, newMedian);

            static bool IsTrendDownwards(long shortTermPrice, long longTerm, long oldMedian, long secondNewestMedian)
            {
                return oldMedian > shortTermPrice && longTerm > secondNewestMedian && secondNewestMedian > shortTermPrice;
            }
        }

        private long CapAtCraftCost(string tag, long medianPrice, KeyWithValueBreakdown key, long currentPrice)
        {
            List<RankElem> breakdown = key.ValueBreakdown;
            var limitedPrice = 0L;
            // stackables
            if (key.Key.Enchants.Count == 0 && key.Key.Modifiers.Count == 0
                && (craftCostService?.TryGetCost(tag, out double craftCost) ?? false) && craftCost > 0)
            {
                var stackSize = key.Key.Count;
                var stackCost = craftCost * stackSize * 1.2;
                if (stackCost < 500_000)
                {
                    stackCost *= 3; // allow higher limit for low cost items
                }
                if (stackCost < medianPrice)
                {
                    logger.LogDebug($"Capped {tag} at {stackCost} {stackSize} craft cost {craftCost} {key}");
                    return (long)stackCost;
                }
            }
            if (breakdown.Any(v => v.Value == 0) || breakdown.Count <= 0)
                return medianPrice; // can't cap nothin added, basically clean

            if (!Lookups.TryGetValue(tag, out var lookup))
            {
                logger.LogInformation($"Could not cap, No lookup for {tag} keeping {currentPrice} on {key.Key}");
                return Math.Min(medianPrice, currentPrice + 10_000 + currentPrice / 100);
            }
            // determine craft cost 
            long minValue = GetCleanItemPrice(tag, key, lookup);
            if (minValue == 0 || currentPrice == minValue)
                return medianPrice;
            if (tag.Contains("RUNE_"))
            {
                return LimitRuneToFuseCost(medianPrice, breakdown, lookup);
            }
            var modifierSum = breakdown.Select(v =>
            {
                if (!Constants.AttributeKeys.Contains(v.Modifier.Key))
                    return v.Value;
                return AttributeValueEstimateForCap(tag, v, breakdown, lookup);
            }).Sum();
            if (modifierSum > 0)
            {
                if (minValue + modifierSum * 1.08 < medianPrice
                    && key.Key.Modifiers.All(m => !Constants.AttributeKeys.Contains(m.Key))
                )
                {
                    CappedKey?.Invoke(new(
                        tag,
                         key,
                         minValue,
                         modifierSum,
                         lookup,
                         medianPrice
                    ));
                }
                limitedPrice = Math.Min(minValue + modifierSum * 11 / 10, medianPrice);
            }
            if (limitedPrice > 0)
                return limitedPrice;
            return medianPrice;

            static long LimitRuneToFuseCost(long medianPrice, List<RankElem> breakdown, PriceLookup lookup)
            {
                // runes are fused and can't be crafted
                if (breakdown.First().Modifier.Value == "1")
                    return medianPrice;
                var lvl1 = lookup.Lookup.Where(v => v.Key.Modifiers.FirstOrDefault().Value == "1").FirstOrDefault().Value?.Price;
                if (lvl1 == null)
                    return medianPrice;
                var targetLevel = int.Parse(breakdown.First().Modifier.Value);
                var targetPrice = lvl1 * Math.Pow(3.5, targetLevel - 1);
                if (targetPrice < medianPrice)
                    return (long)targetPrice;
                return medianPrice;
            }
        }

        private static long GetCleanItemPrice(string tag, KeyWithValueBreakdown key, PriceLookup lookup)
        {
            var select = (NBT.IsPet(tag) ?
                            lookup.Lookup.Where(v => v.Value.Price > 0 && key.Key.Tier == v.Key.Tier).Select(v => v.Value.Price) :
                             lookup.Lookup.Values.Where(v => v.Price > 0).Select(v => v.Price)).ToList();
            var count = select.Count;
            var median = select.DefaultIfEmpty(0).OrderBy(v => v).Skip(count / 3).FirstOrDefault();
            // 2nd percentile to skip low volume outliers on complex items
            var minValue = select.DefaultIfEmpty(0).Where(o => o > median / 20).OrderBy(v => v).Skip(count / 50).FirstOrDefault();
            return minValue;
        }

        private long AttributeValueEstimateForCap(string tag, RankElem v, List<RankElem> breakdown, PriceLookup lookup)
        {
            var comboValue = 0L;
            var both = breakdown.Where(b => Constants.AttributeKeys.Contains(b.Modifier.Key)).ToDictionary(e => e.Modifier.Key, e => e.Modifier.Value);
            if (HasAttributeCombo(v.Modifier, both, tag))
            {
                var lowestTwo = lookup.Lookup
                    .Where(p => p.Value.Price > 0)
                    .Where(l => both.All(b => l.Key.Modifiers.Any(m => m.Key == b.Key)))
                    .OrderBy(p => p.Value.Price).Take(2).SelectMany(l => l.Value.References).ToList();
                var percentile = lowestTwo.OrderBy(r => r.Price)
                            .Skip(lowestTwo.Count / 2).Select(r => r.Price)
                            // deviding by two because both attributes count
                            .DefaultIfEmpty(0).Min() / 2;
                comboValue = percentile;
            }
            var baseLevel = int.Parse(v.Modifier.Value);
            // check lowest value path
            var options = lookup.Lookup.AsEnumerable();
            if (CrimsonArmors.Any(tag.StartsWith))
            {
                // these 4 types can be combined amongst each other
                var secondType = tag.Split("_")[1];
                options = CrimsonArmors.SelectMany(s => Lookups.TryGetValue(s + secondType, out var lookup) ? lookup.Lookup.AsEnumerable() : []);
            }
            double quarterPercentile = GetPercentile(v, baseLevel, options);
            var shards = GetPercentile(v, baseLevel, Lookups.GetValueOrDefault("ATTRIBUTE_SHARD", new PriceLookup()).Lookup);
            if (shards > 0 && shards < quarterPercentile)
            {
                quarterPercentile = shards;
            }
            return (long)(Math.Pow(2, baseLevel) * quarterPercentile * 1.20) + comboValue;

            static double GetPercentile(RankElem v, int baseLevel, IEnumerable<KeyValuePair<AuctionKey, ReferenceAuctions>> options)
            {
                var values = options.Where(l => l.Value.Price > 0
                                            && (l.Key.Modifiers.Count == 2 && l.Key.Modifiers.Last().Key == "virtual" || l.Key.Modifiers.Count == 1) && l.Key.Modifiers.Any(m => m.Key == v.Modifier.Key)
                                            && baseLevel > int.Parse(l.Key.Modifiers.First().Value))
                                .Select(l => l.Value.Price / Math.Pow(2, int.Parse(l.Key.Modifiers.First().Value)))
                                .ToList();
                var quarterPercentile = values.Count > 0 ? values.OrderBy(v => v).Skip(values.Count / 4).First() : 0;
                return quarterPercentile;
            }
        }

        private static List<ReferencePrice> GetShortTermBatch(List<ReferencePrice> deduplicated)
        {
            // if more than half of the references are less than 12 hours old, use more references
            if (deduplicated.Where(d => d.Day >= GetDay(DateTime.Now - TimeSpan.FromHours(12))).Count() > SizeToKeep / 2)
                return deduplicated.Take(6).ToList();
            return deduplicated.Take(3).ToList();
        }

        public (ReferenceAuctions auctions, AuctionKeyWithValue key) GetBucketForAuction(SaveAuction auction)
        {
            var group = GetAuctionGroupTag(auction.Tag);
            var itemGroupTag = group.Item1;
            if (!Lookups.TryGetValue(itemGroupTag, out var lookup) || lookup == null)
            {
                lookup = new PriceLookup();
                Lookups[itemGroupTag] = lookup;
            }
            if (auction.Category == Category.UNKNOWN)
            {
                auction.Category = lookup.Category;
            }
            var key = KeyFromSaveAuction(auction);
            key.ValueSubstract += group.Item2;
            return (GetOrAdd(key, lookup), key);
        }

        private static long GetMedian(List<ReferencePrice> deduplicated, Dictionary<short, long> cleanPricePerDay, float skipAdjust = 2)
        {
            if (deduplicated.Count == 0)
            {
                // can get here if there are no sells in the last 10 days in a bucket that has more than 20 in the last month (sus, so no median is fine)
                return 0;
            }
            var today = cleanPricePerDay?.GetValueOrDefault(GetDay()) ?? cleanPricePerDay?.GetValueOrDefault((short)(GetDay() - 1)) ?? 0;
            return (long)deduplicated
                .OrderByDescending(b => SelectAdjustedPrice(cleanPricePerDay, b, today))
                .Skip((int)(deduplicated.Count / skipAdjust))
                .Select(b => SelectAdjustedPrice(cleanPricePerDay, b, today))
                .First();

            static float SelectAdjustedPrice(Dictionary<short, long> cleanPricePerDay, ReferencePrice b, long today)
            {
                if (today == 0 || !cleanPricePerDay.TryGetValue(b.Day, out var clean))
                    return b.Price;
                var percentDiff = (float)(today - clean) / clean;
                return b.Price - (clean > today ? Math.Min(clean - today, b.Price - percentDiff * b.Price) : 0);
            }
        }

        private ReferenceAuctions CreateAndAddBucket(SaveAuction auction, AuctionKey key)
        {
            var itemBucket = Lookups.GetOrAdd(GetAuctionGroupTag(auction.Tag).tag, new PriceLookup());
            return GetOrAdd(key, itemBucket);
        }

        private static ReferenceAuctions GetOrAdd(AuctionKey key, PriceLookup itemBucket)
        {
            if (itemBucket.Lookup == null)
                itemBucket.Lookup = new();
            return itemBucket.Lookup.GetOrAdd(key, (k) => new ReferenceAuctions());
        }

        private static ReferencePrice CreateReferenceFromAuction(SaveAuction auction, long valueSubstract = 0)
        {
            var basePrice = auction.HighestBidAmount == 0 ? auction.StartingBid : auction.HighestBidAmount;
            // remove at most 50% of the value
            if (basePrice < valueSubstract)
                valueSubstract = Math.Min(valueSubstract, basePrice / 2);
            var buyer = auction.Bids?.OrderByDescending(b => b.Amount).FirstOrDefault();
            return new ReferencePrice()
            {
                AuctionId = auction.UId,
                Day = GetDay(auction.End),
                Price = basePrice - valueSubstract,
                Seller = auction.AuctioneerId == null ? (short)(auction.SellerId % (2 << 14)) : Convert.ToInt16(auction.AuctioneerId.Substring(0, 4), 16),
                Buyer = buyer?.Bidder == null ? (short)0 : Convert.ToInt16(buyer.Bidder.Substring(0, 4), 16)
            };
        }

        public static short GetDay(DateTime date = default)
        {
            if (date == default)
                date = DateTime.UtcNow;
            return (short)(date - StartTime).TotalDays;
        }

        private bool TryGetReferenceAuctions(SaveAuction auction, out ReferenceAuctions bucket)
        {
            bucket = null;
            if (!Lookups.TryGetValue(GetAuctionGroupTag(auction.Tag).tag, out PriceLookup lookup))
                return false;
            var l = lookup.Lookup;
            var basekey = DetailedKeyFromSaveAuction(auction);
            if (l.TryGetValue(basekey, out bucket))
                return true;
            if (l.TryGetValue(basekey.GetReduced(1), out bucket))
                return true;
            if (l.TryGetValue(basekey.GetReduced(2), out bucket))
                return true;
            return l.TryGetValue(basekey.GetReduced(3), out bucket);
        }

        private static readonly System.Collections.ObjectModel.ReadOnlyCollection<KeyValuePair<string, string>> EmptyModifiers = new(new List<KeyValuePair<string, string>>());
        private static readonly System.Collections.ObjectModel.ReadOnlyCollection<KeyValuePair<string, string>> EmptyPetModifiers = new(new List<KeyValuePair<string, string>>() { new("candyUsed", "0"), new("exp", "0") });
        private static readonly DateTime UnlockedIntroduction = new DateTime(2021, 9, 4);
        private static readonly List<string> GemPurities = new() { "PERFECT", "FLAWLESS", "FINE", "ROUGH" };
        public long EstStarCost(string item, int tier)
        {
            var items = itemService.GetStarIngredients(item, tier);
            var sum = 0;
            foreach (var ingred in items)
            {
                if (BazaarPrices.TryGetValue(ingred.itemId, out var cost))
                    sum += (int)cost * ingred.amount;
                else
                {
                    sum += 1_000_000;
                }
            }
            return sum;
        }
        public AuctionKeyWithValue KeyFromSaveAuction(SaveAuction auction, int dropLevel = 0)
        {
            return DetailedKeyFromSaveAuction(auction).GetReduced(dropLevel);
        }
        public KeyWithValueBreakdown ValueKeyForTest(SaveAuction auction)
        {
            return DetailedKeyFromSaveAuction(auction);
        }
        private KeyWithValueBreakdown DetailedKeyFromSaveAuction(SaveAuction auction)
        {
            var shouldIncludeReforge = Constants.RelevantReforges.Contains(auction.Reforge);
            long valueSubstracted = 0;
            bool removedRarity = false;
            List<RankElem> rankElems = [];
            List<Enchant> enchants;
            List<KeyValuePair<string, string>> modifiers;
            (enchants, modifiers) = SelectValuable(auction);

            (valueSubstracted, removedRarity, shouldIncludeReforge, rankElems) = CapKeyLength(enchants, modifiers, auction);


            if (enchants == null)
                enchants = new List<Models.Enchant>();
            var tier = auction.Tier;
            if (auction.Tag == "ENCHANTED_BOOK")
            {
                // rarities don't matter for enchanted books and often used for scamming
                tier = Tier.UNCOMMON;
            }
            if (auction.Tag == "PANDORAS_BOX")
                // pandoras box tier gets set based on the player
                tier = Tier.COMMON;
            if (removedRarity)
            {
                tier = ReduceRarity(tier);
            }
            var reducedEnchants = RemoveNoEffectEnchants(auction, enchants);
            if (reducedEnchants.Count < enchants.Count)
            {
                rankElems = rankElems.Where(r => r.Enchant.Type == default || reducedEnchants.Any(re => re.Type == r.Enchant.Type)).ToList();
                enchants = reducedEnchants;
            }
            if (auction.Tag != null && AttributeToIgnoreOnLookup.TryGetValue(auction.Tag, out var ignore))
            {
                modifiers.RemoveAll(m => ignore.Contains(m.Key));
            }
            if (modifiers.Any(m => m.Key == "rarity_upgrades") && !Constants.DoesRecombMatter(auction.Category, auction.Tag))
            {
                modifiers.RemoveAll(m => m.Key == "rarity_upgrades");
                if (!auction.Tag.Contains("RUNE_"))
                    tier = ReduceRarity(tier);
            }

            return Constructkey(auction, enchants, modifiers, shouldIncludeReforge, valueSubstracted, rankElems, tier);
        }

        private (List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers) SelectValuable(SaveAuction auction)
        {
            var enchants = auction.Enchantments
                            ?.Where(e => MinEnchantMap.TryGetValue(e.Type, out byte value) && e.Level >= value)
                            .Select(e => new Models.Enchant() { Lvl = e.Level, Type = e.Type }).ToList();
            var modifiers = auction.FlatenedNBT?.Where(n =>
                                   IncludeKeys.Contains(n.Key)
                                || n.Value == "PERFECT"
                                || n.Key.StartsWith("RUNE_")
                                || IsSoul(n)) // admins
                            .OrderByDescending(n => n.Key)
                            .Select(i => NormalizeData(i, auction.Tag, auction.FlatenedNBT))
                            .Where(i => i.Key != Ignore.Key).ToList();
            if (auction.ItemCreatedAt < UnlockedIntroduction
                // safe guard for when the creation date is wrong 
                && !auction.FlatenedNBT.ContainsKey("unlocked_slots"))
            {
                var allUnlockable = itemService?.GetUnlockableSlots(auction.Tag).ToList();
                if (auction.FlatenedNBT.TryGetValue("gemstone_slots", out var countString) && int.TryParse(countString, out var count))
                {
                    allUnlockable = allUnlockable.Take(count).ToList();
                    modifiers.RemoveAll(m => m.Key == "gemstone_slots");
                }
                if (allUnlockable?.Count > 0)
                    modifiers.Add(new KeyValuePair<string, string>("unlocked_slots", string.Join(",", allUnlockable.OrderBy(s => s))));
            }
            return (enchants, modifiers);
        }

        private static KeyWithValueBreakdown Constructkey(SaveAuction auction, List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers, bool shouldIncludeReforge, long valueSubstracted, List<RankElem> rankElems, Tier tier)
        {
            var key = new AuctionKeyWithValue()
            {
                // order attributes
                Modifiers = modifiers.OrderBy(m => m.Key).ToList().AsReadOnly(),
                Enchants = enchants.OrderBy(e => e.Type).ToList().AsReadOnly(),
                Tier = tier,
                Reforge = shouldIncludeReforge ? auction.Reforge : ItemReferences.Reforge.Any,
                Count = (byte)auction.Count,
                ValueSubstract = valueSubstracted
            };
            var fullKey = new KeyWithValueBreakdown()
            {
                Key = key,
                SubstractedValue = valueSubstracted,
                ValueBreakdown = rankElems
            };
            return fullKey;
        }

        public KeyWithValueBreakdown GetBreakdownKey(AuctionKey key, string tag)
        {
            var virtualAuction = new SaveAuction() { Reforge = key.Reforge, Tier = key.Tier, Tag = tag, Count = key.Count, FlatenedNBT = key.Modifiers.ToDictionary() };
            var capped = CapKeyLength(key.Enchants.ToList(), key.Modifiers.ToList(), virtualAuction, 0);
            return Constructkey(virtualAuction, [.. key.Enchants], key.Modifiers.ToList(), capped.includeReforge, capped.valueSubstracted, capped.ranked, key.Tier);
        }

        public static Tier ReduceRarity(Tier tier)
        {
            if (tier < Tier.ULTIMATE)
            {
                if (tier == Tier.MYTHIC)
                    tier = Tier.LEGENDARY;
                else if (tier == Tier.DIVINE)
                    tier = Tier.MYTHIC;
                else
                    tier--;
            }

            return tier;
        }

        private static bool IsSoul(KeyValuePair<string, string> n)
        {
            // captured souls
            return n.Key.StartsWith("MASTER_CRYPT_TANK_ZOMBIE")
                    || n.Key.StartsWith("MINOS_CHAMPION_")
                    || n.Key == "MINOS_INQUISITOR_750"
                    || n.Key.StartsWith("MASTER_CRYPT_UNDEAD_") && n.Key.Length > 23;
        }

        /// <summary>
        /// To find more matches the key length is capped.
        /// This is done by removing the lowest value enchantment or modifier
        /// </summary>
        /// <param name="enchants"></param>
        /// <param name="modifiers"></param>
        /// <returns>The coin amount substracted</returns>
        public (long valueSubstracted, bool removedRarity, bool includeReforge, List<RankElem> ranked) CapKeyLength(
            List<Models.Enchant> enchants, List<KeyValuePair<string, string>> modifiers, SaveAuction auction, long threshold = 500000)
        {
            var underlyingItemValue = 0L;
            if (auction.Tag != null && Lookups.TryGetValue(auction.Tag, out var lookups))
            {
                var generalRelevant = lookups.Lookup.Where(v => v.Value.Price > 0).ToList();
                var percise = generalRelevant.Where(v => v.Key.Tier == auction.Tier).ToList();
                var relevant = percise.Count > 0 ? percise : generalRelevant;
                if (relevant.Count > 0)
                {
                    var firthPercentile = relevant.OrderBy(v => v.Value.Price).Skip((int)(relevant.Count * 0.05)).First().Value.Price;
                    underlyingItemValue = firthPercentile;
                    threshold = Math.Max(firthPercentile / 20, threshold);
                }
            }

            if (auction.Tag?.StartsWith("STARRED_SHADOW_ASSASSIN") ?? false)
            {
                // Jasper0 slot can't be accessed on starred (Fragged) items
                modifiers?.RemoveAll(m => m.Key == "JASPER_0");
            }
            var gems = modifiers.Where(m => m.Value == "PERFECT").ToList();
            long valueSubstracted = 0;
            foreach (var item in gems)
            {
                var gemKey = mapper.GetItemKeyForGem(item, auction.FlatenedNBT);
                if (BazaarPrices.TryGetValue(gemKey, out var price))
                {
                    valueSubstracted += (long)price; // no removal cost because this is just add
                    modifiers.Remove(item);
                }
            }
            if (gems.Count == 5)
            {
                modifiers.Add(new("pgems", "5"));
            }
            IEnumerable<RankElem> combined = ComparisonValue(enchants, modifiers, auction.Tag, auction.FlatenedNBT);

            var modifierSum = underlyingItemValue + combined?.Select(m => m.IsEstimate ? m.Value / 20 : m.Value).DefaultIfEmpty(0).Sum() ?? 0;
            threshold = Math.Max(threshold, modifierSum / 22);

            bool removedRarity = false;
            bool includeReforge = AddReforgeValue(auction, ref combined);
            combined = combined.OrderByDescending(i => i.Value).Where(c => c.Value != 0).ToList();
            var percentDiff = (double)auction.HighestBidAmount / modifierSum;
            if (auction.HighestBidAmount == 0 || percentDiff > 1)
                percentDiff = 1;
            // remove all but the top 5 
            var toRemove = combined.Skip(5).Where(c => c.Value > 0)
                    // keep top 2 even if below threshold
                    .Concat(combined.Skip(2).Where(c => c.Value > 0 && c.Value < threshold))
                    // always remove below 500k
                    .Concat(combined.Take(2).Where(c => c.Value > 0 && c.Value < 500_000)).ToList();
            foreach (var item in toRemove)
            {
                // use percentage of full value
                var adjustedRemoveValue = (long)(item.Value * percentDiff);
                // remove all but the top 5
                if (item.Enchant.Type != 0)
                {
                    if (enchants.Remove(item.Enchant))
                        valueSubstracted += adjustedRemoveValue;
                }
                else if (item.Reforge != ItemReferences.Reforge.None)
                {
                    includeReforge = false;
                }
                else
                {
                    if (modifiers.Remove(item.Modifier))
                        valueSubstracted += adjustedRemoveValue;
                    if (item.Modifier.Key == "rarity_upgrades")
                        removedRarity = true;
                }
            }
            var ordered = combined.Where(c => c.Value == 0).Concat(combined.Take(5)).ToList();
            return (valueSubstracted, removedRarity, includeReforge, ordered);

            bool AddReforgeValue(SaveAuction auction, ref IEnumerable<RankElem> combined)
            {
                bool includeReforge = Constants.RelevantReforges.Contains(auction.Reforge);
                if (includeReforge)
                {
                    long reforgeValue = GetReforgeValue(auction.Reforge);
                    combined = combined.Append(new RankElem(auction.Reforge, reforgeValue));
                }

                return includeReforge;
            }
        }

        private IEnumerable<RankElem> ComparisonValue(IEnumerable<Enchant> enchants, List<KeyValuePair<string, string>> modifiers, string tag, Dictionary<string, string> flatNbt)
        {
            var valuePerEnchant = enchants?.Select(item => new RankElem(item, mapper.EnchantValue(new Core.Enchantment(item.Type, item.Lvl), null, BazaarPrices)));

            var handler = (KeyValuePair<string, string> mod) =>
            {
                return ModifierEstimate(modifiers, tag, flatNbt, mod);
            };
            var valuePerModifier = modifiers?.Select(m =>
            {
                try
                {
                    return handler(m);
                }
                catch (Exception e)
                {
                    logger.LogInformation($"Error when calculating value for {m.Key} {m.Value} {tag}\n" + e);
                    return new RankElem(m, 0);
                }
            }).ToList();
            IEnumerable<RankElem> combined = null;
            if (valuePerEnchant != null && valuePerModifier != null)
                combined = valuePerEnchant.Concat(valuePerModifier);
            else if (valuePerEnchant != null)
                combined = valuePerEnchant;
            else if (valuePerModifier != null)
                combined = valuePerModifier;
            return combined;
        }

        private RankElem ModifierEstimate(List<KeyValuePair<string, string>> modifiers, string tag, Dictionary<string, string> flatNbt, KeyValuePair<string, string> mod)
        {
            var items = GetItemKeysForModifier(modifiers, flatNbt, tag, mod);
            var sum = 0L;
            foreach (var item in items)
            {
                if (!Lookups.TryGetValue(item.tag, out var lookup))
                {
                    sum += BazaarPrices.TryGetValue(item.tag, out var price) ? (long)price * item.amount : 0;
                    continue;
                }
                if (mod.Key.StartsWith("RUNE_"))
                {
                    sum += lookup.Lookup.Where(f => f.Value.Price != 0)
                        .OrderBy(v => (v.Key.Count + 1) * (v.Key.Modifiers.Count == 0 ? 1 : int.Parse(v.Key.Modifiers.First().Value)))
                        .FirstOrDefault().Value?.Price * item.amount ?? 0;
                    if (tag.Contains("RUNE") && sum < 500_000)
                    {
                        // do not remove rune levels from runes
                        sum = 0;
                    }
                    continue;
                }
                sum += (lookup.Lookup.Values.FirstOrDefault(f => f.Price != 0)?.Price ?? 0) * item.amount;
            }
            if (items.Count() > 0 && sum == 0)
            {
                // sum += 2_000_000; // would not have been stored if it was cheaper but is apparently currently missing
            }
            if (mod.Key == "upgrade_level")
            {
                sum += EstStarCost(tag, int.Parse(mod.Value));
            }
            if (mod.Key == "unlocked_slots")
            {
                var present = mod.Value.Split(',').ToList();
                var costs = itemService.GetSlotCostSync(tag, new(), present);
                foreach (var cost in costs.Item1)
                {
                    if (cost.Type.ToLower() == "item")
                        sum += GetPriceForItem(cost.ItemId) * cost.Amount ?? 1;
                    else
                        sum += cost.Coins;
                }
                if (costs.unavailable.Count() > 0)
                {
                    modifiers.RemoveAll(m => m.Key == "unlocked_slots");
                    var remaining = present.Except(costs.unavailable);
                    if (remaining.Count() > 0)
                        modifiers.Add(new(mod.Key, string.Join(",", remaining.OrderBy(s => s))));
                }
            }
            if (mod.Key == "scroll_count")
            {
                sum += (GetPriceForItem("IMPLOSION_SCROLL") + GetPriceForItem("SHADOW_WARP_SCROLL") + GetPriceForItem("WITHER_SHIELD_SCROLL")) / 3 * int.Parse(mod.Value);
            }
            if (mod.Key == "winning_bid")
                sum += (int)(float.Parse(mod.Value) * 10_000_000);
            if (mod.Key == "full_bid")
                sum += (int)(float.Parse(mod.Value) * 48_000_000);
            // early return if we have a value before estimates
            if (sum > 0 || mod.Key == null)
                return new RankElem(mod, sum);
            if (mod.Key == "pgems")
            {
                sum += 100_000_000;
            }
            if (mod.Key == "eman_kills")
                sum += 3_000_000 * (int)Math.Pow(2, int.Parse(mod.Value));
            else if (mod.Key == "expertise_kills")
                sum += 3_000_000 * (int)Math.Pow(2, int.Parse(mod.Value));
            else if (mod.Key.EndsWith("_kills"))
            {
                sum += 300_000 * (int)Math.Pow(2, int.Parse(mod.Value)) + 300_000;
            }
            if (mod.Key == "mined_crops")
                sum += 13_000_000 * (int)Math.Pow(2, int.Parse(mod.Value));
            if (mod.Key == "color")
                sum += 10_000_000;
            if (mod.Key == "blocksBroken")
                sum += 1_000_000 * (int)Math.Pow(2, int.Parse(mod.Value));
            if (Constants.AttributeKeys.Contains(mod.Key))
            {
                sum += 200_000 * (long)Math.Pow(2, int.Parse(mod.Value)) + 600_000;
                if (modifiers.Any(m => m.Key != mod.Key && Constants.AttributeKeys.Contains(m.Key)))
                    sum += 50_000_000; // godroll
            }
            if (IsSoul(mod))
            {
                sum += 3_000_000 * (int)Math.Pow(1.5, int.Parse(mod.Value));
            }
            if (mod.Key == "exp")
            {
                var factor = Math.Max(GetPriceForItem(tag) / 6, 10_000_000);
                sum += (int)(factor * (float.Parse(mod.Value) + 1));
            }
            if (mod.Key == "candyUsed")
                sum += Math.Max(GetPriceForItem(tag) / 6, 10_000_000); // for skined pets important
            if (mod.Key == "is_shiny")
                sum += 88_000_000;
            if (mod.Key == "party_hat_color")
                sum += 20_000_000;
            if (mod.Key == "thunder_charge")
                sum += 55_000_000 * int.Parse(mod.Value);
            if (mod.Key == "baseStatBoostPercentage")
                sum += (int)((float.Parse(mod.Value) - 45) * 500_000);
            if (mod.Key == "rarity_upgrades" && sum == 0)
            {
                if (Random.Shared.NextDouble() < 0.01)
                    deferred.Log($"Rarity upgrade missing price {JsonConvert.SerializeObject(flatNbt)}\n{JsonConvert.SerializeObject(items)} ");
                sum += 8_000_000;
            }
            if (mod.Key == "hotpc")
                sum += 3_000_000;
            if (mod.Key == "new_years_cake")
                sum += int.Parse(mod.Value) switch
                {
                    < 20 => 20_000_000,
                    69 => 10_000_000,
                    < 120 => 2_000_000,
                    _ => 600_000,
                };
            if (mod.Key == "party_hat_emoji")
                sum += 8_000_000;
            if (mod.Key == "edition")
                sum += 8_000_000;
            if (mod.Key == "collected_coins")
                sum += 100_000_000 * int.Parse(mod.Value) + 1_000_000;
            return new RankElem(mod, sum)
            {
                IsEstimate = true
            };
        }

        private long GetReforgeValue(ItemReferences.Reforge reforge)
        {
            var reforgeCost = mapper.GetReforgeCost(reforge);
            var itemCost = GetPriceForItem(reforgeCost.Item1);
            var reforgeValue = itemCost + reforgeCost.Item2;
            return reforgeValue;
        }

        private List<Models.Enchant> RemoveNoEffectEnchants(SaveAuction auction, List<Models.Enchant> ench)
        {
            if (auction.Tag == null)
                return ench;
            if (itemService?.IsDungeonItemSync(auction.Tag) ?? false)
                ench = RemoveEnchantFromKey(ench, Enchantment.EnchantmentType.scavenger);
            foreach (var item in mapper.IrrelevantOn(auction.Tag))
            {
                ench = RemoveEnchantFromKey(ench, item.Item1, item.level);
            }
            return ench;
        }

        private static List<Models.Enchant> RemoveEnchantFromKey(List<Models.Enchant> enchList, Core.Enchantment.EnchantmentType ench, int maxLevel = 10)
        {
            if (enchList.Any(e => e.Type == ench))
                return enchList.Where(e => e.Type != ench || e.Lvl > maxLevel).ToList();
            return enchList;
        }

        private static List<KeyValuePair<string, string>> AssignEmptyModifiers(SaveAuction auction)
        {
            if (NBT.IsPet(auction.Tag))
                if (auction.FlatenedNBT.TryGetValue("heldItem", out var val) && val == "PET_ITEM_TIER_BOOST")
                    return new List<KeyValuePair<string, string>>(EmptyPetModifiers) { new(PetItemKey, TierBoostShorthand) };
                else
                    return EmptyPetModifiers.ToList();
            if (auction.Tag.Contains("RUNE_"))
                return auction.FlatenedNBT.ToList();
            if (auction.FlatenedNBT.Any(n => NeverDrop.Contains(n.Key)))
                return auction.FlatenedNBT.Where(n => NeverDrop.Contains(n.Key)).ToList();
            else
                return EmptyModifiers.ToList();
        }


        private KeyValuePair<string, string> NormalizeData(KeyValuePair<string, string> s, string tag, Dictionary<string, string> flattenedNbt)
        {
            if (tag == "ATTRIBUTE_SHARD")
                return s; // don't normalize attribute shards only one attribute on them
            if (s.Key == "exp")
            {
                var expMulti = tag == "PET_GOLDEN_DRAGON" ? GoldenDragonMaxExp / PetExpMaxlevel : 1;
                var exp = GetNumeric(s);
                if (exp > 1_000_000 * expMulti && exp <= 2_500_000 * expMulti)
                    return new KeyValuePair<string, string>(s.Key, "0.3");
                else if (exp > 2_500_000 * expMulti && exp < PetExpMaxlevel * expMulti / 6)
                    return new KeyValuePair<string, string>(s.Key, "0.6");
                if (tag == "PET_GOLDEN_DRAGON")
                    return NormalizeNumberTo(s, GoldenDragonMaxExp / 7, 7);
                else
                    return NormalizeNumberTo(s, PetExpMaxlevel / 6, 6);
            }
            var generalNormalizations = NormalizeGeneral(s, (tag?.StartsWith("MIDAS") ?? false) || (tag?.StartsWith("STARRED_MIDAS") ?? false),
                flattenedNbt,
                tag == "PET_GOLDEN_DRAGON"
                );
            if (generalNormalizations.Value != "continue")
                return generalNormalizations;
            if (s.Key == "hpc")
                return GetNumeric(s) switch
                {
                    15 => new("hotpc", "1"),
                    /// this is mirrored in <see cref="PropertyMapper"/>
                    > 10 => new("hotpc", "0"),
                    _ => Ignore
                };
            if (s.Key == "ability_scroll")
                return new("scroll_count", (s.Value.Where(c => c == ',' || c == ' ').Count() + 1).ToString());
            if (s.Key == "heldItem")
            {
                var heldItem = s.Value switch
                {
                    "MINOS_RELIC" => "MINOS_RELIC",
                    "QUICK_CLAW" => "QUICK_CLAW",
                    "PET_ITEM_QUICK_CLAW" => "QUICK_CLAW",
                    "PET_ITEM_TIER_BOOST" => TierBoostShorthand,
                    "PET_ITEM_LUCKY_CLOVER" => "LUCKY_CLOVER",
                    "PET_ITEM_LUCKY_CLOVER_DROP" => "LUCKY_CLOVER",
                    "GREEN_BANDANA" => "GREEN_BANDANA",
                    "PET_ITEM_COMBAT_SKILL_BOOST_EPIC" => "COMBAT_SKILL_BOOST_EPIC",
                    "PET_ITEM_FISHING_SKILL_BOOST_EPIC" => "FISHING_SKILL_BOOST_EPIC",
                    "PET_ITEM_FORAGING_SKILL_BOOST_EPIC" => "FORAGING_SKILL_BOOST_EPIC",
                    "ALL_SKILLS_SUPER_BOOST" => "ALL_SKILLS_SUPER_BOOST",
                    "PET_ITEM_EXP_SHARE" => "EXP_SHARE",
                    _ => null
                };
                // ignore exp based items if exp is maxed
                if ((heldItem == "EXP_SHARE" || (heldItem?.Contains("_SKILL") ?? false))
                    && GetNumeric(flattenedNbt.FirstOrDefault(f => f.Key == "exp")) >= PetExpMaxlevel)
                    return Ignore;
                if (heldItem == null)
                    return Ignore;
                return new KeyValuePair<string, string>(PetItemKey, heldItem);
            }
            if (s.Key == "is_shiny" && !(tag.EndsWith("_CHESTPLATE") || tag == "HYPERION" || tag == "NECRON_HANDLE"))
            {
                return Ignore;
            }
            if (s.Key == "dungeon_item_level" && flattenedNbt.TryGetValue("upgrade_level", out _))
                return Ignore; // upgrade level is always higher (newer)
            if (s.Key == "dungeon_item_level")
                return new KeyValuePair<string, string>("upgrade_level", s.Value);
            if (ShardAttributes.TryGetValue(s.Key, out var minLvl))
            {
                if (int.Parse(s.Value) >= minLvl)
                    return s;
                if (HasAttributeCombo(s, flattenedNbt, tag))
                    return s;
                return Ignore;
            }
            if (s.Key == "baseStatBoostPercentage")
            {
                var val = int.Parse(s.Value);
                if (val < 46)
                    return Ignore;
                //if (val < 50)
                //    return new KeyValuePair<string, string>("baseStatBoost", "46-49");
                if (val == 50) // max level found
                    return new KeyValuePair<string, string>("baseStatBoost", "50");
                if (val > 50)
                    return new KeyValuePair<string, string>("baseStatBoost", ">50");
            }
            if (s.Key.StartsWith("RUNE_") && !IncludeKeys.Contains(s.Key) && !tag.Contains("RUNE_"))
            {
                return Ignore;
            }
            return s;
        }

        public static KeyValuePair<string, string> NormalizeGeneral(KeyValuePair<string, string> s, bool isMiddas, Dictionary<string, string> flatten, bool isGDrag)
        {
            if (s.Key == "winning_bid")
                if (isMiddas)
                    if (flatten.ContainsKey("additional_coins") || s.Value.Length > 8)
                    {
                        // combine with additional coins
                        var additionalCoins = GetNumeric(flatten.FirstOrDefault(f => f.Key == "additional_coins"));
                        var fullValue = (int)(double.Parse(s.Value) + additionalCoins + 1) / 50_000_000;
                        return new KeyValuePair<string, string>("full_bid", fullValue.ToString());
                    }
                    else
                        return NormalizeNumberTo(s, 25_000_000, 5);
                else
                    return Ignore;
            if (s.Key == "eman_kills")
                return NormalizeGroupNumber(s, 10_000, 25_000, 50_000, 75_000, 100_000, 125_000, 150_000, 200_000);
            if (s.Key == "blood_god_kills")
                return NormalizeGroupNumber(s, 1_000_000, 10_000_000, 20_000_000, 100_000_000);
            if (s.Key == "expertise_kills")
                return NormalizeGroupNumber(s, 5_500, 15_000);
            if (s.Key.EndsWith("_kills"))
                return NormalizeNumberTo(s, 10_000);
            if (s.Key == "yogsKilled")
                return NormalizeNumberTo(s, 5_000, 2);
            if (s.Key == "thunder_charge")
                return NormalizeNumberTo(s, 1_000_000, 5);
            if (s.Key == "mined_crops")
                return NormalizeNumberTo(s, 500_000_000);
            if (s.Key == "blocksBroken")
                return NormalizeNumberTo(s, 20_000, 2);
            if (s.Key == "collected_coins")
                return NormalizeGroupNumber(s, 100_000_000, 1_000_000_000);
            if (s.Key == "candyUsed")
            {
                var expAmount = GetNumeric(flatten.FirstOrDefault(f => f.Key == "exp"));
                var hasSkin = flatten.ContainsKey("skin");
                if ((expAmount >= PetExpMaxlevel && !isGDrag || expAmount >= GoldenDragonMaxExp) && !hasSkin)
                    return Ignore; // not displayed on max exp items
                else
                    // all candied are the same
                    return new KeyValuePair<string, string>(s.Key, (double.Parse(s.Value) > 0 ? 1 : 0).ToString());
            }
            if (s.Key == "edition")
            {
                return NormalizeEdition(s);
            }
            if (s.Key == "talisman_enrichment")
                return new KeyValuePair<string, string>("talisman_enrichment", "yes");
            return new KeyValuePair<string, string>(Ignore.Key, "continue");
        }

        public static KeyValuePair<string, string> NormalizeEdition(KeyValuePair<string, string> s)
        {
            var val = int.Parse(s.Value);
            if (val < 100)
                return new KeyValuePair<string, string>(s.Key, "verylow");
            if (val < 1000)
                return new KeyValuePair<string, string>(s.Key, "low");
            if (val < 10000)
                return new KeyValuePair<string, string>(s.Key, "10k");
            return new KeyValuePair<string, string>(s.Key, "100k");
        }

        /// <summary>
        /// Matches valuable attribute combinations
        /// </summary>
        /// <param name="s"></param>
        /// <param name="auction"></param>
        /// <returns></returns>
        private bool HasAttributeCombo(KeyValuePair<string, string> s, Dictionary<string, string> flattenedNbt, string tag)
        {
            return AttributeComboLookup.TryGetValue(s.Key, out var otherKeys) && otherKeys.Any(otherKey => flattenedNbt.TryGetValue(otherKey, out _))
                || ItemSpecificAttributeComboLookup.TryGetValue(tag, out var combos)
                    && combos.Any(combo => (s.Key == combo.Key && flattenedNbt.TryGetValue(combo.Value, out _) || s.Key == combo.Value && flattenedNbt.TryGetValue(combo.Key, out _)));
        }

        /// <summary>
        /// Returns keys that are higher value and have to be checked before something is declared to be a snipe
        /// </summary>
        /// <param name="baseKey">The actual auction key</param>
        /// <returns></returns>
        private IEnumerable<AuctionKey> HigherValueKeys(AuctionKey baseKey, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, double lbinPrice)
        {
            var exp = baseKey.Modifiers.Where(m => m.Key == "exp").FirstOrDefault();
            if (exp.Key != default && exp.Value != "6")
            {
                for (int i = (int)float.Parse(exp.Value) + 1; i < 7; i++)
                {
                    yield return new AuctionKey(baseKey)
                    {
                        Modifiers = baseKey.Modifiers.Where(m => m.Key != "exp").Append(new("exp", i.ToString())).OrderBy(m => m.Key).ToList().AsReadOnly()
                    };
                }
            }
            foreach (var item in baseKey.Modifiers.Where(m => ShardAttributes.ContainsKey(m.Key)))
            {
                for (int i = int.Parse(item.Value) + 1; i < 10; i++)
                {
                    yield return new AuctionKey(baseKey)
                    {
                        Modifiers = baseKey.Modifiers.Where(m => m.Key != item.Key).Append(new(item.Key, i.ToString())).OrderBy(m => m.Key).ToList().AsReadOnly()
                    };
                }
            }
            if (baseKey.Count <= 1 && lbinPrice > MIN_TARGET * 5)
            {
                for (int i = (int)baseKey.Tier; i < (int)Tier.VERY_SPECIAL + 1; i++)
                {
                    yield return new AuctionKey(baseKey)
                    {
                        Modifiers = baseKey.Modifiers.Append(new("rarity_upgrades", "1")).OrderBy(m => m.Key).ToList().AsReadOnly(),
                        Tier = (Tier)(i + 1)
                    };
                    yield return new AuctionKey(baseKey)
                    {
                        Tier = (Tier)(i + 1)
                    };
                }
                // check against every other item with the same enchants and modifiers (and more) - which should be higher value
                foreach (var item in l.Where(x => x.Value.Price > 0).Select(x => x.Key).Where(k => k != baseKey && baseKey.Modifiers
                    .All(m => k.Modifiers != null && k.Modifiers.Any(km => km.Key == m.Key && km.Value == m.Value))
                            && baseKey.Enchants
                    .All(e => k.Enchants != null && k.Enchants.Any(ek => e.Type == ek.Type && ek.Lvl == e.Lvl)) && k.Tier == baseKey.Tier))
                {
                    if (l.GetValueOrDefault(item)?.Price == 0)
                        continue;
                    yield return item;
                }
            }
            var compactEnch = baseKey.Enchants.FirstOrDefault(e => e.Type == Enchantment.EnchantmentType.compact && e.Lvl >= 5);
            if (compactEnch.Lvl != default)
            {
                for (int i = compactEnch.Lvl + 1; i < 10; i++)
                {
                    yield return new AuctionKey(baseKey)
                    {
                        Enchants = baseKey.Enchants.Where(e => e.Type != Enchantment.EnchantmentType.compact).Append(new()
                        {
                            Type = Enchantment.EnchantmentType.compact,
                            Lvl = (byte)i
                        }).OrderBy(e => e.Type).ToList().AsReadOnly()
                    };
                }
            }

            if (baseKey.Count > 1 && baseKey.Count < 64)
                yield return new AuctionKey(baseKey) { Count = 64 };
            if (baseKey.Count > 1 && baseKey.Count < 16)
                yield return new AuctionKey(baseKey) { Count = 16 };
        }

        public static KeyValuePair<string, string> NormalizeNumberTo(KeyValuePair<string, string> s, int groupingSize, int highestGroup = int.MaxValue)
        {
            var group = GetNumeric(s) / groupingSize;
            return new KeyValuePair<string, string>(s.Key, Math.Min(group, highestGroup).ToString());
        }

        /// <summary>
        /// Determine group by  making less than comparison, smallest group is first
        /// </summary>
        /// <param name="s"></param>
        /// <param name="groups"></param>
        /// <returns></returns>
        public static KeyValuePair<string, string> NormalizeGroupNumber(KeyValuePair<string, string> s, params int[] groups)
        {
            var val = GetNumeric(s);
            for (int i = 0; i < groups.Length; i++)
            {
                if (val < groups[i])
                {
                    return new KeyValuePair<string, string>(s.Key, i.ToString());
                }
            }
            var highestGroup = groups.Length;
            return new KeyValuePair<string, string>(s.Key, highestGroup.ToString());
        }

        public static long GetNumeric(KeyValuePair<string, string> s)
        {
            if (s.Value == null)
                return 0;
            try
            {
                return ((long)double.Parse(s.Value));
            }
            catch (Exception)
            {
                deferred.Log($"could not parse {s.Key} {s.Value}");
                throw;
            }
        }

        public void TestNewAuction(SaveAuction auction, bool triggerEvents = true)
        {
            using var activity = !triggerEvents ? null : activitySource?.StartActivity("TestNewAuction", ActivityKind.Internal);
            activity?.SetTag("uuid", auction.Uuid);
            activity?.SetTag("server", ServerDnsName);
            var itemGroupTag = GetAuctionGroupTag(auction.Tag);
            var lookup = Lookups.GetOrAdd(itemGroupTag.Item1, key => new PriceLookup());
            var l = lookup.Lookup;
            var cost = auction.StartingBid;
            var lbinPrice = auction.StartingBid * 1.03 + itemGroupTag.Item2;
            var medPrice = auction.StartingBid * 1.05 + itemGroupTag.Item2;
            var lastKey = new AuctionKey();
            var shouldTryToFindClosest = false;
            var basekey = DetailedKeyFromSaveAuction(auction);
            for (int i = 0; i < 5; i++)
            {
                var key = basekey.GetReduced(i);
                if (i > 0 && key == lastKey)
                {
                    if (i < 4)
                        shouldTryToFindClosest = true;
                    continue; // already checked that
                }
                lastKey = key;

                if (!l.TryGetValue(key, out ReferenceAuctions bucket))
                {
                    if (triggerEvents && i == 4)
                    {
                        logger.LogInformation($"could not find bucket {key} for {auction.Tag} {l.Count} {auction.Uuid}");
                        if (this.State < SniperState.Ready)
                        {
                            if (auction.UId % 10 == 2)
                                logger.LogInformation($"closest is not available yet, state is {this.State}");
                            return;
                        }
                        var closests = FindClosest(l, key, itemGroupTag.tag).Take(5).ToList();
                        foreach (var item in closests)
                        {
                            logger.LogInformation($"Closest bucket clean: {item.Key}");
                        }
                        if (!closests.Any())
                        {
                            logger.LogInformation($"No closest bucket found for {key} {auction.Uuid}");
                            break;
                        }
                        if (ShouldIgnoreMostSimilar(auction))
                        {
                            return;
                        }
                        bucket = closests.FirstOrDefault().Value;
                        var closestKey = closests.FirstOrDefault().Key;
                        if (bucket.HitsSinceCalculating > 8)
                        {
                            logger.LogInformation($"Bucket {closestKey} for {auction.Uuid} has been hit {bucket.HitsSinceCalculating} times, skipping");
                            TryFindClosestRisky(auction, l, ref lbinPrice, ref medPrice);
                            return;
                        }
                        lbinPrice *= Math.Pow(1.15, bucket.HitsSinceCalculating);
                        medPrice *= Math.Pow(1.25, bucket.HitsSinceCalculating);
                        shouldTryToFindClosest = true;
                        break; // don't use most similar until key lenght limit is added
                    }
                    else if (i != 0)
                        continue;
                    else
                        bucket = CreateAndAddBucket(auction, key);
                }
                if (i == 0)
                    UpdateLbin(auction, bucket, key);
                if (triggerEvents)
                {
                    long extraValue = GetExtraValue(auction, key) - itemGroupTag.Item2;
                    if (FindFlip(auction, lbinPrice, medPrice, bucket, key, lookup, basekey, extraValue, props =>
                    {
                        props["breakdown"] = JsonConvert.SerializeObject(basekey.ValueBreakdown);
                    }))
                        shouldTryToFindClosest = false; // found a snipe, no need to check other lower value buckets
                }
            }
            if (!triggerEvents)
                return; // no need to check for closest, just storing
            var topKey = basekey.GetReduced(0);
            var topAttrib = basekey.ValueBreakdown.FirstOrDefault();
            if (topAttrib != default)
            {
                medPrice = auction.StartingBid * 1.06 + itemGroupTag.Item2;
                CheckCombined(auction, lookup, lbinPrice, medPrice, basekey, topAttrib);
                CheckLowerKeyFull(auction, lookup, lbinPrice, medPrice, basekey, l);
            }
            if (shouldTryToFindClosest && triggerEvents && this.State >= SniperState.Ready)
            {
                TryFindClosestRisky(auction, l, ref lbinPrice, ref medPrice);
            }

            var componentGuess = basekey.ValueBreakdown.Sum(c => c.IsEstimate ? -long.MaxValue / 20 : c.Value);
            if (componentGuess > medPrice / 8) // no need to check if sum is too low
            {
                var valueLookup = basekey.ValueBreakdown.ToDictionary(v =>
                {
                    if (v.Modifier.Key != default)
                        return v.Modifier.Key;
                    if (v.Reforge != default)
                        return v.Reforge.ToString();
                    return v.Enchant.Type.ToString();
                }, c => c.IsEstimate ? c.Value / 20 : c.Value);
                var cleanCost = GetCleanItemPrice(itemGroupTag.tag, basekey, lookup);
                if (basekey.ValueBreakdown.Count == 1 && basekey.Key.Modifiers.FirstOrDefault(m => m.Key == itemGroupTag.tag).Key != default)
                {
                    cleanCost = 0; // breakdown already includes cheapest item (rune probably)
                }
                var componentSum = valueLookup.Select(v => (long)(v.Key switch
                {
                    "skin" => auction.Tag.StartsWith("PET") ? 0.5 : 0.4,
                    "ultimate_fatal_tempo" => 0.65,
                    "rarity_upgrades" => 0.5,
                    "upgrade_level" => 0.8,
                    _ => 0.85
                } * v.Value)).Sum();
                var combined = componentSum + cleanCost;
                if (combined / 1.1 > medPrice || combined - auction.StartingBid > 10_000_000)
                {
                    var props = new Dictionary<string, string>
                    {
                        { "cleanCost", cleanCost.ToString() },
                        { "componentsSum", componentGuess.ToString() },
                        { "breakdown", JsonConvert.SerializeObject(valueLookup) }
                    };
                    FoundAFlip(auction, new(), LowPricedAuction.FinderType.CraftCost, combined, props);
                }
            }
            activity.Log($"BaseKey value {JsonConvert.SerializeObject(basekey.ValueBreakdown)}");
        }

        private void CheckCombined(SaveAuction auction, PriceLookup lookup, double lbinPrice, double medPrice, KeyWithValueBreakdown fullKey, RankElem topAttrib)
        {
            var topKey = fullKey.GetReduced(0);
            var l = lookup.Lookup;
            var similar = l.Where(e => topAttrib.Modifier.Key != default && !e.Key.Modifiers.Any(m => m.Key == "virtual") || e.Key.Enchants.Contains(topAttrib.Enchant)).ToList();
            if (similar.Count == 1)
            {
                // include all if no match otherwise
                similar = l.ToList();
            }
            var targetVolume = 11;
            if(lookup.Lookup.TryGetValue(topKey, out var topBucket) && topBucket.References.Count >= targetVolume)
            {
                return; // enough references in previous check
            }
            var relevant = similar.Where(e => IsHigherValue(auction.Tag, e.Key, topKey)
                                && e.Key.Reforge == topKey.Reforge)
                .OrderByDescending(e => e.Key.Modifiers.Count + e.Key.Enchants.Count)
                .ThenByDescending(e => ComparisonValue(e.Key.Enchants, e.Key.Modifiers.ToList(), GetAuctionGroupTag(auction.Tag).tag, null).Sum(s=>s.Value))
                .ToList();
            if (relevant.Count < 2)
            {
                return; // makes only sense if there is something combined
            }
            // get enough relevant to build a median and try to get highest value (most enchantments and modifiers)
            var combined = relevant.SelectMany(r => r.Value.References.Select(ri => (ri, relevancy: (r.Key.Modifiers.Count + r.Key.Enchants.Count) * 10 + ri.Day)))
                                .Reverse() // get the newest first
                                .OrderByDescending(r => r.relevancy).Select(r => r.ri).Take(targetVolume).ToList();
            if (combined.Count == 0)
            {
                return;
            }
            var lbinBucket = relevant.MinBy(r => r.Value.Lbin.Price).Value;
            var virtualBucket = new ReferenceAuctions()
            {
                Lbins = [lbinBucket.Lbin],
                References = new(combined),
                Price = combined.Count < 4 ? 0 : GetCappedMedian(auction, fullKey, combined),
                OldestRef = (short)(GetDay() - 2),
                Volatility = 123// mark as risky
            };
            // mark with extra value -3
            var foundAndAbort = FindFlip(auction, lbinPrice, medPrice, virtualBucket, topKey, lookup, fullKey, MIN_TARGET == 0 ? 0 : -3, props =>
            {
                var total = 0;
                props.Add("combined", string.Join(",", relevant.TakeWhile(c => (total += c.Value.References.Count) < targetVolume)
                    .Select(c => c.Key.ToString() + ":" + c.Value.References.Count)));
                props.Add("breakdown", JsonConvert.SerializeObject(fullKey.ValueBreakdown));
                logger.LogInformation($"Combined {fullKey} {auction.Uuid} {virtualBucket.Price} {virtualBucket.Lbin.Price} keys: {string.Join(",", relevant.Select(r => r.Key))}");
            });

            long GetCappedMedian(SaveAuction auction, KeyWithValueBreakdown fullKey, List<ReferencePrice> combined)
            {
                var median = GetMedian(combined, []);
                var shortTerm = GetMedian(combined.Take(5).ToList(), new());
                var group = GetAuctionGroupTag(auction.Tag);
                median = CapAtCraftCost(group.tag, Math.Min(median, shortTerm), fullKey, 0);
                return median;
            }
        }

        private void CheckLowerKeyFull(SaveAuction auction, PriceLookup lookup, double lbinPrice, double medPrice, KeyWithValueBreakdown fullKey, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l)
        {
            // check if complicated item
            if (fullKey.ValueBreakdown.Count < 3)
                return; // not complicated
            if (auction.Tag.StartsWith("PET_"))
                return; // eg Enderman gets cheaper at mythic for some reason
            if (auction.Tag.StartsWith("STARRED_MIDAS_"))
                return; // midas references were only recently split
            (var enchant, var modifiers) = SelectValuable(auction);
            var key = new AuctionKeyWithValue()
            {
                Count = 1,
                Enchants = new(enchant ?? new()),
                Modifiers = new(modifiers ?? new()),
                Tier = auction.Tier,
                Reforge = auction.Reforge
            };
            var today = GetDay();
            var containing = l.Where(e => e.Value.Price > 0 && e.Value.References.Count > 5
                            && (e.Key.Reforge == key.Reforge || e.Key.Reforge == ItemReferences.Reforge.Any)
                            && e.Value.References.Any(r => r.Day >= today - 2)
                            && IsHigherValue(auction.Tag, e.Key, key))
                        .OrderByDescending(e => e.Value.Price).FirstOrDefault();
            if (containing.Value == default)
                return;
            FindFlip(auction, lbinPrice, medPrice, containing.Value, key, lookup, fullKey, 0, props =>
            {
                props.Add("fullKey", key.ToString());
                props.Add("usedKey", containing.Key.ToString());
                props.Add("by", "lowerfullkey");
            });
        }

        public static readonly HashSet<string> HyperionGroup = new() { "SCYLLA", "VALKYRIE", "NECRON_BLADE", "ASTRAEA" };

        /// <summary>
        /// Remaps item tags into one item if they are easily switchable
        /// </summary>
        /// <param name="auction"></param>
        /// <returns></returns>
        public (string tag, long costSubstract) GetAuctionGroupTag(string itemGroupTag)
        {
            if (HyperionGroup.Contains(itemGroupTag))
                return ("HYPERION", GetPriceForItem("GIANT_FRAGMENT_LASER") * 8); // easily craftable from one into the other
            if (itemGroupTag.StartsWith("STARRED_")
                && !itemGroupTag.Contains("MIDAS_") && !itemGroupTag.StartsWith("STARRED_DAEDALUS_AXE")) // midas and daedalus needs golden fragments which are expensive
                // technically neds 8 for crafting but looses the value on craft so using 7
                return (itemGroupTag.Substring(8), GetPriceForItem("LIVID_FRAGMENT") * 7);
            return (itemGroupTag, 0);
        }

        private static bool ShouldIgnoreMostSimilar(SaveAuction auction)
        {
            return auction.Tag.StartsWith("RUNE_");
        }

        private void TryFindClosestRisky(SaveAuction auction, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, ref double lbinPrice, ref double medPrice)
        {
            if (auction.Tag.StartsWith("RUNE_")) // TODO: compare levels
                return;
            if (auction.Tag == "NEW_YEAR_CAKE")
                return; // can't use closest for years
            // special case for items that have no reference bucket, search using most similar
            var detailedKey = DetailedKeyFromSaveAuction(auction);
            var key = detailedKey.GetReduced(0);
            var closest = FindClosestTo(l, key, auction.Tag);
            medPrice *= 1.10; // increase price a bit to account for the fact that we are not using the exact same item
            if (closest.Value == null)
            {
                Logs.Enqueue(new LogEntry()
                {
                    Key = key,
                    LBin = -1,
                    Median = -1,
                    Uuid = auction.Uuid,
                    Volume = -1
                });
                return;
            }
            if (auction.FlatenedNBT.Any(f => f.Value == "PET_ITEM_TIER_BOOST") && !closest.Key.Modifiers.Any(m => m.Key == PetItemKey))
                return; // probably overvalues tier boost on enderdragon
            if (closest.Key == key)
                return; // already found - or rather not - by median
            else
                logger.LogInformation($"Would estimate closest to {key} {closest.Key} {auction.Uuid} for {closest.Value.Price}");
            if (closest.Value.Price <= medPrice)
                return;
            var props = new Dictionary<string, string>() { { "closest", closest.Key.ToString() } };
            var missingModifiers = closest.Key.Modifiers.Where(m => !key.Modifiers.Contains(m)).ToList();
            long toSubstract = 0;
            if (key.Modifiers.Any(m => m.Value == TierBoostShorthand) && !closest.Key.Modifiers.Any(m => m.Value == TierBoostShorthand))
            {
                toSubstract += GetCostForItem("PET_ITEM_TIER_BOOST");
            }
            if (missingModifiers.Count > 0)
            {
                toSubstract = GetPriceSumForModifiers(missingModifiers, key.Modifiers, auction);
                toSubstract += AdjustForAttributes(closest.Value.Price, key, missingModifiers, auction);
                if (toSubstract < 0)
                {
                    deferred.Log($"Negative value to substract for {string.Join(",", missingModifiers.Select(m => $"{m.Key}:{m.Value}"))} {auction.Uuid}");
                    toSubstract = Math.Abs(toSubstract);
                }
                var fromExp = GetValueDifferenceForExp(auction, closest.Key, l);
                if (fromExp != 0)
                {
                    props.Add("fromExp", fromExp.ToString());
                }
                toSubstract -= fromExp;
                if (missingModifiers.Any(m => m.Key == "candyUsed" && m.Value == "0"))
                    toSubstract += (long)(closest.Value.Price * 0.1); // 10% for pet candy
                var killModifier = missingModifiers.FirstOrDefault(m => m.Key.EndsWith("kills"));
                if (killModifier.Key != default)
                {
                    var killCount = int.Parse(killModifier.Value);
                    var present = key.Modifiers.FirstOrDefault(n => n.Key == killModifier.Key);
                    var difference = killCount - int.Parse(present.Value ?? "0");
                    var killPrice = difference * 1_000_000;
                    if (difference < 0)
                        killPrice /= 2; // only half for adding kills
                    toSubstract += killPrice;
                }
                var formatted = string.Join(",", missingModifiers.Select(m => $"{m.Key}:{m.Value}"));
                if (toSubstract == 0)
                {
                    deferred.Log($"Could not find value to substract for {formatted} {auction.Uuid}");
                }
                props.Add("missingModifiers", formatted + $" ({toSubstract})");
            }
            var missingEnchants = closest.Key.Enchants.Where(m => !key.Enchants.Contains(m)).ToList();
            if (missingEnchants.Count > 0)
            {
                var enchVal = GetPriceSumForEnchants(missingEnchants);
                toSubstract += enchVal;
                props.Add("missingEnchants", string.Join(",", missingEnchants.Select(e => $"{e.Type}_{e.Lvl}")) + $" ({enchVal})");
            }
            var additionalEnchants = key.Enchants.Where(e => !closest.Key.Enchants.Contains(e)).ToList();
            if (additionalEnchants.Count > 0)
            {
                var valEst = GetPriceSumForEnchants(additionalEnchants) / 2;
                var enchantVal = Math.Min(valEst, closest.Value.Price / 4);
                toSubstract -= enchantVal;
                props.Add("enchValueAdded", string.Join(",", additionalEnchants.Select(e => $"{e.Type}_{e.Lvl}")) + $" ({enchantVal}-{valEst})");
            }
            var targetPrice = (long)((closest.Value.Price - toSubstract) * 0.9);
            // adjust due to count
            if (closest.Key.Count != auction.Count)
            {
                var countDiff = closest.Key.Count - auction.Count;
                var countDiffPrice = countDiff * targetPrice / closest.Key.Count;
                targetPrice -= countDiffPrice;
                props.Add("countDiff", $"{countDiff} ({countDiffPrice})");
            }
            // adjust price of reforge 
            if (closest.Key.Reforge != auction.Reforge)
            {
                var closestDetails = mapper.GetReforgeCost(closest.Key.Reforge, auction.Tier);
                var auctionDetails = mapper.GetReforgeCost(auction.Reforge, auction.Tier);
                var closestItemCost = GetCostForItem(closestDetails.Item1);
                if (closestItemCost == 0 && !string.IsNullOrEmpty(closestDetails.Item1))
                {
                    closestItemCost = 2_000_000; // estimated cost for missing items
                }
                var reforgeDifference = closestItemCost + closestDetails.Item2 - (GetCostForItem(auctionDetails.Item1) - auctionDetails.Item2) / 2;
                targetPrice -= reforgeDifference;
                props.Add("reforge", $"{closest.Key.Reforge} -> {auction.Reforge} ({reforgeDifference})");
            }
            if (closest.Key.Tier > auction.Tier && auction.FlatenedNBT.Any(f => f.Key == "exp"))
            {
                var stepsDiff = closest.Key.Tier - auction.Tier;
                var tierDifference = (long)(closest.Value.Price * 0.9 - closest.Value.Price / Math.Pow(5, Math.Abs(stepsDiff)));
                targetPrice -= tierDifference;
                props.Add("tierVal", $"{closest.Key.Tier} -> {auction.Tier} ({tierDifference})");
            }
            AddMedianSample(closest.Value.References, props);
            FoundAFlip(auction, closest.Value, LowPricedAuction.FinderType.STONKS, targetPrice, props);
        }

        private long AdjustForAttributes(double medPrice, AuctionKey key, List<KeyValuePair<string, string>> missingModifiers, SaveAuction auction)
        {
            var missingAttributes = missingModifiers.Where(m => Constants.AttributeKeys.Contains(m.Key)).ToList();
            if (missingAttributes.Count > 0)
            {
                var biggestDifference = missingAttributes.Select(m => int.Parse(m.Value) - int.Parse(key.Modifiers.Where(km => km.Key == m.Key)?.FirstOrDefault().Value ?? "0")).Max();
                if (biggestDifference < 0)
                // conservatively adjust upwards
                {
                    var baseFactor = 1.5;
                    if (missingModifiers.Count > 1)
                        baseFactor = 1.35;
                    var factor = Math.Pow(baseFactor, Math.Abs(biggestDifference)) - 1;
                    return -(long)(medPrice * factor);
                }
                var keyhasCombo = missingModifiers.Any(m => HasAttributeCombo(m, auction.FlatenedNBT, auction.Tag));
                var defaultDifference = (medPrice - Math.Pow(0.4, biggestDifference) * medPrice);
                if (keyhasCombo)
                {
                    defaultDifference *= 1.6;
                }
                return (long)defaultDifference;

            }

            return 0;
        }

        private long GetCostForItem(string tag)
        {
            if (Lookups.TryGetValue(tag.ToUpper(), out var itemLookup))
            {
                var prices = itemLookup.Lookup.Values.First();
                return prices.Price;
            }
            return 0;
        }

        private long GetPriceSumForEnchants(IEnumerable<Models.Enchant> missingEnchants)
        {
            long toSubstract = 0;
            foreach (var item in missingEnchants)
            {
                toSubstract += mapper.EnchantValue(new Core.Enchantment(item.Type, item.Lvl), null, BazaarPrices);
            }
            return toSubstract;
        }

        private long GetExtraValue(SaveAuction auction, AuctionKey key)
        {
            long extraValue = 0;
            foreach (var item in ItemKeys)
            {
                if (key.Modifiers.Any(m => m.Key == item))
                    continue;
                if (auction.FlatenedNBT.TryGetValue(item, out var value))
                {
                    if (!Lookups.TryGetValue(value.ToUpper(), out var itemLookup))
                        continue;
                    var prices = itemLookup.Lookup.Values.First();
                    extraValue += prices.Lbin.Price == 0 ? prices.Price : Math.Min(prices.Price, prices.Lbin.Price);
                }
            }
            long gemValue = GetGemValue(auction, key);
            extraValue += gemValue;

            return extraValue;
        }

        public long GetGemValue(SaveAuction auction, AuctionKey key)
        {
            var gemValue = 0L;
            foreach (var item in auction.FlatenedNBT)
            {
                if (item.Value != "PERFECT" && item.Value != "FLAWLESS")
                {
                    continue;
                }
                if (auction.Tag?.StartsWith("STARRED_SHADOW_ASSASSIN") ?? false && item.Key.StartsWith("JASPER_0"))
                    // Jasper0 slot can't be accessed on starred (Fragged) items
                    continue;

                var gemkey = mapper.GetItemKeyForGem(item, auction.FlatenedNBT);
                if (item.Value == "PERFECT")
                    if (Lookups.TryGetValue(gemkey, out var gemLookup) && !key.Modifiers.Any(m => m.Key == item.Key))
                        gemValue += gemLookup.Lookup.Values.First().Price - 500_000;
                if (item.Value == "FLAWLESS")
                    if (Lookups.TryGetValue(gemkey, out var gemLookup) && !key.Modifiers.Any(m => m.Key == item.Key))
                        gemValue += gemLookup.Lookup.Values.First().Price - 100_000;
            }

            return gemValue;
        }

        private bool FindFlip(SaveAuction auction,
                              double lbinPrice,
                              double minMedPrice,
                              ReferenceAuctions bucket,
                              AuctionKeyWithValue key,
                              PriceLookup lookup,
                              KeyWithValueBreakdown breakdown,
                              long extraValue = 0,
                              Action<Dictionary<string, string>> addProps = null)
        {
            var l = lookup.Lookup;
            var expValue = GetValueDifferenceForExp(auction, key, l);
            var volume = bucket.Volume;
            var medianPrice = bucket.Price + extraValue;
            var foundSnipe = false;
            if ((bucket.Lbin.Price > lbinPrice || bucket.Price == 0) && (MaxMedianPriceForSnipe(bucket) > lbinPrice)
               )
            {
                foundSnipe = PotentialSnipe(auction, lbinPrice, bucket, key, l, extraValue, breakdown);
            }
            if (medianPrice > minMedPrice && BucketHasEnoughReferencesForPrice(bucket, lookup))
            {
                long adjustedMedianPrice = CheckHigherValueKeyForLowerPrice(bucket, key, l, medianPrice);
                Activity.Current.Log($"Bucket {key} has enough references {bucket.References.Count} and medianPrice > minMedPrice {medianPrice} > {minMedPrice} adjusted {adjustedMedianPrice} {extraValue} {expValue}");
                if (adjustedMedianPrice + extraValue < minMedPrice)
                {
                    LogNonFlip(auction, bucket, key, extraValue, volume, medianPrice, $"Adjusted median {adjustedMedianPrice} lower than min price {minMedPrice} {extraValue}");
                    return false;
                }
                var referenceAuctionId = bucket.References.LastOrDefault().AuctionId;
                var props = CreateReference(referenceAuctionId, key, extraValue, bucket);
                AddMedianSample(bucket.References, props);
                if (key.ValueSubstract != 0)
                {
                    props["valuedropped"] = key.ValueSubstract.ToString();
                }
                if (expValue != 0)
                {
                    props["expvalue"] = expValue.ToString();
                }
                addProps?.Invoke(props);
                FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER_MEDIAN, adjustedMedianPrice + extraValue + expValue, props);
            }
            if (medianPrice - auction.StartingBid < 2_000_000 && bucket.RiskyEstimate > minMedPrice
                && (bucket.Lbin.AuctionId == default || bucket.Lbin.Price * 1.05 > lbinPrice))
            {
                var referenceAuctionId = bucket.References.LastOrDefault().AuctionId;
                var props = CreateReference(referenceAuctionId, key, extraValue, bucket);
                AddMedianSample(bucket.References, props);
                addProps?.Invoke(props);
                props.Add("riskyEst", "true");
                var target = bucket.RiskyEstimate + extraValue + expValue;
                if (bucket.Lbin.Price != 0)
                    target = (long)Math.Min(target, bucket.Lbin.Price * 1.05);
                FoundAFlip(auction, bucket, LowPricedAuction.FinderType.STONKS, target, props);
            }
            else
            {
                Activity.Current.Log($"Bucket {key} has too few references {bucket.References.Count} or medianPrice > minMedPrice {medianPrice} > {minMedPrice}");
                LogNonFlip(auction, bucket, key, extraValue, volume, medianPrice, $"Median {medianPrice} lower than min price {minMedPrice} {bucket.References.Count}");
            }
            return foundSnipe;

            void LogNonFlip(SaveAuction auction, ReferenceAuctions bucket, AuctionKey key, long extraValue, float volume, long medianPrice, string v = null)
            {
                if (auction.UId % 10 == 0)
                    Console.Write("p");
                if (volume == 0 || bucket.Lbin.Price == 0 || bucket.Price == 0 || bucket.Price > MIN_TARGET)
                    Logs.Enqueue(new LogEntry()
                    {
                        Key = key.ToString() + $"+{extraValue} {v}",
                        LBin = bucket.Lbin.Price,
                        Median = medianPrice,
                        Uuid = auction.Uuid,
                        Volume = bucket.Volume
                    });
                if (Logs.Count > 2000)
                    PrintLogQueue();
            }
        }

        private static long GetValueDifferenceForExp(SaveAuction auction, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l)
        {
            // determine extra expvalue
            if (auction.FlatenedNBT.TryGetValue("exp", out var expString))
            {
                var maxExp = auction.Tag == "PET_GOLDEN_DRAGON" ? ("7", GoldenDragonMaxExp) : ("6", PetExpMaxlevel);
                var exp = Math.Min((long)double.Parse(expString), maxExp.Item2);
                var lvl1Key = new AuctionKey(new(), ItemReferences.Reforge.Any, EmptyPetModifiers.ToList(), auction.Tier, 1);
                var lvl100Key = new AuctionKey(new(), ItemReferences.Reforge.Any, new List<KeyValuePair<string, string>>() { new("exp", maxExp.Item1) }, auction.Tier, 1);
                if (l.TryGetValue(lvl1Key, out var lvl1Bucket) && l.TryGetValue(lvl100Key, out var lvl100Bucket))
                {
                    var lvl1Price = lvl1Bucket.Price;
                    var lvl100Price = lvl100Bucket.Price;
                    var accountedFor = double.Parse(key.Modifiers.Where(m => m.Key == "exp").Select(v => v.Value).FirstOrDefault("0"));
                    var accountedMiddle = accountedFor + Math.Min(0.5, accountedFor / 2);
                    var accountedExp = maxExp.Item2 / 7 * accountedMiddle;
                    var perExp = (double)((lvl100Price - lvl1Price) / (double)(maxExp.Item2 - 1));
                    var expValue = (long)(perExp * (exp - 1 - accountedExp));
                    if (exp > 11_600_000 && expValue > 0) // only block upwards price changes
                        return 0; // bad effect with so many exp
                    return expValue;
                }

            }

            return 0;
        }

        /// <summary>
        /// Checks higher value keys for a lower median price
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="l"></param>
        /// <param name="medianPrice"></param>
        /// <returns></returns>
        private long CheckHigherValueKeyForLowerPrice(ReferenceAuctions bucket, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, long medianPrice)
        {
            // this check could be preloaded to calculating the median
            var higherValueLowerPrice = HigherValueKeys(key, l, medianPrice).Select(k =>
            {
                if (l.TryGetValue(k, out ReferenceAuctions altBucket))
                {
                    if (altBucket.Price != 0)
                        return altBucket.Price;
                }
                return long.MaxValue;
            }).DefaultIfEmpty(long.MaxValue).Min();

            if (key.Count > 1)
            {
                var lowerCountKey = new AuctionKey(key)
                {
                    Count = 1
                };
                if (l.TryGetValue(lowerCountKey, out ReferenceAuctions lowerCountBucket))
                {
                    if (lowerCountBucket.Price != 0)
                        higherValueLowerPrice = Math.Min(higherValueLowerPrice, lowerCountBucket.Price * key.Count);
                }
            }
            var adjustedMedianPrice = Math.Min(bucket.Price, higherValueLowerPrice);
            return adjustedMedianPrice;
        }

        private static bool BucketHasEnoughReferencesForPrice(ReferenceAuctions bucket, PriceLookup lookup)
        {
            // high value items need more volume to pop up
            return bucket.Price < 280_000_000 || bucket.References.Count > 5 || bucket.Volume > lookup.Volume / 3;
        }

        public void UpdateBazaar(dev.BazaarPull bazaar)
        {
            var today = GetDay(bazaar.Timestamp);
            foreach (var item in bazaar.Products)
            {
                if (item.SellSummary.Count() == 0 && item.BuySummery.Count() == 0)
                    continue;
                if (!Lookups.TryGetValue(item.ProductId, out var lookup))
                {
                    lookup = new();
                    Lookups[item.ProductId] = lookup;
                    //logger.LogInformation($"Added {item.ProductId} to lookup");
                }
                var bucket = lookup.Lookup.GetOrAdd(defaultKey, _ => new());
                var itemPrice = 0D;
                if (item.SellSummary.Any() && item.BuySummery?.Count > 0 && item.QuickStatus?.BuyOrders >= 20)
                {
                    // some items have undervalued buy orders so use the center of the spread as estimate
                    // only for items with at least 20 buy orders to avoid manipulation
                    var sellPrice = item.SellSummary.First().PricePerUnit;
                    var buyPrice = item.BuySummery.OrderBy(s => s.PricePerUnit).First().PricePerUnit;
                    itemPrice = (long)(sellPrice + buyPrice) / 2;
                }
                else if (item.SellSummary.Any())
                {
                    itemPrice = (long)item.SellSummary.First().PricePerUnit;
                }
                else if (item.BuySummery.Any())
                {
                    itemPrice = (long)item.BuySummery.OrderBy(s => s.PricePerUnit).First().PricePerUnit;
                }
                if (!BazaarPrices.ContainsKey(item.ProductId))
                    BazaarPrices[item.ProductId] = itemPrice;
                if (bucket.References.Count >= 5 && NotEnoughTimePassed(bazaar, bucket))
                    continue; // only sample prices every 10 minutes
                bucket.References.Enqueue(new()
                {
                    Day = today,
                    Price = (long)itemPrice,
                    AuctionId = bazaar.Timestamp.Ticks,
                    Seller = (short)DateTime.Now.Ticks,
                    Buyer = (short)(DateTime.Now.Ticks + 1)
                });
                if (bucket.Price == 0)
                    bucket.Price = (long)itemPrice;
                UpdateMedian(bucket);
                CapBucketSize(bucket);

                // make sure higher enchants are higher value
                if (item.ProductId.StartsWith("ENCHANTMENT"))
                {
                    MakePriceAtLeast90PercentHigherthanLowerLevel(item, bucket);
                }

                if (bucket.Price > 0)
                    BazaarPrices[item.ProductId] = bucket.Price;
            }
            // make sure higher enchants are higher value
            foreach (var item in Lookups)
            {
                var lookup = item.Value;
                var key = item.Key;
                var refernces = lookup.Lookup.GetOrAdd(defaultKey, _ => new());
                if (refernces.Price == 0)
                    continue;
                var higherEnchants = HigherValueKeys(defaultKey, lookup.Lookup, refernces.Price);
                foreach (var higherEnchant in higherEnchants)
                {
                    if (lookup.Lookup.TryGetValue(higherEnchant, out var higherEnchantReference))
                    {
                        if (higherEnchantReference.Price == 0)
                            higherEnchantReference.Price = refernces.Price + 1;
                    }
                }
            }
            logger.LogInformation($"Updated bazaar {Lookups.Count} items");

            void MakePriceAtLeast90PercentHigherthanLowerLevel(dev.ProductInfo item, ReferenceAuctions refernces)
            {
                var currentLevel = int.Parse(item.ProductId.Split("_").Last());
                if (currentLevel <= 1 || item.ProductId.Contains("_MANA_") && currentLevel <= 5)
                { // mana enchants can drop up to lvl 5 and is worth almost the same at all levels
                    return;
                }
                var lowerLevelId = item.ProductId.Replace($"_{currentLevel}", $"_{currentLevel - 1}");
                if (BazaarPrices.TryGetValue(lowerLevelId, out var lowerValue))
                {
                    refernces.Price = (long)Math.Max(refernces.Price, lowerValue * 1.9);
                }
            }

            static bool NotEnoughTimePassed(dev.BazaarPull bazaar, ReferenceAuctions bucket)
            {
                var lastAdded = new DateTime(bucket.References.Last().AuctionId);
                return lastAdded.AddMinutes(10) > bazaar.Timestamp;
            }
        }

        private bool PotentialSnipe(SaveAuction auction, double lbinPrice, ReferenceAuctions bucket, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, long extraValue, KeyWithValueBreakdown breakdown)
        {
            var higherValueLowerBin = bucket.Lbin.Price;
            if (HigherValueKeys(key, l, lbinPrice).Any(k =>
            {
                if (l.TryGetValue(k, out ReferenceAuctions altBucket))
                {
                    if (altBucket.Lbin.Price != 0 && altBucket.Lbin.Price < lbinPrice)
                    {
                        Activity.Current.Log($"Higher value key {k} has lower lbin {altBucket.Lbin.Price} < {lbinPrice}");
                        return true;
                    }
                    if (altBucket.Lbin.Price != 0 && altBucket.Lbin.Price < higherValueLowerBin)
                        higherValueLowerBin = altBucket.Lbin.Price;// cheaper lbin found
                }
                return false;
            }))
            {
                Activity.Current.Log("Higher value key has lower lbin");
                return false;
            }
            if (IsStacksize1Cheaper(lbinPrice, key, l))
            {
                Activity.Current.Log("Stacksize 1 is cheaper");
                return false;
            }
            var props = CreateReference(bucket.Lbin.AuctionId, key, extraValue, bucket);
            AddMedianSample(bucket.References, props);
            props["mVal"] = bucket.Price.ToString();
            var targetPrice = (Math.Min(higherValueLowerBin, MaxMedianPriceForSnipe(bucket)) + extraValue) - MIN_TARGET / 200;
            if (targetPrice < auction.StartingBid * 1.03)
                return false;
            var percentile = long.MaxValue;

            if (bucket.Price == 0 || bucket.Volume < 10)
            {
                // check for 80th percentile from references
                var subsetSize = 20;
                if (bucket.References.Count >= 1)
                    percentile = bucket.References
                        .OrderByDescending(r => r.Day).Take(subsetSize).Select(r => r.Price).OrderBy(p => p)
                        .ElementAt(Math.Min(bucket.References.Count, subsetSize) * 8 / 10);
                else if (bucket.References.Count == 0)
                    // undercut only similar lbin drasticly
                    percentile = targetPrice * 9 / 11;

                // no references, check against all lbins
                // all key modifiers and enchants need to be in the reference bucket or higher
                var higherValueKeys = l.Where(x => IsHigherValue(auction.Tag, key, x.Key)).ToList();
                var lowestLbin = higherValueKeys
                                .Where(x => x.Value.Lbin.Price > 0 && x.Value.Lbin.Price < bucket.Lbin.Price)
                                .Select(x => x.Value.Lbin.Price).DefaultIfEmpty(long.MaxValue).Min();
                // 25th percentile of all references
                var allReferences = higherValueKeys.SelectMany(x => x.Value.References).ToList();
                var referencePrice = allReferences
                                .Select(r => r.Price).OrderBy(p => p).Skip(allReferences.Count / 4)
                                .DefaultIfEmpty(targetPrice / 4).Min() * Math.Max(1, allReferences.Count / 20);
                if (bucket.Price == 0 && bucket.References.Count > 2 && higherValueKeys.Count <= 2) // manip indicator
                {
                    percentile /= 5;
                }
                percentile = Math.Min(percentile, referencePrice);
                percentile = Math.Min(percentile, lowestLbin);
                if (lowestLbin > 10_000_000_000)
                {
                    Activity.Current.Log($"Reduced because no higher value lbin");
                    percentile = Math.Min(percentile, targetPrice * 96 / 100);
                    props["noHigherLbin"] = percentile.ToString();
                }
                var reduced = CapAtCraftCost(auction.Tag, percentile, breakdown, 0);
                if (reduced > 0)
                {
                    percentile = reduced;
                    Activity.Current.Log($"Reduced to craft cost {reduced}");
                }
                Activity.Current.Log($"No references, checking all lbins {percentile} {lowestLbin} {referencePrice}");
                props["referencePrice"] = referencePrice.ToString();
                props["percentile"] = percentile.ToString();
                props["lowestLbin"] = lowestLbin.ToString();
            }
            targetPrice = Math.Min(targetPrice, percentile);
            return FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER, targetPrice, props);
        }

        private static bool IsHigherValue(string tag, AuctionKey baseKey, AuctionKey toCheck)
        {
            return baseKey.Tier <= toCheck.Tier
                    && (toCheck.Tier != Tier.LEGENDARY || tag != "PET_SPIRIT")
                    && baseKey.Count <= toCheck.Count
                    && baseKey.Modifiers.All(m => toCheck.Modifiers.Any(other => other.Key == m.Key
                                            && (other.Value == m.Value ||
                                                float.TryParse(other.Value, out var otherVal)
                                            && float.TryParse(m.Value, out var ownVal) && (InvertedValueKey.Contains(other.Key) ? otherVal < ownVal : otherVal > ownVal)
                                            || other.Value.Contains(m.Value) && !float.TryParse(other.Value, out _)
                                                // has any space or comma for contains
                                                && other.Value.Any(c => new char[] { ' ', ',' }.Contains(c))
                                            )
                                            && MatchesTierBoostOrLowerTier(baseKey, toCheck, m))
                                                || InvertedValueKey.Contains(m.Key) && !toCheck.Modifiers.Any(other => other.Key == m.Key)
                                            )
                    && baseKey.Enchants.All(e => toCheck.Enchants.Any(other => other.Type == e.Type && other.Lvl >= e.Lvl));

            static bool MatchesTierBoostOrLowerTier(AuctionKey baseKey, AuctionKey toCheck, KeyValuePair<string, string> m)
            {
                if (m.Key != "exp")
                    return true;
                var toCheckModifiers = toCheck.Modifiers.Where(other => other.Key == "petItem" && other.Value == "TIER_BOOST").FirstOrDefault();
                if (toCheckModifiers.Key == default)
                {
                    return true;
                }
                var res = baseKey.Modifiers.Any(other => other.Key == "petItem" && other.Value == "TIER_BOOST")
                                                               ;
                return res || baseKey.Tier < toCheck.Tier;
            }
        }

        private static bool IsStacksize1Cheaper(double lbinPrice, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l)
        {
            if (key.Count > 1)
            {
                var lowerCountKey = new AuctionKey(key)
                {
                    Count = 1
                };
                if (l.TryGetValue(lowerCountKey, out ReferenceAuctions lowerCountBucket))
                {
                    if (lowerCountBucket.Lbin.Price != 0 && lowerCountBucket.Lbin.Price * key.Count < lbinPrice)
                        return true;
                }
            }
            return false;
        }

        private static void AddMedianSample(IEnumerable<ReferencePrice> bucket, Dictionary<string, string> props)
        {
            props["med"] = string.Join(',', bucket.Reverse().Take(10).Select(a => AuctionService.Instance.GetUuid(a.AuctionId)));
        }

        private static long MaxMedianPriceForSnipe(ReferenceAuctions bucket)
        {
            var price = bucket.Price;
            if (price == 0)
                return long.MaxValue; // disabled with 0 volume
            if (price < 15_000_000)
                return price * 13 / 10;
            if (price < 100_000_000)
                return price * 14 / 12;
            return price * 21 / 20;
        }

        public void PrintLogQueue()
        {
            while (Logs.TryDequeue(out LogEntry result))
            {
                var finderName = result.Finder == LowPricedAuction.FinderType.UNKOWN ? "NF" : result.Finder.ToString();
                logger.LogInformation($"Info: {finderName} {result.Uuid} {result.Median} \t{result.LBin} {result.Volume} {result.Key}");
            }
            deferred.PrintQueue();
        }

        private void UpdateLbin(SaveAuction auction, ReferenceAuctions bucket, AuctionKeyWithValue key)
        {
            LbinUpdates.Enqueue((auction, bucket, key));
        }

        private bool FoundAFlip(SaveAuction auction, ReferenceAuctions bucket, LowPricedAuction.FinderType type, long targetPrice, Dictionary<string, string> props)
        {
            if (targetPrice < MIN_TARGET || targetPrice < auction.StartingBid * 1.03)
                return false; // to low
            var refAge = (GetDay() - bucket.OldestRef);
            if (bucket.OldestRef != 0 && (refAge > 60 || State < SniperState.FullyLoaded && refAge > 5))
            {
                Activity.Current.Log("References too old");
                return false; // too old
            }
            props["refAge"] = refAge.ToString();
            props["server"] = ServerDnsName;
            props["refCount"] = bucket.DeduplicatedReferenceCount.ToString();
            props["oldRef"] = (GetDay() - (bucket.References?.Select(r => r.Day).FirstOrDefault(GetDay()) ?? GetDay())).ToString();
            props["volat"] = bucket.Volatility.ToString();

            if (type == LowPricedAuction.FinderType.SNIPER_MEDIAN && bucket.HitsSinceCalculating < 10
                && IsProbablyNotBait(auction, targetPrice))
            {
                targetPrice = (long)(targetPrice / Math.Pow(1.05, bucket.HitsSinceCalculating));
                bucket.HitsSinceCalculating++;
            }

            FoundSnipe?.Invoke(new LowPricedAuction()
            {
                Auction = auction,
                Finder = type,
                TargetPrice = targetPrice,
                DailyVolume = bucket.Volume,
                AdditionalProps = props
            });
            Logs.Enqueue(new LogEntry()
            {
                Key = props.GetValueOrDefault("key"),
                LBin = bucket.Lbin.Price,
                Median = bucket.Price,
                Uuid = auction.Uuid,
                Volume = bucket.Volume,
                Finder = type
            });
            return true;

            static bool IsProbablyNotBait(SaveAuction auction, long targetPrice)
            {
                return targetPrice < auction.StartingBid * 10;
            }
        }

        private static Dictionary<string, string> CreateReference(long reference, AuctionKey key, long extraValue, ReferenceAuctions bucket)
        {
            var dict = new Dictionary<string, string>() {
                { "reference", AuctionService.Instance.GetUuid(reference) },
                { "key", key.ToString() + (extraValue == 0 ? "" : $" +{extraValue}")}
            };
            if (extraValue != 0)
                dict["extraValue"] = extraValue.ToString();
            return dict;
        }

        public async System.Threading.Tasks.Task Init()
        {
            await itemService.GetItemsAsync();
            await mapper.LoadNeuConstants();
        }
    }
}
