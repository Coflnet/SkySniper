using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Core;
using Prometheus;
using Coflnet.Sky.Core.Services;

namespace Coflnet.Sky.Sniper.Services
{
    public class SniperService
    {
        public const string PetItemKey = "petItem";
        public const string TierBoostShorthand = "TIER_BOOST";
        private const int SizeToKeep = 80;
        public const int PetExpMaxlevel = 4_225_538 * 6;
        private const int GoldenDragonMaxExp = 30_036_483 * 7;
        public static int MIN_TARGET = 200_000;
        public ConcurrentDictionary<string, PriceLookup> Lookups = new ConcurrentDictionary<string, PriceLookup>(3, 2000);

        private readonly ConcurrentQueue<LogEntry> Logs = new ConcurrentQueue<LogEntry>();
        private readonly ConcurrentQueue<(SaveAuction, ReferenceAuctions)> LbinUpdates = new();
        private readonly ConcurrentQueue<string> RecentSnipeUids = new();
        private readonly AuctionKey defaultKey = new AuctionKey();
        public SniperState State { get; set; } = SniperState.LoadingLbin;
        private readonly PropertyMapper mapper = new();
        private readonly (string, int)[] EmptyArray = new (string, int)[0];
        private readonly Dictionary<string, double> BazaarPrices = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (PriceEstimate result, DateTime addedAt)> ClosetLbinMapLookup = new();
        private readonly ConcurrentDictionary<(string, AuctionKey), (PriceEstimate result, DateTime addedAt)> ClosetMedianMapLookup = new();

        private readonly Counter sellClosestSearch = Metrics.CreateCounter("sky_sniper_sell_closest_search", "Number of searches for closest sell");
        private readonly Counter closestMedianBruteCounter = Metrics.CreateCounter("sky_sniper_closest_median_brute", "Number of brute force searches for closest median");
        private readonly Counter closestLbinBruteCounter = Metrics.CreateCounter("sky_sniper_closest_lbin_brute", "Number of brute force searches for closest median");

        public event Action<LowPricedAuction> FoundSnipe;
        public void MockFoundFlip(LowPricedAuction auction)
        {
            FoundSnipe?.Invoke(auction);
        }
        private readonly HashSet<string> IncludeKeys = new HashSet<string>()
        {
            "baseStatBoostPercentage", // has an effect on drops from dungeons, is filtered to only max level
            "dye_item",
            "backpack_color",
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
            "ability_scroll", // applied to hyperions worth ~50m https://discord.com/channels/267680588666896385/1031668335731019886/1031668607479975976
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
            "expertise_kills", // unkown kind of kills
            "bow_kills", // huricane bow
            "raider_kills", // raiders axe
            "sword_kills",
            "yogsKilled", // yog armor
            "ethermerge",
            "edition", // great spook stuff
            "hpc", // hot potato books
            "thunder_charge", // Pulse ring modifer
            //"tuned_transmission", // aotv upgrade
            //"power_ability_scroll", // disabled as suggested by Coyu because comonly not worth 1m (up to 2m at most)
            "captured_player", // cake souls
            "handles_found", // NECRON ladder
            "MUSIC", //rune
            "DRAGON", //rune
            "TIDAL", //rune
            "GRAND_SEARING", //rune
            "ENCHANT" // rune
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
            {"magic_find", 2},
            {"veteran", 4},
            {"lifeline", 3} // low volume but valuable
            // life recovery 3
        };

        // combos that are worth more starting at lvl 1 because they are together
        private readonly KeyValuePair<string, string>[] AttributeCombos = new KeyValuePair<string, string>[]{
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
        };

        private readonly KeyValuePair<List<string>, List<KeyValuePair<string, string>>>[] ItemSpecificAttribCombo = new KeyValuePair<List<string>, List<KeyValuePair<string, string>>>[]
        {
            new(new(){"LAVA_SHELL_NECKLACE"}, new (){new("lifeline", "mana_pool"), new("lifeline", "lifeline")}),
            new(new (){"TERROR_BOOTS", "TERROR_LEGGINGS", "TERROR_CHESTPLATE"}, new (){new("lifeline", "mana_pool")}),
            new(new (){"MAGMA_LORD_BOOTS", "MAGMA_LORD_LEGGINGS", "MAGMA_LORD_CHESTPLATE", "MAGMA_LORD_HELMET"},
                new (){new("blazing_fortune", "mana_pool"), new("blazing_fortune", "fishing_experience"), new("blazing_fortune", "magic_find")}),
            new(new (){"AURORA_BOOTS", "AURORA_LEGGINGS", "AURORA_CHESTPLATE", "AURORA_HELMET", // not high but still noticable
                    "CRIMSON_BOOTS", "CRIMSON_LEGGINGS", "CRIMSON_CHESTPLATE", "CRIMSON_HELMET"}, new (){new("veteran", "mana_regeneration")})
        };
        public readonly Dictionary<string, List<KeyValuePair<string, string>>> ItemSpecificAttributeComboLookup = new();
        public readonly ConcurrentDictionary<string, HashSet<string>> AttributeComboLookup = new();

        public void FinishedUpdate()
        {
            ProcessLbins();
            var removeBefore = DateTime.UtcNow.AddHours(-0.5);
            foreach (var item in ClosetLbinMapLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                ClosetLbinMapLookup.TryRemove(item.Key, out _);
            }
            removeBefore = DateTime.UtcNow.AddHours(-1);
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
                var (auction, bucket) = update;
                var key = auction.Uuid;
                var item = CreateReferenceFromAuction(auction);
                if (bucket.Lbins == null)
                    bucket.Lbins = new();
                if (!bucket.Lbins.Contains(item))
                {
                    bucket.Lbins.Add(item);
                    bucket.Lbins.Sort(ReferencePrice.Compare);
                    if (bucket.Lbins.First().AuctionId == item.AuctionId)
                    {
                        Console.WriteLine($"New lowest lbin {auction.Uuid} {auction.StartingBid}");
                    }
                }
            }
            Console.WriteLine($"Finished processing {count} lbin updates");
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
            "new_years_cake" // not that valuable but the only attribute
        };

        public static KeyValuePair<string, string> Ignore { get; } = new KeyValuePair<string, string>(string.Empty, string.Empty);


        public SniperService(HypixelItemService itemService)
        {

            this.FoundSnipe += la =>
            {
                if (la.Finder == LowPricedAuction.FinderType.SNIPER && (float)la.Auction.StartingBid / la.TargetPrice < 0.8 && la.TargetPrice > 1_000_000)
                    Console.WriteLine($"A: {la.Auction.Uuid} {la.Auction.StartingBid} -> {la.TargetPrice}  {KeyFromSaveAuction(la.Auction)}");
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
        }

        public PriceEstimate GetPrice(SaveAuction auction)
        {
            if (auction == null || auction.Tag == null)
                return null;
            if (BazaarPrices.TryGetValue(auction.Tag, out var bazaar))
                return new() { Median = (long)bazaar };
            var tagGroup = GetAuctionGroupTag(auction);

            var result = new PriceEstimate();
            if (!Lookups.TryGetValue(tagGroup.Item1, out PriceLookup lookup))
            {
                return result;
            }
            var l = lookup.Lookup;
            var itemKey = KeyFromSaveAuction(auction);
            result.ItemKey = itemKey.ToString();

            // add back gem value
            var gemVal = GetGemValue(auction, itemKey);
            if (l.TryGetValue(itemKey, out ReferenceAuctions bucket))
            {
                if (result.Lbin.AuctionId == default && bucket.Lbin.AuctionId != default)
                {
                    result.Lbin = new(bucket.Lbin)
                    {
                        Price = bucket.Lbin.Price + gemVal
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
                                      _ => GetEstimatedMedian(auction, result, l, itemKey, gemVal, now));
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
            // correct for combined items
            if (tagGroup.Item2 != 0)
            {
                result.Median += tagGroup.Item2;
                result.MedianKey += $"+star:{tagGroup.Item2}";
            }
            return result;
        }


        private (PriceEstimate result, DateTime addedAt) GetEstimatedMedian(SaveAuction auction, PriceEstimate result, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKeyWithValue itemKey, long gemVal, DateTime now)
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
                foreach (var item in itemKey.Modifiers.Where(m => Constants.AttributeKeys.Contains(m.Key)))
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
                    Console.WriteLine($"no match found for {auction.Tag} {itemKey} options: {l.Count} {c.Key}");
                if (result.Median > 0)
                    break;
            }
            return (result, now);
        }

        private (PriceEstimate result, DateTime addedAt) ClosestLbin(SaveAuction auction, PriceEstimate result, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKeyWithValue itemKey, DateTime now)
        {
            closestLbinBruteCounter.Inc();
            var closest = l.Where(l => l.Key != null && l.Value?.Price > 0 && l.Value?.Lbin.Price > 0)
                .OrderByDescending(m => itemKey.Similarity(m.Key) + Math.Min(m.Value.Volume, 2)).FirstOrDefault();
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

        private void GetDifferenceSum(SaveAuction auction, PriceEstimate result, AuctionKeyWithValue itemKey, KeyValuePair<AuctionKey, ReferenceAuctions> c, out string diffExp, out long changeAmount)
        {
            (var modVal, var modExp) = AdjustMedianForModifiers(result, itemKey, c, auction);
            (var enchal, var enchExp) = AdjustForMissingEnchants(result, itemKey, c);
            var reforgediff = GetReforgeValue(c.Key.Reforge) - GetReforgeValue(itemKey.Reforge);
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
                long median = GetPriceSumForModifiers(missingModifiers, itemKey.Modifiers, auction);
                median += AdjustForAttributes(result.Median, itemKey, missingModifiers);
                if (median != 0)
                {
                    return (median, $"- {string.Join(",", missingModifiers.Select(m => m.Value))}");
                }
            }
            return (0, string.Empty);
        }

        private long GetPriceSumForModifiers(List<KeyValuePair<string, string>> missingModifiers, IEnumerable<KeyValuePair<string, string>> modifiers, SaveAuction auction)
        {
            if (missingModifiers == null)
                return 0;
            var values = missingModifiers.SelectMany(m =>
            {
                return GetItemKeysForModifier(modifiers, auction.FlatenedNBT, auction.Tag, m);
            }).Where(m => m.Item1 != null).Select(k =>
            {
                if (Lookups.TryGetValue(k.Item1, out var lookup))
                {
                    return lookup.Lookup.Values.Where(v => v.Price > 0).FirstOrDefault();
                }
                return null;
            }).Where(m => m != null).ToList();
            var medianSumIngredients = values.Select(m => m.Price).DefaultIfEmpty(0).Sum();
            return medianSumIngredients;
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
            if (mapper.TryGetIngredients(m.Key, m.Value, modifiers?.Where(mi => mi.Key == m.Key).Select(mi => mi.Value).FirstOrDefault(), out var ingredients))
            {
                return ingredients.Select(i => (i, 1));
            }
            return EmptyArray;
        }

        private long GetPriceForItem(string item)
        {
            if (Lookups.TryGetValue(item, out var lookup))
            {
                return lookup.Lookup.Values.FirstOrDefault()?.Price ?? 0;
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
        }

        internal void Move(string tag, long auctionId, AuctionKey from, AuctionKey to)
        {
            var oldBucket = Lookups[tag].Lookup[from];
            var newBucket = GetOrAdd(to, Lookups[tag]);

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
                        && item.Modifiers.Any(m => m.Key == "exp" && m.Value == "6"))
                    loadedVal.Lookup.TryRemove(item, out _); // have been dropped
                var value = loadedVal.Lookup.GetValueOrDefault(item);
                if (value == null)
                    continue;
                if (value.References.Count == 0 || value.References.All(r => r.Day < GetDay() - 21))
                    loadedVal.Lookup.TryRemove(item, out _); // unimportant
            }
            var current = Lookups.AddOrUpdate(itemTag, loadedVal, (tag, value) =>
            {
                foreach (var item in loadedVal.Lookup)
                {
                    if (!value.Lookup.TryGetValue(item.Key, out ReferenceAuctions existingBucket))
                    {
                        value.Lookup[item.Key] = item.Value;
                        continue;
                    }
                    CombineBuckets(item, existingBucket);
                    item.Value.Lbins.Sort(ReferencePrice.Compare);
                }
                return value;
            });

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
                    Deduplicate(itemTag, current, item);
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

            void Deduplicate(string itemTag, PriceLookup value, AuctionKey item)
            {
                if (item.Enchants?.Count == 0)
                    return;
                var keyWithoutEnchants = new AuctionKey(item) { Enchants = new(new Models.Enchant[0]) };
                var without = value.Lookup.GetValueOrDefault(keyWithoutEnchants);
                var with = value.Lookup.GetValueOrDefault(item);
                if (without == null || with == null)
                    return;
                if (!without.References.Any(r => with.References.Any(w => w.AuctionId == r.AuctionId && r.AuctionId != 0)))
                    return;
                // remove without enchants
                value.Lookup.TryRemove(keyWithoutEnchants, out _);
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
                if (key.Enchants.Count > 1 || key.Modifiers.Count > 3)
                    return; // only add attributes for (almost) clean items, one allowed for things that drop with extra enchants
                var groupTag = GetAuctionGroupTag(auction);
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
            // move reference to sold
            bucket.References.Enqueue(reference);
            bucket.Lbins.RemoveAll(l => l.AuctionId == auction.UId);
            CapBucketSize(bucket);
            if (!preventMedianUpdate)
            {
                var key = KeyFromSaveAuction(auction);
                UpdateMedian(bucket, (auction.Tag, key));
            }
        }

        public void UpdateMedian(ReferenceAuctions bucket, (string tag, AuctionKey) keyCombo = default)
        {
            var size = bucket.References.Count;
            if (size < 4)
                return; // can't have enough volume
            var deduplicated = bucket.References.Reverse()
                .OrderByDescending(b => b.Day)
                .GroupBy(a => a.Seller)
                .Select(a => a.First())  // only use one (the latest) price from each seller
                .Take(60)
                .ToList();
            size = deduplicated.Count();
            if (size <= 3 || deduplicated.Count(d => d.Day > GetDay() - 14) < 3)
            {
                bucket.Price = 0; // to low vol
                return;
            }
            // short term protects against price drops after updates
            List<ReferencePrice> shortTermList = GetShortTermBatch(deduplicated).OrderByDescending(b => b.Day).ToList();
            var shortTermPrice = GetMedian(shortTermList);
            bucket.OldestRef = shortTermList.Take(4).Min(s => s.Day);
            if (shortTermList.Count >= 3 && bucket.OldestRef - shortTermList.First().Day <= -5
                && shortTermList.First().AuctionId != shortTermList.OrderByDescending(o => o.Price).First().AuctionId)
            {
                // probably derpy or weird price drop
                if (bucket.OldestRef == shortTermList.Skip(1).First().Day)
                    shortTermPrice = shortTermList.OrderBy(s => s.Price).First().Price;
                else
                    shortTermPrice = (shortTermList.OrderBy(s => s.Price).First().Price + shortTermPrice) / 2;
            }
            // long term protects against market manipulation
            var longSpanPrice = GetMedian(deduplicated.Take(29).ToList());
            if (deduplicated.All(d => d.Day >= GetDay()))
            {
                // all prices are from today, use 25th percentile instead
                longSpanPrice = deduplicated.OrderBy(d => d.Price).Take((int)Math.Max(deduplicated.Count() * 0.25, 1)).Max(d => d.Price);
            }
            var medianPrice = Math.Min(shortTermPrice, longSpanPrice);
            bucket.HitsSinceCalculating = 0;
            // get price of item without enchants and add enchant value 
            if (keyCombo != default)
            {
                var key = new AuctionKey(keyCombo.Item2)
                {
                    Enchants = new(new List<Enchant>())
                };

                if (keyCombo.Item2.Count > 1)
                {
                    var lowerCountKey = new AuctionKey(keyCombo.Item2)
                    {
                        Count = 1
                    };
                    if (Lookups.GetOrAdd(keyCombo.tag, new PriceLookup()).Lookup.TryGetValue(lowerCountKey, out var lowerCountBucket))
                    {
                        if (lowerCountBucket.Price != 0)
                            medianPrice = Math.Min(medianPrice, lowerCountBucket.Price * key.Count);
                    }
                }
                var enchantPrice = GetPriceSumForEnchants(keyCombo.Item2.Enchants);
                if (enchantPrice <= 0)
                {
                    // early return
                    bucket.Price = medianPrice;
                    return;
                }
                if (!Lookups.GetOrAdd(keyCombo.tag, new PriceLookup()).Lookup.TryGetValue(key, out var clean))
                {
                    sellClosestSearch.Inc();
                    var closest = FindClosest(Lookups[keyCombo.tag].Lookup, key, keyCombo.tag).Take(5).ToList();
                    if (closest.Count > 0)
                        clean = closest.MinBy(m => m.Value.Price).Value;
                }
                var combined = (clean?.Price ?? 0) + enchantPrice;
                if (enchantPrice != 0 && clean != default && clean.Price > 10_000 && clean.Volume > 1 && medianPrice > combined)
                {
                    bucket.Price = Math.Min(medianPrice, combined);
                    Console.WriteLine($"Adjusted for enchat cost {keyCombo.tag} -> {medianPrice}  {key} - {enchantPrice} {clean.Price} {clean.Volume}");
                    return;
                }
            }
            bucket.Price = medianPrice;
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
            var group = GetAuctionGroupTag(auction);
            var itemGroupTag = group.Item1;
            if (!Lookups.TryGetValue(itemGroupTag, out var lookup) || lookup == null)
            {
                lookup = new PriceLookup();
                Lookups[itemGroupTag] = lookup;
            }
            var key = KeyFromSaveAuction(auction);
            key.ValueSubstract += group.Item2;
            return (GetOrAdd(key, lookup), key);
        }

        private static long GetMedian(List<ReferencePrice> deduplicated)
        {
            return deduplicated
                .OrderByDescending(b => b.Price)
                .Skip(deduplicated.Count / 2)
                .Select(b => b.Price)
                .First();
        }

        private ReferenceAuctions CreateAndAddBucket(SaveAuction auction, int dropLevel = 0)
        {
            var key = KeyFromSaveAuction(auction, dropLevel);
            var itemBucket = Lookups.GetOrAdd(auction.Tag, new PriceLookup());
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
            return new ReferencePrice()
            {
                AuctionId = auction.UId,
                Day = GetDay(auction.End),
                Price = (auction.HighestBidAmount == 0 ? auction.StartingBid : auction.HighestBidAmount) - valueSubstract,
                Seller = auction.AuctioneerId == null ? (short)(auction.SellerId % (2 << 14)) : Convert.ToInt16(auction.AuctioneerId.Substring(0, 4), 16)
            };
        }

        public static short GetDay(DateTime date = default)
        {
            if (date == default)
                date = DateTime.UtcNow;
            return (short)(date - new DateTime(2021, 9, 25)).TotalDays;
        }

        private bool TryGetReferenceAuctions(SaveAuction auction, out ReferenceAuctions bucket)
        {
            bucket = null;
            if (!Lookups.TryGetValue(auction.Tag, out PriceLookup lookup))
                return false;
            var l = lookup.Lookup;
            if (l.TryGetValue(KeyFromSaveAuction(auction), out bucket))
                return true;
            if (l.TryGetValue(KeyFromSaveAuction(auction, 1), out bucket))
                return true;
            if (l.TryGetValue(KeyFromSaveAuction(auction, 2), out bucket))
                return true;
            return l.TryGetValue(KeyFromSaveAuction(auction, 3), out bucket);
        }

        private static readonly System.Collections.ObjectModel.ReadOnlyCollection<KeyValuePair<string, string>> EmptyModifiers = new(new List<KeyValuePair<string, string>>());
        private static readonly System.Collections.ObjectModel.ReadOnlyCollection<KeyValuePair<string, string>> EmptyPetModifiers = new(new List<KeyValuePair<string, string>>() { new("candyUsed", "0"), new("exp", "0") });
        private static readonly DateTime UnlockedIntroduction = new DateTime(2021, 9, 4);
        private static readonly List<string> GemPurities = new() { "PERFECT", "FLAWLESS", "FINE", "ROUGH" };
        public class RankElem
        {
            public long Value { get; set; }
            public Models.Enchant Enchant { get; set; }
            public KeyValuePair<string, string> Modifier { get; set; }
            public ItemReferences.Reforge Reforge { get; set; }
            public bool IsEstimate { get; set; }

            public long GetValueOrDefault(double defaultVal)
            {
                if (Value == 0)
                    return (long)defaultVal;
                return Value;
            }

            public override string ToString()
            {
                return $"{Enchant} {Modifier} {Value}";
            }

            public RankElem(Models.Enchant enchant, long value)
            {
                Enchant = enchant;
                Value = value;
            }

            public RankElem(KeyValuePair<string, string> modifier, long value)
            {
                Modifier = modifier;
                Value = value;
            }

            public RankElem(ItemReferences.Reforge reforge, long value)
            {
                Reforge = reforge;
                Value = value;
            }
        }
        public long EstStarCost(string item, int tier)
        {
            var items = itemService.GetStarIngredients(item, tier);
            var sum = 0;
            foreach (var ingred in items)
            {
                if (BazaarPrices.TryGetValue(ingred.itemId, out var cost))
                    sum += (int)cost * ingred.amount;
                else
                    sum += 1_000_000;
            }
            return sum;
        }
        public AuctionKeyWithValue KeyFromSaveAuction(SaveAuction auction, int dropLevel = 0)
        {
            var enchants = new List<Models.Enchant>();
            var modifiers = new List<KeyValuePair<string, string>>();

            var shouldIncludeReforge = Constants.RelevantReforges.Contains(auction.Reforge) && dropLevel < 3;
            long valueSubstracted = 0;
            bool removedRarity = false;
            if (dropLevel == 0)
            {
                enchants = auction.Enchantments
                    ?.Where(e => MinEnchantMap.TryGetValue(e.Type, out byte value) && e.Level >= value)
                    .Select(e => new Models.Enchant() { Lvl = e.Level, Type = e.Type }).ToList();

                modifiers = auction.FlatenedNBT?.Where(n =>
                                       IncludeKeys.Contains(n.Key)
                                    || n.Value == "PERFECT"
                                    || n.Key.StartsWith("MASTER_CRYPT_TANK_ZOMBIE")
                                    || n.Key.StartsWith("MINOS_CHAMPION_")
                                    || n.Key == "MINOS_INQUISITOR_750"
                                    || n.Key.StartsWith("MASTER_CRYPT_UNDEAD_") && n.Key.Length > 23) // admins
                                .OrderByDescending(n => n.Key)
                                .Select(i => NormalizeData(i, auction.Tag, auction.FlatenedNBT))
                                .Where(i => i.Key != Ignore.Key).ToList();
                if (auction.ItemCreatedAt < UnlockedIntroduction
                    && auction.FlatenedNBT.Any(v => GemPurities.Contains(v.Value))
                    // safe guard for when the creation date is wrong 
                    && !auction.FlatenedNBT.ContainsKey("unlocked_slots"))
                {
                    var allUnlockable = itemService.GetUnlockableSlots(auction.Tag).ToList();
                    if (allUnlockable.Count > 0)
                        modifiers.Add(new KeyValuePair<string, string>("unlocked_slots", string.Join(",", allUnlockable)));
                }

                (valueSubstracted, removedRarity, shouldIncludeReforge) = CapKeyLength(enchants, modifiers, auction);
            }
            else if (dropLevel == 1 || dropLevel == 2)
            {
                modifiers = auction.FlatenedNBT?.Where(n => VeryValuable.Contains(n.Key) || Increadable.Contains(n.Key) || n.Value == "PERFECT" || n.Value == "PET_ITEM_TIER_BOOST")
                            .OrderByDescending(n => n.Key)
                            .Select(i => NormalizeData(i, auction.Tag, auction.FlatenedNBT))
                                .Where(i => i.Key != Ignore.Key)
                            .ToList();
                enchants = auction.Enchantments
                    ?.Where(e => Constants.RelevantEnchants.Where(el => el.Type == e.Type && el.Level <= e.Level).Any())
                            .Select(e => new Models.Enchant()
                            {
                                Lvl = e.Level,
                                Type = e.Type
                            }).ToList();
                if (enchants?.Count == 0)
                {
                    var enchant = Constants.SelectBest(auction.Enchantments);
                    enchants = new List<Models.Enchant>() { new Models.Enchant() { Lvl = enchant.Level, Type = enchant.Type
                        } };
                }
            }
            else if (dropLevel == 3)
            {
                var enchant = Constants.SelectBest(auction.Enchantments);
                if (enchant == default)
                    enchants = new List<Models.Enchant>();
                else
                    enchants = new List<Models.Enchant>() { new Models.Enchant() { Lvl = enchant.Level, Type = enchant.Type } };
                modifiers = AssignEmptyModifiers(auction);
            }
            else
            {
                //key.Modifiers = new List<KeyValuePair<string, string>>();
                enchants = new List<Models.Enchant>();
                modifiers = AssignEmptyModifiers(auction);
            }

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
            if (removedRarity && tier < Tier.ULTIMATE)
            {
                if (tier == Tier.MYTHIC)
                    tier = Tier.LEGENDARY;
                else if (tier == Tier.DIVINE)
                    tier = Tier.MYTHIC;
                else
                    tier--;
            }
            enchants = RemoveNoEffectEnchants(auction, enchants);

            return new AuctionKeyWithValue()
            {
                // order attributes
                Modifiers = modifiers.OrderBy(m => m.Key).ToList().AsReadOnly(),
                Enchants = enchants.OrderBy(e => e.Type).ToList().AsReadOnly(),
                Tier = tier,
                Reforge = shouldIncludeReforge ? auction.Reforge : ItemReferences.Reforge.Any,
                Count = (byte)auction.Count,
                ValueSubstract = valueSubstracted
            };
        }

        /// <summary>
        /// To find more matches the key length is capped.
        /// This is done by removing the lowest value enchantment or modifier
        /// </summary>
        /// <param name="enchants"></param>
        /// <param name="modifiers"></param>
        /// <returns>The coin amount substracted</returns>
        public (long, bool removedRarity, bool includeReforge) CapKeyLength(List<Models.Enchant> enchants, List<KeyValuePair<string, string>> modifiers, SaveAuction auction)
        {
            var threshold = 500_000L;
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
            combined = combined.OrderByDescending(i => i.Value).ToList();
            foreach (var item in combined.Skip(5).Where(c => c.Value > 0).Concat(combined.Where(c => c.Value > 0 && c.Value < threshold)))
            {
                // remove all but the top 5
                if (item.Enchant.Type != 0)
                {
                    if (enchants.Remove(item.Enchant))
                        valueSubstracted += item.Value;
                }
                else if (item.Reforge != ItemReferences.Reforge.None)
                {
                    includeReforge = false;
                }
                else
                {
                    if (modifiers.Remove(item.Modifier))
                        valueSubstracted += item.Value;
                    if (item.Modifier.Key == "rarity_upgrades")
                        removedRarity = true;
                }
            }
            return (valueSubstracted, removedRarity, includeReforge);

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
                var items = GetItemKeysForModifier(modifiers, flatNbt, tag, mod);
                var sum = 0L;
                foreach (var item in items)
                {
                    if (Lookups.TryGetValue(item.tag, out var lookup))
                    {
                        sum += (lookup.Lookup.Values.FirstOrDefault()?.Price ?? 0) * item.amount;
                    }
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
                            sum += GetPriceForItem(cost.ItemId);
                        else
                            sum += cost.Coins;
                    }
                    if (costs.unavailable.Count() > 0)
                    {
                        modifiers.RemoveAll(m => m.Key == "unlocked_slots");
                        var remaining = present.Except(costs.unavailable);
                        if (remaining.Count() > 0)
                            modifiers.Add(new(mod.Key, string.Join(",", remaining)));
                    }
                }
                // early return if we have a value before estimates
                if (sum > 0)
                    return new RankElem(mod, sum);
                if (mod.Key == "pgems")
                {
                    sum += 100_000_000;
                }
                if (mod.Key.EndsWith("_kills"))
                {
                    sum += 300_000 * (int)Math.Pow(2, int.Parse(mod.Value)) + 300_000;
                }
                if (mod.Key == "color")
                    sum += 10_000_000;
                if (Constants.AttributeKeys.Contains(mod.Key))
                {
                    sum += 200_000 * (long)Math.Pow(2, int.Parse(mod.Value)) + 600_000;
                    if (modifiers.Any(m => m.Key != mod.Key && Constants.AttributeKeys.Contains(m.Key)))
                        sum += 50_000_000; // godroll
                }
                return new RankElem(mod, sum)
                {
                    IsEstimate = true
                };
            };
            var valuePerModifier = modifiers?.Select(m =>
            {
                try
                {
                    return handler(m);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error when calculating value for {m.Key} {m.Value} {tag}\n" + e);
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

        private long GetReforgeValue(ItemReferences.Reforge reforge)
        {
            var reforgeCost = mapper.GetReforgeCost(reforge);
            var itemCost = GetPriceForItem(reforgeCost.Item1);
            var reforgeValue = itemCost + reforgeCost.Item2;
            return reforgeValue;
        }

        private static List<Models.Enchant> RemoveNoEffectEnchants(SaveAuction auction, List<Models.Enchant> ench)
        {
            if (auction.Tag == null)
                return ench;
            if (auction.Tag.Contains("GAUNTLET") || auction.Tag.Contains("DRILL"))
                ench = RemoveEnchantFromKey(ench, Enchantment.EnchantmentType.ultimate_wise);
            if (auction.Tag.StartsWith("DIVAN_"))
            {
                ench = RemoveEnchantFromKey(ench, Enchantment.EnchantmentType.ultimate_legion);
                ench = RemoveEnchantFromKey(ench, Enchantment.EnchantmentType.ultimate_wisdom);
            }
            if (!auction.Tag.EndsWith("KATANA"))
                ench = RemoveEnchantFromKey(ench, Enchantment.EnchantmentType.ender_slayer, 6);
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
            if (auction.Tag.StartsWith("PET_") && !auction.Tag.StartsWith("PET_ITEM") && !auction.Tag.StartsWith("PET_SKIN"))
                if (auction.FlatenedNBT.TryGetValue("heldItem", out var val) && val == "PET_ITEM_TIER_BOOST")
                    return new List<KeyValuePair<string, string>>(EmptyPetModifiers) { new(PetItemKey, TierBoostShorthand) };
                else
                    return EmptyPetModifiers.ToList();
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
                var exp = GetNumeric(s);
                if (exp > 1_000_000 && exp <= 2_500_000)
                    return new KeyValuePair<string, string>(s.Key, "0.3");
                else if (exp > 2_500_000 && exp < PetExpMaxlevel / 6)
                    return new KeyValuePair<string, string>(s.Key, "0.6");
                if (tag == "PET_GOLDEN_DRAGON")
                    return NormalizeNumberTo(s, GoldenDragonMaxExp / 7, 7);
                else
                    return NormalizeNumberTo(s, PetExpMaxlevel / 6, 6);
            }
            var generalNormalizations = NormalizeGeneral(s, tag?.StartsWith("MIDAS") ?? false,
                GetNumeric(flattenedNbt.FirstOrDefault(f => f.Key == "exp")),
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

            return s;
        }

        public static KeyValuePair<string, string> NormalizeGeneral(KeyValuePair<string, string> s, bool isMiddas, long expAmount, bool isGDrag)
        {
            if (s.Key == "winning_bid")
                if (isMiddas)
                    return NormalizeNumberTo(s, 10_000_000, 10);
                else
                    return Ignore;
            if (s.Key == "eman_kills")
                return NormalizeGroupNumber(s, 10_000, 25_000, 50_000, 75_000, 100_000, 125_000, 150_000, 200_000);
            if (s.Key.EndsWith("_kills"))
                return NormalizeNumberTo(s, 10_000);
            if (s.Key == "yogsKilled")
                return NormalizeNumberTo(s, 5_000, 2);
            if (s.Key == "thunder_charge")
                return NormalizeNumberTo(s, 1_000_000, 5);
            if (s.Key == "candyUsed")
                if (expAmount >= PetExpMaxlevel && !isGDrag || expAmount >= GoldenDragonMaxExp)
                    return Ignore; // not displayed on max exp items
                else
                    // all candied are the same
                    return new KeyValuePair<string, string>(s.Key, (double.Parse(s.Value) > 0 ? 1 : 0).ToString());
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
            var highestGroup = groups.Length - 1;
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
                Console.WriteLine($"could not parse {s.Key} {s.Value}");
                throw;
            }
        }

        public void TestNewAuction(SaveAuction auction, bool triggerEvents = true)
        {
            var itemGroupTag = GetAuctionGroupTag(auction);
            var lookup = Lookups.GetOrAdd(itemGroupTag.Item1, key => new PriceLookup());
            var l = lookup.Lookup;
            var cost = auction.StartingBid;
            var lbinPrice = auction.StartingBid * 1.03 + itemGroupTag.Item2;
            var medPrice = auction.StartingBid * 1.05 + itemGroupTag.Item2;
            var lastKey = new AuctionKey();
            var shouldTryToFindClosest = false;
            for (int i = 0; i < 5; i++)
            {
                var key = KeyFromSaveAuction(auction, i);
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
                        Console.WriteLine($"could not find bucket {key} for {auction.Tag} {l.Count} {auction.Uuid}");
                        if (this.State < SniperState.Ready)
                        {
                            if (auction.UId % 10 == 2)
                                Console.WriteLine($"closest is not available yet, state is {this.State}");
                            return;
                        }
                        var closests = FindClosest(l, key, auction.Tag).Take(5).ToList();
                        foreach (var item in closests)
                        {
                            Console.WriteLine($"Closest bucket clean: {item.Key}");
                        }
                        if (!closests.Any())
                        {
                            Console.WriteLine($"No closest bucket found for {key} {auction.Uuid}");
                            return;
                        }
                        if (ShouldIgnoreMostSimilar(auction))
                        {
                            return;
                        }
                        bucket = closests.FirstOrDefault().Value;
                        var closestKey = closests.FirstOrDefault().Key;
                        if (bucket.HitsSinceCalculating > 8)
                        {
                            Console.WriteLine($"Bucket {closestKey} for {auction.Uuid} has been hit {bucket.HitsSinceCalculating} times, skipping");
                            TryFindClosestRisky(auction, l, ref lbinPrice, ref medPrice);
                            return;
                        }
                        lbinPrice *= Math.Pow(1.15, bucket.HitsSinceCalculating);
                        medPrice *= Math.Pow(1.25, bucket.HitsSinceCalculating);
                        bucket.HitsSinceCalculating++;
                        shouldTryToFindClosest = true;
                        break; // don't use most similar until key lenght limit is added
                    }
                    else if (i != 0)
                        continue;
                    else
                        bucket = CreateAndAddBucket(auction);
                }
                if (i == 0)
                    UpdateLbin(auction, bucket);
                if (triggerEvents)
                {
                    long extraValue = GetExtraValue(auction, key) - itemGroupTag.Item2;
                    if (FindFlip(auction, lbinPrice, medPrice, bucket, key, l, extraValue))
                        return; // found a snipe, no need to check other lower value buckets
                }
            }
            if (shouldTryToFindClosest && triggerEvents && this.State >= SniperState.Ready)
            {
                TryFindClosestRisky(auction, l, ref lbinPrice, ref medPrice);
            }
        }

        /// <summary>
        /// Remaps item tags into one item if they are easily switchable
        /// </summary>
        /// <param name="auction"></param>
        /// <returns></returns>
        private (string, long) GetAuctionGroupTag(SaveAuction auction)
        {
            var itemGroupTag = auction.Tag;
            if (itemGroupTag == "SCYLLA" || itemGroupTag == "VALKYRIE" || itemGroupTag == "NECRON_BLADE")
                return ("HYPERION", GetPriceForItem("GIANT_FRAGMENT_LASER") * 8); // easily craftable from one into the other
            if (itemGroupTag.StartsWith("STARRED_"))
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
            // special case for items that have no reference bucket, search using most similar
            var key = KeyFromSaveAuction(auction, 0);
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
            if (closest.Key == key)
                return; // already found - or rather not - by median
            else
                Console.WriteLine($"Would estimate closest to {key} {closest.Key} {auction.Uuid} for {closest.Value.Price}");
            if (closest.Value.Price <= medPrice)
                return;
            var props = new Dictionary<string, string>() { { "closest", closest.Key.ToString() } };
            var missingModifiers = closest.Key.Modifiers.Where(m => !key.Modifiers.Contains(m)).ToList();
            long toSubstract = 0;
            if (missingModifiers.Count > 0)
            {
                toSubstract = GetPriceSumForModifiers(missingModifiers, key.Modifiers, auction);
                toSubstract += AdjustForAttributes(closest.Value.Price, key, missingModifiers);
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
                    var difference = killCount - int.Parse(present.Value);
                    var killPrice = difference * 1_000_000;
                    if (difference < 0)
                        killPrice /= 2; // only half for adding kills
                    toSubstract += killPrice;
                }

                props.Add("missingModifiers", string.Join(",", missingModifiers.Select(m => $"{m.Key}:{m.Value}")) + $" ({toSubstract})");
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
                var enchantVal = Math.Min(GetPriceSumForEnchants(additionalEnchants) / 2, closest.Value.Price / 2);
                toSubstract -= enchantVal;
                props.Add("additionalEnchants", string.Join(",", additionalEnchants.Select(e => $"{e.Type}_{e.Lvl}")) + $" ({enchantVal})");
            }
            var targetPrice = (long)((closest.Value.Price - toSubstract) * 0.9);
            // adjust due to count
            if (closest.Key.Count != auction.Count)
            {
                var countDiff = closest.Key.Count - auction.Count;
                var countDiffPrice = (long)(countDiff * targetPrice / closest.Key.Count);
                targetPrice -= countDiffPrice;
                props.Add("countDiff", $"{countDiff} ({countDiffPrice})");
                Console.WriteLine($"Adjusting target price due to count diff {countDiff} {countDiffPrice} {targetPrice}");
            }
            // adjust price of reforge 
            if (closest.Key.Reforge != auction.Reforge)
            {
                var closestDetails = mapper.GetReforgeCost(closest.Key.Reforge, auction.Tier);
                var auctionDetails = mapper.GetReforgeCost(auction.Reforge, auction.Tier);
                var reforgeDifference = GetCostForItem(closestDetails.Item1) + closestDetails.Item2 - (GetCostForItem(auctionDetails.Item1) - auctionDetails.Item2) / 2;
                Console.WriteLine($"Adjusting target price due to reforge {closestDetails.Item1} {closestDetails.Item2} {auctionDetails.Item1} {auctionDetails.Item2} {reforgeDifference}");
                targetPrice -= reforgeDifference;
                props.Add("reforge", $"{closest.Key.Reforge} -> {auction.Reforge} ({reforgeDifference})");
            }
            AddMedianSample(closest.Value, props);
            FoundAFlip(auction, closest.Value, LowPricedAuction.FinderType.STONKS, targetPrice, props);
        }

        private long AdjustForAttributes(double medPrice, AuctionKey key, List<KeyValuePair<string, string>> missingModifiers)
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
                    Console.WriteLine($"Adjusting target price due to attribute diff on {biggestDifference} {medPrice} {factor}");
                    return -(long)(medPrice * factor);
                }
                var keyhasCombo = AttributeComboLookup.TryGetValue(missingAttributes.Select(m => m.Key).First(), out var combo) && key.Modifiers.Any(m => combo.Contains(m.Key));
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

        private bool FindFlip(SaveAuction auction, double lbinPrice, double minMedPrice, ReferenceAuctions bucket, AuctionKeyWithValue key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, long extraValue = 0)
        {
            var expValue = GetValueDifferenceForExp(auction, key, l);
            var volume = bucket.Volume;
            var medianPrice = bucket.Price + extraValue;
            var foundSnipe = false;
            if (bucket.Lbin.Price > lbinPrice && (MaxMedianPriceForSnipe(bucket) > lbinPrice) && volume > 0.2f
               )// || bucket.Price == 0))
            {
                foundSnipe = PotentialSnipe(auction, lbinPrice, bucket, key, l, extraValue);
            }
            if (medianPrice > minMedPrice && BucketHasEnoughReferencesForPrice(bucket))
            {
                long adjustedMedianPrice = CheckHigherValueKeyForLowerPrice(bucket, key, l, medianPrice);
                if (adjustedMedianPrice + extraValue < minMedPrice)
                {
                    LogNonFlip(auction, bucket, key, extraValue, volume, medianPrice, $"Adjusted median {adjustedMedianPrice} lower than min price {minMedPrice}");
                    return false;
                }
                var props = CreateReference(bucket.References.LastOrDefault().AuctionId, key, extraValue);
                AddMedianSample(bucket, props);
                if (key.ValueSubstract != 0)
                {
                    props["valuedropped"] = key.ValueSubstract.ToString();
                }
                if (expValue != 0)
                {
                    props["expvalue"] = expValue.ToString();
                }
                FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER_MEDIAN, adjustedMedianPrice + extraValue + expValue, props);
            }
            else
            {
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

        private static bool BucketHasEnoughReferencesForPrice(ReferenceAuctions bucket)
        {
            // high value items need more volume to pop up
            return bucket.Price < 200_000_000 || bucket.References.Count > 5;
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
                    Console.WriteLine($"Added {item.ProductId} to lookup");
                }
                var bucket = lookup.Lookup.GetOrAdd(defaultKey, _ => new());
                var itemPrice = 0D;
                if (item.SellSummary.Any() && item.BuySummery?.Count > 0 && item.QuickStatus?.BuyOrders >= 10)
                {
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

                if (bucket.References.Count < 5 || new DateTime(bucket.References.Last().AuctionId) < bazaar.Timestamp.AddMinutes(10))
                    bucket.References.Enqueue(new()
                    {
                        Day = today,
                        Price = (long)itemPrice,
                        AuctionId = bazaar.Timestamp.Ticks,
                        Seller = (short)DateTime.Now.Ticks
                    });
                if(bucket.Price == 0)
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
            Console.WriteLine($"Updated bazaar {Lookups.Count} items");

            void MakePriceAtLeast90PercentHigherthanLowerLevel(dev.ProductInfo item, ReferenceAuctions refernces)
            {
                var currentLevel = int.Parse(item.ProductId.Split("_").Last());
                if (currentLevel > 1)
                {
                    var lowerLevelId = item.ProductId.Replace($"_{currentLevel}", $"_{currentLevel - 1}");
                    if (BazaarPrices.TryGetValue(lowerLevelId, out var lowerValue))
                    {
                        refernces.Price = (long)Math.Max(refernces.Price, lowerValue * 1.9);
                    }
                }
            }
        }

        private bool PotentialSnipe(SaveAuction auction, double lbinPrice, ReferenceAuctions bucket, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, long extraValue)
        {
            var higherValueLowerBin = bucket.Lbin.Price;
            if (HigherValueKeys(key, l, lbinPrice).Any(k =>
            {
                if (l.TryGetValue(k, out ReferenceAuctions altBucket))
                {
                    if (altBucket.Lbin.Price != 0 && altBucket.Lbin.Price < lbinPrice)
                    {
                        return true;
                    }
                    if (altBucket.Lbin.Price != 0 && altBucket.Lbin.Price < higherValueLowerBin)
                        higherValueLowerBin = altBucket.Lbin.Price;// cheaper lbin found
                }
                return false;
            }))
                return false;
            if (IsStacksize1Cheaper(lbinPrice, key, l))
                return false;
            var props = CreateReference(bucket.Lbin.AuctionId, key, extraValue);
            AddMedianSample(bucket, props);
            props["mVal"] = bucket.Price.ToString();
            var targetPrice = (Math.Min(higherValueLowerBin, MaxMedianPriceForSnipe(bucket)) + extraValue) - MIN_TARGET / 200;
            if (targetPrice < auction.StartingBid * 1.03)
                return false;
            // check for 90th percentile from references
            var subsetSize = 20;
            var percentile = bucket.References
                    .OrderByDescending(r => r.Day).Take(subsetSize).Select(r => r.Price).OrderBy(p => p)
                    .ElementAt(Math.Min(bucket.References.Count, subsetSize) * 9 / 10);
            targetPrice = Math.Min(targetPrice, percentile);
            return FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER, targetPrice, props);
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

        private static void AddMedianSample(ReferenceAuctions bucket, Dictionary<string, string> props)
        {
            props["med"] = string.Join(',', bucket.References.Reverse().Take(10).Select(a => AuctionService.Instance.GetUuid(a.AuctionId)));
        }

        private static long MaxMedianPriceForSnipe(ReferenceAuctions bucket)
        {
            if (bucket.Price < 15_000_000)
                return bucket.Price * 13 / 10;
            if (bucket.Price < 100_000_000)
                return bucket.Price * 14 / 12;
            return bucket.Price * 21 / 20;
        }

        public void PrintLogQueue()
        {
            while (Logs.TryDequeue(out LogEntry result))
            {
                var finderName = result.Finder == LowPricedAuction.FinderType.UNKOWN ? "NF" : result.Finder.ToString();
                Console.WriteLine($"Info: {finderName} {result.Uuid} {result.Median} \t{result.LBin} {result.Volume} {result.Key}");
            }
        }

        private void UpdateLbin(SaveAuction auction, ReferenceAuctions bucket)
        {
            LbinUpdates.Enqueue((auction, bucket));
        }

        private bool FoundAFlip(SaveAuction auction, ReferenceAuctions bucket, LowPricedAuction.FinderType type, long targetPrice, Dictionary<string, string> props)
        {
            if (targetPrice < MIN_TARGET)
                return false; // to low
            var refAge = (GetDay() - bucket.OldestRef);
            if (refAge > 60 || State < SniperState.FullyLoaded && refAge > 5)
                return false; // too old
            props["refAge"] = refAge.ToString();
            if (auction.Tag.StartsWith("PET_") && auction.FlatenedNBT.Any(f => f.Value == "PET_ITEM_TIER_BOOST") && !props["key"].Contains(TierBoostShorthand))
                throw new Exception("Tier boost missing " + props["key"] + " " + JSON.Stringify(auction));
            if (auction.FlatenedNBT.TryGetValue("uid", out var uid))
            {
                uid += type;
            }
            var profitPercent = (targetPrice - auction.StartingBid) / (double)auction.StartingBid;
            if (RecentSnipeUids.Contains(uid) && profitPercent > 0.5)
            {
                Console.WriteLine($"Already found {uid} recently");
                return true;
            }
            else if (uid != null)
                RecentSnipeUids.Enqueue(uid);
            if (RecentSnipeUids.Count > 50)
                RecentSnipeUids.TryDequeue(out _);

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
        }

        private static Dictionary<string, string> CreateReference(long reference, AuctionKey key, long extraValue = 0)
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
