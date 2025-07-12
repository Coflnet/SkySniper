using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Core;
using Prometheus;
using Coflnet.Sky.Core.Services;
using Newtonsoft.Json;
using System.Net;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using System.Globalization;

namespace Coflnet.Sky.Sniper.Services
{
    public partial class SniperService
    {
        public const string PetItemKey = "petItem";
        public const string TierBoostShorthand = "TIER_BOOST";
        private const int SizeToKeep = 80;
        public static int WorkingSize {get; set;} = 60;
        public const int PetExpMaxlevel = 4_225_538 * 6;
        private const int GoldenDragonMaxExp = 30_036_483 * 7;
        public static short CurrentDayCache = 0;
        public static int MIN_TARGET = 200_000;
        private static DateTime _startTime = new DateTime(2021, 9, 25);
        public static DateTime StartTime
        {
            get => _startTime; set
            {
                _startTime = value;
                UpdateToday(); // important for tests
            }
        }
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
        private readonly ConcurrentDictionary<ModifierLookupKey, (RankElem, DateTime)> ModifierValueLookup = new();
        private readonly ConcurrentDictionary<ItemReferences.Reforge, (RankElem, DateTime)> ReforgeValueLookup = new();
        private readonly ConcurrentDictionary<(string, KeyValuePair<string, string>), (long, DateTime)> AttributeValueLookup = new();

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

        public ConcurrentQueue<Dictionary<string, string>> AllocatedDicts = new();

        internal readonly string[] CrimsonArmors = new string[] { "CRIMSON_", "TERROR_", "AURORA_", "FERVOR_" };
        private readonly HashSet<string> IncludeKeys = new HashSet<string>()
        {
            "baseStatBoostPercentage", // has an effect on drops from dungeons, is filtered to only max level, skelotor master and ice sprays
            "dye_item",
            // "backpack_color", no difference anymore
            "party_hat_color",
            "party_hat_emoji",
            "color", // armour
            "model", // abicase
            // potion "level", // not engough impact
            "item_tier", // mostly found on armor, unsure what it does
            "talisman_enrichment", // talismans can be enriched with additional stats
            "divan_powder_coating",
            "ability_scroll", // applied to hyperions worth ~250m https://discord.com/channels/267680588666896385/1031668335731019886/1031668607479975976
            "power_ability_scroll",
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
            //"expertise_kills", // kills for expertise enchant counting sea creatures, replaced by enchant value caluclation
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
            "RUNE_RAINY_DAY",
            "bass_weight",
            "polarvoid",
            "chimera_found", // Diana's Bookshelf
            "is_shiny", // cosmetic effect on wither armor ~5% drop chance on Master Mode 7
        };

        private Dictionary<string, Func<ModifierMetadata, RankElem>> Converters = new()
        {
            { "full_bid", m => new (m.Modifier, (int)(float.Parse(m.Modifier.Value) * 48_000_000))},
            { "winning_bid", m => new (m.Modifier, (int)(float.Parse(m.Modifier.Value) * 25_000_000))},
            //{ "rarity_upgrades", m => new (m.Modifier, 50_000_000)}, item value
            { "eman_kills", m => new (m.Modifier, 3_000_000 * (int)Math.Pow(2, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "expertise_kills", m => new (m.Modifier, 3_000_000 * (int)Math.Pow(2, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "mined_crops", m => new (m.Modifier, 13_000_000 * (int)Math.Pow(2, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "blocksBroken", m => new (m.Modifier, 1_000_000 * (int)Math.Pow(2, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "color", m => new (m.Modifier, 50_000_000) {IsEstimate=true}}, // if its default its removed
            { "pgems", m => new (m.Modifier, 100_000_000){IsEstimate=true}},
            { "is_shiny", m => new (m.Modifier, 88_000_000) {IsEstimate=true}},
            { "party_hat_color", m => new (m.Modifier, 20_000_000) {IsEstimate=true}},
            { "thunder_charge", m => new (m.Modifier, 50_000_000 * int.Parse(m.Modifier.Value) + 10_000_000){IsEstimate=true} },
            { "baseStatBoost", m => new (m.Modifier, (int)((float.Parse(m.Modifier.Value) - 45) * 500_000)) {IsEstimate=true}},
            { "hotpc", m => new ("hotpc", m.Modifier.Value, m.Modifier.Value switch {
                "1" => 5_000_000,
                "0.1" => 2_000_000,
                _ => 800_000 // no fuming
            }) {IsEstimate=true}},
            { "chimera_found", m => new ("chimera_found", m.Modifier.Value, int.Parse(m.Modifier.Value) * 10_000_000) {IsEstimate=true}},
            { "new_years_cake", m => new (m.Modifier, int.Parse(m.Modifier.Value) switch
                {
                    < 20 => 20_000_000,
                    69 => 10_000_000,
                    < 120 => 2_000_000,
                    400 => 10_000_000, // check this in the future
                    _ => 600_000,
                }){IsEstimate=true} },
            { "party_hat_emoji", m => new (m.Modifier, 8_000_000){IsEstimate=true} },
            { "edition", m => new (m.Modifier, 8_000_000){IsEstimate=true} },
            { "collected_coins", m => new (m.Modifier, (long)(20_000_000 * Math.Pow(10, int.Parse(m.Modifier.Value)) + 1_000_000)){IsEstimate=true} },
        };

        public static readonly HashSet<string> InvertedValueKey = new()
        {
            "edition",
            "new_years_cake",
            "candyUsed",
        };

        private static readonly HashSet<string> ImportantCakeYears = new()
        { "69", "420", "400"};

        /// <summary>
        /// Keys containing itemTags that should be added separately (cause its removable)
        /// </summary>
        private readonly HashSet<string> ItemKeys = new()
        {
            "drill_part_engine",
            "drill_part_fuel_tank",
            "drill_part_upgrade_module",
            "line.part",
            "sinker.part",
            "hook.part",
        };

        private static readonly HashSet<string> KillKeys = [
            "blaze_kills",
            "blood_god_kills",
            "bow_kills",
            "eman_kills",
            "expertise_kills",
            "raider_kills",
            "runic_kills",
            "skeletorKills",
            "spider_kills",
            "sword_kills",
            "zombie_kills"
            ];

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
            new(["LAVA_SHELL_NECKLACE"], [new("lifeline", "mana_regeneration")]),
            new(new (){"TERROR_BOOTS", "TERROR_LEGGINGS", "TERROR_CHESTPLATE"}, new (){new("lifeline", "mana_pool")}),
            new(new (){"MAGMA_LORD_BOOTS", "MAGMA_LORD_LEGGINGS", "MAGMA_LORD_CHESTPLATE", "MAGMA_LORD_HELMET"},
                new (){new("blazing_fortune", "mana_pool"), new("blazing_fortune", "fishing_experience"),
                       new("blazing_fortune", "magic_find"), new("blazing_fortune","blazing_fortune")}),
            new(new (){"AURORA_BOOTS", "AURORA_LEGGINGS", "AURORA_CHESTPLATE", "AURORA_HELMET", // not high but still noticable
                    "CRIMSON_BOOTS", "CRIMSON_LEGGINGS", "CRIMSON_CHESTPLATE", "CRIMSON_HELMET"}, new (){new("veteran", "mana_regeneration")}),
            new(new (){"CRIMSON_BOOTS", "CRIMSON_LEGGINGS", "CRIMSON_CHESTPLATE", "CRIMSON_HELMET", "MOLTEN_BRACELET"},
                    new (){new("magic_find", "mending")}), // vitality = mending
            new(new(){"MOLTEN_BRACELET", "MOLTEN_CLOAK", "MAGMA_LORD_GAUNTLET"}, new (){new("lifeline", "mana_pool")}),
            new(new(){"TAURUS_HELMET", "FLAMING_CHESTPLATE", "MOOGMA_LEGGINGS", "SLUG_BOOTS"}, new (){new("magic_find", "magic_find")}),
            new(new(){"GAUNTLET_OF_CONTAGION"}, new (){new("veteran", "mana_regeneration"),new("veteran", "breeze"),new("veteran", "mana_pool"),new("lifeline", "mana_pool")}),
        ];

        private readonly List<KeyValuePair<List<string>, List<string>>> AttributesToIgnoreOn = [
            // https://discord.com/channels/267680588666896385/1303058935430774904/1303072543145459784
            new(["SHIMMERING_LIGHT_TROUSERS", "SHIMMERING_LIGHT_TUNIC", "SHIMMERING_LIGHT_HOOD", "SHIMMERING_LIGHT_SLIPPERS"], Constants.AttributeKeys.ToList()),
            new(["VELVET_TOP_HAT","CASHMERE_JACKET", "SATIN_TROUSERS", "OXFORD_SHOES"],["color"]) // random https://hypixel-skyblock.fandom.com/wiki/Seymour%27s_Special_Armor
        ];
        private readonly Dictionary<string, string[]> AttributeToIgnoreOnLookup = new();

        public readonly Dictionary<string, List<KeyValuePair<string, string>>> ItemSpecificAttributeComboLookup = new();
        public readonly ConcurrentDictionary<string, HashSet<string>> AttributeComboLookup = new();

        public void FinishedUpdate()
        {
            ProcessLbins();
            UpdateToday();
            var removeBefore = DateTime.UtcNow.AddMinutes(-10);
            foreach (var item in HigherValueLbinMapLookup.Where(c => c.Value.addedAt < removeBefore).ToList())
            {
                HigherValueLbinMapLookup.TryRemove(item.Key, out _);
            }
            foreach (var item in ModifierValueLookup.Where(c => c.Value.Item2 < removeBefore).ToList())
            {
                ModifierValueLookup.TryRemove(item.Key, out _);
            }
            foreach (var item in AttributeValueLookup.Where(c => c.Value.Item2 < removeBefore).ToList())
            {
                AttributeValueLookup.TryRemove(item.Key, out _);
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
            foreach (var item in ReforgeValueLookup.Where(c => c.Value.Item2 < removeBefore).ToList())
            {
                ReforgeValueLookup.TryRemove(item.Key, out _);
            }
        }

        private static void UpdateToday()
        {
            CurrentDayCache = (short)(DateTime.UtcNow - StartTime).TotalDays; // os time for UtcNow takes time
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
                    item.SellTime = (short)(auction.Start - auction.End.Date).TotalMinutes;
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
            {"drill_part_engine", string.Empty},
            {"drill_part_fuel_tank", string.Empty},
            {"drill_part_upgrade_module", string.Empty},
            {"hook.part", string.Empty},
            {"line.part", string.Empty},
            {"sinker.part", string.Empty},
            {"skin", "PET_SKIN_"},
            {"dye_item", string.Empty},
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

            string[] armorPieces = ["HELMET", "CHESTPLATE", "LEGGINGS", "BOOTS"];
            string[] tiers = ["", "HOT_", "BURNING_", "FIERY_", "INFERNAL_"];
            foreach (var toAdd in CrimsonArmors)
            {
                // each kind of armor piece is combinable with each other of the same type
                // eg "CRIMSON_HELMET", "AURORA_HELMET", "FERVOR_HELMET" etc.
                foreach (var item in armorPieces)
                {
                    var list = new List<string>();
                    foreach (var adding in CrimsonArmors)
                    {
                        list.Add($"{adding}{item}");
                    }
                    foreach (var tier in tiers)
                    {
                        SharedAttributeGroup[$"{tier}{toAdd}{item}"] = list.ToArray();
                    }
                }
            }

            this.itemService = itemService;
            this.activitySource = activitySource;
            this.logger = logger;
            this.craftCostService = craftCostService;

            Converters["exp"] = m => new(m.Modifier, GetExpValue(m.ItemTag, m.Modifier)) { IsEstimate = true };
            Converters["candyUsed"] = m => new(m.Modifier, GetCandyPrice(m.ItemTag, m.RelevantModifiers.ToDictionary())) { IsEstimate = true };
            /*
            { "upgrade_level", m => new (m.Modifier, EstStarCost(m.ItemTag, int.Parse(m.Modifier.Value))) {IsEstimate=true}},
            { "unlocked_slots", m => new (m.Modifier, itemService.GetSlotCostSync(m.ItemTag, m.FlatNbt, m.Modifier.Value.Split(',').ToList()).Item2){IsEstimate=true}},
            { "scroll_count", m => new (m.Modifier, (GetPriceForItem("IMPLOSION_SCROLL") + GetPriceForItem("SHADOW_WARP_SCROLL") + GetPriceForItem("WITHER_SHIELD_SCROLL")) / 3 * int.Parse(m.Modifier.Value)){IsEstimate=true}},
            */
            Converters["upgrade_level"] = m => new(m.Modifier, EstStarCost(m.ItemTag, int.Parse(m.Modifier.Value)));
            Converters["unlocked_slots"] = m => new(m.Modifier, GetGemstoneSlotWorth(m.RelevantModifiers, m.ItemTag, m.Modifier));
            Converters["scroll_count"] = m => new(m.Modifier, (GetPriceForItem("IMPLOSION_SCROLL") + GetPriceForItem("SHADOW_WARP_SCROLL") + GetPriceForItem("WITHER_SHIELD_SCROLL")) / 3 * int.Parse(m.Modifier.Value));
            Converters["bass_weight"] = m => new(m.Modifier, 5_000_000 * int.Parse(m.Modifier.Value.Split(',')[0]));
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
                return new() { Median = (long)bazaar * auction.Count };
            var tagGroup = GetAuctionGroupTag(auction.Tag);

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
            ReferencePrice lbinCap = GetLbinCap(tagGroup.tag, l, itemKey);
            if (lbinCap.Price != 0 && result.Lbin.Price > lbinCap.Price)
            {
                result.Lbin = lbinCap;
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

        public ReferencePrice GetLbinCap(string tag, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, AuctionKey itemKey)
        {
            var lbinCap = HigherValueLbinMapLookup.GetOrAdd((tag, itemKey), a =>
            {
                var higherValue = l.Where(k => k.Value.Lbin.Price != 0
                                    && IsHigherValue(tag, itemKey, k.Key) && k.Key.Reforge == itemKey.Reforge);
                var MaxValue = higherValue.OrderBy(b => b.Value.Lbin.Price).FirstOrDefault();
                if (MaxValue.Key == a.Item2)
                    return (default, DateTime.UtcNow); // best match is itself, skip
                return (MaxValue.Value?.Lbin ?? default, DateTime.UtcNow);
            }).result;
            return lbinCap;
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
                    result.MedianKey += diffExp;
                    var lookup = Lookups[this.GetAuctionGroupTag(auction.Tag).Item1];
                    var cleanItemValue = GetCleanItemPrice(auction.Tag, itemKey, lookup);
                    if (changeAmount > result.Median / 4 * 3)
                    {
                        var closestCraftCost = ComparisonValue(c.Key.Enchants, c.Key.Modifiers.ToList(), auction.Tag, null);
                        AddReforgeValue(c.Key.Reforge, ref closestCraftCost);
                        var percentDiff = (float)(cleanItemValue + itemKey.ValueBreakdown.Sum(v => v.Value)) / (cleanItemValue + closestCraftCost.Sum(m => m.Value) + 1);
                        result.Median = (long)(result.Median * Math.Min(percentDiff, 1.5));
                        result.MedianKey += "*" + percentDiff.ToString("F2", System.Globalization.CultureInfo.InvariantCulture);
                    }
                    else
                        result.Median -= changeAmount;
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
            (var enchal, var enchExp) = AdjustForMissingEnchants(result, auction.Tag, itemKey, c);
            var reforgediff = 0L;
            if (c.Key.Reforge != itemKey.Reforge)
                reforgediff = GetReforgeValue(c.Key.Reforge) - GetReforgeValue(itemKey.Reforge) / 20;
            diffExp = modExp + enchExp;
            changeAmount = modVal + enchal + reforgediff;
        }

        private (long substract, string add) AdjustForMissingEnchants(PriceEstimate result, string itemTag, AuctionKey itemKey, KeyValuePair<AuctionKey, ReferenceAuctions> closest)
        {
            // closest should be bigger 
            var missingEnchants = closest.Key.Enchants.Where(m => !itemKey.Enchants.Any(e => e.Type == m.Type && e.Lvl >= m.Lvl)).ToList();
            if (missingEnchants.Count > 0)
            {
                var enchCost = GetPriceSumForEnchants(missingEnchants, itemTag);
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
                    || m.Key == "exp" || m.Key == "candyUsed" || KillKeys.Contains(m.Key))
                {
                    return 0;
                }
                var elem = ModifierEstimate(missingModifiers.ToList(), auction.Tag, auction.FlatenedNBT, m);
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

        private IEnumerable<(string tag, int amount)> GetItemKeysForModifier(string tag, KeyValuePair<string, string> m)
        {
            if (m.Key == null)
                return EmptyArray;
            if (ModifierItemPrefixes.TryGetValue(m.Key, out var prefix))
                if (prefix == string.Empty)
                    return new (string, int)[] { (prefix + m.Value.ToUpper(), 1) };
                else
                    // some of the items actually don't have the prefix, skins on pets may but other skins don't
                    return new (string, int)[] { (prefix + m.Value.ToUpper(), 1), (m.Value.ToUpper(), 1) };

            if (m.Key == "upgrade_level" && !(itemService?.IsDungeonItemSync(tag) ?? true))
            {
                return EmptyArray;
            }
            if (mapper.TryGetIngredients(tag, m.Key, m.Value, null, out var ingredients))
            {
                return ingredients.GroupBy(i => i).Select(i => (i.Key, i.Count()));
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
                            .OrderByDescending(m => itemKey.Similarity(m.Key, this, [.. ComparisonValue(m.Key.Enchants, m.Key.Modifiers, itemTag, null)], values) + (m.Value.OldestRef > minDay ? 0 : -10));
        }

        void AssignMedian(PriceEstimate result, AuctionKey key, ReferenceAuctions bucket, long gemVal)
        {
            result.Median = bucket.Price + gemVal + (((key as AuctionKeyWithValue)?.ValueSubstract - gemVal * 20 / 19) / 3 ?? 0);
            result.Volume = bucket.Volume;
            result.MedianKey = key.ToString();
            result.Volatility = bucket.Volatility;
            result.LastSale = bucket.References.LastOrDefault();
            result.AvgSellTime = (short)bucket.TimeToSell;
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

            UpdateMedian(oldBucket, (GetAuctionGroupTag(tag).tag, GetBreakdownKey(from, tag)));
            UpdateMedian(newBucket, (GetAuctionGroupTag(toTag).tag, GetBreakdownKey(to, tag)));
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
                var value = loadedVal.Lookup.GetValueOrDefault(item);
                if (value == null)
                    continue;
                if (value.References.Count == 0 && value.Lbins.Count == 0 || value.References.All(r => r.Day == 1047) // lost nbt data that day
                    || value.References.All(r => r.Day < GetDay() - 21) && !item.IsClean())
                    loadedVal.Lookup.TryRemove(item, out _); // unimportant
                if (NBT.IsPet(itemTag) && !item.Modifiers.Any(m => m.Key == "exp"))
                    loadedVal.Lookup.TryRemove(item, out _); // bugged
            }
            if (IsRune(itemTag))
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
                    if (item.Key.Count == 0 && item.Key.Tier == Tier.UNKNOWN && item.Value.References.Count == 0)
                        continue;
                    loadedVal.Lookup[item.Key] = item.Value;
                }
                if (loadedVal.CleanKey?.Count == default || !loadedVal.Lookup.TryGetValue(loadedVal.CleanKey, out var b) || b?.Volume * 10 < loadedVal.Lookup.Max(l => l.Value.Volume))
                {
                    UpdateCleanKey(loadedVal);
                }
                return loadedVal;
            }, (tag, value) =>
            {
                foreach (var item in loadedVal.Lookup)
                {
                    if (!value.Lookup.TryGetValue(item.Key, out ReferenceAuctions existingBucket))
                    {
                        item.Value.References = new ConcurrentQueue<ReferencePrice>(item.Value.References.Where(r => r.Price > 0).OrderBy(r => r.Day));
                        if (item.Key.Count == 0 && item.Key.Tier == Tier.UNKNOWN && item.Value.References.Count == 0)
                            continue;
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
            UpdateFragged(itemTag);

            void CombineBuckets(KeyValuePair<AuctionKey, ReferenceAuctions> item, ReferenceAuctions existingBucket)
            {
                var existingRef = existingBucket.References;
                existingBucket.References = item.Value.References;
                if (existingRef != null)
                {
                    existingBucket.References = new(existingRef.Concat(item.Value.References).ToList()
                        .DistinctBy(d => d.AuctionId)
                        .OrderBy(r => r.Day));

                    var today = GetDay();
                    if (existingBucket.References.Count > 7 && existingBucket.References.TryPeek(out var r) && r.Day < today - 30)
                    {
                        var cleaned = ApplyAntiMarketManipulation(existingBucket);
                        var cleanedMedian = GetMedian(cleaned, null);
                        var median = GetMedian(existingBucket.References.Where(ri => ri.Day > today - 30).ToList(), null);
                        var increaseRate = (double)median / cleanedMedian;
                        var allowedIncrease = 1.5;
                        if (increaseRate > allowedIncrease)
                        {
                            increaseRate /= allowedIncrease;
                        }
                        else if (increaseRate < 1)
                        {
                            increaseRate = 1;
                        }
                        while (existingBucket.References.Count > 7 && existingBucket.References.TryPeek(out r) && r.Day < today - 30 * increaseRate)
                        {
                            existingBucket.References.TryDequeue(out _);
                        }
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

        private static void UpdateFragged(string itemTag)
        {
            if (itemTag.StartsWith("STARRED_") &&
            // midas and daedalus needs golden fragments which are expensive
            !MidasTags.Contains(itemTag) && !itemTag.StartsWith("STARRED_DAEDALUS_AXE"))
            {
                CombinableStarred.Add(itemTag);
            }
            if (itemTag.Contains("RUNE_"))
            {
                RuneLookup.Add(itemTag);
                RuneLookup.Add(itemTag.Replace("UNIQUE_", ""));
            }
        }

        private static bool IsRune(string itemTag)
        {
            return RuneLookup.Contains(itemTag);
        }

        private static void CapBucketSize(ReferenceAuctions bucket)
        {
            while (bucket.References.Count > SizeToKeep && bucket.References.TryDequeue(out _)) { }
        }

        public short AddSoldItem(SaveAuction auction, bool preventMedianUpdate = false)
        {
            UpdateFragged(auction.Tag);
            (ReferenceAuctions bucket, var key) = GetBucketForAuction(auction);
            var extraValue = GetExtraValue(auction, key);
            var time = AddAuctionToBucket(auction, preventMedianUpdate, bucket, key.ValueSubstract, extraValue);
            HigherValueLbinMapLookup.TryRemove(((string, AuctionKey))(auction.Tag, key), out _);
            try
            {
                var attributesOnAuction = auction.FlatenedNBT.Where(a => Constants.AttributeKeys.Contains(a.Key)).ToList();
                if (attributesOnAuction.Count == 0)
                    return time;
                if (key.Enchants.Count > 1 || key.Modifiers.Count > 2)
                    return time; // only add attributes for (almost) clean items, one allowed for things that drop with extra enchants
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
                    foreach (var updateItem in AttributeValueLookup.Where(l =>
                        (l.Key.Item1 == groupTag.tag || groupTag.tag == "ATTRIBUTE_SHARD")
                        && l.Key.Item2.Key == itemKey.Modifiers.First().Key).ToList())
                    {
                        AttributeValueLookup.TryRemove(updateItem.Key, out _);
                    }
                }
                OnSold?.Invoke((auction, key));
            }
            catch (System.Exception e)
            {
                dev.Logger.Instance.Error(e, $"Occured when trying to store attribue value");
            }
            return time;
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
        public short AddAuctionToBucket(SaveAuction auction, bool preventMedianUpdate, ReferenceAuctions bucket, long valueSubstract = 0, long extraValue = 0)
        {
            if (bucket.References.Where(r => r.AuctionId == auction.UId).Any())
                return -1; // duplicate
            var reference = CreateReferenceFromAuction(auction, valueSubstract, extraValue);
            if (reference.Price < 0 && valueSubstract > 1_000_000)
            {
                logger.LogInformation($"Negative price {JsonConvert.SerializeObject(auction)} {reference.Price} {valueSubstract}");
            }
            // move reference to sold
            var activeLbin = bucket.Lbins.FirstOrDefault(l => l.AuctionId == auction.UId);
            if (activeLbin.AuctionId != default)
            {
                var listedAt = StartTime.AddDays(activeLbin.Day) + TimeSpan.FromMinutes(activeLbin.SellTime);
                var soldAt = auction.End;
                reference.SellTime = (short)(soldAt - listedAt).TotalMinutes;
            }
            else if (auction.Start != default)
            {
                reference.SellTime = (short)((auction.End - auction.Start).TotalMinutes + 1); // add one in case it sold within a minute
            }
            bucket.References.Enqueue(reference);
            bucket.Lbins.RemoveAll(l => l.AuctionId == auction.UId);
            CapBucketSize(bucket);
            if (!preventMedianUpdate)
            {
                var key = DetailedKeyFromSaveAuction(auction);
                if (NBT.IsPet(auction.Tag) && !key.Key.Modifiers.Any(m => m.Key == "exp"))
                {
                    logger.LogError($"Exp lost {auction.Uuid} {JsonConvert.SerializeObject(key)}");
                }
                UpdateMedian(bucket, (GetAuctionGroupTag(auction.Tag).tag, key));
            }
            return reference.SellTime;
        }

        public void UpdateMedian(ReferenceAuctions bucket, (string tag, KeyWithValueBreakdown key) keyCombo = default)
        {
            var size = bucket.References.Count;
            if (size < 4)
            {
                if (keyCombo != default)
                    bucket.Price = 0;
                return; // can't have enough volume
            }
            List<ReferencePrice> deduplicated = ApplyAntiMarketManipulation(bucket);
            DropUnderlistings(deduplicated);

            size = deduplicated.Count();
            if (size <= 3 || deduplicated.Count(d => d.Day >= GetDay() - 20) < 3 && !(keyCombo.Item2?.Key.IsClean() ?? false) && !IsMaxAttrib(keyCombo))
            {
                bucket.Price = 0; // to low vol
                return;
            }
            var days = GetDay() - deduplicated.OrderBy(d => d.Day).First().Day + 1;
            if (days == 0)
                days = 1;
            bucket.Volume = deduplicated.Count / days;
            // short term protects against price drops after updates
            List<ReferencePrice> shortTermList = GetShortTermBatch(deduplicated, bucket.Volume).OrderByDescending(b => b.Day).ToList();
            PriceLookup lookup;
            Dictionary<short, long> cleanPriceLookup;
            bool isCleanitem;
            GetCleanPriceLookup(keyCombo, out lookup, out cleanPriceLookup, out isCleanitem);
            var shortTermPrice = GetMedian(shortTermList, cleanPriceLookup);
            if (IsDropping(bucket, shortTermPrice, out var rate))
            {
                shortTermPrice = (long)(shortTermPrice * rate);
            }
            bucket.OldestRef = shortTermList.Take(4).Min(s => s.Day);
            if (shortTermList.Count >= 3 && bucket.OldestRef - shortTermList.First().Day <= -5
                && shortTermList.First().AuctionId != shortTermList.OrderByDescending(o => o.Price).First().AuctionId
                && bucket.References.OrderByDescending(r => r.Day).Skip(5).FirstOrDefault().Day <= GetDay() - 5 // check if anti market manipulation made a hole
                && bucket.Volume > 0.3) // 5 day gaps are to be expected at ~0.2 volume
            {
                // probably derpy or weird price drop
                var reduced = (shortTermList.OrderBy(s => s.Price).First().Price + shortTermPrice * 2) / 3;
                shortTermPrice = Math.Max(shortTermPrice * 8 / 10, reduced);
            }
            // long term protects against market manipulation
            var monthSpan = deduplicated.Where(d => d.Day >= GetDay() - 30).ToList();
            var longSpanPrice = monthSpan.Count switch
            {
                > 24 => HighReferenceCount(cleanPriceLookup, monthSpan),
                > 5 => GetMedian(monthSpan, cleanPriceLookup),
                _ => GetMedian(deduplicated.Take(29).ToList(), cleanPriceLookup)
            };
            var today = GetDay();
            if (deduplicated.All(d => d.Day >= today))
            {
                // all prices are from today, use 25th percentile instead
                longSpanPrice = deduplicated.OrderBy(d => d.Price).Take((int)Math.Max(deduplicated.Count() * 0.25, 1)).Max(d => d.Price);
            }
            var uncappedMedian = Math.Min(shortTermPrice, longSpanPrice);
            var medianPrice = uncappedMedian;
            if (keyCombo == default)
            {
                bucket.Price = medianPrice; // bazaar price
                return;
            }
            var lbinMedian = bucket.Lbins.Where(l => l.Price > medianPrice / 2 && l.Day > GetDay() + 5).OrderBy(l => l.Price).Skip(2).FirstOrDefault();
            if (lbinMedian.AuctionId != default)
            {
                medianPrice = Math.Min(medianPrice, lbinMedian.Price);
                shortTermPrice = Math.Min(shortTermPrice, lbinMedian.Price);
            }
            (bucket.Volatility, medianPrice) = GetVolatility(lookup, bucket, shortTermPrice, longSpanPrice);
            bucket.HitsSinceCalculating = 0;
            bucket.StonksHits = 0;
            bucket.DeduplicatedReferenceCount = (short)deduplicated.Count();
            PreCalculateVolume(keyCombo);
            bucket.TimeToSell = (int)EstimateTimeTosell(deduplicated, medianPrice, bucket.Volume);
            // get price of item without enchants and add enchant value 
            if (keyCombo != default)
            {
                var breakdown = keyCombo.key.ValueBreakdown;
                var volatMedian = medianPrice;
                var limitedPrice = CapPriceAtHigherLevelKey(keyCombo, medianPrice, bucket);

                // check higher value keys for lower price 
                if (keyCombo.key.Key.Modifiers.Any(m => m.Key == "pgems"))
                {
                    var lookupKey = new AuctionKey(keyCombo.key)
                    {
                        Modifiers = new(keyCombo.key.Key.Modifiers.Where(m => m.Key != "pgems").ToList())
                    };
                    if (lookup.Lookup.TryGetValue(lookupKey, out var lookupBucket))
                    {
                        limitedPrice = Math.Max(limitedPrice, lookupBucket.Price);
                    }
                }
                else if (lookup.CleanPricePerTier?.TryGetValue(keyCombo.key.Key.Tier, out var tierval) ?? false)
                {
                    if (keyCombo.key.Key.Modifiers.Count == 0 && keyCombo.key.Key.Reforge == ItemReferences.Reforge.jaded)
                    {
                        var lowest = lookup.Lookup.Where(l => l.Value.Price > 0).OrderBy(l => l.Value.Price).Take(5).ToList();
                    }
                    if (limitedPrice < tierval / 1.2 && !keyCombo.key.Key.Modifiers.Any(m => m.Key == "virtual" || Constants.AttributeKeys.Contains(m.Key)))
                        limitedPrice = Math.Max(limitedPrice, tierval);
                }
                limitedPrice = CapAtCraftCost(keyCombo.tag, limitedPrice, keyCombo.key, bucket.Price);
                var craftCostCap = limitedPrice;
                if (limitedPrice == 0 || IsLevel100CleanPet(keyCombo.key))
                {
                    limitedPrice = medianPrice;
                }

                if (size > 40 || bucket.Volatility <= 8 && size > 8)
                {
                    bucket.RiskyEstimate = Get66thPercentile(cleanPriceLookup, monthSpan, limitedPrice, medianPrice);
                }
                else if (size > 10 && bucket.Volatility < 18)
                {
                    var riskyEst = Get66thPercentile(cleanPriceLookup, monthSpan, limitedPrice, medianPrice);
                    bucket.RiskyEstimate = (limitedPrice + riskyEst) / 2;
                }
                else
                {
                    bucket.RiskyEstimate = limitedPrice;
                }

                if (limitedPrice == 0)
                {
                    if (longSpanPrice != 0)
                        logger.LogWarning($"Price capped {keyCombo.tag} -> {limitedPrice} ({craftCostCap}) {keyCombo.key.Key} {medianPrice} {bucket.Price} - {volatMedian} {shortTermPrice} {longSpanPrice}");
                    limitedPrice = 11;
                }
                var preLimitedPrice = medianPrice;
                medianPrice = limitedPrice;
                if (medianPrice < 0)
                {
                    logger.LogWarning($"Negative price {keyCombo.tag} -> {limitedPrice}  {keyCombo.key} {medianPrice} {bucket.Price}");
                }
                else
                    bucket.Price = medianPrice;

                var recent = bucket.References.AsEnumerable().Reverse().Take(Math.Max(bucket.Volume < 0.5 ? 6 : 12, (int)bucket.Volume)).ToList();
                var percentileRecent = GetMedian(recent, cleanPriceLookup, 3f);
                if (bucket.Volume >= 4 && bucket.Lbin.AuctionId != default && bucket.Lbin.Day < GetDay() + 3)
                { // volume high enought to risk higher percentile
                    var cappedPrice = preLimitedPrice == medianPrice ? preLimitedPrice * 12 / 10 : limitedPrice;
                    medianPrice = Math.Min(Math.Max(bucket.RiskyEstimate, medianPrice), Math.Min(cappedPrice, percentileRecent));
                    var deduplicatedRecent = deduplicated.OrderByDescending(r => r.Day).Take(9).OrderBy(r => r.Price).Skip(6).FirstOrDefault();
                    if (deduplicatedRecent.AuctionId != default && deduplicatedRecent.Price < medianPrice)
                    {
                        medianPrice = Math.Min(medianPrice, deduplicatedRecent.Price);
                    }
                }
                else if (keyCombo.key.Key.Modifiers.Count <= 1) // this condition could be extended to everything but the calculation can cause undervaluations on eg gems
                    medianPrice = Math.Min(medianPrice, percentileRecent);
                lookup.HasMultipleRarities = lookup.Lookup
                        .Where(l => l.Key.Tier != Tier.UNKNOWN)
                        .GroupBy(l => l.Key.Tier).Count() > 2;
                var cleanPrice = GetCleanItemPrice(keyCombo.tag, keyCombo.key, lookup, true);
                lookup.CleanPricePerTier ??= new();
                if (cleanPrice > 0)
                    lookup.CleanPricePerTier[keyCombo.key.Key.Tier] = cleanPrice;

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
                        medianPrice = Math.Min(medianPrice, lowerCountBucket.Price * keyCombo.Item2.Key.Count);
                    }
                }
            }
            else
            {
                logger.LogWarning($"No key combo {keyCombo.tag} {bucket.Price} {bucket.References.First().AuctionId}");
            }
            if (isCleanitem)
            {
                lookup.CleanPricePerDay ??= new();
                if (medianPrice > 0)
                    lookup.CleanPricePerDay[shortTermList.OrderByDescending(s => s.Day).First().Day] = uncappedMedian;
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

            long CapPriceAtHigherLevelKey((string tag, KeyWithValueBreakdown key) keyCombo, long limitedPrice, ReferenceAuctions bucket)
            {
                var oldestDay = bucket.OldestRef;
                if (keyCombo.key.Key.Modifiers.Any(m => m.Key == "new_years_cake" && ImportantCakeYears.Contains(m.Value)))
                {
                    return limitedPrice;
                }
                var cheaperHigherValue = Lookups[keyCombo.tag].Lookup
                    .Where(k =>
                            k.Value.Price < limitedPrice && k.Value.Price != 0
                            && keyCombo.key.Key != k.Key
                            && !k.Key.Modifiers.Any(m => m.Key == "virtual")
                            && k.Value.OldestRef >= oldestDay // only relevant if price dropped recently
                            && k.Value.DeduplicatedReferenceCount > 3
                            && k.Value.Volume * 5 >= bucket.Volume
                            && IsHigherValue(keyCombo.tag, keyCombo.key, k.Key) && k.Key.Reforge == keyCombo.key.Key.Reforge)
                    .OrderBy(b => b.Value.Price).FirstOrDefault();
                if (cheaperHigherValue.Value != default
                    && cheaperHigherValue.Key != keyCombo.key.Key
                    && cheaperHigherValue.Value.Price < limitedPrice)
                {
                    limitedPrice = cheaperHigherValue.Value.Price;
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
                    UpdateCleanKey(lookup);
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

            static long Get66thPercentile(Dictionary<short, long> cleanPriceLookup, List<ReferencePrice> monthSpan, long limitedPrice, long medianPrice)
            {
                var riskyLongTerm = GetMedian(monthSpan.Where(d => d.Day >= GetDay() - 10).ToList(), cleanPriceLookup, 3f);
                var riskyShort = GetMedian(monthSpan.Where(d => d.Day >= GetDay() - 2).ToList(), cleanPriceLookup, 3f);
                var marketManipLimit = limitedPrice * 10 / 9 + 1_000_000;
                if (medianPrice > limitedPrice)
                {// already capped by craft cost reduce limit
                    marketManipLimit = limitedPrice * 11 / 10;
                }
                var estimate = Math.Min(Math.Min(riskyShort, riskyLongTerm), marketManipLimit);
                return estimate;
            }

            static double EstimateTimeTosell(List<ReferencePrice> deduplicated, long medianPrice, float volume)
            {
                var relevant = deduplicated
                    .Where(d => d.SellTime > 0 && d.Price > medianPrice * 0.96 && d.Price < medianPrice * 1.1);
                return relevant.DefaultIfEmpty().Average(d => d.SellTime) * Math.Max(1, 2 / volume);
            }
        }

        static void DropUnderlistings(List<ReferencePrice> deduplicated, int scanSize = 5)
        {
            var bucketSize = deduplicated.Count();
            var toRemove = new List<ReferencePrice>();
            var lookup = deduplicated.ToLookup(d => d.Buyer);
            for (int i = 0; i < bucketSize; i++)
            {
                var targetAuction = deduplicated[i];
                var batch = deduplicated.Skip(i).Take(scanSize).ToList();
                if (batch.Count < 3)
                    break;
                var hit = lookup.Contains(targetAuction.Seller) ? lookup[targetAuction.Seller].FirstOrDefault() : default;
                if (hit.AuctionId == default)
                    continue;
                if (i < 3 && batch.Take(scanSize).Where(a => a.AuctionId != hit.AuctionId).Select(a => a.Price).Average() < hit.Price)
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

        private bool IsLevel100CleanPet(KeyWithValueBreakdown key)
        {
            return key.Key.Modifiers.Count == 1 && key.Key.Modifiers[0].Key == "exp" && key.Key.Modifiers[0].Value == "6";
        }

        private bool IsDropping(ReferenceAuctions bucket, long shortTermPrice, out float rate)
        {
            var relevant = bucket.References.Reverse().Take(6).ToList();
            var buyLookup = relevant.GroupBy(r => r.Buyer).ToDictionary(g => g.Key, g => g.First());
            List<float> drops = new();
            foreach (var item in relevant)
            {
                if (buyLookup.TryGetValue(item.Seller, out var purchase))
                {
                    if (purchase.Price > item.Price * 0.97 && item.Price < shortTermPrice)
                    {
                        drops.Add((float)item.Price / Math.Max(purchase.Price, shortTermPrice * 0.97f) * 0.97f);
                    }
                }
            }
            if (drops.Count <= 1)
            {
                rate = 1;
                return false;
            }
            rate = drops.OrderByDescending(e => e).Skip(1).First();
            return true;
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
                .Select(a => a.OrderBy(ai => ai.Price).Skip(a.Count() / 3).First())  // only use one (the cheapest) price from each seller
                .GroupBy(a => a.Buyer == 0 ? buyerCounter++ : a.Buyer)
                .Select(a => a.OrderBy(ai => ai.Price).Skip(a.Count() / 3).First())  // only use cheapest price from each buyer 
                .Take(WorkingSize)
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

        private static void UpdateCleanKey(PriceLookup lookup)
        {
            lookup.CleanKey = lookup.Lookup.Where(l => !l.Key.Modifiers.Any(m => m.Key == "virtual")).OrderByDescending(l => l.Value.Volume - l.Key.Modifiers.Count * 5).Select(l => l.Key).FirstOrDefault();
            lookup.CleanPricePerDay = new();
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
            var oldMedian = GetMedian(bucket.References.AsEnumerable().Take(5).ToList(), lookup?.CleanPricePerDay);
            var secondNewestMedian = 0L;
            if (bucket.References.Count > 8)
            {
                var secondSample = bucket.References.AsEnumerable().Reverse().Skip(5).Take(5).ToList();
                secondNewestMedian = GetMedian(secondSample, lookup?.CleanPricePerDay);
            }
            var thirdMedian = 0L;
            if (bucket.References.Count > 11)
            {
                var thirdSample = bucket.References.AsEnumerable().Reverse().Skip(9).Take(4).ToList();
                thirdMedian = GetMedian(thirdSample, lookup?.CleanPricePerDay);
            }
            var medianList = new float[] { oldMedian, secondNewestMedian, longTerm, shortTermPrice }.OrderByDescending(m => m).ToList();
            var mean = medianList.Average();
            medianList = medianList.Select(m => m / mean).ToList();
            mean = medianList.Average();
            var variance = medianList.Select(m => Math.Pow(m - mean, 2)).Sum() / medianList.Count;
            var volatility = Math.Sqrt(variance);
            var volatilityReduced = (byte)Math.Clamp(volatility * 100, -120, 120);
            var newMedian = Math.Min(shortTermPrice, longTerm);
            if (IsTrendDownwards(shortTermPrice, longTerm, thirdMedian, secondNewestMedian))
            {
                var difference = secondNewestMedian - shortTermPrice;
                var differenceToLong = longTerm - shortTermPrice;
                difference = Math.Min(difference, differenceToLong);
                var inPercent = (float)difference / secondNewestMedian / 2;
                if (difference > 0 && newMedian > difference && inPercent < 0.2)
                {
                    newMedian = newMedian - (long)(newMedian * inPercent);
                    deferred.Log($"Trend downwards {bucket.References.First().AuctionId} - {bucket.Price} {shortTermPrice} {longTerm} {secondNewestMedian} diff:{difference} {inPercent}% {newMedian}");
                }
            }

            return (volatilityReduced, newMedian);

            static bool IsTrendDownwards(long shortTermPrice, long longTerm, long oldMedian, long secondNewestMedian)
            {
                return oldMedian > secondNewestMedian && longTerm > shortTermPrice && secondNewestMedian > shortTermPrice;
            }
        }

        private long CapAtCraftCost(string tag, long medianPrice, KeyWithValueBreakdown key, long currentPrice)
        {
            List<RankElem> breakdown = key.ValueBreakdown;
            var limitedPrice = 0L;
            if (!Lookups.TryGetValue(tag, out var lookup))
            {
                logger.LogInformation($"Could not cap, No lookup for {tag} keeping {currentPrice} on {key.Key}");
                return Math.Min(medianPrice, currentPrice + 10_000 + currentPrice / 100);
            }
            // stackables
            if (key.Key.Enchants.Count == 0 && key.Key.Modifiers.Count == 0 && !lookup.HasMultipleRarities
                && (craftCostService?.TryGetCost(tag, out double craftCost) ?? false) && craftCost > 0)
            {
                var stackSize = key.Key.Count;
                if (stackSize == 0)
                {
                    return medianPrice; // don't limit bazaar items
                }
                var stackCost = craftCost * stackSize * 1.08;
                if (stackCost < 500_000)
                {
                    stackCost *= 2; // allow higher limit for low cost items
                }
                stackCost += 750_000; // lazyness factor
                if (stackCost < medianPrice)
                {
                    logger.LogDebug($"Capped {tag} at {stackCost} {stackSize} craft cost {craftCost} {key}");
                    return (long)stackCost;
                }
            }
            if (breakdown.Any(v => v.Value == 0) || breakdown.Count <= 0)
                return medianPrice; // can't cap nothin added, basically clean

            // determine craft cost 
            long minValue = GetCleanItemPrice(tag, key, lookup);
            if (minValue == 0 || currentPrice == minValue)
                return medianPrice;
            if (IsRune(tag))
            {
                return LimitRuneToFuseCost(medianPrice, breakdown, lookup);
            }
            var modifierSum = breakdown.Select(v =>
            {
                if (v.Modifier.Key == "candyUsed")
                {
                    return 0;
                }
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

        private static long GetCleanItemPrice(string tag, KeyWithValueBreakdown key, PriceLookup lookup, bool force = false)
        {
            if (!force && lookup.CleanPricePerTier.TryGetValue(key.Key.Tier, out var cleanPrice))
            {
                return cleanPrice;
            }
            var matchRarity = tag == "THEORETICAL_HOE_WHEAT_3";
            var minRarity = matchRarity ? key.Key.Tier : key.Key.Tier - 1;
            var select = (NBT.IsPet(tag) ?
                            lookup.Lookup.Where(v => key.Key.Tier == v.Key.Tier && !v.Key.Modifiers.Any(m => m.Value == TierBoostShorthand || m.Key == "pgems")).Select(v => v.Value) :
                             lookup.Lookup.Where(v => minRarity <= v.Key.Tier && !v.Key.Modifiers.Any(m => m.Key == "pgems")).Select(l => l.Value)).ToList();
            var count = select.Count;
            var all = select.SelectMany(v => v.References).ToList();

            if (NBT.IsPet(tag) || matchRarity)
                DropUnderlistings(all, 18);
            var size = (int)Math.Max(lookup.Volume * 10, 50);
            var sample = all.OrderByDescending(a => a.Day).ThenBy(l => l.Price)
                .Take(size).OrderBy(r => r.Price);
            var devider = matchRarity ? 10 : 30;
            var target = sample.Skip(size / devider + 1).FirstOrDefault();
            return target.Price;
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
            var key = (tag, v.Modifier);
            if (AttributeValueLookup.TryGetValue(key, out var value))
            {
                return value.Item1 + comboValue;
            }
            var elementValue = NewMethod(tag, v, lookup);
            if (elementValue > 0)
                AttributeValueLookup[key] = (elementValue, DateTime.UtcNow);
            return elementValue + comboValue;

            static double GetPercentile(RankElem v, IEnumerable<KeyValuePair<AuctionKey, ReferenceAuctions>> options)
            {
                var values = options.Where(l => l.Value.Price > 0
                                            && (l.Key.Modifiers.Count == 2 && l.Key.Modifiers.Last().Key == "virtual" || l.Key.Modifiers.Count == 1) && l.Key.Modifiers.Any(m => m.Key == v.Modifier.Key))
                                .SelectMany(l => l.Value.References.Select(r => r.Price / Math.Pow(2, int.Parse(l.Key.Modifiers.First().Value))))
                                .ToList();
                var quarterPercentile = values.Count > 0 ? values.OrderBy(v => v).Skip(values.Count / 5).First() : 0;
                return quarterPercentile;
            }

            double CheckPaths(RankElem v, IEnumerable<KeyValuePair<AuctionKey, ReferenceAuctions>> options)
            {
                double quarterPercentile = GetPercentile(v, options);
                var shards = GetPercentile(v, Lookups.GetValueOrDefault("ATTRIBUTE_SHARD", new PriceLookup()).Lookup);
                if (shards > 0 && shards < quarterPercentile)
                {
                    quarterPercentile = shards;
                }

                return quarterPercentile;
            }

            long NewMethod(string tag, RankElem v, PriceLookup lookup)
            {
                var baseLevel = int.Parse(v.Modifier.Value);
                // check lowest value path
                var options = lookup.Lookup.AsEnumerable();
                if (CrimsonArmors.Any(tag.StartsWith))
                {
                    // these 4 types can be combined amongst each other
                    var secondType = tag.Split("_")[1];
                    options = CrimsonArmors.SelectMany(s => Lookups.TryGetValue(s + secondType, out var lookup) ? lookup.Lookup.AsEnumerable() : []);
                }
                if (tag == "HELLFIRE_ROD")
                {
                    // hellfire rod can be easily upgraded from magma rod
                    if (Lookups.TryGetValue("INFERNO_ROD", out var inferno))
                        options = options.Concat(inferno.Lookup);
                    if (Lookups.TryGetValue("MAGMA_ROD", out var magma))
                        options = options.Concat(magma.Lookup);
                }
                double quarterPercentile = CheckPaths(v, options);
                return (long)(Math.Pow(2, baseLevel) * quarterPercentile * 1.20);
            }
        }

        private static List<ReferencePrice> GetShortTermBatch(List<ReferencePrice> deduplicated, float volume)
        {
            // if more than half of the references are less than 12 hours old, use more references
            if (deduplicated.Where(d => d.Day >= GetDay(DateTime.Now - TimeSpan.FromHours(12))).Count() > SizeToKeep / 2 || volume > 4)
                return deduplicated.Take(6).ToList();
            return deduplicated.Take(5).ToList();
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

        private static ReferencePrice CreateReferenceFromAuction(SaveAuction auction, long valueSubstract = 0, long extraValue = 0)
        {
            var basePrice = auction.HighestBidAmount == 0 ? auction.StartingBid : auction.HighestBidAmount;
            // remove at most 50% of the value
            if (basePrice < valueSubstract)
                valueSubstract = Math.Min(valueSubstract, basePrice / 2);
            var buyer = auction.Bids?.OrderByDescending(b => b.Amount).FirstOrDefault();
            if (basePrice - valueSubstract - extraValue < 0)
            {
                extraValue = 0;
            }
            return new ReferencePrice()
            {
                AuctionId = auction.UId,
                Day = GetDay(auction.End),
                Price = basePrice - valueSubstract - extraValue,
                Seller = GetSellerId(auction),
                Buyer = buyer?.Bidder == null ? (short)0 : Convert.ToInt16(buyer.Bidder.Substring(0, 4), 16)
            };
        }

        private static short GetSellerId(SaveAuction auction)
        {
            return auction.AuctioneerId == null ? (short)(auction.SellerId % (2 << 14)) : Convert.ToInt16(auction.AuctioneerId.Substring(0, 4), 16);
        }

        public static short GetDay(DateTime date = default)
        {
            if (date == default)
                if (CurrentDayCache != 0)
                    return CurrentDayCache;
                else
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
        private static readonly DateTime UnlockedIntroduction = new DateTime(2021, 9, 11);
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
        private KeyWithValueBreakdown DetailedKeyFromSaveAuction(SaveAuction auction, bool fastMode = false, int limit = 5)
        {
            var shouldIncludeReforge = Constants.RelevantReforges.Contains(auction.Reforge);
            long valueSubstracted = 0;
            bool removedRarity = false;
            List<RankElem> rankElems = [];
            List<Enchant> enchants;
            List<KeyValuePair<string, string>> modifiers;
            (enchants, modifiers) = SelectValuable(auction, fastMode);

            (valueSubstracted, removedRarity, shouldIncludeReforge, rankElems) = CapKeyLength(enchants, modifiers, auction, limit);

            if (enchants == null)
                enchants = new List<Enchant>();
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
                if (!IsRune(auction.Tag))
                    tier = ReduceRarity(tier);
            }

            return Constructkey(auction, enchants, modifiers, shouldIncludeReforge, valueSubstracted, rankElems, tier);
        }

        private (List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers) SelectValuable(SaveAuction auction, bool fastMode = false)
        {
            var enchants = auction.Enchantments
                            ?.Where(e => MinEnchantMap.TryGetValue(e.Type, out byte value) && e.Level >= value)
                            .OrderBy(e => e.Type)
                            .Select(e => new Models.Enchant() { Lvl = e.Level, Type = e.Type }).ToList();

            if (!AllocatedDicts.TryDequeue(out var modifiers))
                modifiers = new Dictionary<string, string>(5);
            if (auction.FlatenedNBT != null)
                foreach (var item in auction.FlatenedNBT)
                {
                    if (!IncludeKeys.Contains(item.Key) && item.Value != "PERFECT" && !IsRune(item.Key) && !IsSoul(item))
                    {
                        continue;
                    }
                    var normalized = NormalizeData(item, auction.Tag, auction.FlatenedNBT);
                    if (normalized.Key != Ignore.Key)
                        modifiers.Add(normalized.Key, normalized.Value);
                }
            if (auction.ItemCreatedAt < UnlockedIntroduction
                // safe guard for when the creation date is wrong 
                && !auction.FlatenedNBT.ContainsKey("unlocked_slots"))
            {
                var allUnlockable = itemService?.GetUnlockableSlots(auction.Tag).ToList();
                if (auction.FlatenedNBT.TryGetValue("gemstone_slots", out var countString) && int.TryParse(countString, out var count))
                {
                    allUnlockable = allUnlockable.Take(count).ToList();
                    modifiers.Remove("gemstone_slots");
                }
                if (allUnlockable?.Count > 0)
                    modifiers.Add("unlocked_slots", string.Join(",", allUnlockable.OrderBy(s => s)));
            }
            return (enchants, modifiers.ToList());
        }

        private static KeyWithValueBreakdown Constructkey(SaveAuction auction, List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers, bool shouldIncludeReforge, long valueSubstracted, List<RankElem> rankElems, Tier tier)
        {
            var key = new AuctionKeyWithValue()
            {
                // order attributes
                Modifiers = modifiers.AsReadOnly(),
                Enchants = enchants.AsReadOnly(),
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


        private static HashSet<string> SoulKeys = new()
        {
            "MASTER_CRYPT_TANK_ZOMBIE_70",
            "MASTER_CRYPT_TANK_ZOMBIE_80",
            "MASTER_CRYPT_TANK_ZOMBIE_60",
            "MINOS_INQUISITOR_750",
            "MINOS_CHAMPION_310",
            "MINOS_CHAMPION_175",
            "MASTER_CRYPT_UNDEAD_25",
            "MASTER_CRYPT_UNDEAD_HYPIXEL_25",
            "MASTER_CRYPT_UNDEAD_VALENTIN_40",
            "MASTER_CRYPT_UNDEAD_CONNORLINFOOT_25",
            "MASTER_CRYPT_UNDEAD_PIETER_40",
            "MASTER_CRYPT_UNDEAD_NICHOLAS_40",
            "MASTER_CRYPT_UNDEAD_MINIKLOON_25",
            "MASTER_CRYPT_UNDEAD_LIKAOS_25",
            "MASTER_CRYPT_UNDEAD_JAMIETHEGEEK_25",
            "MASTER_CRYPT_UNDEAD_FRIEDRICH_40",
            "MASTER_CRYPT_UNDEAD_PLANCKE_25",
            "MASTER_CRYPT_UNDEAD_BLOOZING_25",
            "MASTER_CRYPT_UNDEAD_SYLENT_25",
            "MASTER_CRYPT_UNDEAD_CODENAME_B_25",
            "MASTER_CRYPT_UNDEAD_JAYAVARMEN_25",
            "MASTER_CRYPT_UNDEAD_ALEXANDER_40",
            "MASTER_CRYPT_UNDEAD_ORANGEMARSHALL_25",
            "MASTER_CRYPT_UNDEAD_BERNHARD_40",
            "MASTER_CRYPT_UNDEAD_CECER_25",
            "MASTER_CRYPT_UNDEAD_APUNCH_25",
            "MASTER_CRYPT_UNDEAD_DCTR_25",
            "MASTER_CRYPT_UNDEAD_RELENTER_25",
            "MASTER_CRYPT_UNDEAD_NITROHOLIC__25",
            "MASTER_CRYPT_UNDEAD_DONPIRESO_25",
            "MASTER_CRYPT_UNDEAD_LADYBLEU_25",
            "MASTER_CRYPT_UNDEAD_DUECES_25",
            "MASTER_CRYPT_UNDEAD_JUDG3_25",
            "MASTER_CRYPT_UNDEAD_BEMBO_25",
            "MASTER_CRYPT_UNDEAD_REZZUS_25",
            "MASTER_CRYPT_UNDEAD_SKYERZZ_25",
            "MASTER_CRYPT_UNDEAD_THEMGRF_25",
            "MASTER_CRYPT_UNDEAD__ONAH_25",
            "MASTER_CRYPT_UNDEAD_EXTERNALIZABLE_25",
            "MASTER_CRYPT_UNDEAD_REVENGEEE_25",
            "MASTER_CRYPT_UNDEAD_MAGICBOYS_25",
            "MASTER_CRYPT_UNDEAD_AGENTK_25",
            "MASTER_CRYPT_UNDEAD_FLAMEBOY101_25",
            "MASTER_CRYPT_UNDEAD_SFARNHAM_25",
            "MASTER_CRYPT_UNDEAD_THORLON_25",
            "MASTER_CRYPT_UNDEAD_CHILYNN_25",
            "MASTER_CRYPT_UNDEAD_MARIUS_40",
            "MASTER_CRYPT_UNDEAD_WILLIAMTIGER_25",
            "MASTER_CRYPT_UNDEAD_CHRISTIAN_40"
        };
        private static bool IsSoul(KeyValuePair<string, string> n)
        {
            // captured souls
            return SoulKeys.Contains(n.Key);
        }

        /// <summary>
        /// To find more matches the key length is capped.
        /// This is done by removing the lowest value enchantment or modifier
        /// </summary>
        /// <param name="enchants"></param>
        /// <param name="modifiers"></param>
        /// <returns>The coin amount substracted</returns>
        public (long valueSubstracted, bool removedRarity, bool includeReforge, List<RankElem> ranked) CapKeyLength(
            List<Enchant> enchants, List<KeyValuePair<string, string>> modifiers, SaveAuction auction, long threshold = 500000, int elements = 5)
        {
            long underlyingItemValue = GetCleanItemValue(auction, ref threshold);
            long valueSubstracted = HandleGems(modifiers, auction);
            IEnumerable<RankElem> combined = ComparisonValue(enchants, modifiers, auction.Tag, auction.FlatenedNBT);

            bool includeReforge = AddReforgeValue(auction.Reforge, ref combined);
            combined = SortCombined(combined);

            var modifierSum = underlyingItemValue + combined?.Select(m => m.IsEstimate ? m.Value / 20 : m.Value).DefaultIfEmpty(0).Sum() ?? 0;
            threshold = Math.Max(threshold, modifierSum / 22);
            var percentDiff = (double)auction.HighestBidAmount / modifierSum;
            if (auction.HighestBidAmount == 0 || percentDiff > 1)
                percentDiff = 1;
            // remove all but the top 5 
            List<RankElem> toRemove = GetItemsToRemove(threshold, combined);
            bool removedRarity = false;
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
                    if (item.Modifier.Key == "exp")
                        continue; // even if its valued at very little this needs to stay
                    if (adjustedRemoveValue > 50_000_000 && Constants.AttributeKeys.Contains(item.Modifier.Key))
                        adjustedRemoveValue -= 50_000_000;
                    if (item.IsEstimate)
                        adjustedRemoveValue /= 10;
                    if (modifiers.Remove(item.Modifier))
                        valueSubstracted += adjustedRemoveValue;
                    if (item.Modifier.Key == "skin")
                        modifiers.RemoveAll(m => m.Key == "candyUsed");
                    if (item.Modifier.Key == "rarity_upgrades")
                        removedRarity = true;
                }
            }
            List<RankElem> ordered = GetOrdered(elements, combined);
            return (valueSubstracted, removedRarity, includeReforge, ordered);

            static IEnumerable<RankElem> SortCombined(IEnumerable<RankElem> combined)
            {
                var list = combined as ICollection<RankElem> ?? combined.ToList();
                var filtered = new List<RankElem>(list.Count);
                foreach (var c in list)
                {
                    if (c.Value != 0)
                        filtered.Add(c);
                }
                filtered.Sort((a, b) => b.Value.CompareTo(a.Value));
                return filtered;
            }

            static List<RankElem> GetOrdered(int elements, IEnumerable<RankElem> combined)
            {
                return combined.Where(c => c.Value == 0).Concat(combined.Take(elements)).ToList();
            }
        }

        private static List<RankElem> GetItemsToRemove(long threshold, IEnumerable<RankElem> combined)
        {
            var toRemove = new List<RankElem>(5);
            int i = 0;
            foreach (var c in combined)
            {
                // keep top 1 even if below threshold
                // always remove below 500k or ~1.6%
                if ((i >= 5 && c.Value > 0)
                    || (i >= 1 && i < 5 && c.Value > 0 && c.Value < threshold)
                    || (i < 1 && c.Value > 0 && (c.Value < 500_000 || c.Value < threshold / 4)))
                {
                    toRemove.Add(c);
                }
                i++;
            }

            return toRemove;
        }

        private long GetCleanItemValue(SaveAuction auction, ref long threshold)
        {
            var underlyingItemValue = 0L;
            if (auction.Tag != null && Lookups.TryGetValue(auction.Tag, out var lookups))
            {
                var price = lookups.CleanPricePerTier.GetValueOrDefault(auction.Tier);
                if (price > 0)
                {
                    underlyingItemValue = price;
                    threshold = Math.Max(price / 20, threshold);
                }
            }

            return underlyingItemValue;
        }

        private long HandleGems(List<KeyValuePair<string, string>> modifiers, SaveAuction auction)
        {
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

            return valueSubstracted;
        }

        bool AddReforgeValue(ItemReferences.Reforge reforge, ref IEnumerable<RankElem> combined)
        {
            bool includeReforge = Constants.RelevantReforges.Contains(reforge);
            if (includeReforge)
            {
                if (ReforgeValueLookup.TryGetValue(reforge, out var value))
                {
                    combined = combined.Append(value.Item1);
                    return includeReforge;
                }
                long reforgeValue = GetReforgeValue(reforge);
                var element = new RankElem(reforge, reforgeValue);
                combined = combined.Append(element);
                if (reforgeValue > 0)
                    ReforgeValueLookup[reforge] = (element, DateTime.UtcNow);
            }

            return includeReforge;
        }

        private IEnumerable<RankElem> ComparisonValue(IEnumerable<Enchant> enchants, IList<KeyValuePair<string, string>> modifiers, string tag, Dictionary<string, string> flatNbt)
        {
            var valuePerEnchant = enchants?.Select(item => new RankElem(item, mapper.EnchantValue(new Core.Enchantment(item.Type, item.Lvl), null, BazaarPrices, tag)));

            var handler = (KeyValuePair<string, string> mod) =>
            {
                var lookupKey = new ModifierLookupKey() { ItemTag = tag, Modifier = mod, RelevantModifiers = modifiers.GroupBy(m => m.Key).Select(m => m.First()).ToDictionary() };
                if (ModifierValueLookup.TryGetValue(lookupKey, out var value))
                {
                    return value.Item1;
                }
                var calculated = ModifierEstimate(modifiers, tag, flatNbt, mod);
                if (calculated.Value > 0)
                    ModifierValueLookup[lookupKey] = (calculated, DateTime.UtcNow);
                return calculated;
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

        private RankElem ModifierEstimate(IList<KeyValuePair<string, string>> modifiers, string tag, Dictionary<string, string> flatNbt, KeyValuePair<string, string> mod)
        {
            var items = GetItemKeysForModifier(tag, mod);
            var sum = 0L;
            foreach (var item in items)
            {
                if (!Lookups.TryGetValue(item.tag, out var lookup))
                {
                    sum += BazaarPrices.TryGetValue(item.tag, out var price) ? (long)price * item.amount : 0;
                    continue;
                }
                if (IsRune(mod.Key))
                {
                    var fromlevel1 = lookup.Lookup.Where(f => f.Value.Price != 0)
                        .OrderBy(v => (v.Key.Count + 1) * (v.Key.Modifiers.Count == 0 ? 1 : int.Parse(v.Key.Modifiers.First().Value)))
                        .FirstOrDefault().Value?.Price * item.amount ?? 0;
                    var matchingLevel = lookup.Lookup.Where(f => f.Value.Price != 0 && f.Key.Modifiers.First().Value == mod.Value)
                        .Select(f => f.Value.Price).OrderBy(p => p).FirstOrDefault();
                    if (matchingLevel != 0 && matchingLevel < fromlevel1)
                        sum += matchingLevel;
                    else
                        sum += fromlevel1;
                    if (tag.Contains("RUNE") && sum < 500_000)
                    {
                        // do not remove rune levels from runes
                        sum = 0;
                    }
                    continue;
                }
                sum += (lookup.Lookup.Values.OrderBy(v => v.Price).FirstOrDefault(f => f.Price != 0)?.Price ?? 0) * item.amount;
            }

            // early return if we have a value before estimates
            if (sum > 0 || mod.Key == null)
                return new RankElem(mod, sum);

            if (Converters.TryGetValue(mod.Key, out var converter))
            {
                return converter(new()
                {
                    ItemTag = tag,
                    Modifier = mod,
                    RelevantModifiers = modifiers
                });
            }
            if (KillKeys.Contains(mod.Key))
            {
                sum += 300_000 * (int)Math.Pow(2, int.Parse(mod.Value)) + 300_000;
            }

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
            return new RankElem(mod, sum)
            {
                IsEstimate = true
            };
        }

        private long GetGemstoneSlotWorth(IList<KeyValuePair<string, string>> modifiers, string tag, KeyValuePair<string, string> mod)
        {
            var valueAdd = 0L;
            var present = mod.Value.Split(',').ToList();
            var costs = itemService.GetSlotCostSync(tag, new(), present);
            foreach (var cost in costs.Item1)
            {
                if (cost.Type.ToLower() == "item")
                    valueAdd += GetPriceForItem(cost.ItemId) * cost.Amount ?? 1;
                else
                    valueAdd += cost.Coins;
            }
            if (costs.unavailable.Count() > 0 && modifiers is List<KeyValuePair<string, string>> modList)
            {
                modList.RemoveAll(m => m.Key == "unlocked_slots");
                var remaining = present.Except(costs.unavailable);
                if (remaining.Count() > 0)
                    modList.Add(new(mod.Key, string.Join(",", remaining.OrderBy(s => s))));
            }

            return valueAdd;
        }

        private long GetCandyPrice(string tag, Dictionary<string, string> flatNbt)
        {
            return Math.Max(GetPriceForItem(tag) / 6, 10_000_000) * (flatNbt == null ? 2 : flatNbt.ContainsKey("skin") ? 10 : 1);// for skined pets important
        }

        private int GetExpValue(string tag, KeyValuePair<string, string> mod)
        {
            (var maxExp, var second) = tag == "PET_GOLDEN_DRAGON" ? ("7", GoldenDragonMaxExp) : ("6", PetExpMaxlevel);
            var lvl1Key = new AuctionKey(new(), ItemReferences.Reforge.Any, EmptyPetModifiers.ToList(), Tier.LEGENDARY, 1);
            var maxLevel = new AuctionKey(new(), ItemReferences.Reforge.Any, new List<KeyValuePair<string, string>>() { new("exp", maxExp) }, Tier.LEGENDARY, 1);
            if (Lookups.TryGetValue(tag, out var lookup) && lookup.Lookup.TryGetValue(lvl1Key, out var baseLevel)
                && lookup.Lookup.TryGetValue(maxLevel, out var maxLevelValue) && maxLevelValue.Price > 100)
            {
                var precise = Math.Max((maxLevelValue.Price - baseLevel.Price) / int.Parse(maxExp), 200_000);
                return (int)(precise * Math.Max(float.Parse(mod.Value, CultureInfo.InvariantCulture), 0.5));
            }
            var factor = Math.Max(GetPriceForItem(tag) / 6, 10_000_000);
            var value = (int)(factor * (float.Parse(mod.Value) + 1));
            return value;
        }

        private class ModifierMetadata
        {
            public string ItemTag;
            public IList<KeyValuePair<string, string>> RelevantModifiers;
            public KeyValuePair<string, string> Modifier;
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
            if (auction.ItemName == "Enchanted Book Bundle")
                ench = new(); // book bundles themselfs are equivilient to the enchants
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
            if (IsRune(auction.Tag))
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
                if (exp >= 1_000_000 * expMulti && exp <= 2_500_000 * expMulti)
                    return new KeyValuePair<string, string>(s.Key, "0.3");
                else if (exp > 2_500_000 * expMulti && exp < PetExpMaxlevel * expMulti / 6)
                    return new KeyValuePair<string, string>(s.Key, "0.6");
                if (tag == "PET_GOLDEN_DRAGON")
                    return NormalizeNumberTo(s, GoldenDragonMaxExp / 7, 7);
                else
                    return NormalizeNumberTo(s, PetExpMaxlevel / 6, 6);
            }
            var generalNormalizations = NormalizeGeneral(s, IsMidas(tag),
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
                    > 10 => new("hotpc", "0.1"),
                    10 => new("hotpc", "0"),
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
                    "BROWN_BANDANA" => "BROWN_BANDANA",
                    "PET_ITEM_COMBAT_SKILL_BOOST_EPIC" => "COMBAT_SKILL_BOOST_EPIC",
                    "PET_ITEM_FISHING_SKILL_BOOST_EPIC" => "FISHING_SKILL_BOOST_EPIC",
                    "PET_ITEM_FORAGING_SKILL_BOOST_EPIC" => "FORAGING_SKILL_BOOST_EPIC",
                    "ALL_SKILLS_SUPER_BOOST" => "ALL_SKILLS_SUPER_BOOST",
                    "PET_ITEM_EXP_SHARE" => "EXP_SHARE",
                    "REINFORCED_SCALES" => "REINFORCED_SCALES",
                    "SERRATED_CLAWS" => "SERRATED_CLAWS",
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
                if (val < 50)
                    return Ignore;
                if (val == 50) // max level found
                    return new KeyValuePair<string, string>("baseStatBoost", "50");
                if (val > 50)
                    return new KeyValuePair<string, string>("baseStatBoost", ">50");
            }
            if (s.Key == "item_tier")
            {
                if (s.Value == "10" && tag == "SKELETON_MASTER_CHESTPLATE")
                    return new KeyValuePair<string, string>("item_tier", "10");
                return Ignore;
            }
            if (IsRune(s.Key) && !IncludeKeys.Contains(s.Key) && !IsRune(tag))
            {
                return Ignore;
            }
            if (s.Key == "color" && (flattenedNbt.ContainsKey("dye_item") || itemService.GetDefaultColorAndCategory(tag).color == s.Value.Replace(':', ',')))
                return Ignore;
            return s;
        }

        private static bool IsMidas(string tag)
        {
            return tag != null && MidasTags.Contains(tag);
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
            if (s.Key == "raider_kills")
                return NormalizeGroupNumber(s, 10_000, 17500);
            if (KillKeys.Contains(s.Key))
                return NormalizeNumberTo(s, 10_000);
            if (s.Key == "yogsKilled")
                return NormalizeNumberTo(s, 5_000, 2);
            if (s.Key == "thunder_charge")
                return NormalizeGroupNumber(s, 150_000, 1_000_000, 2_500_000, 5_000_000);
            if (s.Key == "mined_crops")
                return NormalizeNumberTo(s, 500_000_000);
            if (s.Key == "blocksBroken")
                return NormalizeNumberTo(s, 20_000, 2);
            if (s.Key == "collected_coins")
                return NormalizeGroupNumber(s, 100_000_000, 1_000_000_000);
            if (s.Key == "bass_weight")
                return NormalizeGroupNumber(s, 2, 5, 10, 20, 50, 100, 200);
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
                        && (baseKey.Reforge == k.Reforge || baseKey.Reforge == ItemReferences.Reforge.Any)
                            && baseKey.Enchants
                    .All(e => k.Enchants != null && k.Enchants.Any(ek => e.Type == ek.Type && ek.Lvl == e.Lvl)) && k.Tier == baseKey.Tier))
                {
                    if (item.Modifiers.Any(m => m.Value == TierBoostShorthand))
                        continue;
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
            var value = GetNumeric(s);
            if (s.Key != "exp" && value < groupingSize / 4)
                return Ignore;
            var group = value / groupingSize;
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
            if (val < 800)
                return Ignore;
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

        public void TestNewAuction(SaveAuction auction, bool triggerEvents = true, bool fastMode = false)
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
            var basekey = DetailedKeyFromSaveAuction(auction, fastMode);

            for (int i = 0; i < 4; i++)
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
                    if (triggerEvents && i == 4 && !fastMode)
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
                    using var tryFind = !triggerEvents ? null : activitySource?.StartActivity("TryFind", ActivityKind.Internal);
                    long extraValue = GetExtraValue(auction, key) - itemGroupTag.Item2;
                    if (FindFlip(auction, lbinPrice, medPrice, bucket, key, lookup, basekey, extraValue, props =>
                    {
                        props["breakdown"] = JsonConvert.SerializeObject(basekey.ValueBreakdown);
                    }, fastMode))
                        shouldTryToFindClosest = false; // found a snipe, no need to check other lower value buckets
                }
                else
                    return;
            }
            if (!triggerEvents || fastMode)
                return; // no need to check for closest, just storing

            using var alternateFinders = !triggerEvents ? null : activitySource?.StartActivity("AlternateFinders", ActivityKind.Internal);
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
                var riskyFind = System.Threading.Tasks.Task.Run(() =>
                {
                    using var risky = !triggerEvents ? null : activitySource?.StartActivity("Risky", ActivityKind.Internal);
                    try
                    {
                        TryFindClosestRisky(auction, lookup, basekey, ref medPrice);
                    }
                    catch (System.Exception e)
                    {
                        risky?.SetTag("error", "true");
                        risky?.Log(e.ToString());
                    }
                });
                if (MIN_TARGET == 0)
                    riskyFind.Wait(); // test
            }

            CraftCostFinder(auction, itemGroupTag, lookup, medPrice, basekey);
            activity.Log($"BaseKey value {JsonConvert.SerializeObject(basekey.ValueBreakdown)}");
        }


        private static Dictionary<string, string[]> SharedAttributeGroup = new()
        {
            {"VANQUISHED_GLOWSTONE_GAUNTLET", ["GLOWSTONE_GAUNTLET"]},
            {"VANQUISHED_GHAST_CLOAK",[ "GHAST_CLOAK"]},
            {"VANQUISHED_BLAZE_BELT", ["BLAZE_BELT"]},
            {"VANQUISHED_MAGMA_NECKLACE", ["MAGMA_NECKLACE"]},
            {"VANQUISHED_INFERNO_ROD", ["INFERNO_ROD"]},
            { "HELLFIRE_ROD",["MAGMA_ROD", "INFERNO_ROD"]},
            {"INFERNO_ROD", ["MAGMA_ROD"]}
        };

        private void CraftCostFinder(SaveAuction auction, (string tag, long costSubstract) itemGroupTag, PriceLookup lookup, double medPrice, KeyWithValueBreakdown basekey)
        {
            var componentGuess = basekey.ValueBreakdown.Sum(c => c.IsEstimate ? GetValueEstimate(c) : c.Value);
            if (componentGuess <= medPrice / 8) // no need to check if sum is too low
            {
                return;
            }
            if (IsRune(itemGroupTag.tag))
            {
                return; // runes are not crafted so makes not sense to report them
            }
            var valueLookup = basekey.ValueBreakdown.ToDictionary(v =>
            {
                if (v.Modifier.Key != default)
                    return v.Modifier.Key;
                if (v.Reforge != default)
                    return v.Reforge.ToString();
                return v.Enchant.Type.ToString();
            }, c => c.IsEstimate ? GetValueEstimate(c) : AdjustFixedValue(c));
            var cleanCost = GetCleanItemPrice(itemGroupTag.tag, basekey, lookup);
            if (BreakDownIncludesItem(itemGroupTag, basekey))
            {
                cleanCost = 0;
            }
            cleanCost = Math.Min(cleanCost, auction.Tag switch
            {
                "STARRED_MIDAS_STAFF" => 200_000_000,
                "MIDAS_STAFF" => 100_000_000,
                "MIDAS_SWORD" => 80_000_000,
                "STARRED_MIDAS_SWORD" => 150_000_000,
                _ => cleanCost
            });
            var componentSum = valueLookup.Select(v => (long)(v.Key switch
            {
                "skin" => auction.Tag.StartsWith("PET") ? 0.5 : 0.4,
                "ultimate_fatal_tempo" => 0.65,
                "rarity_upgrades" => 0.5,
                "upgrade_level" => 0.84,
                "talisman_enrichment" => 0.10,
                var s when IsRune(s) => 0.55,
                var s when Constants.AttributeKeys.Contains(s) => 0.75,
                _ => 0.85
            } * v.Value)).Sum();
            if (cleanCost == componentGuess)
            {
                componentSum = 0; // unique runes shouldn't be counted twice
            }
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

            static bool BreakDownIncludesItem((string tag, long costSubstract) itemGroupTag, KeyWithValueBreakdown basekey)
            {
                // breakdown already includes cheapest item (rune probably)
                return basekey.ValueBreakdown.Count == 1 && basekey.Key.Modifiers.FirstOrDefault(m => m.Key == itemGroupTag.tag).Key != default;
            }

            long GetValueEstimate(RankElem c)
            {
                if (c.Modifier.Key == "candyUsed")
                    return 0;

                if (Constants.AttributeKeys.Contains(c.Modifier.Key))
                {
                    var key = VirtualAttributeKey(c.Modifier);
                    //var virtualList = lookup.Lookup.Where(e => e.Key.Modifiers.Any(m => m.Key == "virtual")).ToList();
                    if (!lookup.Lookup.TryGetValue(key, out var references) || references.Price == 0)
                        return 0;
                    if (Lookups.TryGetValue("ATTRIBUTE_SHARD", out var shardLookup) && shardLookup.Lookup.TryGetValue(key, out var shardBucket))
                    {
                        if (shardBucket.References.Count > 5 && shardBucket.Price > 0 && shardBucket.Price < references.Price)
                            references = shardBucket; // use shard price if it is cheaper as it can be used on all items
                    }
                    if (SharedAttributeGroup.TryGetValue(auction.Tag, out var sharedGroup))
                    {
                        foreach (var item in sharedGroup)
                        {
                            if (Lookups.TryGetValue(item, out var sharedLookup)
                                && sharedLookup.Lookup.TryGetValue(key, out var sharedBucket)
                                && sharedBucket.References.Count > 5 && sharedBucket.Price > 0 && sharedBucket.Price < references.Price)
                            {
                                references = sharedBucket; // use the shared group price if available
                            }
                        }
                    }
                    if (!int.TryParse(c.Modifier.Value, out var level))
                        return 0;
                    return (long)(references.Price * Math.Pow(2, level - 1));
                }
                if (KillKeys.Contains(c.Modifier.Key))
                    return c.Value / 50;
                return c.Value / 20;
            }

            static long AdjustFixedValue(RankElem c)
            {
                if (c.Modifier.Key == PetItemKey)
                    if (c.Modifier.Value == TierBoostShorthand)
                        return -120_000_000; // this negates the upgrade cost of a pet item
                    else
                        return c.Value / 2;
                return c.Value;
            }
        }

        private void CheckCombined(SaveAuction auction, PriceLookup lookup, double lbinPrice, double medPrice, KeyWithValueBreakdown longKey, RankElem topAttrib)
        {
            var topKey = longKey.GetReduced(0);
            var targetVolume = 11;
            if (lookup.Lookup.TryGetValue(topKey, out var topBucket) && topBucket.References.Count >= targetVolume)
            {
                return; // enough references in previous check
            }
            var l = lookup.Lookup;
            var similar = l.Where(e => topAttrib.Modifier.Key != default && !e.Key.Modifiers.Any(m => m.Key == "virtual") || e.Key.Enchants.Contains(topAttrib.Enchant)).ToList();
            if (similar.Count == 1)
            {
                // include all if no match otherwise
                similar = l.ToList();
            }
            var fullKey = GetFullKey(auction);
            var relevant = similar.Where(e => (e.Key.Reforge == topKey.Reforge || topKey.Reforge == ItemReferences.Reforge.Any)
                        && IsHigherValue(auction.Tag, e.Key, fullKey))
                .Select(e => (e, value: Math.Max(e.Value.Volume, 0.5) * Math.Pow(ComparisonValue(e.Key.Enchants, e.Key.Modifiers.ToList(), GetAuctionGroupTag(auction.Tag).tag, null).Sum(s => s.Value), 1.8)))
                .OrderByDescending(e => e.value)
                .ToList();
            if (relevant.Count < 2)
            {
                return; // makes only sense if there is something combined
            }
            // get enough relevant to build a median and try to get highest value (most enchantments and modifiers)
            var combined = relevant.SelectMany(r => r.e.Value.References.Select(ri => (ri, relevancy: r.value * (ri.Day - GetDay() + 12) * Math.Log10(ri.Price + 1))))
                                .OrderByDescending(r => r.relevancy).Select(r => r.ri).Take(targetVolume).ToList();
            if (combined.Count == 0)
            {
                return;
            }
            var lbinBucket = relevant.Select(r => r.e.Value.Lbin).Where(r => r.Price != default).DefaultIfEmpty().MinBy(r => r.Price);
            var virtualBucket = new ReferenceAuctions()
            {
                Lbins = [lbinBucket],
                References = new(combined),
                Price = combined.Count < 4 ? 0 : GetCappedMedian(auction, longKey, combined) * 98 / 100,
                OldestRef = (short)(GetDay() - 2),
                Volatility = 123// mark as risky
            };
            // mark with extra value -3
            var foundAndAbort = FindFlip(auction, lbinPrice, medPrice, virtualBucket, topKey, lookup, longKey, MIN_TARGET == 0 ? 0 : -3, props =>
            {
                var total = 0;
                props.Add("combined", string.Join(",", relevant.TakeWhile(c => (total += c.e.Value.References.Count) < targetVolume)
                    .Select(c => c.e.Key.ToString() + ":" + c.e.Value.References.Count)));
                props.Add("breakdown", JsonConvert.SerializeObject(longKey.ValueBreakdown));
                logger.LogInformation($"Combined {longKey} {auction.Uuid} {virtualBucket.Price} {virtualBucket.Lbin.Price} keys: {string.Join(",", relevant.Select(r => r.e.Key))}");
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
            AuctionKeyWithValue key = GetFullKey(auction);
            var today = GetDay();
            var containing = l.Where(e => e.Value.Price > 0 && e.Value.References.Count > 5
                            && (e.Key.Reforge == key.Reforge || e.Key.Reforge == ItemReferences.Reforge.Any)
                            && e.Value.References.Any(r => r.Day >= today - 2)
                            && IsHigherValue(auction.Tag, e.Key, key))
                        .OrderByDescending(e => e.Value.Price).FirstOrDefault();
            if (containing.Value == default)
                return;
            var extraValue = GetExtraValue(auction, key);
            FindFlip(auction, lbinPrice, medPrice, containing.Value, key, lookup, fullKey, extraValue, props =>
            {
                props.Add("fullKey", key.ToString());
                props.Add("usedKey", containing.Key.ToString());
                props.Add("by", "lowerfullkey");
            });
        }

        private AuctionKeyWithValue GetFullKey(SaveAuction auction)
        {
            (var enchant, var modifiers) = SelectValuable(auction);
            var key = new AuctionKeyWithValue()
            {
                Count = 1,
                Enchants = new(enchant ?? new()),
                Modifiers = new(modifiers ?? new()),
                Tier = auction.Tier,
                Reforge = auction.Reforge
            };
            return key;
        }

        public static readonly HashSet<string> HyperionGroup = new() { "SCYLLA", "VALKYRIE", "NECRON_BLADE", "ASTRAEA" };
        public static readonly HashSet<string> WinterFragmentGroup = new() { "STARRED_GLACIAL_SCYTHE", "STARRED_ICE_SPRAY_WAND", "STARRED_YETI_SWORD" };
        private static readonly HashSet<string> MidasTags = new HashSet<string>
        {
            "MIDAS_STAFF",
            "MIDAS_SWORD",
            "STARRED_MIDAS_STAFF",
            "STARRED_MIDAS_SWORD"
        };
        private static readonly HashSet<string> CombinableStarred = new();
        private static readonly HashSet<string> RuneLookup = new();
        /// <summary>
        /// Remaps item tags into one item if they are easily switchable
        /// </summary>
        /// <param name="auction"></param>
        /// <returns></returns>
        public (string tag, long costSubstract) GetAuctionGroupTag(string itemGroupTag)
        {
            if (HyperionGroup.Contains(itemGroupTag))
                return ("HYPERION", GetPriceForItem("GIANT_FRAGMENT_LASER") * 8); // easily craftable from one into the other
            if (CombinableStarred.Contains(itemGroupTag))
            {
                // technically neds 8 for crafting but looses the value on craft so using 7
                var isFrozen = WinterFragmentGroup.Contains(itemGroupTag);
                var cost = GetPriceForItem(isFrozen ? "WINTER_FRAGMENT" : "LIVID_FRAGMENT") * 7;
                return (itemGroupTag[8..], cost);
            }
            return (itemGroupTag, 0);
        }

        private static bool ShouldIgnoreMostSimilar(SaveAuction auction)
        {
            return IsRune(auction.Tag);
        }

        private void TryFindClosestRisky(SaveAuction auction, PriceLookup l, KeyWithValueBreakdown keyWithBreakdown, ref double medPrice)
        {
            if (IsRune(auction.Tag)) // TODO: compare levels
                return;
            if (auction.Tag == "NEW_YEAR_CAKE")
                return; // can't use closest for years
            if (auction.FlatenedNBT.TryGetValue("exp", out var exp) && double.Parse(exp) < 1234567)
                return; // don't use closest for low exp pets

            // special case for items that have no reference bucket, search using most similar
            var detailedKey = DetailedKeyFromSaveAuction(auction);
            var key = detailedKey.GetReduced(0);
            var closest = FindClosestTo(l.Lookup, key, auction.Tag);
            if (NBT.IsPet(auction.Tag) && closest.Key?.Modifiers != null && !closest.Key.Modifiers.Any(m => m.Key == "exp"))
            {
                logger.LogWarning($"Pet without exp {auction.Uuid}");
                return; // don't use closest for pets without exp
            }
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
            if (closest.Value.Price <= medPrice)
                return;
            var props = new Dictionary<string, string>() { { "closest", closest.Key.ToString() } };
            var missingModifiers = closest.Key.Modifiers.Where(m => !key.Modifiers.Contains(m)).ToList();
            long toSubstract = 0;
            if (closest.Value.StonksHits > 0)
            {
                if (closest.Value.StonksHits > 10)
                    return;
                // risk increases with more hits
                toSubstract += (long)(closest.Value.Price * 0.1 * Math.Pow(1.05, closest.Value.StonksHits));
            }
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
                var fromExp = GetValueDifferenceForExp(auction, closest.Key, l.Lookup);
                if (fromExp != 0)
                {
                    props.Add("fromExp", fromExp.ToString());
                }
                toSubstract -= fromExp;
                if (missingModifiers.Any(m => m.Key == "candyUsed" && m.Value == "1"))
                    toSubstract += (long)(closest.Value.Price * 0.1 + 400_000); // 10% for pet candy
                var killModifier = missingModifiers.FirstOrDefault(m => KillKeys.Contains(m.Key));
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
                var enchVal = GetPriceSumForEnchants(missingEnchants, auction.Tag);
                toSubstract += enchVal;
                props.Add("missingEnchants", string.Join(",", missingEnchants.Select(e => $"{e.Type}_{e.Lvl}")) + $" ({enchVal})");
            }
            var additionalEnchants = key.Enchants.Where(e => !closest.Key.Enchants.Contains(e)).ToList();
            if (additionalEnchants.Count > 0)
            {
                var valEst = GetPriceSumForEnchants(additionalEnchants, auction.Tag) / 2;
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
                if (countDiff < 0)
                    countDiffPrice /= 2; // only half for adding items
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
            props.Add("breakdown", JsonConvert.SerializeObject(detailedKey.ValueBreakdown));
            var cleanPrice = GetCleanItemPrice(auction.Tag, detailedKey, l);
            props.Add("cleanPrice", cleanPrice.ToString());
            var modifierValue = (detailedKey.ValueBreakdown.Where(v => v?.Modifier.Key != "candyUsed").Sum(v => v.Value) + cleanPrice) * 1.1;
            targetPrice = Math.Min(targetPrice, (long)modifierValue);
            closest.Value.StonksHits++;
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

        private long GetPriceSumForEnchants(IEnumerable<Models.Enchant> missingEnchants, string itemTag)
        {
            long toSubstract = 0;
            foreach (var item in missingEnchants)
            {
                toSubstract += mapper.EnchantValue(new Core.Enchantment(item.Type, item.Lvl), null, BazaarPrices, itemTag);
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
                    var prices = itemLookup.Lookup.GetValueOrDefault(itemLookup.CleanKey ?? itemLookup.Lookup.FirstOrDefault(k => k.Value.Price > 0).Key);
                    const int RemovalCost = 50_000;
                    extraValue += (prices.Lbin.Price == 0 ? prices.Price : Math.Min(prices.Price, prices.Lbin.Price)) * 97 / 100 - RemovalCost;
                }
            }
            long gemValue = GetGemValue(auction, key);
            extraValue += gemValue;

            if (NBT.IsPet(auction.Tag) && key.Modifiers.Any(m => m.Value == TierBoostShorthand))
            {
                extraValue -= 110_000_000;
            }

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
                var gemkey = mapper.GetItemKeyForGem(item, auction.FlatenedNBT);
                if (item.Value == "PERFECT")
                    if (BazaarPrices.TryGetValue(gemkey, out var gemLookup) && !key.Modifiers.Any(m => m.Key == item.Key))
                        gemValue += (long)gemLookup - 500_000;
                if (item.Value == "FLAWLESS")
                    if (BazaarPrices.TryGetValue(gemkey, out var gemLookup) && !key.Modifiers.Any(m => m.Key == item.Key))
                        gemValue += (long)gemLookup - 100_000;
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
                              Action<Dictionary<string, string>> addProps = null,
                              bool fastMode = false)
        {
            var l = lookup.Lookup;
            var expValue = GetValueDifferenceForExp(auction, key, l);
            var volume = bucket.Volume;
            var medianPrice = bucket.Price + extraValue;
            var groupTag = GetAuctionGroupTag(auction.Tag);
            var foundSnipe = false;
            if ((bucket.Lbin.Price > lbinPrice || bucket.Price == 0) && (MaxMedianPriceForSnipe(bucket, breakdown) > lbinPrice)
                && (!fastMode || bucket.Volume > 5)
               )
            {
                foundSnipe = PotentialSnipe(auction, groupTag, lbinPrice, bucket, key, l, extraValue, breakdown);
            }
            if (medianPrice > minMedPrice && BucketHasEnoughReferencesForPrice(bucket, lookup))
            {
                long adjustedMedianPrice = bucket.Price;
                if (key.Count > 1)
                    adjustedMedianPrice = CheckHigherValueKeyForLowerPrice(bucket, key, l, medianPrice);
                Activity.Current.Log($"Bucket has enough references {bucket.References.Count} and medianPrice > minMedPrice {medianPrice} > {minMedPrice} adjusted {adjustedMedianPrice} {extraValue} {expValue}");
                if (adjustedMedianPrice + extraValue < minMedPrice)
                {
                    LogNonFlip(auction, bucket, key, extraValue, volume, medianPrice, $"Adjusted median {adjustedMedianPrice} lower than min price {minMedPrice} {extraValue}");
                    return false;
                }
                var referenceAuctionId = bucket.References.LastOrDefault().AuctionId;
                var props = CreateReference(referenceAuctionId, key, extraValue, bucket);
                if (key.ValueSubstract != 0)
                {
                    props["valuedropped"] = key.ValueSubstract.ToString();
                }
                if (expValue != 0)
                {
                    props["expvalue"] = expValue.ToString();
                }
                addProps?.Invoke(props);
                if (lookup.Volume < 1 && lookup.Volume > 0 && bucket.Price > 100_000_000 && bucket.Lbin.Price > 0 && bucket.Price > bucket.Lbin.Price)
                {
                    props.Add("lbin", JsonConvert.SerializeObject(bucket.Lbin));
                    adjustedMedianPrice = Math.Min(adjustedMedianPrice, bucket.Lbin.Price);
                }
                var keyMissing = key.ValueSubstract > 5_000_000 ? (key.ValueSubstract - extraValue - 1_000_000) / 2 + MoreIfExpensive(bucket.Price, key) : 0;
                if (keyMissing > 0 && lookup.CleanPricePerTier.TryGetValue(key.Tier, out var cleanPricePerTier))
                {
                    var basePrice = Math.Min(auction.StartingBid, cleanPricePerTier * 2);
                    // on very cheap items expensive modifiers don't add much value
                    var adjusted = Math.Min(keyMissing, Math.Max(basePrice * 3 - 100_000, basePrice));
                    if (adjusted > 0)
                        keyMissing = adjusted;
                }
                props.Add("keyMissing", keyMissing.ToString());
                FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER_MEDIAN, adjustedMedianPrice + extraValue + expValue + keyMissing, props);
            }
            if (medianPrice - auction.StartingBid < 2_500_000 && bucket.RiskyEstimate > minMedPrice
                && (bucket.Lbin.AuctionId == default || bucket.Lbin.Price * 1.04 > lbinPrice))
            {
                var referenceAuctionId = bucket.References.LastOrDefault().AuctionId;
                var props = CreateReference(referenceAuctionId, key, extraValue, bucket);
                addProps?.Invoke(props);
                props.Add("riskyEst", bucket.RiskyEstimate.ToString());
                var target = bucket.RiskyEstimate + extraValue + expValue;
                if (bucket.Lbin.Price != 0)
                    target = (long)Math.Min(target, bucket.Lbin.Price * 1.05);
                if (bucket.Price != 0)
                    target = (long)Math.Min(target, bucket.Price * 1.10 + 1_000_000);
                if ((GetDay() - bucket.OldestRef) > 10 || bucket.DeduplicatedReferenceCount < 8)
                    target = target * 11 / 12;
                FoundAFlip(auction, bucket, LowPricedAuction.FinderType.STONKS, target, props);
            }
            else
            {
                Activity.Current.Log($"Bucket has too few references {bucket.References.Count} or medianPrice > minMedPrice {medianPrice} > {minMedPrice}");
                LogNonFlip(auction, bucket, key, extraValue, volume, medianPrice, $"Median {medianPrice} lower than min price {minMedPrice} {bucket.References.Count}");
            }
            return foundSnipe;

            long MoreIfExpensive(long Median, AuctionKeyWithValue key)
            {
                // attributes keep more of their value on expensive items
                if (Median < 500_000_000)
                    return 0;
                return (key.ValueSubstract - extraValue - 2_000_000) / 4;
            }
        }
        void LogNonFlip(SaveAuction auction, ReferenceAuctions bucket, AuctionKey key, long extraValue, float volume, long medianPrice, string v = null)
        {
            if (volume == 0 || bucket.Lbin.Price == 0 || bucket.Price == 0 || bucket.Price > MIN_TARGET)
                Logs.Enqueue(new LogEntry()
                {
                    Key = key,
                    ExtraContext = $"+{extraValue} {v}",
                    LBin = bucket.Lbin.Price,
                    Median = medianPrice,
                    Uuid = auction.Uuid,
                    Volume = bucket.Volume
                });
            if (Logs.Count > 2000)
                PrintLogQueue();
        }

        private static long GetValueDifferenceForExp(SaveAuction auction, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l)
        {
            // determine extra expvalue
            if (auction.FlatenedNBT.TryGetValue("exp", out var expString))
            {
                var maxExp = auction.Tag == "PET_GOLDEN_DRAGON" ? ("7", GoldenDragonMaxExp) : ("6", PetExpMaxlevel);
                var exp = Math.Min((long)double.Parse(expString), maxExp.Item2);
                var lvl1Key = new AuctionKey(new(), ItemReferences.Reforge.Any, EmptyPetModifiers.ToList(), Tier.LEGENDARY, 1);
                var lvl100Key = new AuctionKey(new(), ItemReferences.Reforge.Any, new List<KeyValuePair<string, string>>() { new("exp", maxExp.Item1) }, Tier.LEGENDARY, 1);
                if (l.TryGetValue(lvl1Key, out var lvl1Bucket) && l.TryGetValue(lvl100Key, out var lvl100Bucket))
                {
                    var lvl1Price = lvl1Bucket.Price;
                    var lvl100Price = lvl100Bucket.Price;
                    var accountedFor = double.Parse(key.Modifiers.Where(m => m.Key == "exp").Select(v => v.Value).FirstOrDefault("0"));
                    if (auction.Tier == Tier.EPIC)
                        accountedFor += 1;
                    var accountedMiddle = accountedFor + Math.Min(0.5, accountedFor / 2);
                    var accountedExp = maxExp.Item2 / 7 * accountedMiddle;
                    var perExp = (double)((lvl100Price - lvl1Price) / (double)(maxExp.Item2 - 1));
                    var expValue = (long)(perExp * (exp - 1 - accountedExp));
                    if (exp > 11_600_000 && expValue > 0) // only block upwards price changes
                        return 0; // bad effect with so many exp
                    if (exp > 4_000_000 && expValue > 0)
                        return expValue / 2; // graceful reduce
                    if (expValue < 0 && accountedFor <= 1)
                    {
                        var matchingKey = new AuctionKey(new(), ItemReferences.Reforge.Any, EmptyPetModifiers.ToList(), auction.Tier, 1);
                        var matchingTier = l.GetValueOrDefault(matchingKey)?.Price ?? 0;
                        return Math.Max(expValue, -matchingTier / 4); // reduce target by max 25% the starting bid
                    }
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
            if (key.Count > 1)
            {
                var lowerCountKey = new AuctionKey(key)
                {
                    Count = 1
                };
                if (l.TryGetValue(lowerCountKey, out ReferenceAuctions lowerCountBucket))
                {
                    if (lowerCountBucket.Price != 0)
                        return Math.Min(bucket.Price, lowerCountBucket.Price * key.Count);
                }
            }
            return bucket.Price;
        }

        private static bool BucketHasEnoughReferencesForPrice(ReferenceAuctions bucket, PriceLookup lookup)
        {
            // high value items need more volume to pop up
            return bucket.Price < 280_000_000 || bucket.References.Count > 5 || bucket.Volume > lookup.Volume / 3;
        }

        public void UpdateBazaar(dev.BazaarPull bazaar)
        {
            var today = GetDay(bazaar.Timestamp);
            foreach (var item in bazaar.Products.OrderBy(p => p.Id))
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

                    if (item.ProductId.StartsWith("ENCHANTMENT"))
                    {
                        itemPrice = MakePriceAtMost40PercentLowerthanLowerLevel(item.ProductId, (long)itemPrice);
                    }
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
                UpdateMedian(bucket, default);
                CapBucketSize(bucket);

                // make sure higher enchants are higher value
                if (item.ProductId.StartsWith("ENCHANTMENT"))
                {
                    var cheapestBuy = item.BuySummery.OrderBy(s => s.PricePerUnit).FirstOrDefault()?.PricePerUnit;
                    MakePriceAtLeast90PercentHigherthanLowerLevel(item, bucket, cheapestBuy);
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
            logger.LogInformation($"Updated bazaar {Lookups.Count} items {bazaar.Timestamp}");

            void MakePriceAtLeast90PercentHigherthanLowerLevel(dev.ProductInfo item, ReferenceAuctions refernces, double? cheapestBuy)
            {
                var currentLevel = int.Parse(item.ProductId.Split("_").Last());
                if (currentLevel <= 1 || item.ProductId.Contains("_MANA_") && currentLevel <= 5)
                { // mana enchants can drop up to lvl 5 and is worth almost the same at all levels
                    return;
                }
                var lowerLevelId = item.ProductId.Replace($"_{currentLevel}", $"_{currentLevel - 1}");
                if (BazaarPrices.TryGetValue(lowerLevelId, out var lowerValue))
                {
                    refernces.Price = (long)Math.Min(Math.Max(refernces.Price, lowerValue * 1.9), cheapestBuy ?? long.MaxValue);
                }
            }
            long MakePriceAtMost40PercentLowerthanLowerLevel(string id, long estimate)
            {
                var currentLevel = int.Parse(id.Split("_").Last());
                if (currentLevel <= 1 || id.Contains("_MANA_") && currentLevel <= 5)
                { // mana enchants can drop up to lvl 5 and is worth almost the same at all levels
                    return estimate;
                }
                var higherLevelId = id.Replace($"_{currentLevel}", $"_{currentLevel + 1}");
                if (BazaarPrices.TryGetValue(higherLevelId, out var higherValue))
                {
                    return (long)Math.Min(estimate, higherValue * 0.6);
                }
                return estimate;
            }

            static bool NotEnoughTimePassed(dev.BazaarPull bazaar, ReferenceAuctions bucket)
            {
                var lastAdded = new DateTime(bucket.References.Last().AuctionId);
                return lastAdded.AddMinutes(10) > bazaar.Timestamp;
            }
        }

        private bool PotentialSnipe(SaveAuction auction, (string tag, long costSubstract) groupTag, double lbinPrice, ReferenceAuctions bucket, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l, long extraValue, KeyWithValueBreakdown breakdown)
        {
            var lowestHigherBin = GetLbinCap(groupTag.tag, l, breakdown);
            var higherValueLowerBin = bucket.Lbin.Price;
            if (lowestHigherBin.AuctionId != default)
                if (lowestHigherBin.Price < lbinPrice)
                    return false;
                else if (higherValueLowerBin > lowestHigherBin.Price || higherValueLowerBin == 0)
                    higherValueLowerBin = lowestHigherBin.Price;

            if (IsStacksize1Cheaper(lbinPrice, key, l))
            {
                Activity.Current.Log("Stacksize 1 is cheaper");
                return false;
            }
            var targetPrice = Math.Min(higherValueLowerBin - 1, MaxMedianPriceForSnipe(bucket, breakdown)) + extraValue - MIN_TARGET / 200;
            if (bucket.Price != 0)
                targetPrice = Math.Min(targetPrice, bucket.Price * 2);
            if (targetPrice < auction.StartingBid * 1.03)
                return false;
            var percentile = long.MaxValue;

            var props = CreateReference(bucket.Lbin.AuctionId, key, extraValue, bucket);
            props["mVal"] = bucket.Price.ToString();
            props["hvlbin"] = higherValueLowerBin.ToString();

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
                var lowestLbin = lowestHigherBin.Price;
                if (lowestHigherBin.AuctionId == default)
                    lowestLbin = long.MaxValue;
                var referencePrice = bucket.Price;
                if (bucket.References.Count < 5)
                {
                    // all key modifiers and enchants need to be in the reference bucket or higher
                    var higherValueKeys = l.Where(x => IsHigherValue(groupTag.tag, key, x.Key)).ToList();
                    lowestLbin = higherValueKeys
                                .Where(x => x.Value.Lbin.Price > 0 && x.Value.Lbin.Price < bucket.Lbin.Price)
                                .Select(x => x.Value.Lbin.Price).DefaultIfEmpty(long.MaxValue).Min();
                    // 25th percentile of all references
                    var allReferences = higherValueKeys.SelectMany(x => x.Value.References.Select(r => r.Price / (x.Key.Count == 0 ? 1 : x.Key.Count))).ToList();
                    referencePrice = allReferences
                                    .OrderBy(p => p).Skip(allReferences.Count / 4)
                                    .DefaultIfEmpty(targetPrice / 2).Min();

                    if (bucket.Price == 0 && bucket.References.Count > 2 && higherValueKeys.Count <= 2) // manip indicator
                    {
                        percentile /= 5;
                    }
                    else if (bucket.References.Count < 4 && allReferences.Count < 5)
                    {
                        percentile = Math.Min(percentile, referencePrice / 2);
                        if (allReferences.Count == 0)
                            percentile /= 2;
                    }
                    else if (bucket.References.Count <= 3)
                        percentile = percentile / 3; // not enough statistical evidence
                }
                percentile = Math.Min(percentile, referencePrice);
                if (bucket.Price == 0 && bucket.Lbin.Seller == GetSellerId(auction))
                {
                    props["sellerMatch"] = percentile.ToString();
                    // seller matching is sus 
                    percentile /= 2;
                }
                percentile = Math.Min(percentile, lowestLbin);
                if (lowestLbin > 10_000_000_000)
                {
                    Activity.Current.Log($"Reduced because no higher value lbin");
                    percentile = Math.Min(percentile, Math.Min(targetPrice * (60 + (int)(bucket.Volume * 5)) / 100, (long)(referencePrice * 1.2)));
                    props["noHigherLbin"] = percentile.ToString();
                }
                if (percentile < lbinPrice)
                    return false; // to low already don't waste time
                var reduced = CapAtCraftCost(groupTag.tag, percentile, breakdown, 0);
                if (reduced > 0)
                {
                    if (percentile != reduced)
                        if (lowestLbin > 10_000_000_000 && higherValueLowerBin > 10_000_000_000)
                            reduced = reduced * 80 / 100; // 80% for no higher lbin
                        else
                            reduced = reduced * 21 / 20; // 5% extra for snipe
                    percentile = Math.Min(reduced, percentile);
                    Activity.Current.Log($"Reduced to craft cost {reduced}");
                    props["craftCost"] = reduced.ToString();
                }
                Activity.Current.Log($"No references, checking all lbins {percentile} {lowestLbin} {referencePrice}");
                props["referencePrice"] = referencePrice.ToString();
                props["lowestLbin"] = lowestLbin.ToString();
            }
            else
            {
                CapHighValue(groupTag, bucket, key, breakdown, higherValueLowerBin, ref targetPrice, ref percentile, props);
            }
            props["percentile"] = percentile.ToString();
            targetPrice = Math.Min(targetPrice, percentile);
            return FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER, targetPrice, props);
        }

        private void CapHighValue((string tag, long costSubstract) groupTag, ReferenceAuctions bucket, AuctionKey key, KeyWithValueBreakdown breakdown, long higherValueLowerBin, ref long targetPrice, ref long percentile, Dictionary<string, string> props)
        {
            long capped = 0;
            if ((craftCostService?.TryGetCost(groupTag.tag, out var craftCost) ?? false) || key.Modifiers.Count > 0 || key.Enchants.Count > 0)
                capped = CapAtCraftCost(groupTag.tag, higherValueLowerBin, breakdown, 0);
            else
            {
                targetPrice = Math.Min(higherValueLowerBin * 99 / 100, bucket.Price * 4 / 3 + 1_000_000); // pull target up for non craftable clean
                if (bucket.References.Count < WorkingSize && bucket.References.All(r => r.Day >= GetDay() - 1)) // no full context window (~80 sales) indicates new item that is probably dorpping in price
                    targetPrice = Math.Min(targetPrice, bucket.Price); // limit at median (which may also still drop)
            }
            if (capped > 0)
            {
                percentile = Math.Min(percentile, capped * 12 / 11) + 500_000; // 500k extra since this is high volume
                Activity.Current.Log($"Capped at craft cost {capped}");
                props["breakdown"] = JsonConvert.SerializeObject(breakdown.ValueBreakdown);
                props["craftCost"] = capped.ToString();
            }
            else
                props["nocapped"] = capped.ToString();
        }

        private bool IsHigherValue(string tag, AuctionKey baseKey, AuctionKey toCheck)
        {
            return baseKey.Tier <= toCheck.Tier
                    && (toCheck.Tier != Tier.LEGENDARY || tag != "PET_SPIRIT")
                    && baseKey.Count <= toCheck.Count
                    && baseKey.Modifiers.All(m => toCheck.Modifiers.Any(other => other.Key == m.Key
                                            && (other.Value == m.Value ||
                                                float.TryParse(other.Value, out var otherVal)
                                            && (baseKey.Modifiers.FirstOrDefault().Key != "new_years_cake" || !ImportantCakeYears.Contains(other.Value))
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
            if (key.Count <= 1)
            {
                return false;
            }
            var lowerCountKey = new AuctionKey(key)
            {
                Count = 1
            };
            if (l.TryGetValue(lowerCountKey, out ReferenceAuctions lowerCountBucket))
            {
                if (lowerCountBucket.Price != 0 && lowerCountBucket.Price * key.Count < lbinPrice)
                    return true;
            }
            return false;
        }

        private static void AddMedianSample(IEnumerable<ReferencePrice> bucket, Dictionary<string, string> props)
        {
            var references = bucket.Reverse().Take(5).ToArray();
            if (references.Length > 0)
            {
                var sb = new System.Text.StringBuilder(180);
                for (int i = 0; i < references.Length; i++)
                {
                    if (i > 0) sb.Append(',');
                    sb.Append(references[i].AuctionId);
                }
                props["med"] = sb.ToString();
            }
        }

        private static long MaxMedianPriceForSnipe(ReferenceAuctions bucket, KeyWithValueBreakdown breakdown)
        {
            var price = bucket.RiskyEstimate == 0 ? bucket.Price : Math.Min(bucket.RiskyEstimate, bucket.Price * 11 / 10 + 1_000_000);
            var reducedCraftValue = breakdown.ValueBreakdown.Sum(v => v.Value) * 0.7;
            if (price == 0)
                return long.MaxValue; // disabled with 0 volume
            var fromPrice = price * 21 / 20;
            if (price < 15_000_000)
                fromPrice = price * 13 / 10;
            else if (price < 100_000_000)
                fromPrice = price * 14 / 12;
            if (bucket.Volume > 10 && bucket.DeduplicatedReferenceCount > 32)
                fromPrice = fromPrice * 12 / 10; // stable unbiased price, allow higher snipes
            return Math.Max((long)reducedCraftValue, fromPrice);
        }

        public void PrintLogQueue()
        {
            while (Logs.TryDequeue(out LogEntry result))
            {
                var finderName = result.Finder == LowPricedAuction.FinderType.UNKOWN ? "NF" : result.Finder.ToString();
                logger.LogInformation($"Info: {finderName} {result.Uuid} m:{result.Median} \t{result.LBin} {result.Volume} {result.Key}{result.ExtraContext}");
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
            {
                LogNonFlip(auction, bucket, defaultKey, 0, bucket.Volume, targetPrice, "Target price too low " + targetPrice);
                return false; // to low
            }
            var refAge = (GetDay() - bucket.OldestRef);
            if (bucket.OldestRef != 0 && (refAge > 60 && IsNotClean(auction) || State < SniperState.FullyLoaded && refAge > 10))
            {
                Activity.Current.Log($"References too old {refAge} {State}");
                LogNonFlip(auction, bucket, defaultKey, 0, bucket.Volume, targetPrice, $"References too old for {State} ({refAge})");
                return false; // too old
            }
            AddMedianSample(bucket.References, props);
            props["refAge"] = refAge.ToString();
            props["server"] = ServerDnsName;
            props["refCount"] = bucket.DeduplicatedReferenceCount.ToString();
            props["oldRef"] = (GetDay() - (bucket.References?.Select(r => r.Day).FirstOrDefault(GetDay()) ?? GetDay())).ToString();
            props["volat"] = bucket.Volatility.ToString();
            props["minToSell"] = (bucket.TimeToSell * (refAge / 10 + 1)).ToString();

            if (type == LowPricedAuction.FinderType.SNIPER_MEDIAN && bucket.HitsSinceCalculating < 10
                && IsProbablyNotBait(auction, targetPrice))
            {
                if (bucket.HitsSinceCalculating > bucket.Volume / 10)
                    targetPrice = (long)(targetPrice / Math.Pow(1.05, bucket.HitsSinceCalculating));
                props["hits"] = bucket.HitsSinceCalculating.ToString();
                bucket.HitsSinceCalculating++;
            }
            using var found = activitySource?.StartActivity("FoundFlip", ActivityKind.Internal);
            found?.AddTag("uuid", auction.Uuid);
            found.Log($"Found flip {auction.Uuid} {targetPrice} {type} {bucket.Volume}");

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
                ExtraContext = props.GetValueOrDefault("key") ?? (props.GetValueOrDefault("breakdown") + "+" + props.GetValueOrDefault("cleanCost")),
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

        private static bool IsNotClean(SaveAuction auction)
        {
            return auction.Enchantments.Count > 0 || auction.FlatenedNBT.Any(f => !f.Key.Contains("uid"));
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
            UpdateToday();
            AddRunes();
        }


        private void AddRunes()
        {
            foreach (var item in new string[]{
                "RUNE",
                "RUNEBOOK",
                "RUNE_BITE",
                "RUNE_BLOOD_2",
                "RUNE_CLOUDS",
                "RUNE_COUTURE",
                "RUNE_DRAGON",
                "RUNE_ENCHANT",
                "RUNE_ENDERSNAKE",
                "RUNE_FIERY_BURST",
                "RUNE_FIRE_SPIRAL",
                "RUNE_GEM",
                "RUNE_GOLDEN",
                "RUNE_GRAND_SEARING",
                "RUNE_HEARTS",
                "RUNE_HOT",
                "RUNE_ICE",
                "RUNE_JERRY",
                "RUNE_LAVA",
                "RUNE_LAVATEARS",
                "RUNE_LIGHTNING",
                "RUNE_MAGIC",
                "RUNE_MUSIC",
                "RUNE_RAINBOW",
                "RUNE_REDSTONE",
                "RUNE_SACK",
                "RUNE_SLIMY",
                "RUNE_SMOKEY",
                "RUNE_SNAKE",
                "RUNE_SNOW",
                "RUNE_SOULTWIST",
                "RUNE_SPARKLING",
                "RUNE_SPELLBOUND",
                "RUNE_SPIRIT",
                "RUNE_TIDAL",
                "RUNE_WAKE",
                "RUNE_WHITE_SPIRAL",
                "RUNE_ZAP",
                "RUNE_ZOMBIE_SLAYER",
                "SMALL_RUNES_SACK",
                "UNIQUE_RUNE",
                "UNIQUE_RUNE_BARK_TUNES",
                "UNIQUE_RUNE_GOLDEN_CARPET",
                "UNIQUE_RUNE_GRAND_FREEZING",
                "UNIQUE_RUNE_HEARTSPLOSION",
                "UNIQUE_RUNE_ICE_SKATES",
                "UNIQUE_RUNE_MEOW_MUSIC",
                "UNIQUE_RUNE_ORNAMENTAL",
                "UNIQUE_RUNE_PRIMAL_FEAR",
                "UNIQUE_RUNE_RAINY_DAY",
                "UNIQUE_RUNE_SMITTEN",
                "UNIQUE_RUNE_SPELLBOUND",
                "UNIQUE_RUNE_SUPER_PUMPKIN"
            })
            {
                RuneLookup.Add(item);
            }
        }
    }
}
