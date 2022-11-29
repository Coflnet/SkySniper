using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Sniper.Services
{
    public class SniperService
    {
        public static int MIN_TARGET = 200_000;
        public ConcurrentDictionary<string, PriceLookup> Lookups = new ConcurrentDictionary<string, PriceLookup>();

        private ConcurrentQueue<LogEntry> Logs = new ConcurrentQueue<LogEntry>();
        private ConcurrentQueue<(SaveAuction, ReferenceAuctions)> LbinUpdates = new();

        public event Action<LowPricedAuction> FoundSnipe;
        private readonly HashSet<string> IncludeKeys = new HashSet<string>()
        {
            "baseStatBoostPercentage", // has an effect on drops from dungeons, is filtered to only max level

            "dye_item",
            "backpack_color",
            "party_hat_color",
            "color", // armour
            "model", // abicase
            // potion "level", // not engough impact
            // "item_tier", // mostly found on armor, unsure what it does
            "talisman_enrichment", // talismans can be enriched with additional stats
            "drill_part_engine",
            "drill_part_fuel_tank",
            // deemend to low difference "drill_part_upgrade_module",
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
            "ethermerge",
            "edition", // great spook stuff
            "hpc", // hot potato books
            //"tuned_transmission", // aotv upgrade
            //"power_ability_scroll", // disabled as suggested by Coyu because comonly not worth 1m (up to 2m at most)
            "captured_player", // cake souls
            "MUSIC", //rune
            "DRAGON", //rune
            "TIDAL", //rune
            "ENCHANT" // rune
        };

        private static readonly Dictionary<string, short> ShardAttributes = new(){
            {"mana_pool", 1},
            {"breeze", 1},
            {"speed", 2},
            {"life_regeneration", 2}, // especially valuable in combination with mana_pool
            {"fishing_experience", 2},
            {"ignition", 2},
            {"blazing_fortune", 2},
            {"double_hook", 3},
            {"mana_regeneration", 2},
            {"mending", 3},
            {"dominance", 3}
            //{"lifeline", 3} to low volume
            // life recovery 3
        };

        // combos that are worth more starting at lvl 1 because they are together
        private readonly Dictionary<string, string> AttributeCombos = new(){
            {"blazing_fortune", "fishing_experience"},
            {"life_regeneration", "mana_pool"},
            {"veteran", "mending"},
            {"mana_regeneration", "mana_pool"}
        };
        private readonly ConcurrentDictionary<string, HashSet<string>> AttributeComboLookup = new();

        public void FinishedUpdate()
        {
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
                }
            }
        }

        private Dictionary<Core.Enchantment.EnchantmentType, byte> MinEnchantMap = new();

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

        // stuff changing value by 5+M
        public static HashSet<string> VeryValuable = new HashSet<string>()
        {
            "rarity_upgrades",
            "winning_bid",
            "skin",
            "exp",
            "color"
        };
        private static KeyValuePair<string, string> Ignore = new KeyValuePair<string, string>(string.Empty, string.Empty);


        public SniperService()
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
            }
            foreach (var item in AttributeCombos)
            {
                IncludeKeys.Add(item.Key);
            }
            foreach (var item in ShardAttributes)
            {
                IncludeKeys.Add(item.Key);
            }

            foreach (var enchant in Enum.GetValues<Core.Enchantment.EnchantmentType>())
            {
                MinEnchantMap[enchant] = 6;
            }

            foreach (var item in Coflnet.Sky.Core.Constants.RelevantEnchants)
            {
                MinEnchantMap[item.Type] = item.Level;
            }
        }

        public PriceEstimate GetPrice(SaveAuction auction)
        {
            if (auction == null || auction.Tag == null)
                return null;

            var result = new PriceEstimate();
            if (Lookups.TryGetValue(auction.Tag, out PriceLookup lookup))
            {
                var l = lookup.Lookup;
                var itemKey = KeyFromSaveAuction(auction);
                result.ItemKey = itemKey.ToString();
                if (l.TryGetValue(itemKey, out ReferenceAuctions bucket))
                {
                    if (result.Lbin.AuctionId == default && bucket.Lbin.AuctionId != default)
                    {
                        result.Lbin = bucket.Lbin;
                        result.LbinKey = itemKey.ToString();
                    }
                    if (result.Median == default && bucket.Price != default)
                    {
                        AssignMedian(result, itemKey, bucket);
                    }
                }

                if (result.Median == default)
                {
                    if (itemKey.GetHashCode() % 3 == 0 && DateTime.Now.Millisecond % 30 == 0)
                        Console.WriteLine("Finding closest median brute for " + auction.Tag + itemKey);
                    var closest = l.Where(l => l.Key != null && l.Value?.References != null && l.Value.Price > 0 && l.Value.References.Count > 3)
                                    .OrderByDescending(m => itemKey.Similarity(m.Key)).FirstOrDefault();

                    if (closest.Key != default)
                    {
                        AssignMedian(result, closest.Key, closest.Value);
                    }

                }
                if (result.Lbin.Price == default && l.Count > 0)
                {
                    var closest = l.Where(l => l.Key != null && l.Value?.Lbin.Price > 0).OrderByDescending(m => itemKey.Similarity(m.Key) + Math.Min(m.Value.Volume, 6)).FirstOrDefault();
                    if (closest.Key != default)
                    {
                        result.Lbin = closest.Value.Lbin;
                        result.LbinKey = closest.Key.ToString();
                    }
                }
            }
            return result;
        }

        private static void AssignMedian(PriceEstimate result, AuctionKey key, ReferenceAuctions bucket)
        {
            result.Median = bucket.Price;
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
            Lookups.AddOrUpdate(itemTag, loadedVal, (key, value) =>
            {
                foreach (var item in loadedVal.Lookup)
                {
                    if (!value.Lookup.TryGetValue(item.Key, out ReferenceAuctions existingBucket))
                    {
                        value.Lookup[item.Key] = item.Value;
                        continue;
                    }
                    existingBucket.References = item.Value.References;
                    existingBucket.Price = item.Value.Price;
                    if (item.Value.Lbins == null)
                        item.Value.Lbins = new();
                    // migrate last lbin
                    if (item.Value.LastLbin.Price != default)
                    {
                        existingBucket.Lbins.Add(item.Value.LastLbin);
                    }
                    // load all non-empty lbins
                    foreach (var binAuction in item.Value.Lbins)
                    {
                        if (!existingBucket.Lbins.Contains(binAuction) && binAuction.Price > 0)
                            existingBucket.Lbins.Add(binAuction);
                    }
                    item.Value.Lbins.Sort(ReferencePrice.Compare);
                }
                return value;
            });
        }

        public void AddSoldItem(SaveAuction auction)
        {
            ReferenceAuctions bucket = GetBucketForAuction(auction);
            if (bucket.References.Where(r => r.AuctionId == auction.UId).Any())
                return; // duplicate
            var reference = CreateReferenceFromAuction(auction);
            // move reference to sold
            bucket.References.Enqueue(reference);
            bucket.Lbins.Remove(reference);
            UpdateMedian(bucket);
        }

        private static void UpdateMedian(ReferenceAuctions bucket)
        {
            var size = bucket.References.Count;
            if (size > 90)
                bucket.References.TryDequeue(out ReferencePrice ra);
            var deduplicated = bucket.References
                .OrderByDescending(b => b.Day)
                .Take(60)
                .GroupBy(a => a.Seller)
                .Select(a => a.Last())  // only use one (the last) price from each seller
                .ToList();
            size = deduplicated.Count();
            if (size < 2)
            {
                bucket.Price = 0; // to low vol
                return;
            }
            // short term protects against price drops after updates
            var shortTermList = deduplicated.OrderByDescending(b => b.Day).Take(3).ToList();
            var shortTermPrice = GetMedian(shortTermList);
            bucket.OldestRef = shortTermList.Min(s => s.Day);
            // long term protects against market manipulation
            var longSpanPrice = GetMedian(deduplicated.Take(45).ToList());
            bucket.Price = Math.Min(shortTermPrice, longSpanPrice);
        }

        public ReferenceAuctions GetBucketForAuction(SaveAuction auction)
        {
            if (!Lookups.ContainsKey(auction.Tag))
            {
                Lookups[auction.Tag] = new PriceLookup();
            }
            return GetOrAdd(KeyFromSaveAuction(auction), Lookups[auction.Tag]);
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
            var itemBucket = Lookups[auction.Tag];
            return GetOrAdd(key, itemBucket);
        }

        private static ReferenceAuctions GetOrAdd(AuctionKey key, PriceLookup itemBucket)
        {
            return itemBucket.Lookup.GetOrAdd(key, (k) => new ReferenceAuctions());
        }

        private static ReferencePrice CreateReferenceFromAuction(SaveAuction auction)
        {
            return new ReferencePrice()
            {
                AuctionId = auction.UId,
                Day = GetDay(auction.End),
                Price = auction.HighestBidAmount == 0 ? auction.StartingBid : auction.HighestBidAmount,
                Seller = auction.AuctioneerId == null ? (short)(auction.SellerId % (2 << 14)) : Convert.ToInt16(auction.AuctioneerId.Substring(0, 4), 16)
            };
        }

        public static short GetDay(DateTime date = default)
        {
            if (date == default)
                date = DateTime.Now;
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

        private static List<KeyValuePair<string, string>> EmptyModifiers = new();
        private static DateTime UnlockedIntroduction = new DateTime(2021, 9, 4);
        private static List<string> GemPurities = new() { "PERFECT", "FLAWLESS", "FINE", "ROUGH" };
        public AuctionKey KeyFromSaveAuction(SaveAuction auction, int dropLevel = 0)
        {
            var key = new AuctionKey();


            key.Reforge = Coflnet.Sky.Core.Constants.RelevantReforges.Contains(auction.Reforge) ? auction.Reforge : ItemReferences.Reforge.Any;
            if (dropLevel == 0)
            {
                key.Enchants = auction.Enchantments
                    ?.Where(e => e.Level >= MinEnchantMap[e.Type])
                    .Select(e => new Models.Enchantment() { Lvl = e.Level, Type = e.Type }).ToList();

                key.Modifiers = auction.FlatenedNBT?.Where(n =>
                                       IncludeKeys.Contains(n.Key)
                                    || n.Value == "PERFECT"
                                    || n.Key.StartsWith("MASTER_CRYPT_TANK_ZOMBIE")
                                    || n.Key.StartsWith("MINOS_CHAMPION_")
                                    || n.Key == "MINOS_INQUISITOR_750"
                                    || n.Key.StartsWith("MASTER_CRYPT_UNDEAD_") && n.Key.Length > 23) // admins
                                .OrderByDescending(n => n.Key)
                                .Select(i => NormalizeData(i, auction))
                                .Where(i => i.Key != Ignore.Key).ToList();
                if (auction.ItemCreatedAt < UnlockedIntroduction && auction.FlatenedNBT.Any(v => GemPurities.Contains(v.Value)))
                    key.Modifiers.Add(new KeyValuePair<string, string>("unlocked_slots", "all"));
            }
            else if (dropLevel == 1)
            {
                key.Modifiers = auction.FlatenedNBT?.Where(n => VeryValuable.Contains(n.Key) || n.Value == "PERFECT")
                            .OrderByDescending(n => n.Key)
                            .ToList();
                key.Enchants = auction.Enchantments
                    ?.Where(e => Coflnet.Sky.Core.Constants.RelevantEnchants.Where(el => el.Type == e.Type && el.Level <= e.Level).Any())
                    .Select(e => new Models.Enchantment() { Lvl = e.Level, Type = e.Type }).ToList();
                if (key?.Enchants?.Count == 0)
                {
                    var enchant = Constants.SelectBest(auction.Enchantments);
                    key.Enchants = new List<Models.Enchantment>() { new Models.Enchantment() { Lvl = enchant.Level, Type = enchant.Type } };
                }
            }
            else if (dropLevel == 2)
            {
                var enchant = Constants.SelectBest(auction.Enchantments);
                if (enchant == default)
                    key.Enchants = new List<Models.Enchantment>();
                else
                    key.Enchants = new List<Models.Enchantment>() { new Models.Enchantment() { Lvl = enchant.Level, Type = enchant.Type } };
                key.Modifiers = EmptyModifiers;
            }
            else
            {
                //key.Modifiers = new List<KeyValuePair<string, string>>();
                key.Enchants = new List<Models.Enchantment>();
                key.Modifiers = EmptyModifiers;
            }

            key.Tier = auction.Tier;
            if (auction.Tag == "ENCHANTED_BOOK")
            {
                // rarities don't matter for enchanted books and often used for scamming
                key.Tier = Tier.UNCOMMON;
            }
            key.Count = (byte)auction.Count;

            return key;
        }

        private KeyValuePair<string, string> NormalizeData(KeyValuePair<string, string> s, SaveAuction auction)
        {
            if (s.Key == "exp")
                if (auction.Tag == "PET_GOLDEN_DRAGON")
                    return NormalizeNumberTo(s, 30_036_483, 7);
                else
                    return NormalizeNumberTo(s, 4_225_538, 6);
            if (s.Key == "winning_bid")
                return NormalizeNumberTo(s, 10_000_000);
            if (s.Key.EndsWith("_kills"))
                return NormalizeNumberTo(s, 10_000);
            if (s.Key == "candyUsed") // all candied are the same
                return new KeyValuePair<string, string>(s.Key, (double.Parse(s.Value) > 0 ? 1 : 0).ToString());
            if (s.Key == "edition")
            {
                var val = int.Parse(s.Value);
                if (val < 100)
                    return new KeyValuePair<string, string>(s.Key, "0");
                if (val < 1000)
                    return new KeyValuePair<string, string>(s.Key, "1000");
                if (val < 10000)
                    return new KeyValuePair<string, string>(s.Key, "10k");
                return new KeyValuePair<string, string>(s.Key, "100k");
            }
            if (s.Key == "hpc")
                return GetNumeric(s) switch
                {
                    15 => new(s.Key, "1"),
                    > 10 => new(s.Key, "0"),
                    _ => Ignore
                };
            if (s.Key == "heldItem")
            {
                var heldItem = s.Value switch
                {
                    "MINOS_RELIC" => "MINOS_RELIC",
                    "DWARF_TURTLE_SHELMET" => "DWARF_TURTLE_SHELMET",
                    "QUICK_CLAW" => "QUICK_CLAW",
                    "PET_ITEM_QUICK_CLAW" => "QUICK_CLAW",
                    "PET_ITEM_TIER_BOOST" => "TB",
                    "PET_ITEM_LUCKY_CLOVER" => "LUCKY",
                    "PET_ITEM_LUCKY_CLOVER_DROP" => "LUCKY",
                    _ => null
                };
                if (heldItem == null)
                    return Ignore;
                return new KeyValuePair<string, string>("petItem", heldItem);
            }
            if (s.Key == "upgrade_level")
                return new KeyValuePair<string, string>("dungeon_item_level", s.Value);
            if (ShardAttributes.TryGetValue(s.Key, out var minLvl))
            {
                if (int.Parse(s.Value) >= minLvl)
                    return s;
                if (HasAttributeCombo(s, auction))
                    return s;
                return Ignore;
            }
            if (s.Key == "talisman_enrichment")
                return new KeyValuePair<string, string>("talisman_enrichment", "yes");
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

        /// <summary>
        /// Matches valuable attribute combinations
        /// </summary>
        /// <param name="s"></param>
        /// <param name="auction"></param>
        /// <returns></returns>
        private bool HasAttributeCombo(KeyValuePair<string, string> s, SaveAuction auction)
        {
            return AttributeComboLookup.TryGetValue(s.Key, out var otherKeys) && otherKeys.Any(otherKey => auction.FlatenedNBT.TryGetValue(otherKey, out _));
        }

        /// <summary>
        /// Returns keys that are higher value and have to be checked before something is declared to be a snipe
        /// </summary>
        /// <param name="baseKey">The actual auction key</param>
        /// <returns></returns>
        private IEnumerable<AuctionKey> HigherValueKeys(AuctionKey baseKey)
        {
            var exp = baseKey.Modifiers.Where(m => m.Key == "exp").FirstOrDefault();
            if (exp.Key != default && exp.Value != "6")
            {
                for (int i = int.Parse(exp.Value) + 1; i < 7; i++)
                {
                    yield return new AuctionKey(baseKey)
                    {
                        Modifiers = baseKey.Modifiers.Where(m => m.Key != "exp").Append(new("exp", i.ToString())).ToList()
                    };
                }
            }
        }

        public static KeyValuePair<string, string> NormalizeNumberTo(KeyValuePair<string, string> s, int groupingSize, int highestGroup = int.MaxValue)
        {
            var group = GetNumeric(s) / groupingSize;
            return new KeyValuePair<string, string>(s.Key, Math.Min(group, highestGroup).ToString());
        }

        private static long GetNumeric(KeyValuePair<string, string> s)
        {
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
            var lookup = Lookups.GetOrAdd(auction.Tag, key => new PriceLookup());
            var l = lookup.Lookup;
            var cost = auction.StartingBid;
            var lbinPrice = auction.StartingBid * 1.05;
            var medPrice = auction.StartingBid * 1.1;
            var lastKey = new AuctionKey();
            for (int i = 0; i < 4; i++)
            {
                var key = KeyFromSaveAuction(auction, i);
                if (i > 0 && key == lastKey)
                {
                    continue; // already checked that
                }
                lastKey = key;

                if (!l.TryGetValue(key, out ReferenceAuctions bucket))
                {
                    if (i != 0)
                        continue;
                    bucket = CreateAndAddBucket(auction);
                }
                if (bucket == null)
                {
                    Console.WriteLine("is null");
                }
                if (triggerEvents)
                    FindFlip(auction, lbinPrice, medPrice, bucket, key, l);

                UpdateLbin(auction, bucket);
            }
        }

        private void FindFlip(SaveAuction auction, double lbinPrice, double medPrice, ReferenceAuctions bucket, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l)
        {
            // only trigger lbin if also below median or median is not set
            var volume = bucket.Volume;
            if (bucket.Lbin.Price > lbinPrice && (bucket.Price > lbinPrice) && volume > 0.2f
               )// || bucket.Price == 0))
            {
                PotentialSnipe(auction, lbinPrice, bucket, key, l);
            }
            else if (bucket.Price > medPrice)
            {
                var props = CreateReference(bucket.References.Last().AuctionId, key);
                props["med"] = string.Join(',', bucket.References.Reverse().Take(10).Select(a => AuctionService.Instance.GetUuid(a.AuctionId)));
                FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER_MEDIAN, bucket.Price, props);
            }
            else
            {
                if (auction.UId % 10 == 0)
                    Console.Write("p");
                if (volume == 0 || bucket.Lbin.Price == 0 || bucket.Price == 0 || bucket.Price > MIN_TARGET)
                    Logs.Enqueue(new LogEntry()
                    {
                        Key = key,
                        LBin = bucket.Lbin.Price,
                        Median = bucket.Price,
                        Uuid = auction.Uuid,
                        Volume = bucket.Volume
                    });
                if (Logs.Count > 2000)
                    PrintLogQueue();
            }
        }

        private void PotentialSnipe(SaveAuction auction, double lbinPrice, ReferenceAuctions bucket, AuctionKey key, ConcurrentDictionary<AuctionKey, ReferenceAuctions> l)
        {
            var higherValueLowerBin = bucket.Lbin.Price;
            if (HigherValueKeys(key).Any(k =>
            {
                if (l.TryGetValue(k, out ReferenceAuctions altBucket))
                {
                    if (altBucket.Lbin.Price < lbinPrice)
                    {
                        return true;
                    }
                    if (altBucket.Lbin.Price != 0 && altBucket.Lbin.Price < higherValueLowerBin)
                        higherValueLowerBin = altBucket.Lbin.Price;// cheaper lbin found
                }
                return false;
            }))
                return;
            var props = CreateReference(bucket.Lbin.AuctionId, key);
            props["med"] = string.Join(',', bucket.References.Reverse().Take(10).Select(a => AuctionService.Instance.GetUuid(a.AuctionId)));
            props["mVal"] = bucket.Price.ToString();
            FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER, Math.Min(higherValueLowerBin, bucket.Price), props);
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

        private void FoundAFlip(SaveAuction auction, ReferenceAuctions bucket, LowPricedAuction.FinderType type, long targetPrice, Dictionary<string, string> props)
        {
            if (targetPrice < MIN_TARGET)
                return; // to low
            props["refAge"] = (GetDay() - bucket.OldestRef).ToString();
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
        }

        private static Dictionary<string, string> CreateReference(long reference, AuctionKey key)
        {
            return new Dictionary<string, string>() {
                { "reference", AuctionService.Instance.GetUuid(reference) },
                { "key", key.ToString() }
            };
        }
    }
}
