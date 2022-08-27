using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Hosting;
using Minio.DataModel;
using Newtonsoft.Json;
using Coflnet.Sky;
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
            "baseStatBoostPercentage",

            "dye_item",
            "backpack_color",
            // potion "level", // not engough impact
            // "item_tier", // mostly found on armor, unsure what it does
            "talisman_enrichment", // talismans can be enriched with additional stats
            "drill_part_engine",
            "drill_part_fuel_tank",
            // deemend to low difference "drill_part_upgrade_module",
            // could not observe price change "ability_scroll", // applied to hyperions
            // magma armor is to cheap "magmaCubesKilled"
            "captured_player", // cake soul 
            "event", // year+eventtype
            "wood_singularity_count", // self explanatory
            "art_of_war_count", //       ^^
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
            "tuned_transmission", // aotv upgrade
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
            {"life_regeneration", 2},
            {"fishing_experience", 2},
            {"ignition", 2},
            {"blazing_fortune", 2},
            {"double_hook", 3},
            //{"lifeline", 3} to low volume
        };

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
            "exp"
        };
        private static KeyValuePair<string, string> Ignore = new KeyValuePair<string, string>(string.Empty, string.Empty);


        public SniperService()
        {

            this.FoundSnipe += la =>
            {
                if (la.Finder == LowPricedAuction.FinderType.SNIPER && (float)la.Auction.StartingBid / la.TargetPrice < 0.8 && la.TargetPrice > 1_000_000)
                    Console.WriteLine($"A: {la.Auction.Uuid} {la.Auction.StartingBid} -> {la.TargetPrice}  {KeyFromSaveAuction(la.Auction)}");
            };
            foreach (var item in ShardAttributes)
            {
                IncludeKeys.Add(item.Key);
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
                for (int i = 0; i < 1; i++)
                {
                    var bkey = KeyFromSaveAuction(auction, i);
                    if (l.TryGetValue(bkey, out ReferenceAuctions bucket))
                    {
                        if (result.Lbin.AuctionId == default && bucket.Lbin.AuctionId != default)
                        {
                            result.Lbin = bucket.Lbin;
                            result.LbinKey = KeyFromSaveAuction(auction, i).ToString();
                        }
                        if (result.Median == default && (bucket.Price != default && bucket.Volume > 1))
                        {
                            result.Median = bucket.Price;
                            result.Volume = bucket.Volume;
                            result.MedianKey = KeyFromSaveAuction(auction, i).ToString();
                        }
                    }
                }
                var key = KeyFromSaveAuction(auction);
                if (result.Median == default)
                {
                    if (key.GetHashCode() % 3 == 0 && DateTime.Now.Millisecond % 30 == 0)
                        Console.WriteLine("Finding closest median brute for " + auction.Tag + key);
                    var closest = l.Where(l => l.Key != null && l.Value?.References != null && l.Value.Price > 0 && l.Value.References.Count > 3)
                                    .OrderByDescending(m => key.Similarity(m.Key)).FirstOrDefault();

                    if (closest.Key != default)
                    {
                        result.Median = closest.Value.Price;
                        result.Volume = closest.Value.Volume;
                        result.MedianKey = closest.Key.ToString();
                    }

                }
                if (result.Lbin.Price == default && l.Count > 0)
                {
                    var closest = l.Where(l => l.Key != null && l.Value?.Lbin.Price > 0).OrderByDescending(m => key.Similarity(m.Key)).FirstOrDefault();
                    if (closest.Key != default)
                    {
                        result.Lbin = closest.Value.Lbin;
                        result.LbinKey = closest.Key.ToString();
                    }
                }
            }
            return result;
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
            if (!TryGetReferenceAuctions(auction, out ReferenceAuctions bucket))
            {
                bucket = CreateAndAddBucket(auction);
            }

            return bucket;
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
            ReferenceAuctions bucket = new ReferenceAuctions();
            Lookups[auction.Tag].Lookup.TryAdd(KeyFromSaveAuction(auction, dropLevel), bucket);
            return bucket;
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
        public AuctionKey KeyFromSaveAuction(SaveAuction auction, int dropLevel = 0)
        {
            var key = new AuctionKey();


            key.Reforge = Coflnet.Sky.Core.Constants.RelevantReforges.Contains(auction.Reforge) ? auction.Reforge : ItemReferences.Reforge.Any;
            if (dropLevel == 0)
            {
                key.Enchants = auction.Enchantments
                    ?.Where(e => e.Level >= 6 && e.Type != Core.Enchantment.EnchantmentType.feather_falling
                         && e.Type != Core.Enchantment.EnchantmentType.infinite_quiver
                    || Coflnet.Sky.Core.Constants.RelevantEnchants.Where(el => el.Type == e.Type && el.Level <= e.Level).Any())
                    .Select(e => new Models.Enchantment() { Lvl = e.Level, Type = e.Type }).ToList();

                key.Modifiers = auction.FlatenedNBT?.Where(n => 
                                       IncludeKeys.Contains(n.Key) 
                                    || n.Value == "PERFECT" 
                                    || n.Key.StartsWith("MASTER_CRYPT_TANK_ZOMBIE")
                                    || n.Key.StartsWith("MINOS_CHAMPION_")
                                    || n.Key == "MINOS_INQUISITOR_750"
                                    || n.Key.StartsWith("MASTER_CRYPT_UNDEAD_") && n.Key.Length > 23) // admins
                                .OrderByDescending(n => n.Key)
                                .Select(i => NormalizeData(i, auction.Tag))
                                .Where(i => i.Key != Ignore.Key).ToList();
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

        private static KeyValuePair<string, string> NormalizeData(KeyValuePair<string, string> s, string tag)
        {
            if (s.Key == "exp")
                if (tag == "PET_GOLDEN_DRAGON")
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
                return NormalizeNumberTo(s, 15);
            if (s.Key == "heldItem")
                return new KeyValuePair<string, string>("petItem", s.Value switch
                {
                    "MINOS_RELIC" => "MINOS_RELIC",
                    "DWARF_TURTLE_SHELMET" => "DWARF_TURTLE_SHELMET",
                    "QUICK_CLAW" => "QUICK_CLAW",
                    "PET_ITEM_QUICK_CLAW" => "QUICK_CLAW",
                    "PET_ITEM_TIER_BOOST" => "TB",
                    _ => null
                });
            if (s.Key == "upgrade_level")
                return new KeyValuePair<string, string>("dungeon_item_level", s.Value);
            if (ShardAttributes.TryGetValue(s.Key, out var minLvl))
            {
                if (int.Parse(s.Value) >= minLvl)
                    return s;

                return Ignore;
            }
            if(s.Key == "baseStatBoostPercentage")
            {
                var val = int.Parse(s.Value);
                if (val < 46)
                    return Ignore;
                if (val < 50)
                    return new KeyValuePair<string, string>("baseStatBoost", "46-49");
                if (val == 50)
                    return new KeyValuePair<string, string>("baseStatBoost", "50");
                if (val > 50)
                    return new KeyValuePair<string, string>("baseStatBoost", ">50");
            }

            return s;
        }

        public static KeyValuePair<string, string> NormalizeNumberTo(KeyValuePair<string, string> s, int groupingSize, int highestGroup = int.MaxValue)
        {
            var group = ((long)double.Parse(s.Value)) / groupingSize;
            return new KeyValuePair<string, string>(s.Key, Math.Min(group, highestGroup).ToString());
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
                    i = FindFlip(auction, lbinPrice, medPrice, i, bucket, key);

                UpdateLbin(auction, bucket);
            }
        }

        private int FindFlip(SaveAuction auction, double lbinPrice, double medPrice, int i, ReferenceAuctions bucket, AuctionKey key)
        {
            // only trigger lbin if also below median or median is not set
            var volume = bucket.Volume;
            if (bucket.Lbin.Price > lbinPrice && (bucket.Price > lbinPrice) && volume > 0.2f)// || bucket.Price == 0))
            {
                var props = CreateReference(bucket.Lbin.AuctionId, key);
                props["med"] = string.Join(',', bucket.References.Reverse().Take(10).Select(a => AuctionService.Instance.GetUuid(a.AuctionId)));
                props["mVal"] = bucket.Price.ToString();
                FoundAFlip(auction, bucket, LowPricedAuction.FinderType.SNIPER, Math.Min(bucket.Lbin.Price, bucket.Price), props);
                i += 10;
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

            return i;
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