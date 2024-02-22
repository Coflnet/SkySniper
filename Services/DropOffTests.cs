using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using dev;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;
public class DropOffTests
{
    string loaded = null;
    SniperService sniperService;
    List<LowPricedAuction> found;

    [SetUp]
    public void Setup()
    {
        if (loaded == null)
        {
            loaded = File.ReadAllText("Mock/boots.json");
        }
        sniperService = new SniperService(new(null, null), null);
        var parsed = Newtonsoft.Json.JsonConvert.DeserializeObject<LookupLoad>(loaded);
        var xy =
                parsed.Lookup.Where(l => l.Key.Contains("BLACK")).ToDictionary(l => ParseKey(l), l => l.Value);
        var converted = new PriceLookup()
        {
            Lookup = new System.Collections.Concurrent.ConcurrentDictionary<AuctionKey, ReferenceAuctions>(xy)
            /*    parsed.Lookup.Select(e =>
            {
                // parse string ultimate_wisdom=5,mana_vampire=5 Any [hotpc, 1],[upgrade_level, 5] LEGENDARY 1
                // into  Enchants = [ultimate_wisdom=5,mana_vampire=5], Reforge = Any, Tier = LEGENDARY, Count = 1, Modifiers = [hotpc, 1][upgrade_level, 5]
                AuctionKey key = ParseKey(e);

                return (new KeyValuePair<AuctionKey, ReferenceAuctions>(key, e.Value), e.Key);
            }).ToDictionary(e => e.Item1.Key, e => e.Item1.Value))
            .GroupBy(e => e.Item1.Key).Select(e =>
            {
                if (e.Count() > 1)
                {
                    Console.WriteLine("This had multiple " + e.Key);
                    foreach (var item in e)
                    {
                        Console.WriteLine(item.Key);
                    }
                }
                return e;
            }).ToDictionary(e => e.Key, e => e.OrderByDescending(x => x.Item1.Value.References.Count).First().Item1.Value))*/
        };
        SniperService.StartTime += TimeSpan.FromDays(10000);
        sniperService.AddLookupData("WISE_WITHER_BOOTS", converted);

        found = new List<LowPricedAuction>();
        sniperService.AddLookupData("RUNE_TIDAL", new PriceLookup()
        {
            Lookup = new(new Dictionary<AuctionKey, ReferenceAuctions>()
            {
                {
                    new AuctionKey()
                    {
                        Enchants = new(new List<Enchant>()),
                        Reforge = ItemReferences.Reforge.Any,
                        Tier = Tier.COMMON,
                        Count = 1,
                        Modifiers = new(new List<KeyValuePair<string, string>>()
                        {
                            new("RUNE_TIDAL", "3")
                        })
                    },
                    new ReferenceAuctions()
                    {
                        References = new(),
                        Price = 10_000_000
                    }
                }
            })
        });
        sniperService.FoundSnipe += found.Add;
    }

    private static AuctionKey ParseKey(KeyValuePair<string, ReferenceAuctions> e)
    {
        var parts = e.Key.Replace(", ", ",").Split(' ');
        var key = new AuctionKey()
        {
            Enchants = new(parts[0].Split(',').Where(e => !string.IsNullOrWhiteSpace(e)).Select(e =>
            {
                var eparts = e.Split('=');
                return new Enchant() { Type = Enum.Parse<Enchantment.EnchantmentType>(eparts.First()), Lvl = byte.Parse(eparts.Last()) };
            }).ToArray()),
            Reforge = Enum.Parse<ItemReferences.Reforge>(parts[1]),
            Tier = Enum.Parse<Tier>(parts.Reverse().Skip(1).First()),
            Count = byte.Parse(parts.Last()),
            Modifiers = new(parts.Skip(2).First().Split("],[").Select(e =>
            {
                var mparts = e.Split(',', 2);
                return new KeyValuePair<string, string>(mparts.First().TrimStart('['), mparts.Last().TrimEnd(']'));
            }).Where(m => !string.IsNullOrEmpty(m.Value)).ToList())
        };
        return key;
    }

    public class LookupLoad
    {
        public Dictionary<string, ReferenceAuctions> Lookup;
    }
    [Test]
    public void Test1()
    {
        var json = """
        {
        "uuid": "ece41041a8e944e08f514fecb2bb6169",
        "count": 1,
        "startingBid": 1000,
        "tag": "WISE_WITHER_BOOTS",
        "itemName": "✿ Necrotic Storm's Boots ✪✪✪✪✪➎",
        "start": "2024-02-08T19:26:23.471Z",
        "end": "2024-02-09T01:26:23.471Z",
        "auctioneerId": "5279f862b2db4b76b86bc574a6788119",
        "highestBidAmount": 0,
        "bids": [],
        "anvilUses": 0,
        "enchantments": [
          {
            "type": "depth_strider",
            "level": 3
          },
          {
            "type": "sugar_rush",
            "level": 3
          },
          {
            "type": "thorns",
            "level": 3
          },
          {
            "type": "ultimate_refrigerate",
            "level": 5
          },
          {
            "type": "mana_vampire",
            "level": 10
          },
          {
            "type": "feather_falling",
            "level": 10
          },
          {
            "type": "rejuvenate",
            "level": 5
          },
          {
            "type": "growth",
            "level": 7
          },
          {
            "type": "protection",
            "level": 7
          }
        ],
        "nbtData": {
          "Data": {
            "rarity_upgrades": 1,
            "color": "28:212:228",
            "runes": {
              "TIDAL": 3
            },
            "dungeon_item_level": 5,
            "upgrade_level": 10,
            "uid": "ea2bde7b8323",
            "uuid": "c7ca7340-a561-4b13-8355-ea2bde7b8323",
            "dye_item": "DYE_PURE_BLACK",
            "hpc": 15,
            "gems": {
              "COMBAT_0": {
                "uuid": "7256ca25-f711-482e-a406-1397309b6c9b",
                "quality": "PERFECT"
              },
              "COMBAT_0_gem": "AMETHYST",
              "SAPPHIRE_0": "PERFECT"
            },
            "artOfPeaceApplied": 1
          }
        },
        "itemCreatedAt": "2021-09-11T03:21:00Z",
        "reforge": "Necrotic",
        "category": "ARMOR",
        "tier": "MYTHIC",
        "bin": true,
        "flatNbt": {
          "rarity_upgrades": "1",
          "color": "28:212:228",
          "dungeon_item_level": "5",
          "upgrade_level": "10",
          "uid": "ea2bde7b8323",
          "uuid": "c7ca7340-a561-4b13-8355-ea2bde7b8323",
          "dye_item": "DYE_PURE_BLACK",
          "hpc": "15",
          "COMBAT_0_gem": "AMETHYST",
          "SAPPHIRE_0": "PERFECT",
          "artOfPeaceApplied": "1",
          "COMBAT_0": "PERFECT",
          "COMBAT_0.uuid": "7256ca25-f711-482e-a406-1397309b6c9b",
          "RUNE_TIDAL": "3"
        }}
        """;
        var item = Newtonsoft.Json.JsonConvert.DeserializeObject<ApiSaveAuction>(json);
        // ultimate_refrigerate=5,growth=7,protection=7,mana_vampire=10 Any [artOfPeaceApplied, 1],[COMBAT_0, PERFECT],[dye_item, DYE_PURE_BLACK],[hotpc, 1],[rarity_upgrades, 1],[RUNE_TIDAL, 3]
        SetBazaarPrice("FIRST_MASTER_STAR", 13_000_000);
        SetBazaarPrice("DYE_PURE_BLACK", 337_000_000);
        SetBazaarPrice("PERFECT_SAPPHIRE_GEM", 12_000_000);
        SetBazaarPrice("PERFECT_AMETHYST_GEM", 12_000_000);
        SetBazaarPrice("ENCHANTMENT_ULTIMATE_REFRIGERATE_5", 26_000_000);
        SetBazaarPrice("ENCHANTMENT_GROWTH_7", 144_000_000);
        SetBazaarPrice("ENCHANTMENT_PROTECTION_7", 40_000_000);
        SetBazaarPrice("ENCHANTMENT_MANA_VAMPIRE_10", 36_000_000);
        SetBazaarPrice("THE_ART_OF_PEACE", 48_000_000);
        SetBazaarPrice("RUNE_TIDAL", 8_000_000);
        SetBazaarPrice("RECOMBOBULATOR_3000", 8_000_000);
        SetBazaarPrice("HOT_POTATO_BOOK", 80_000);
        sniperService.State = SniperState.Ready;
        sniperService.TestNewAuction(item);
        Assert.GreaterOrEqual(found.Count, 1);
        // combines buckets to reach estimation
        Assert.AreEqual(279629280 - 3, found.Last().TargetPrice, JsonConvert.SerializeObject(found, Formatting.Indented));
    }

    [Test]
    public void UsesLessSpiderKills()
    {
        // ultimate_last_stand=5 Any [color, 0:0:0],[hotpc, 1],[rarity_upgrades, 1],[spider_kills, 2]
        var auction = new SaveAuction()
        {
            Tag = "FIRST_MASTER_STAR",
            Tier = Tier.LEGENDARY,
            Enchantments = new(new List<Enchantment>()
            {
                new() { Type = Enchantment.EnchantmentType.ultimate_last_stand, Level = 5 }
            }),
            FlatenedNBT = new(new Dictionary<string, string>()
            {
                { "color", "0:0:0" },
                { "rarity_upgrades", "1" },
                { "spider_kills", "100000" }
            }),
        };
        var lowerKills = auction.Dupplicate();
        lowerKills.FlatenedNBT["spider_kills"] = "61000";
        sniperService.AddSoldItem(lowerKills.Dupplicate(30760795, DateTime.UtcNow-TimeSpan.FromDays(30)));
        sniperService.AddSoldItem(lowerKills.Dupplicate(24716394, DateTime.UtcNow-TimeSpan.FromDays(25)));
        sniperService.AddSoldItem(lowerKills.Dupplicate(21760795, DateTime.UtcNow-TimeSpan.FromDays(6)));

        sniperService.AddSoldItem(auction.Dupplicate(49742295, DateTime.UtcNow-TimeSpan.FromDays(2)));

        sniperService.TestNewAuction(auction.Dupplicate());
        Assert.AreEqual(1, found.Count);
        Assert.AreEqual(24716394 -3, found.First().TargetPrice);
    }


    private void SetBazaarPrice(string tag, int value, int buyValue = 0)
    {
        var sellOrder = new List<SellOrder>();
        if (value > 0)
            sellOrder.Add(new SellOrder() { PricePerUnit = value });
        sniperService.UpdateBazaar(new()
        {
            Products = new(){
                new (){
                    ProductId =  tag,
                    SellSummary = sellOrder,
                    BuySummery = new(){
                        new (){
                            PricePerUnit = buyValue
                        }
                    }
                }
            }
        });
    }
}
