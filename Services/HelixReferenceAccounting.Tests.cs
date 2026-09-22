using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class HelixReferenceAccountingTests
{
    // Identifier-free completed auctions from the supplied component/comparable evidence.
    private static SaveAuction Auction(int variant) => JsonConvert.DeserializeObject<SaveAuction>(
        Auctions[variant], new JsonSerializerSettings { ObjectCreationHandling = ObjectCreationHandling.Replace });

    private static readonly string[] Auctions =
    [
        """
{
  "tag": "HELIX_CHOPPER",
  "itemName": "Moonglade Helix Chopper \u272a\u272a\u272a\u272a\u272a",
  "tier": "LEGENDARY",
  "reforge": "moonglade",
  "category": "MISC",
  "bin": true,
  "count": 1,
  "startingBid": 45000000,
  "highestBidAmount": 45000000,
  "start": "2026-09-09T22:38:02",
  "end": "2026-09-09T22:38:52",
  "enchantments": [
    {
      "type": "absorb",
      "level": 8
    },
    {
      "type": "ultimate_first_impression",
      "level": 5
    },
    {
      "type": "arcane",
      "level": 6
    },
    {
      "type": "unknown",
      "level": 1
    },
    {
      "type": "efficiency",
      "level": 5
    },
    {
      "type": "unknown",
      "level": 1
    }
  ],
  "flatNbt": {
    "rarity_upgrades": "1",
    "absorb_logs_chopped": "3118325",
    "sweep": "1",
    "fighting": "1",
    "foraging_wisdom": "1",
    "foraging_fortune": "1",
    "unlocked_slots": "CITRINE_0,CITRINE_1",
    "CITRINE_0": "PERFECT",
    "CITRINE_1": "PERFECT",
    "logs_cut": "342986",
    "upgrade_level": "5",
    "wood_singularity_count": "1"
  },
  "Bids": [
    {
      "Amount": 45000000
    }
  ]
}
""",
        """
{
  "tag": "HELIX_CHOPPER",
  "itemName": "Moonglade Helix Chopper \u272a\u272a\u272a\u272a\u272a",
  "tier": "LEGENDARY",
  "reforge": "moonglade",
  "category": "MISC",
  "bin": true,
  "count": 1,
  "startingBid": 59900000,
  "highestBidAmount": 59900000,
  "start": "2026-09-08T15:47:29",
  "end": "2026-09-08T21:28:01",
  "enchantments": [
    {
      "type": "absorb",
      "level": 8
    },
    {
      "type": "ultimate_first_impression",
      "level": 5
    },
    {
      "type": "arcane",
      "level": 6
    },
    {
      "type": "efficiency",
      "level": 5
    }
  ],
  "flatNbt": {
    "rarity_upgrades": "1",
    "absorb_logs_chopped": "5010458",
    "unlocked_slots": "CITRINE_0,CITRINE_1",
    "CITRINE_0": "FINE",
    "CITRINE_1": "FINE",
    "sweep": "1",
    "fighting": "1",
    "foraging_wisdom": "1",
    "foraging_fortune": "1",
    "logs_cut": "200000",
    "upgrade_level": "5"
  },
  "Bids": [
    {
      "Amount": 59900000
    }
  ]
}
""",
        """
{
  "tag": "HELIX_CHOPPER",
  "itemName": "Moonglade Helix Chopper \u272a\u272a\u272a\u272a\u272a",
  "tier": "LEGENDARY",
  "reforge": "moonglade",
  "category": "MISC",
  "bin": true,
  "count": 1,
  "startingBid": 89990000,
  "highestBidAmount": 89990000,
  "start": "2026-09-08T00:54:07",
  "end": "2026-09-09T13:25:59",
  "enchantments": [
    {
      "type": "absorb",
      "level": 8
    },
    {
      "type": "ultimate_first_impression",
      "level": 5
    },
    {
      "type": "arcane",
      "level": 6
    },
    {
      "type": "unknown",
      "level": 1
    },
    {
      "type": "efficiency",
      "level": 5
    },
    {
      "type": "smelting_touch",
      "level": 1
    },
    {
      "type": "unknown",
      "level": 1
    }
  ],
  "flatNbt": {
    "rarity_upgrades": "1",
    "absorb_logs_chopped": "4239357",
    "unlocked_slots": "CITRINE_0,CITRINE_1",
    "CITRINE_0": "PERFECT",
    "CITRINE_1": "PERFECT",
    "sweep": "1",
    "hunting_wisdom": "1",
    "foraging_wisdom": "1",
    "foraging_fortune": "1",
    "logs_cut": "200000",
    "upgrade_level": "5"
  },
  "Bids": [
    {
      "Amount": 89990000
    }
  ]
}
""",
    ];

    private static SniperService CreateService(bool priceGems = true)
    {
        var service = new SniperService(
            new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null,
            NullLogger<SniperService>.Instance, new SniperServiceTests.CraftCostMock());
        // Exact clean tier aggregate from the September 10 immutable HELIX_CHOPPER capture.
        // This is a controlled state-building test, not an atomic historical finder replay.
        service.Lookups["HELIX_CHOPPER"] = new PriceLookup
        {
            CleanPricePerTier = new() { [Tier.EPIC] = 6_300_000 }
        };
        if (priceGems)
            service.UpdateBazaar(new()
            {
                Timestamp = new DateTime(2026, 9, 9, 22, 38, 0, DateTimeKind.Utc).AddMilliseconds(991),
                Products = new()
                {
                    new()
                    {
                        ProductId = "PERFECT_CITRINE_GEM",
                        SellSummary = new() { new() { PricePerUnit = 12_000_000.3, Amount = 10 } },
                        BuySummery = new()
                    }
                }
            });
        return service;
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    public void CompletedHelixSaleDeductsPerfectGemsOnce(int variant)
    {
        var service = CreateService();
        var auction = Auction(variant);
        Assert.That(auction.FlatenedNBT.ContainsKey("CITRINE_0"), Is.True, "fixture NBT");
        var key = service.KeyFromSaveAuction(auction);
        var gems = service.GetGemValue(auction, key);
        Assert.That(gems, Is.EqualTo(variant == 1 ? 0 : 23_000_000),
            "Two gems at executable demand, less 500k extraction each; integral coin truncation.");
        Assert.That(key.Modifiers.Any(m => m.Value == "PERFECT"), Is.False);
        var withoutGems = Auction(variant);
        withoutGems.FlatenedNBT.Remove("CITRINE_0");
        withoutGems.FlatenedNBT.Remove("CITRINE_1");
        var embeddedKey = service.KeyFromSaveAuction(withoutGems);
        Assert.That(key.ValueSubstract - embeddedKey.ValueSubstract,
            Is.EqualTo(variant == 1 ? 0 : 24_000_000),
            "Key removal counts gross Perfect gems once without duplicating shared upgrades.");

        service.AddSoldItem(auction, preventMedianUpdate: true);
        var reference = service.GetBucketForAuction(auction).auctions.References.Single();
        Assert.That(reference.Price + key.ValueSubstract, Is.EqualTo(auction.HighestBidAmount),
            "Restoring key-removed upgrades must recover the sale: Perfect gems are already in ValueSubstract.");
    }

    [Test]
    public void PerfectComparableMedianRestoresNetGemsAfterReferenceStorage()
    {
        var service = CreateService();
        var auction = Auction(2);
        var key = service.KeyFromSaveAuction(auction);
        service.AddSoldItem(auction, preventMedianUpdate: true);
        var bucket = service.GetBucketForAuction(auction).auctions;
        // Isolate retrieval from statistical reference-count/craft caps; this does not
        // claim that one sale establishes a production median or triggers a finder.
        bucket.Price = bucket.References.Single().Price;
        var estimate = service.GetPrice(auction);
        var nonGemRemovedValue = key.ValueSubstract - 24_000_000;
        Assert.That(estimate.Median, Is.GreaterThanOrEqualTo(
            auction.HighestBidAmount - nonGemRemovedValue - 1_000_000),
            "Retrieval must lose at most the gem extraction fees plus uncredited embedded upgrades.");
        Assert.That(estimate.Median, Is.LessThanOrEqualTo(auction.HighestBidAmount));
    }

    [Test]
    public void UnpricedPerfectGemsStayEmbeddedInReference()
    {
        var service = CreateService(priceGems: false);
        var auction = Auction(2);
        Assert.That(auction.FlatenedNBT.ContainsKey("CITRINE_0"), Is.True, "fixture NBT");
        var key = service.KeyFromSaveAuction(auction);
        Assert.That(key.Modifiers.Count(m => m.Value == "PERFECT"), Is.EqualTo(2));
        Assert.That(service.GetGemValue(auction, key), Is.Zero);
        service.AddSoldItem(auction, preventMedianUpdate: true);
        var reference = service.GetBucketForAuction(auction).auctions.References.Single();
        Assert.That(reference.Price + key.ValueSubstract, Is.EqualTo(auction.HighestBidAmount));
    }
}
