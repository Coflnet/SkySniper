using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class PartialCalcTests
{
    private PartialCalcService Service = null!;
    private SniperService sniper = null!;
    private CraftcostMock craftcost = null!;
    private Core.Services.HypixelItemService itemService = null!;
    private class CraftcostMock : ICraftCostService
    {
        public Dictionary<string, double> Costs { get; } = new();

        public bool TryGetCost(string itemId, out double cost)
        {
            return Costs.TryGetValue(itemId, out cost);
        }
    }
    private class MayorMock : IMayorService
    {
        public string GetMayor(DateTime time)
        {
            return "jerry";
        }
    }
    [SetUp]
    public void Setup()
    {
        craftcost = new CraftcostMock();
        itemService = new Core.Services.HypixelItemService(null, NullLogger<Core.Services.HypixelItemService>.Instance);

        sniper = new SniperService(itemService, null, NullLogger<SniperService>.Instance, null);
        Service = new PartialCalcService(
            sniper,
            craftcost,
            new MayorMock(),
            new MockPersistenceManager(),
            NullLogger<PartialCalcService>.Instance,
            itemService);
        AddSell(new SaveAuction()
        {
            Tag = "CLEAN",
            Tier = Tier.EPIC,
            HighestBidAmount = 1000,
        });
        Service.SetLearningRate(0.1);
        Service.IsPrimary = true;
    }
    [Test]
    public void BasicIncrement()
    {
        var item = new Item()
        {
            Tag = "CLEAN",
            ExtraAttributes = new()
            {
                { "tier", "EPIC" },
                { "recombobulated", 1 }
            }
        };
        AddSell(new SaveAuction()
        {
            Tag = "CLEAN",
            Tier = Tier.EPIC,
            HighestBidAmount = 2000,
            FlatenedNBT = new()
            {
                { "recombobulated", "1" }
            }
        }, 100);
        var result = Service.GetPrice(item);
        Assert.That(80000, Is.GreaterThan(result.Price));
    }

    [Test]
    public void ExpensiveEnchant()
    {
        var item = new Item()
        {
            Tag = "CLEAN",
            ExtraAttributes = new()
            {
                { "tier", "EPIC" }
            },
            Enchantments = new()
            {
                { "ULTIMATE_LEGION", 5 }
            }
        };
        AddSell(new SaveAuction()
        {
            Tag = "CLEAN",
            Tier = Tier.EPIC,
            HighestBidAmount = 11000000,
            FlatenedNBT = new(),
            Enchantments = new()
            {
                new Core.Enchantment(Enchantment.EnchantmentType.ultimate_legion, 5)
            }
        }, 100);
        var result = Service.GetPrice(item);
        Assert.That(result.Price, Is.GreaterThan(5_000_000));
    }

    [Test]
    public void PetWithExp()
    {
        var item = new Item()
        {
            Tag = "CLEAN",
            ExtraAttributes = new()
            {
                { "tier", "LEGENDARY" },
                { "exp", "20000000" }
            },
        };
        AddSell(new SaveAuction()
        {
            Tag = "CLEAN",
            Tier = Tier.LEGENDARY,
            HighestBidAmount = 19000000,
            FlatenedNBT = new()
            {
                { "exp", "20000000" }
            }
        }, 100);
        AddSell(new SaveAuction()
        {
            Tag = "CLEAN",
            Tier = Tier.LEGENDARY,
            HighestBidAmount = 600000,
            FlatenedNBT = new()
            {
                { "exp", "8000" }
            }
        }, 100);
        var result = Service.GetPrice(item, true);
        Assert.That(result.Price, Is.GreaterThan(5_000_000));
    }

    [Test]
    public void OverValuedDoesNotScrew()
    {
        var item = new Item()
        {
            Tag = "CLEAN",
            ExtraAttributes = new()
            {
                { "tier", "LEGENDARY" },
                { "exp", "8000" }
            },
        };
        Service.SetLearningRate(0.03);
        var normalPriced = new SaveAuction()
        {
            Tag = "CLEAN",
            Tier = Tier.LEGENDARY,
            HighestBidAmount = 600000,
            FlatenedNBT = new()
            {
                { "exp", "8000" }
            }
        };
        for (int i = 0; i < 50; i++)
        {
            AddSell(normalPriced, 50);
            AddSell(new SaveAuction()
            {
                Tag = "CLEAN",
                Tier = Tier.LEGENDARY,
                HighestBidAmount = 1_000_000_000,
                FlatenedNBT = new()
            {
                { "exp", "8000" }
            }
            }, 1);
            AddSell(normalPriced, 20);
        }
        var result = Service.GetPrice(item, true);
        Assert.That(700000, Is.GreaterThan(result.Price));
    }

    [Test]
    public async Task CapTier()
    {
        var item = new Item()
        {
            Tag = "CLEAN",
            ExtraAttributes = new()
            {
                { "tier", "EPIC" }
            },
        };
        Service.SetLearningRate(0.1);
        AddSell(new SaveAuction()
        {
            Tag = "CLEAN",
            Tier = Tier.EPIC,
            HighestBidAmount = 19000000
        }, 100);
        AddSell(new SaveAuction()
        {
            Tag = "CLEAN",
            Tier = Tier.LEGENDARY,
            HighestBidAmount = 600000
        }, 100);
        await Service.CapAtCraftCost();
        var result = Service.GetPrice(item, true);
        Assert.That(600000, Is.GreaterThan(result.Price));
    }

    [Test]
    public async Task CapEnchatAtCraftCost()
    {
        var item = new Item()
        {
            Tag = "HYPERION",
            ExtraAttributes = new()
            {
                { "tier", "EPIC" }
            },
            Enchantments = new()
            {
                { "impaling", 3 }
            }
        };
        Service.SetLearningRate(0.1);
        AddSell(new SaveAuction()
        {
            Tag = "HYPERION",
            Tier = Tier.EPIC,
            HighestBidAmount = 800_000_000,
            Enchantments = new()
            {
                new Core.Enchantment(Enchantment.EnchantmentType.impaling, 3),
                new Core.Enchantment(Enchantment.EnchantmentType.aiming, 2)
            }
        }, 100);
        craftcost.Costs["ENCHANTMENT_IMPALING_5"] = 1500;

        await Service.CapAtCraftCost();
        var result = Service.GetPrice(item, true);
        Assert.That("ench.impaling 3: 787.5",Is.EqualTo(result.BreakDown[1].Replace(",", ".")));
    }

    [Test]
    public async Task CapIfEnchantTableEnchant()
    {
        var item = new Item()
        {
            Tag = "HYPERION",
            ExtraAttributes = new()
            {
                { "tier", "EPIC" }
            },
            Enchantments = new()
            {
                { "cleave", 5 }
            }
        };
        Service.SetLearningRate(0.1);
        AddSell(new SaveAuction()
        {
            Tag = "HYPERION",
            Tier = Tier.EPIC,
            HighestBidAmount = 800_000_000,
            Enchantments = new()
            {
                new Core.Enchantment(Enchantment.EnchantmentType.cleave, 5)
            }
        }, 100);
        await Service.CapAtCraftCost();
        var result = Service.GetPrice(item, true);
        // capped at 50k - no enchant from ench table is worth more than that
        Assert.That("ench.cleave 5: 50000.0",Is.EqualTo(result.BreakDown[1].Replace(",", ".")));
    }

    [Test]
    public async Task CapLevel5Star()
    {
        var item = new Item()
        {
            Tag = "HYPERION",
            ExtraAttributes = new()
            {
                { "tier", "EPIC" },
                { "upgrade_level", "5" }
            }
        };
        Service.SetLearningRate(0.1);
        AddSell(new SaveAuction()
        {
            Tag = "HYPERION",
            Tier = Tier.EPIC,
            HighestBidAmount = 800_000_000,
            FlatenedNBT = new()
            {
                { "upgrade_level", "5" }
            }
        }, 100);
        await itemService.GetItemsAsync();
        sniper.UpdateBazaar(new()
        {
            Products = new(){
                new   (){
                    ProductId = "ESSENCE_WITHER",
                    BuySummery = new (){
                        new (){
                            PricePerUnit = 2345,
                        }
                    },
                    SellSummary = new()
                }
            }
        });
        await Service.CapAtCraftCost();
        var result = Service.GetPrice(item, true);
        // capped at sum of essence cost
        Assert.That("upgrade_level 5: 7855750.0",Is.EqualTo(result.BreakDown[1].Replace(",", ".")));
    }

    private void AddSell(SaveAuction sell, int volume = 1)
    {
        for (int i = 0; i < 4; i++)
            sniper.AddSoldItem(SniperServiceTests.Dupplicate(sell));
        for (int i = 0; i < volume; i++)
            Service.AddSell(sell);
    }
}
#nullable disable
