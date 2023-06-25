using System;
using Coflnet.Sky.Core;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class PartialCalcTests
{
    private PartialCalcService Service = null!;
    private SniperService sniper = null!;
    private class CraftcostMock : ICraftCostService
    {
        public bool TryGetCost(string itemId, out double cost)
        {
            cost = 0;
            return false;
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
        sniper = new SniperService();
        Service = new PartialCalcService(sniper.Lookups, new CraftcostMock(), new MayorMock());
        AddSell(new SaveAuction()
        {
            Tag = "CLEAN",
            Tier = Tier.EPIC,
            HighestBidAmount = 1000,
        });
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
        Assert.Greater(80000, result.Price);
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
                new Core.Enchantment(Core.Enchantment.EnchantmentType.ultimate_legion, 5)
            }
        }, 100);
        var result = Service.GetPrice(item);
        Assert.Greater(result.Price, 5_000_000);
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
        Console.WriteLine(string.Join("\n", result.BreakDown));
        Assert.Greater(result.Price, 5_000_000);
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
