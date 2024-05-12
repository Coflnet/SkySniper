using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class CraftCostServiceTests
{
    [TestCase("QUARTZ_BLOCK:1")]
    [TestCase("STONE_SLAB2")]
    public void FindsVanilla(string tag)
    {
        var service = new CraftCostService(null, null);
        Assert.That(service.TryGetCost(tag, out var value), Is.True);
        Assert.That(value, Is.EqualTo(10));
    }
}
#nullable disable
