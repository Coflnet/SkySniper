using System.IO;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;
public class PropertyMapperTests
{
    private readonly PropertyMapper mapper = new();
    [Test]
    public void MasterStarsIngredients()
    {
        Assert.That(mapper.TryGetIngredients("HYPERION", "upgrade_level", "10", "5", out var ingredients));
        Assert.That(5,Is.EqualTo(ingredients.Count));
        Assert.That("FIFTH_MASTER_STAR",Is.EqualTo(ingredients[0]));
        Assert.That("FOURTH_MASTER_STAR",Is.EqualTo(ingredients[1]));
        Assert.That("THIRD_MASTER_STAR",Is.EqualTo(ingredients[2]));
        Assert.That("SECOND_MASTER_STAR",Is.EqualTo(ingredients[3]));
        Assert.That("FIRST_MASTER_STAR",Is.EqualTo(ingredients[4]));
    }

    [Test]
    public void MasterStarsAre5()
    {
        Assert.That(mapper.TryGetIngredients("HYPERION", "upgrade_level", "10", "1", out var ingredients));
        Assert.That(5,Is.EqualTo(ingredients.Count));
    }

    [Test]
    public void ArtOfWar()
    {
        Assert.That(mapper.TryGetIngredients("HYPERION", "art_of_war_count", "1", null, out var ingredients));
        Assert.That(1,Is.EqualTo(ingredients.Count));
        Assert.That("THE_ART_OF_WAR",Is.EqualTo(ingredients[0]));
    }

    [Test]
    public async Task LoadNeuConstants()
    {
        await mapper.LoadNeuConstants();
        var cost = mapper.GetReforgeCost(ItemReferences.Reforge.aote_stone, Tier.EPIC);
        Assert.That("AOTE_STONE",Is.EqualTo(cost.Item1));
        Assert.That(NeuReforgeCost("AOTE_STONE", "EPIC"),Is.EqualTo(cost.Item2));
    }

    /// <summary>
    /// The NEU repo is cloned at its current tip, so the expected cost is read from the same file instead of hardcoded
    /// </summary>
    private static int NeuReforgeCost(string stoneTag, string tier)
    {
        var stones = JObject.Parse(File.ReadAllText("NEU-REPO/constants/reforgestones.json"));
        var cost = stones[stoneTag]["reforgeCosts"][tier].Value<int>();
        Assert.That(cost, Is.GreaterThan(0));
        return cost;
    }
}