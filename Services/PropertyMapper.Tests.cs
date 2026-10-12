using System.Threading.Tasks;
using Coflnet.Sky.Core;
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
        // the exact coin cost comes from the live NEU repo and changes over time, only check that it loaded
        Assert.That(cost.Item2, Is.GreaterThan(0));
    }
}