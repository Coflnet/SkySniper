using Coflnet.Sky.Core;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;
public class PropertyMapperTests
{
    private readonly PropertyMapper mapper = new();
    [Test]
    public void MasterStarsIngredients()
    {
        Assert.IsTrue(mapper.TryGetIngredients("upgrade_level", "10", "5", out var ingredients));
        Assert.AreEqual(5, ingredients.Count);
        Assert.AreEqual("FIFTH_MASTER_STAR", ingredients[0]);
        Assert.AreEqual("FOURTH_MASTER_STAR", ingredients[1]);
        Assert.AreEqual("THIRD_MASTER_STAR", ingredients[2]);
        Assert.AreEqual("SECOND_MASTER_STAR", ingredients[3]);
        Assert.AreEqual("FIRST_MASTER_STAR", ingredients[4]);
    }

    [Test]
    public void MasterStarsAre5()
    {
        Assert.IsTrue(mapper.TryGetIngredients("upgrade_level", "10", "1", out var ingredients));
        Assert.AreEqual(5, ingredients.Count);
    }

    [Test]
    public void ArtOfWar()
    {
        Assert.IsTrue(mapper.TryGetIngredients("art_of_war_count", "1", null, out var ingredients));
        Assert.AreEqual(1, ingredients.Count);
        Assert.AreEqual("THE_ART_OF_WAR", ingredients[0]);
    }
}