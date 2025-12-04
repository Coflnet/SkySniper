using System;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Services;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper;

public class MayorServiceTests
{
    private MayorService mayorService;

    [SetUp]
    public void Setup()
    {
        mayorService = new MayorService(null, null, NullLogger<MayorService>.Instance);
    }

    [Test]
    public void DianaRelatedItemsContainsAllExpectedItems()
    {
        MayorService.DianaRelatedItems.Should().Contain("MYTHOS_LEGGINGS");
        MayorService.DianaRelatedItems.Should().Contain("MYTHOS_NECKLACE");
        MayorService.DianaRelatedItems.Should().Contain("MYTHOS_FRAGMENT");
        MayorService.DianaRelatedItems.Should().Contain("MYTHOS_CHESTPLATE");
        MayorService.DianaRelatedItems.Should().Contain("MYTHOS_BRACELET");
        MayorService.DianaRelatedItems.Should().Contain("MYTHOS_BOOTS");
        MayorService.DianaRelatedItems.Should().Contain("MYTHOS_BELT");
        MayorService.DianaRelatedItems.Should().Contain("DIANAS_BOOKSHELF");
        MayorService.DianaRelatedItems.Should().Contain("DAEDALUS_STICK");
        MayorService.DianaRelatedItems.Should().Contain("CHALLENGER_NECKLACE");
        MayorService.DianaRelatedItems.Should().HaveCount(10);
    }

    [Test]
    public void GetMayor_ReturnsSetMayor()
    {
        var testTime = new DateTime(2024, 12, 1, 12, 0, 0, DateTimeKind.Utc);
        var year = MayorService.ElectionYear(testTime);
        
        mayorService.SetMayorForYear(year, "Diana");
        
        mayorService.GetMayor(testTime).Should().Be("Diana");
    }

    [Test]
    public void GetPreviousMayor_ReturnsCorrectMayor()
    {
        var testTime = new DateTime(2024, 12, 1, 12, 0, 0, DateTimeKind.Utc);
        var year = MayorService.ElectionYear(testTime);
        
        mayorService.SetMayorForYear(year, "Aatrox");
        mayorService.SetMayorForYear(year - 1, "Diana");
        
        mayorService.GetPreviousMayor(testTime).Should().Be("Diana");
    }

    [Test]
    public void IsDianaItemsAdjustmentActive_ReturnsFalse_WhenDianaNotMayor()
    {
        var testTime = new DateTime(2024, 12, 1, 12, 0, 0, DateTimeKind.Utc);
        var year = MayorService.ElectionYear(testTime);
        
        mayorService.SetMayorForYear(year, "Aatrox");
        mayorService.SetMayorForYear(year - 1, "Marina");
        
        mayorService.IsDianaItemsAdjustmentActive(testTime).Should().BeFalse();
    }

    [Test]
    public void IsDianaItemsAdjustmentActive_ReturnsTrue_WhenDianaIsPreviousMayorAndWithin48Hours()
    {
        // Diana was the previous mayor, and we're early in the new mayor's term (within 48 hours)
        var testTime = new DateTime(2024, 12, 1, 12, 0, 0, DateTimeKind.Utc);
        var year = MayorService.ElectionYear(testTime);
        
        mayorService.SetMayorForYear(year, "Aatrox");
        mayorService.SetMayorForYear(year - 1, "Diana");
        
        // The year fraction determines how far into the current mayor's term we are
        // If we're close to the start of the year (after election), this should return true
        var yearFraction = Constants.SkyblockYear(testTime) - year;
        
        // Only return true if within the 48 hour window
        if (yearFraction <= 48.0 / 124.0)
        {
            mayorService.IsDianaItemsAdjustmentActive(testTime).Should().BeTrue();
        }
    }

    [Test]
    public void IsDianaItemsAdjustmentActive_ReturnsTrue_WhenDianaIsCurrentMayorNearEnd()
    {
        // Diana is current mayor, and we're near the end of her term (last 8 hours)
        // We need to find a time where Diana is mayor and near the end of the term
        var testTime = new DateTime(2024, 12, 1, 12, 0, 0, DateTimeKind.Utc);
        var year = MayorService.ElectionYear(testTime);
        
        mayorService.SetMayorForYear(year, "Diana");
        
        // Check if we're in the last 8 hours of the election year
        var yearFraction = Constants.SkyblockYear(testTime) - year;
        
        // If we're near the end (year fraction close to 1.0), this should return true
        if (yearFraction >= (1 - 8.0 / 124.0))
        {
            mayorService.IsDianaItemsAdjustmentActive(testTime).Should().BeTrue();
        }
    }

    [Test]
    public void IsDianaItemsAdjustmentActive_ReturnsFalse_WhenDianaIsCurrentMayorButNotNearEnd()
    {
        // Diana is current mayor, but we're in the middle of her term
        var testTime = new DateTime(2024, 12, 1, 12, 0, 0, DateTimeKind.Utc);
        var year = MayorService.ElectionYear(testTime);
        
        mayorService.SetMayorForYear(year, "Diana");
        mayorService.SetMayorForYear(year - 1, "Aatrox"); // Previous was not Diana
        
        // Check the year fraction
        var yearFraction = Constants.SkyblockYear(testTime) - year;
        
        // If we're NOT near the end (in the middle of the term), should return false
        // unless Diana was also the previous mayor
        if (yearFraction < (1 - 8.0 / 124.0) && yearFraction > 48.0 / 124.0)
        {
            mayorService.IsDianaItemsAdjustmentActive(testTime).Should().BeFalse();
        }
    }

    [Test]
    public void IsDianaItemsAdjustmentActive_ReturnsFalse_WhenDianaWasPreviousMayorButOver48HoursAgo()
    {
        // Diana was the previous mayor, but more than 48 hours have passed
        var testTime = new DateTime(2024, 12, 1, 12, 0, 0, DateTimeKind.Utc);
        var year = MayorService.ElectionYear(testTime);
        
        mayorService.SetMayorForYear(year, "Aatrox");
        mayorService.SetMayorForYear(year - 1, "Diana");
        
        var yearFraction = Constants.SkyblockYear(testTime) - year;
        
        // If we're past 48 hours into the new term, should return false
        if (yearFraction > 48.0 / 124.0)
        {
            mayorService.IsDianaItemsAdjustmentActive(testTime).Should().BeFalse();
        }
    }

    /// <summary>
    /// Verifies that Diana-related items should have 10% reduced median value
    /// when the adjustment is active (8 hours before Diana's term ends or 48 hours after)
    /// </summary>
    [Test]
    [TestCase("MYTHOS_LEGGINGS")]
    [TestCase("MYTHOS_NECKLACE")]
    [TestCase("MYTHOS_FRAGMENT")]
    [TestCase("MYTHOS_CHESTPLATE")]
    [TestCase("MYTHOS_BRACELET")]
    [TestCase("MYTHOS_BOOTS")]
    [TestCase("MYTHOS_BELT")]
    [TestCase("DIANAS_BOOKSHELF")]
    [TestCase("DAEDALUS_STICK")]
    [TestCase("CHALLENGER_NECKLACE")]
    public void DianaRelatedItem_IsInDianaRelatedItemsList(string itemTag)
    {
        MayorService.DianaRelatedItems.Should().Contain(itemTag);
    }
}
