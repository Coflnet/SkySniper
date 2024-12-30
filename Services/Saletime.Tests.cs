using System;
using System.Linq;
using Coflnet.Sky.Core;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;
public class SaletimeTests
{
    SniperService service;
    [SetUp]
    public void Setup()
    {

        SniperService.MIN_TARGET = 0;
        SniperService.StartTime = new DateTime(2021, 9, 25);
        // console logger
        var factory = LoggerFactory.Create(builder => builder.AddConsole());
        service = new SniperService(new(null, null), null, factory.CreateLogger<SniperService>(), null);
    }
    [Test]
    public void TwoMinutes()
    {
        Simulate(new DateTime(2024, 9, 20, 12, 0, 0), new DateTime(2024, 9, 20, 12, 2, 4), 2);
    }
    [Test]
    public void SameDay()
    {
        Simulate(new DateTime(2024, 9, 20, 12, 0, 0), new DateTime(2024, 9, 20, 14, 0, 0), 120);
    }
    [Test]
    public void NextDay()
    {
        Simulate(new DateTime(2024, 9, 20, 12, 0, 0), new DateTime(2024, 9, 21, 12, 0, 0), 1440);
    }
    [Test]
    public void AWeek()
    {
        Simulate(new DateTime(2024, 9, 20, 12, 0, 0), new DateTime(2024, 9, 27, 12, 0, 0), 10080);
    }

    private void Simulate(DateTime start, DateTime sellAt, short minutes)
    {
        var a = new SaveAuction()
        {
            Tag = "test",
            Enchantments = [],
            FlatenedNBT = new(),
            UId = 1,
            Start = start,
            End = new DateTime(2024, 9, 29, 12, 0, 1)
        };
        service.TestNewAuction(a);
        service.FinishedUpdate();
        var sell = a.Dupplicate();
        sell.End = sellAt;
        sell.UId = 1;
        service.AddSoldItem(sell);
        service.Lookups[a.Tag].Lookup.First().Value.References.First().SellTime.Should().Be(minutes);
    }
}