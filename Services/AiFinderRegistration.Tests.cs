using System;
using System.Collections.Generic;
using System.IO;
using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

public class AiFinderRegistrationTests
{
    private static IConfiguration Config(string? enabled)
        => new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["AI_FINDER:ENABLED"] = enabled }).Build();

    /// <summary>Only "false" used to switch the finder off; 0 and no were read as on</summary>
    [TestCase("false")]
    [TestCase("0")]
    [TestCase("no")]
    [TestCase("Off")]
    public void SwitchedOffWithAnyUsualFalse(string value)
    {
        AiFinderRegistration.IsEnabled(Config(value)).Should().BeFalse();
        AiFinderRegistration.Describe(Config(value)).active.Should().BeFalse();
    }

    [TestCase(null)]
    [TestCase("true")]
    [TestCase("1")]
    public void OnByDefault(string? value) => AiFinderRegistration.IsEnabled(Config(value)).Should().BeTrue();

    /// <summary>An optional feature must not crash-loop the pod: a bad value switches the finder off and the start continues</summary>
    [Test]
    public void UnknownValueDisablesTheFinderAndTheStartContinues()
    {
        var error = new StringWriter();
        var previous = Console.Error;
        Console.SetError(error);
        try
        {
            var services = new ServiceCollection().AddAiFinder(Config("maybe"), trains: false).BuildServiceProvider();

            var finder = services.GetRequiredService<IAiFinder>();
            finder.Active.Should().BeFalse();
            finder.Variant.Should().Be(NoAiFinder.InvalidName);
            AiFinderRegistration.Describe(Config("maybe")).Should().Be((false, NoAiFinder.InvalidName));
            error.ToString().Should().Contain("AI_FINDER:ENABLED").And.Contain("maybe");
        }
        finally
        {
            Console.SetError(previous);
        }
    }

    [Test]
    public void InvalidSettingsAreIgnoredWhileTheFinderIsSwitchedOff()
        => AiFinderRegistration.ConfigurationProblem(Config("false")).Should().BeNull();
}
