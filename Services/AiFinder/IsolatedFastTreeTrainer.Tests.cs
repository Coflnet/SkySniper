using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.AIFinder;
using Microsoft.ML;
using Microsoft.ML.Data;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services.Tests;

[TestFixture]
[NonParallelizable]
public class IsolatedFastTreeTrainerTests
{
    public sealed class Sample
    {
        [VectorType(2)]
        public float[] Features { get; set; } = Array.Empty<float>();
        public float TrainLabel { get; set; }
    }

    /// <summary>The fit itself is tested in SkyAIFinder; here the child is this service's own entry point</summary>
    [Test]
    public void ChildFitRunsInTheSniperHost()
    {
        var context = new MLContext(seed: 17);
        var data = context.Data.LoadFromEnumerable(Enumerable.Range(0, 80).Select(i => new Sample
        {
            Features = new[] { (float)(i % 2), i / 80f },
            TrainLabel = i % 2 == 0 ? 2 : 8
        }));
        var previousHost = IsolatedFastTreeTrainer.HostAssemblyPath;
        IsolatedFastTreeTrainer.HostAssemblyPath = typeof(SnipProgram).Assembly.Location;
        try
        {
            var fitted = IsolatedFastTreeTrainer.Fit(context, data, new FastTreeParams(20, 50, 0.2, 1, 17, false));
            Assert.That(fitted.Model.TrainedTreeEnsemble.Trees.Count, Is.GreaterThan(0));
        }
        finally
        {
            IsolatedFastTreeTrainer.HostAssemblyPath = previousHost;
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task MalformedChildInputExitsWithoutStartingTheWebHost(bool incompleteArguments)
    {
        var directory = Directory.CreateTempSubdirectory("sky-sniper-child-test-");
        using var child = new Process();
        var started = false;
        try
        {
            var input = Path.Combine(directory.FullName, "invalid.idv");
            var output = Path.Combine(directory.FullName, "model.zip");
            File.WriteAllText(input, "not an IDataView binary");
            child.StartInfo = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            child.StartInfo.ArgumentList.Add(typeof(SnipProgram).Assembly.Location);
            child.StartInfo.ArgumentList.Add("--fit-fast-tree");
            if (!incompleteArguments)
                foreach (var argument in new[] { input, output, "20", "50", "0.2", "1", "17", "0" })
                    child.StartInfo.ArgumentList.Add(argument);
            started = child.Start();
            var stdout = child.StandardOutput.ReadToEndAsync();
            var stderr = child.StandardError.ReadToEndAsync();
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            var logs = await stdout;
            await stderr;
            Assert.Multiple(() =>
            {
                Assert.That(child.ExitCode, Is.Not.EqualTo(0));
                Assert.That(File.Exists(output), Is.False);
                Assert.That(logs, Does.Not.Contain("Now listening on:"));
            });
        }
        finally
        {
            if (started && !child.HasExited)
            {
                child.Kill(entireProcessTree: true);
                if (!child.WaitForExit(5000))
                    throw new TimeoutException("Malformed trainer child did not stop");
            }
            directory.Delete(recursive: true);
        }
    }
}
