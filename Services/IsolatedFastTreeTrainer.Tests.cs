using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
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
        public float Label { get; set; }
        public float TrainLabel { get; set; }
    }

    public sealed class Prediction
    {
        public float Score { get; set; }
    }

    private static IDataView TrainingData(MLContext context) => context.Data.LoadFromEnumerable(
        Enumerable.Range(0, 80).Select(i => new Sample
        {
            Features = new[] { (float)(i % 2), i / 80f },
            TrainLabel = i % 2 == 0 ? 2 : 8,
            Label = i % 2 == 0 ? -2 : -8
        }));

    [Test]
    public void ChildFitPreservesFixedVectorTrainLabelAndPersistedScores()
    {
        var context = new MLContext(seed: 17);
        var data = TrainingData(context);
        var fitted = IsolatedFastTreeTrainer.Fit(context, data, 1, 50, 17);
        using var predictor = context.Model.CreatePredictionEngine<Sample, Prediction>(fitted);
        var low = new Sample { Features = new[] { 0f, 0.5f } };
        var high = new Sample { Features = new[] { 1f, 0.5f } };
        var lowScore = predictor.Predict(low).Score;
        var highScore = predictor.Predict(high).Score;

        using var saved = new MemoryStream();
        context.Model.Save(fitted, data.Schema, saved);
        saved.Position = 0;
        var reloaded = context.Model.Load(saved, out var schema);
        using var restoredPredictor = context.Model.CreatePredictionEngine<Sample, Prediction>(reloaded);

        Assert.Multiple(() =>
        {
            Assert.That(((VectorDataViewType)schema[nameof(Sample.Features)].Type).Size, Is.EqualTo(2));
            Assert.That(schema[nameof(Sample.TrainLabel)].Type, Is.EqualTo(NumberDataViewType.Single));
            // Label deliberately has the opposite sign: fitting the wrong column fails these checks.
            Assert.That(lowScore, Is.EqualTo(2).Within(1));
            Assert.That(highScore, Is.EqualTo(8).Within(1));
            Assert.That(restoredPredictor.Predict(low).Score, Is.EqualTo(lowScore).Within(0.000001));
            Assert.That(restoredPredictor.Predict(high).Score, Is.EqualTo(highScore).Within(0.000001));
            Assert.That(fitted.Model.TrainedTreeEnsemble.Trees.Count, Is.GreaterThan(0));
        });
    }

    [Test]
    public void TimedOutChildDoesNotFallBackAndRemovesItsTemporaryFiles()
    {
        var before = Directory.GetDirectories(Path.GetTempPath(), "sky-sniper-fit-*").ToHashSet();
        var context = new MLContext(seed: 17);
        Assert.Throws<TimeoutException>(() =>
            IsolatedFastTreeTrainer.Fit(context, TrainingData(context), 1, 50, 17, TimeSpan.FromMilliseconds(1)));
        Assert.That(Directory.GetDirectories(Path.GetTempPath(), "sky-sniper-fit-*").Except(before), Is.Empty);
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
                foreach (var argument in new[] { input, output, "1", "50", "17" })
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
