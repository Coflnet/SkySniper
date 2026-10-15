using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.ML.Trainers.FastTree;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>FastTree forces full GCs while fitting; run those in a short-lived process, not the serving process.</summary>
public static class IsolatedFastTreeTrainer
{
    private const string Mode = "--fit-fast-tree";

    public static RegressionPredictionTransformer<FastTreeRegressionModelParameters> Fit(
        MLContext context, IDataView data, int minimumExampleCountPerLeaf, int numberOfTrees,
        int seed, TimeSpan? timeout = null)
    {
        var duration = timeout ?? TimeSpan.FromMinutes(2);
        if (duration <= TimeSpan.Zero || duration.TotalMilliseconds > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(timeout));
        var directory = Directory.CreateTempSubdirectory("sky-sniper-fit-");
        try
        {
            var input = Path.Combine(directory.FullName, "training.idv");
            var output = Path.Combine(directory.FullName, "model.zip");
            using (var stream = File.Create(input)) context.Data.SaveAsBinary(data, stream);
            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
            {
                UseShellExecute = false,
                RedirectStandardError = true
            };
            foreach (var argument in new[] { typeof(IsolatedFastTreeTrainer).Assembly.Location, Mode, input, output,
                minimumExampleCountPerLeaf.ToString(CultureInfo.InvariantCulture), numberOfTrees.ToString(CultureInfo.InvariantCulture),
                seed.ToString(CultureInfo.InvariantCulture) }) start.ArgumentList.Add(argument);
            using var process = Process.Start(start) ?? throw new InvalidOperationException("Could not start isolated FastTree trainer");
            var error = ReadError(process.StandardError);
            try
            {
                if (!process.WaitForExit((int)Math.Ceiling(duration.TotalMilliseconds)))
                    throw new TimeoutException("Isolated FastTree training exceeded its time limit");
                if (process.ExitCode != 0)
                    throw new InvalidOperationException($"Isolated FastTree training failed ({process.ExitCode}): {error.GetAwaiter().GetResult()}");
                return (RegressionPredictionTransformer<FastTreeRegressionModelParameters>)context.Model.Load(output, out _);
            }
            finally
            {
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                    if (!process.WaitForExit(5000)) throw new TimeoutException("Isolated FastTree trainer did not stop");
                }
                error.GetAwaiter().GetResult();
            }
        }
        finally { directory.Delete(recursive: true); }
    }

    /// <summary>Handle the internal child mode before host configuration or OpenBao initialization.</summary>
    public static bool TryRun(string[] args)
    {
        if (args.Length == 0 || args[0] != Mode) return false;
        if (args.Length != 6) throw new ArgumentException("Invalid isolated FastTree arguments");
        var context = new MLContext(seed: int.Parse(args[5], CultureInfo.InvariantCulture));
        var data = context.Data.LoadFromBinary(args[1]);
        var trainer = context.Regression.Trainers.FastTree(featureColumnName: "Features", labelColumnName: "TrainLabel",
            numberOfLeaves: 20, minimumExampleCountPerLeaf: int.Parse(args[3], CultureInfo.InvariantCulture),
            numberOfTrees: int.Parse(args[4], CultureInfo.InvariantCulture), learningRate: 0.2);
        context.Model.Save(trainer.Fit(data), data.Schema, args[2]);
        return true;
    }

    private static async Task<string> ReadError(StreamReader reader)
    {
        var result = new StringBuilder();
        var buffer = new char[512];
        int count;
        while ((count = await reader.ReadAsync(buffer)) != 0)
            if (result.Length < 2048) result.Append(buffer, 0, Math.Min(count, 2048 - result.Length));
        return result.ToString();
    }
}
