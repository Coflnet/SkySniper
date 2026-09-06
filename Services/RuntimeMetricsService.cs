using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Prometheus;

namespace Coflnet.Sky.Sniper.Services;

/// <summary>
/// Exposes the runtime signals needed to distinguish pricing work from GC or thread-pool stalls.
/// </summary>
public sealed class RuntimeMetricsService : BackgroundService
{
    private static readonly Gauge HeapSize = Metrics.CreateGauge("sky_dotnet_gc_heap_bytes", "Managed heap size in bytes");
    private static readonly Gauge Fragmented = Metrics.CreateGauge("sky_dotnet_gc_fragmented_bytes", "Fragmented managed heap bytes");
    private static readonly Gauge Allocated = Metrics.CreateGauge("sky_dotnet_gc_allocated_bytes", "Total managed bytes allocated since process start");
    private static readonly Gauge LastPause = Metrics.CreateGauge("sky_dotnet_gc_last_pause_seconds", "Combined pause duration of the most recent GC");
    private static readonly Gauge PausePercent = Metrics.CreateGauge("sky_dotnet_gc_pause_time_percent", "Percentage of time paused by GC in the latest GC window");
    private static readonly Gauge Collections = Metrics.CreateGauge("sky_dotnet_gc_collections", "GC collection count since process start", ["generation"]);
    private static readonly Gauge ThreadPoolThreads = Metrics.CreateGauge("sky_dotnet_threadpool_threads", "Current thread-pool thread count");
    private static readonly Gauge ThreadPoolPending = Metrics.CreateGauge("sky_dotnet_threadpool_pending_work_items", "Pending thread-pool work items");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                UpdateMetrics();
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    internal static void UpdateMetrics()
    {
        var info = GC.GetGCMemoryInfo();
        var pause = TimeSpan.Zero;
        foreach (var duration in info.PauseDurations)
            pause += duration;

        HeapSize.Set(info.HeapSizeBytes);
        Fragmented.Set(info.FragmentedBytes);
        Allocated.Set(GC.GetTotalAllocatedBytes(false));
        LastPause.Set(pause.TotalSeconds);
        PausePercent.Set(info.PauseTimePercentage);
        for (var generation = 0; generation <= GC.MaxGeneration; generation++)
            Collections.WithLabels(generation.ToString()).Set(GC.CollectionCount(generation));
        ThreadPoolThreads.Set(ThreadPool.ThreadCount);
        ThreadPoolPending.Set(ThreadPool.PendingWorkItemCount);
    }
}
