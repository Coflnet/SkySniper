using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services;

public class ConsumeRetryLoopTests
{
    private static Task NoDelay(TimeSpan _, CancellationToken __) => Task.CompletedTask;

    [Test]
    public void Backoff_DoublesFromTwoSecondsToSixtyAndResets()
    {
        var b = new ConsumeBackoff();
        var waits = Enumerable.Range(0, 7).Select(_ => b.Next().TotalSeconds).ToList();
        Assert.That(waits, Is.EqualTo(new double[] { 2, 4, 8, 16, 32, 60, 60 }));
        b.Reset();
        Assert.That(b.Next().TotalSeconds, Is.EqualTo(2));
    }

    [Test]
    public async Task Exception_IsRetriedWithGrowingBackoff()
    {
        using var cts = new CancellationTokenSource();
        var attempts = 0;
        var delays = new List<TimeSpan>();
        await ConsumeRetryLoop.RunAsync(
            () =>
            {
                if (++attempts < 3) throw new InvalidOperationException("preflight");
                cts.Cancel();
                return Task.CompletedTask;
            },
            new ConsumeBackoff(),
            (_, _) => { },
            (d, _) => { delays.Add(d); return Task.CompletedTask; },
            cts.Token);

        Assert.That(attempts, Is.EqualTo(3));
        Assert.That(delays.Select(d => d.TotalSeconds), Is.EqualTo(new double[] { 2, 4 }));
    }

    [Test]
    public async Task NormalReturnWithoutShutdown_IsRetriedAsFailure()
    {
        using var cts = new CancellationTokenSource();
        var attempts = 0;
        var failures = new List<Exception>();
        await ConsumeRetryLoop.RunAsync(
            () =>
            {
                if (++attempts == 2) cts.Cancel();
                return Task.CompletedTask;
            },
            new ConsumeBackoff(),
            (ex, _) => failures.Add(ex),
            NoDelay,
            cts.Token);

        Assert.That(attempts, Is.EqualTo(2));
        Assert.That(failures, Has.Count.EqualTo(1));
        Assert.That(failures[0], Is.TypeOf<InvalidOperationException>());
    }

    [Test]
    public async Task ResetAfterProcessedBatch_RestartsBackoffAtTwoSeconds()
    {
        using var cts = new CancellationTokenSource();
        var backoff = new ConsumeBackoff();
        var attempts = 0;
        var delays = new List<TimeSpan>();
        await ConsumeRetryLoop.RunAsync(
            () =>
            {
                attempts++;
                if (attempts == 3) backoff.Reset(); // a batch was processed before this failure
                if (attempts == 4) cts.Cancel();
                throw new InvalidOperationException("boom");
            },
            backoff,
            (_, _) => { },
            (d, _) => { delays.Add(d); return Task.CompletedTask; },
            cts.Token);

        Assert.That(delays.Select(d => d.TotalSeconds), Is.EqualTo(new double[] { 2, 4, 2 }));
    }

    [Test]
    public async Task Cancellation_ExitsQuietlyWithoutReportingFailure()
    {
        using var cts = new CancellationTokenSource();
        var attempts = 0;
        var failureReported = false;
        await ConsumeRetryLoop.RunAsync(
            () => { attempts++; cts.Cancel(); throw new OperationCanceledException(); },
            new ConsumeBackoff(),
            (_, _) => failureReported = true,
            NoDelay,
            cts.Token);

        Assert.That(attempts, Is.EqualTo(1));
        Assert.That(failureReported, Is.False);
    }

    [Test]
    public async Task CancellationDuringBackoffDelay_ExitsQuietly()
    {
        using var cts = new CancellationTokenSource();
        var attempts = 0;
        await ConsumeRetryLoop.RunAsync(
            () => { attempts++; throw new InvalidOperationException("boom"); },
            new ConsumeBackoff(),
            (_, _) => { },
            (_, ct) => { cts.Cancel(); ct.ThrowIfCancellationRequested(); return Task.CompletedTask; },
            cts.Token);

        Assert.That(attempts, Is.EqualTo(1));
    }

    [Test]
    public async Task RunTilStopped_CompletedConsumeIsRestartedWithDelayInsteadOfSpinning()
    {
        using var cts = new CancellationTokenSource();
        var invocations = 0;
        var delays = new List<TimeSpan>();
        await InternalDataLoader.RunTilStopped(
            "test",
            _ => { invocations++; return Task.CompletedTask; },
            NullLogger.Instance,
            (d, _) => { delays.Add(d); if (delays.Count == 4) cts.Cancel(); return Task.CompletedTask; },
            cts.Token);

        // a fresh consume call per round, each one preceded by a growing backoff (old code re-awaited one finished task)
        Assert.That(invocations, Is.EqualTo(4));
        Assert.That(delays.Select(d => d.TotalSeconds), Is.EqualTo(new double[] { 2, 4, 8, 16 }));
    }

    [Test]
    public async Task RunTilStopped_ResetFromHandlerRestartsBackoff()
    {
        using var cts = new CancellationTokenSource();
        var invocations = 0;
        var delays = new List<TimeSpan>();
        await InternalDataLoader.RunTilStopped(
            "test",
            backoff =>
            {
                if (++invocations == 3) backoff.Reset();
                throw new InvalidOperationException("boom");
            },
            NullLogger.Instance,
            (d, _) => { delays.Add(d); if (delays.Count == 3) cts.Cancel(); return Task.CompletedTask; },
            cts.Token);

        Assert.That(delays.Select(d => d.TotalSeconds), Is.EqualTo(new double[] { 2, 4, 2 }));
    }
}
