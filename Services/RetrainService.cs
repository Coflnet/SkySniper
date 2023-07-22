using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Coflnet.Sky.Sniper.Services;

public class RetrainService : BackgroundService
{
    private PartialCalcService partialCalcService;
    private InternalDataLoader internalDataLoader;
    private IConnectionMultiplexer redis;
    private ILogger<RetrainService> logger;
    string streamName = "retrain";
    string groupName = "retrain";
    private Dictionary<string, DateTime> lastRetrain = new();

    public RetrainService(PartialCalcService partialCalcService, InternalDataLoader internalDataLoader, IConnectionMultiplexer redis, ILogger<RetrainService> logger)
    {
        this.partialCalcService = partialCalcService;
        this.internalDataLoader = internalDataLoader;
        this.redis = redis;
        this.logger = logger;

        this.internalDataLoader.FoundPartialFlip += (flip) =>
        {
            if (flip.TargetPrice / flip.Auction.StartingBid < 2 || flip.TargetPrice - flip.Auction.StartingBid < 1_000_000)
                return;
            SheduleRetrain(flip.Auction.Tag);
        };
    }

    public void SheduleRetrain(string tag)
    {
        if (!partialCalcService.ItemKeys.Contains(tag))
        {
            logger.LogWarning("Blocked retrain for unknown item " + tag);
            return;
        }
        if (lastRetrain.TryGetValue(tag, out var last) && last > DateTime.UtcNow.AddMinutes(-20))
        {
            return;
        }
        var db = redis.GetDatabase();
        db.StreamAdd("retrain", new[] { new NameValueEntry("tag", tag) });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // optain lock 
        var db = redis.GetDatabase();
        RedisValue token = Environment.MachineName;
        if (!(await db.KeyExistsAsync(streamName)) ||
            (await db.StreamGroupInfoAsync(streamName)).All(x => x.Name != groupName))
        {
            await db.StreamCreateConsumerGroupAsync(streamName, groupName, "0-0", true);
        }
        var sub = redis.GetSubscriber();
        sub.Subscribe("retrained", (channel, value) =>
        {
            // check if we are the one who retrained
            if (value == Environment.MachineName)
                return;
            logger.LogInformation("Loading retrained from " + value);
            _ = partialCalcService.Load();
        });
        while (!stoppingToken.IsCancellationRequested)
        {
            if (db.LockTake(streamName + "lock", token, TimeSpan.FromMinutes(10)))
            {
                logger.LogInformation("Optained retrain lock " + token);
                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        await RetrainOne(db, stoppingToken);
                        db.LockExtend(streamName + "lock", token, TimeSpan.FromMinutes(10));
                        logger.LogInformation("Extended retrain lock");
                    }
                }
                catch (System.Exception e)
                {
                    logger.LogError(e, "Failed to retrain");
                }
                finally
                {
                    db.LockRelease(streamName + "lock", token);
                }
            }
            else
            {
                var lockInfo = await db.LockQueryAsync(streamName + "lock");
                if (Random.Shared.NextDouble() < 0.05)
                    logger.LogInformation("could not optain retrain lock - " + lockInfo);
                if (lockInfo == token)
                {
                    db.LockRelease(streamName + "lock", token);
                    logger.LogInformation("Released own retrain lock");
                }
            }
            await Task.Delay(TimeSpan.FromSeconds(20), stoppingToken);
        }
    }

    private async Task RetrainOne(IDatabase db, CancellationToken stoppingToken)
    {
        // read oldest entry
        var entry = await db.StreamReadGroupAsync(streamName, groupName, Environment.MachineName, ">", 1);
        if (entry.Length == 0)
        {
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            return;
        }
        var id = entry[0].Id;
        var tag = entry[0].Values.First().Value.ToString();
        if (lastRetrain.TryGetValue(tag, out var last) && last > DateTime.UtcNow.AddHours(-1))
        {
            logger.LogInformation("Skipping " + tag + " because it was retrained recently");
            await db.StreamAcknowledgeAsync(streamName, groupName, id);
            return;
        }
        lastRetrain[tag] = DateTime.UtcNow;
        logger.LogInformation("Retraining " + tag);
        try
        {
            var trainToken = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, new CancellationTokenSource(TimeSpan.FromMinutes(8)).Token).Token;
            await internalDataLoader.PartialAnalysis(tag, trainToken, DateTime.UtcNow - TimeSpan.FromDays(14));
        }
        catch (System.Exception e)
        {
            logger.LogError(e, "Failed to retrain " + tag);
        }

        var lockInfo = await db.LockQueryAsync(streamName + "lock");
        await db.StreamAcknowledgeAsync(streamName, groupName, id);
        if (lockInfo != Environment.MachineName)
            return;
        await partialCalcService.Save();
        await db.PublishAsync("retrained", Environment.MachineName);
        logger.LogInformation($"Saved retrain results for {tag}");
    }
}
#nullable disable
