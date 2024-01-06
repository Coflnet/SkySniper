using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Coflnet.Sky.Sniper.Services;

public class RetrainService : BackgroundService
{
    private readonly PartialCalcService partialCalcService;
    private readonly InternalDataLoader internalDataLoader;
    private readonly IConnectionMultiplexer redis;
    private readonly ILogger<RetrainService> logger;
    private readonly IConfiguration configuration;
    readonly string streamName = "retrain";
    readonly string groupName = "retrain";
    private readonly Dictionary<string, DateTime> lastRetrain = new();
    public static bool IsManager = false;

    public RetrainService(
        PartialCalcService partialCalcService,
        InternalDataLoader internalDataLoader,
        IConnectionMultiplexer redis,
        ILogger<RetrainService> logger,
        IConfiguration configuration)
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
        this.configuration = configuration;
    }

    public void SheduleRetrain(string tag)
    {
        if (!partialCalcService.ItemKeys.Contains(tag))
        {
            logger.LogWarning("Blocked retrain for unknown item " + tag);
            return;
        }
        if (lastRetrain.TryGetValue(tag, out var last) && last > DateTime.UtcNow.AddMinutes(-30))
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
        sub.Subscribe(RedisChannel.Literal("retrained"), (channel, value) =>
        {
            // check if we are the one who retrained
            if (value == Environment.MachineName)
                return;
            logger.LogInformation("Loading retrained from " + value);
            _ = partialCalcService.Load();
        });
        if(!bool.TryParse(configuration["IS_MANAGER"], out var isManaer) || !isManaer)
        {
            logger.LogInformation("Retrain disabled");
            IsManager = false;
            return;
        }
        IsManager = true;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await RetrainOne(db, stoppingToken);
                    logger.LogInformation("Extended retrain lock");
                }
            }
            catch (System.Exception e)
            {
                logger.LogError(e, "Failed to retrain");
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

        await db.StreamAcknowledgeAsync(streamName, groupName, id);
        await partialCalcService.Save();
        await db.PublishAsync(RedisChannel.Literal("retrained"), Environment.MachineName);
        logger.LogInformation($"Saved retrain results for {tag}");
    }
}
#nullable disable
