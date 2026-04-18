using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using MessagePack;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

#nullable enable

namespace Coflnet.Sky.Sniper.Services
{
    public class S3PersistanceManager : IPersitanceManager
    {
        private const string BucketName = "sky-sniper";
        private const string PartialObjectStoreKey = "partialsMidJuly";
        private readonly IConfiguration config;
        private readonly ILogger<S3PersistanceManager> logger;
        private readonly AmazonS3Client s3Client;
        private readonly ICraftCostService craftCostService;
        internal const short StaleGroupAgeDays = 2;

        public S3PersistanceManager(IConfiguration config, ILogger<S3PersistanceManager> logger, ICraftCostService craftCostService)
        {
            this.config = config;
            this.logger = logger;
            AmazonS3Config awsCofig = new();
            var prefix = "https://";
            if (config["MINIO_HOST"]?.StartsWith("minio") ?? false)
                prefix = "http://";
            awsCofig.ServiceURL = prefix + (config["S3_HOST"] ?? config["MINIO_HOST"]);

            awsCofig.RequestChecksumCalculation = Amazon.Runtime.RequestChecksumCalculation.WHEN_REQUIRED;
            awsCofig.ResponseChecksumValidation = Amazon.Runtime.ResponseChecksumValidation.WHEN_REQUIRED;
            // use path style access
            awsCofig.ForcePathStyle = true;

            s3Client = new AmazonS3Client(
                    config["ACCESS_KEY"] ?? config["MINIO_KEY"],
                    config["SECRET_KEY"] ?? config["MINIO_SECRET"],
                    awsCofig
                    );
            this.craftCostService = craftCostService;
        }

        public async Task LoadLookups(SniperService service)
        {
            logger.LogInformation("loading groups ");
            var attempts = 0;
            await Parallel.ForEachAsync(Enumerable.Range(0, 100), new ParallelOptions()
            {
                MaxDegreeOfParallelism = 3
            }, async (groupId, cancleToken) =>
            {
                while (true)
                    try
                    {
                        var lookups = await LoadGroup(groupId);
                        logger.LogInformation("loaded " + groupId + " " + lookups.Count);
                        foreach (var lookup in lookups)
                            service.AddLookupData(lookup.Key, lookup.Value);
                        break;
                    }
                    catch (Exception e)
                    {
                        if(e.Message.Contains("The specified bucket does not exist"))
                            break; // no data yet
                        await Task.Delay(200);
                        logger.LogError(e, "Could not load group {groupId}, first item", groupId);
                        attempts++;
                        if (attempts > 1000)
                            break;
                    }
            });
        }


        public async Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            // group by md5 hash
            await SaveGroups(lookups);
            await SaveCrafts();
        }

        public async Task FlushDueGroups(ConcurrentDictionary<string, PriceLookup> lookups, TimeSpan maxAge, CancellationToken cancellationToken = default)
        {
            var dueGroups = new List<IGrouping<int, KeyValuePair<string, PriceLookup>>>();
            foreach (var group in GetGroups(lookups).OrderBy(_ => Random.Shared.Next()))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!await IsGroupFlushDue(group.Key, maxAge, cancellationToken))
                    continue;

                dueGroups.Add(group);
            }

            foreach (var group in dueGroups)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await SaveGroup(group.Key, group.ToList());
                }
                catch (Exception e)
                {
                    logger.LogError(e, "failed to flush group {groupId}", group.Key);
                }
            }

            if (dueGroups.Count > 0)
                logger.LogInformation("Flushed {groupCount} persisted groups older than {maxAge}", dueGroups.Count, maxAge);
        }

        private async Task SaveCrafts()
        {
            var craftCost = craftCostService.Costs;
            using var stream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(stream, craftCost, GroupOptions());
            await SaveData("group-craft", stream);
        }

        private async Task SaveGroups(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            IEnumerable<IGrouping<int, KeyValuePair<string, PriceLookup>>> grouped = GetGroups(lookups);
            foreach (var group in grouped)
            {
                try
                {
                    await SaveGroup(group.Key, group.ToList());
                }
                catch (Exception e)
                {
                    logger.LogError(e, "failed to serialize group {groupId}", group.Key);
                }
            }
        }

        public static IEnumerable<IGrouping<int, KeyValuePair<string, PriceLookup>>> GetGroups(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            return lookups.GroupBy(l => GetMd5HashCode(l));
        }

        private async Task SaveGroup(int key, List<KeyValuePair<string, PriceLookup>> list)
        {
            using var stream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(stream, CreateSerializableGroupCopy(list), GroupOptions());
            await SaveData(GetGroupObjectKey(key), stream);
        }

        internal static List<KeyValuePair<string, PriceLookup>> CreateSerializableGroupCopy(List<KeyValuePair<string, PriceLookup>> list)
        {
            return list
                .Where(entry => !string.IsNullOrWhiteSpace(entry.Key) && entry.Value is not null)
                .Select(entry => new KeyValuePair<string, PriceLookup>(entry.Key, CreateSerializableLookupCopy(entry.Value)))
                .ToList();
        }

        internal static PriceLookup CreateSerializableLookupCopy(PriceLookup lookup)
        {
            var sanitizedLookup = new ConcurrentDictionary<AuctionKey, ReferenceAuctions>(
                (lookup.Lookup ?? new ConcurrentDictionary<AuctionKey, ReferenceAuctions>())
                    .Where(entry => entry.Key is not null && entry.Value is not null)
                    .Select(entry => new KeyValuePair<AuctionKey, ReferenceAuctions>(
                        CreateSerializableAuctionKeyCopy(entry.Key),
                        CreateSerializableReferenceAuctionsCopy(entry.Value))));

            return new PriceLookup
            {
                Lookup = sanitizedLookup,
                Category = lookup.Category,
                Volume = lookup.Volume,
                CleanPricePerDay = lookup.CleanPricePerDay ?? new Dictionary<short, long>(),
                CleanKey = GetCleanKeyCopy(lookup.CleanKey, sanitizedLookup),
                CleanPricePerTier = lookup.CleanPricePerTier ?? new Dictionary<Tier, long>(),
                HasMultipleRarities = lookup.HasMultipleRarities
            };
        }

        internal static AuctionKey CreateSerializableAuctionKeyCopy(AuctionKey key)
        {
            if (key is null)
                return new AuctionKey();

            return new AuctionKey(
                key.Enchants?.ToList() ?? new List<Enchant>(),
                key.Reforge,
                (key.Modifiers ?? AuctionKey.EmptyModifiers)
                    .Where(modifier => !string.IsNullOrWhiteSpace(modifier.Key))
                    .Select(modifier => new KeyValuePair<string, string>(modifier.Key, modifier.Value ?? string.Empty))
                    .ToList(),
                key.Tier,
                key.Count);
        }

        internal static ReferenceAuctions CreateSerializableReferenceAuctionsCopy(ReferenceAuctions bucket)
        {
            if (bucket == null)
                return new ReferenceAuctions();

            return new ReferenceAuctions
            {
                Price = bucket.Price,
                References = bucket.References != null ? new ConcurrentQueue<ReferencePrice>(bucket.References) : new ConcurrentQueue<ReferencePrice>(),
                OldestRef = bucket.OldestRef,
                Lbins = bucket.Lbins?.ToList() ?? new List<ReferencePrice>(),
                HitsSinceCalculating = bucket.HitsSinceCalculating,
                Volatility = bucket.Volatility,
                Volume = bucket.Volume
            };
        }

        internal static AuctionKey GetCleanKeyCopy(AuctionKey cleanKey, ConcurrentDictionary<AuctionKey, ReferenceAuctions> sanitizedLookup)
        {
            var sanitizedCleanKey = CreateSerializableAuctionKeyCopy(cleanKey);
            if (cleanKey is not null && sanitizedLookup.ContainsKey(sanitizedCleanKey))
                return sanitizedCleanKey;

            return sanitizedLookup
                .Where(entry => !(entry.Key.Modifiers ?? AuctionKey.EmptyModifiers).Any(modifier => modifier.Key == "virtual"))
                .OrderByDescending(entry => entry.Value.Volume - (entry.Key.Modifiers?.Count ?? 0) * 5)
                .Select(entry => entry.Key)
                .FirstOrDefault() ?? sanitizedCleanKey;
        }

        private async Task SaveData(string key, MemoryStream stream)
        {
            stream.Position = 0;
            var length = stream.Length;
            try
            {
                var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest()
                {
                    BucketName = BucketName,
                    Key = key,
                    DisablePayloadSigning = true,
                    InputStream = stream
                });
                Console.Write($" saved group {key} {length} {putResponse.HttpStatusCode}");
            }
            catch (Exception e)
            {
                logger.LogError(e, "failed to save group " + key);
            }
        }

        private static MessagePackSerializerOptions GroupOptions()
        {
            return MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray);
        }

        private static int GetMd5HashCode(KeyValuePair<string, PriceLookup> l)
        {
            // modulate the hash to get a number between 0 and 100
            return Math.Abs(BitConverter.ToInt32(MD5.HashData(System.Text.Encoding.UTF8.GetBytes(l.Key)), 0) % 100);
        }

        private async Task SaveLookup(string tag, PriceLookup lookup)
        {
            var day = SniperService.GetDay();
            if (!lookup.Lookup.Any(l => l.Value.References.Any(r => r.Day >= day)))
                return; // has not changed in the last day
            using var itemStream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(itemStream, lookup);
            itemStream.Position = 0;
            var length = itemStream.Length;
            try
            {
                var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest()
                {
                    BucketName = BucketName,
                    Key = tag,
                    DisablePayloadSigning = true,
                    InputStream = itemStream
                });
                if (!string.IsNullOrEmpty(tag) && tag.StartsWith('S') || tag == "test")
                    Console.Write($" saved {tag} {length} {putResponse.HttpStatusCode}");
            }
            catch (System.Exception e)
            {
                logger.LogError(e, "failed to save " + tag);
            }
        }

        private async Task<List<string>> GetIemIds()
        {
            List<string> items = new List<string>();

            try
            {
                await s3Client.PutBucketAsync(BucketName);
            }
            catch (System.Exception e)
            {
                // bucket already exists
                if (e.Message.Contains("succeeded"))
                    logger.LogInformation("bucket already exists or other error while creating");
                else
                    logger.LogError(e, "failed to create bucket");
            }
            using var response = await s3Client.GetObjectAsync(BucketName, "itemList");
            // var response = await GetStreamForObject(client, "itemList");
            try
            {
                items = await MessagePackSerializer.DeserializeAsync<List<string>>(response.ResponseStream);
                logger.LogInformation("loaded ids " + items.Count);
            }
            catch (Exception e)
            {
                logger.LogError(e, "failed to load ids " + response.PartsCount);
            }
            return items;
        }

        private async Task<Stream> GetStreamForObject(string objectName)
        {
            var response = await s3Client.GetObjectAsync(BucketName, objectName);
            var stream = response.ResponseStream;
            return stream;
        }

        public async Task<Dictionary<string,double>> LoadCraftCost()
        {
            using var result = await GetStreamForObject("group-craft");
            return await MessagePackSerializer.DeserializeAsync<Dictionary<string,double>>(result, GroupOptions());
        }

        public async Task SaveBlob(string key, Stream data)
        {
            try
            {
                // Copy the provided stream into a local memory stream so we don't depend on the
                // caller's stream lifetime (the caller may dispose the stream after calling SaveBlob).
                try
                {
                    data.Position = 0;
                }
                catch { /* ignore if stream is not seekable */ }

                using var buffer = new MemoryStream();
                await data.CopyToAsync(buffer);
                buffer.Position = 0;

                var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest()
                {
                    BucketName = BucketName,
                    Key = key,
                    DisablePayloadSigning = true,
                    InputStream = buffer
                });
                logger.LogInformation("Saved blob {Key} status {Status}", key, putResponse.HttpStatusCode);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to save blob {Key}", key);
            }
        }

        public async Task<Stream> LoadBlob(string key)
        {
            var response = await s3Client.GetObjectAsync(BucketName, key);
            var ms = new MemoryStream();
            await response.ResponseStream.CopyToAsync(ms);
            ms.Position = 0;
            return ms;
        }

        public async Task<List<KeyValuePair<string, PriceLookup>>> LoadGroup(int key)
        {
            using var response = await s3Client.GetObjectAsync(BucketName, GetGroupObjectKey(key));
            var loaded = await MessagePackSerializer.DeserializeAsync<List<KeyValuePair<string, PriceLookup>>>(response.ResponseStream, GroupOptions())
                ?? new List<KeyValuePair<string, PriceLookup>>();
            var sanitized = CreateSerializableGroupCopy(loaded);
            var currentDay = SniperService.GetDay();
            var newestObservedDay = GetNewestObservedDay(sanitized);
            if (IsGroupStale(sanitized, currentDay))
            {
                logger.LogWarning(
                    "Skipped stale persisted group {groupId}; newest observed day {newestObservedDay}, current day {currentDay}, last modified {lastModified:o}",
                    key,
                    newestObservedDay,
                    currentDay,
                    response.LastModified?.ToUniversalTime());
                return new List<KeyValuePair<string, PriceLookup>>();
            }

            return sanitized;
        }

        internal static bool IsGroupStale(IEnumerable<KeyValuePair<string, PriceLookup>> group, short currentDay, short staleGroupAgeDays = StaleGroupAgeDays)
        {
            var newestObservedDay = GetNewestObservedDay(group);
            if (newestObservedDay == short.MinValue)
                return false;

            var cutoffDay = (short)(currentDay - staleGroupAgeDays);
            return newestObservedDay < cutoffDay;
        }

        internal static short GetNewestObservedDay(IEnumerable<KeyValuePair<string, PriceLookup>> group)
        {
            if (group == null)
                return short.MinValue;

            return group
                .Where(entry => entry.Value != null)
                .Select(entry => GetNewestObservedDay(entry.Value))
                .DefaultIfEmpty(short.MinValue)
                .Max();
        }

        internal static short GetNewestObservedDay(PriceLookup lookup)
        {
            if (lookup?.Lookup == null)
                return short.MinValue;

            return lookup.Lookup.Values
                .Where(bucket => bucket != null)
                .Select(GetNewestObservedDay)
                .DefaultIfEmpty(short.MinValue)
                .Max();
        }

        internal static bool ShouldFlushGroup(DateTime? lastModifiedUtc, DateTime utcNow, TimeSpan maxAge)
        {
            if (!lastModifiedUtc.HasValue)
                return true;

            return utcNow - lastModifiedUtc.Value.ToUniversalTime() >= maxAge;
        }

        private async Task<bool> IsGroupFlushDue(int key, TimeSpan maxAge, CancellationToken cancellationToken)
        {
            var lastModified = await GetGroupLastModified(key, cancellationToken);
            return ShouldFlushGroup(lastModified, DateTime.UtcNow, maxAge);
        }

        private async Task<DateTime?> GetGroupLastModified(int key, CancellationToken cancellationToken)
        {
            try
            {
                var metadata = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                {
                    BucketName = BucketName,
                    Key = GetGroupObjectKey(key)
                }, cancellationToken);

                return metadata.LastModified?.ToUniversalTime();
            }
            catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound || string.Equals(e.ErrorCode, "NoSuchKey", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }
        }

        private static string GetGroupObjectKey(int key)
        {
            return "group" + key;
        }

        internal static short GetNewestObservedDay(ReferenceAuctions bucket)
        {
            if (bucket == null)
                return short.MinValue;

            var newestReferenceDay = bucket.References?
                .Select(reference => reference.Day)
                .DefaultIfEmpty(short.MinValue)
                .Max() ?? short.MinValue;

            var newestLbinDay = bucket.Lbins?
                .Select(reference => reference.Day)
                .DefaultIfEmpty(short.MinValue)
                .Max() ?? short.MinValue;

            return Math.Max(newestReferenceDay, newestLbinDay);
        }

        public async Task<ConcurrentDictionary<string, AttributeLookup>> GetWeigths()
        {
            using var result = await GetStreamForObject(PartialObjectStoreKey);
            return await MessagePackSerializer.DeserializeAsync<ConcurrentDictionary<string, AttributeLookup>>(result);
        }

        public async Task SaveWeigths(ConcurrentDictionary<string, AttributeLookup> lookups)
        {
            using var stream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(stream, lookups);
            stream.Position = 0;
            logger.LogInformation("saving partials data " + stream.Length);
            await s3Client.PutObjectAsync(new PutObjectRequest()
            {
                BucketName = BucketName,
                Key = PartialObjectStoreKey,
                DisablePayloadSigning = true,
                InputStream = stream
            });
        }
    }
}