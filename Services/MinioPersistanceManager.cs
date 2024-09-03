using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Coflnet.Sky.Sniper.Models;
using MessagePack;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper.Services
{
    public class S3PersistanceManager : IPersitanceManager
    {
        private const string PartialObjectStoreKey = "partialsMidJuly";
        private readonly IConfiguration config;
        private readonly ILogger<S3PersistanceManager> logger;
        private readonly AmazonS3Client s3Client;
        private DateTime LastSave = DateTime.Now;

        public S3PersistanceManager(IConfiguration config, ILogger<S3PersistanceManager> logger)
        {
            this.config = config;
            this.logger = logger;
            AmazonS3Config awsCofig = new AmazonS3Config();
            var prefix = "https://";
            if (config["MINIO_HOST"]?.StartsWith("minio") ?? false)
                prefix = "http://";
            awsCofig.ServiceURL = prefix + (config["S3_HOST"] ?? config["MINIO_HOST"]);
            // use path style access
            awsCofig.ForcePathStyle = true;

            s3Client = new AmazonS3Client(
                    config["ACCESS_KEY"] ?? config["MINIO_KEY"],
                    config["SECRET_KEY"] ?? config["MINIO_SECRET"],
                    awsCofig
                    );
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
            using var stream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(stream, lookups.Keys.ToList());
            stream.Position = 0;
            logger.LogInformation("saving list" + stream.Length);
            // group by md5 hash
            await SaveGroups(lookups);
            return;
            // upload to s3
            await s3Client.PutObjectAsync(new PutObjectRequest()
            {
                BucketName = "sky-sniper",
                Key = "itemList",
                DisablePayloadSigning = true,
                InputStream = stream
            });
            logger.LogInformation("saved list ");
            await Parallel.ForEachAsync(lookups, new ParallelOptions()
            {
                MaxDegreeOfParallelism = 2
            }, async (item, cancleToken) =>
            {
                await SaveLookup(item.Key, item.Value);
            });
            Console.WriteLine();
        }

        private async Task SaveGroups(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            IEnumerable<IGrouping<int, KeyValuePair<string, PriceLookup>>> grouped = GetGroups(lookups);
            foreach (var group in grouped)
            {
                await SaveGroup(group.Key, group.ToList());
            }
        }

        public static IEnumerable<IGrouping<int, KeyValuePair<string, PriceLookup>>> GetGroups(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            return lookups.GroupBy(l => GetMd5HashCode(l));
        }

        private async Task SaveGroup(int key, List<KeyValuePair<string, PriceLookup>> list)
        {
            using var stream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(stream, list, GroupOptions());
            stream.Position = 0;
            var length = stream.Length;
            try
            {
                var putResponse = await s3Client.PutObjectAsync(new PutObjectRequest()
                {
                    BucketName = "sky-sniper",
                    Key = "group" + key,
                    DisablePayloadSigning = true,
                    InputStream = stream
                });
                Console.Write($" saved group {key} {length} {putResponse.HttpStatusCode}");
            }
            catch (Exception e)
            {
                logger.LogError(e, "failed to save group " + list.First().Key);
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
                    BucketName = "sky-sniper",
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
                await s3Client.PutBucketAsync("sky-sniper");
            }
            catch (System.Exception e)
            {
                // bucket already exists
                if (e.Message.Contains("succeeded"))
                    logger.LogInformation("bucket already exists or other error while creating");
                else
                    logger.LogError(e, "failed to create bucket");
            }
            using var response = await s3Client.GetObjectAsync("sky-sniper", "itemList");
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
            var response = await s3Client.GetObjectAsync("sky-sniper", objectName);
            var stream = response.ResponseStream;
            return stream;
        }

        private async Task<PriceLookup> LoadItem(string itemName)
        {
            using var result = await GetStreamForObject(itemName);
            return await MessagePackSerializer.DeserializeAsync<PriceLookup>(result);
        }

        public async Task<List<KeyValuePair<string, PriceLookup>>> LoadGroup(int key)
        {
            using var result = await GetStreamForObject("group" + key);
            return await MessagePackSerializer.DeserializeAsync<List<KeyValuePair<string, PriceLookup>>>(result, GroupOptions());
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
                BucketName = "sky-sniper",
                Key = PartialObjectStoreKey,
                DisablePayloadSigning = true,
                InputStream = stream
            });
        }
    }
}