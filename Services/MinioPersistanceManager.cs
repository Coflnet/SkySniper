using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        private IConfiguration config;
        private ILogger<S3PersistanceManager> logger;
        private AmazonS3Client s3Client;

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
            List<string> items = await GetIemIds();
            logger.LogInformation("loaded item ids " + items.Count);
            await Parallel.ForEachAsync(items, new ParallelOptions()
            {
                MaxDegreeOfParallelism = 5
            }, async (itemTag, cancleToken) =>
            {
                try
                {
                    PriceLookup lookup = new();
                    try
                    {
                        lookup = await LoadItem(itemTag);
                        if (lookup.Lookup.Count > 500)
                            logger.LogInformation("loaded " + itemTag + " " + lookup.Lookup.Count);
                    }
                    catch (Exception ex)
                    {
                        await Task.Delay(5000);
                        logger.LogError(ex, "Could not load item once " + itemTag);
                        // retry
                        lookup = await LoadItem(itemTag);
                    }
                    service.AddLookupData(itemTag, lookup);
                }
                catch (Exception e)
                {
                    await Task.Delay(200);
                    await SaveLookup(itemTag, new PriceLookup());
                    logger.LogError(e, "Could not load item twice " + itemTag);
                }
            });
        }


        public async Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            using var stream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(stream, lookups.Keys.ToList());
            stream.Position = 0;
            logger.LogInformation("saving list" + stream.Length);
            // upload to s3
            await s3Client.PutObjectAsync(new PutObjectRequest()
            {
                BucketName = "sky-sniper",
                Key = "itemList",
                InputStream = stream
            });
            logger.LogInformation("saved list ");
            await Parallel.ForEachAsync(lookups, new ParallelOptions()
            {
                MaxDegreeOfParallelism = 10
            }, async (item, cancleToken) =>
            {
                await SaveLookup(item.Key, item.Value);
            });
            Console.WriteLine();
        }

        private async Task SaveLookup(string tag, PriceLookup lookup)
        {
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
                InputStream = stream
            });
        }
    }
}