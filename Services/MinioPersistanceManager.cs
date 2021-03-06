using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Sniper.Models;
using MessagePack;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Minio;

namespace Coflnet.Sky.Sniper.Services
{
    public class MinioPersistanceManager : IPersitanceManager
    {
        private IConfiguration config;
        private ILogger<MinioPersistanceManager> logger;

        public MinioPersistanceManager(IConfiguration config, ILogger<MinioPersistanceManager> logger)
        {
            this.config = config;
            this.logger = logger;
        }

        public async Task LoadLookups(SniperService service)
        {
            var client = new MinioClient(config["MINIO_HOST"], config["MINIO_KEY"], config["MINIO_SECRET"]);
            List<string> items = await GetIemIds(client);
            foreach (var itemTag in items)
            {
                try
                {
                    PriceLookup lookup = null;
                    try
                    {
                        lookup = await LoadItem(client, itemTag);
                    }
                    catch (Exception ex)
                    {
                        await Task.Delay(300);
                        logger.LogError(ex, "Could not load item once " + itemTag);
                        // retry
                        lookup = await LoadItem(client, itemTag);
                    }
                    service.AddLookupData(itemTag, lookup);
                }
                catch (Exception e)
                {
                    await Task.Delay(200);
                    logger.LogError(e, "Could not load item twice " + itemTag);
                }
            }
        }


        public async Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            var client = new MinioClient(config["MINIO_HOST"], config["MINIO_KEY"], config["MINIO_SECRET"]);
            using var stream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(stream, lookups.Keys.ToList());
            stream.Position = 0;
            logger.LogInformation("saving list");
            await client.PutObjectAsync("sky-sniper", "itemList", stream, stream.Length);
            logger.LogInformation("saved list " + stream.Length);
            foreach (var item in lookups)
            {
                using var itemStream = new MemoryStream();
                await MessagePackSerializer.SerializeAsync(itemStream, item.Value);
                itemStream.Position = 0;
                await client.PutObjectAsync("sky-sniper", item.Key, itemStream, itemStream.Length);
                if (!string.IsNullOrEmpty(item.Key) && item.Key.StartsWith('S'))
                    Console.Write(" saved " + item.Key);
            }
            Console.WriteLine();
        }

        private async Task<List<string>> GetIemIds(MinioClient client)
        {
            List<string> items = new List<string>();
            if (!await client.BucketExistsAsync("sky-sniper"))
                await client.MakeBucketAsync("sky-sniper");

            var response = new MemoryStream();
            await client.GetObjectAsync("sky-sniper", "itemList", stream =>
            {
                stream.CopyTo(response);
            });
            try
            {
                response.Position = 0;
                items = await MessagePackSerializer.DeserializeAsync<List<string>>(response);
            }
            catch (Exception e)
            {
                logger.LogError(e, "failed to load ids " + response.Length);
            }
            return items;
        }

        private async Task<PriceLookup> LoadItem(MinioClient client, string itemName)
        {
            var result = new MemoryStream();
            await client.GetObjectAsync("sky-sniper", itemName, stream =>
            {
                stream.CopyTo(result);

            });
            result.Position = 0;
            return await MessagePackSerializer.DeserializeAsync<PriceLookup>(result);
            //service.AddLookupData(itemName,loadedVal);
        }
    }
}