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
        //private SniperService service;
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

                var lookup = await LoadItem(client, itemTag);
                service.AddLookupData(itemTag, lookup);
                } catch(Exception e)
                {
                    logger.LogError(e,"Could not load item " + itemTag);
                }
            }
        }


        public async Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups)
        {
            var client = new MinioClient(config["MINIO_HOST"], config["MINIO_KEY"], config["MINIO_SECRET"]);
            using var stream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(stream, lookups.Keys.ToList());
            stream.Position = 0;
            Console.WriteLine("saving list");
            await client.PutObjectAsync("sky-sniper", "itemList", stream, stream.Length);
            Console.WriteLine("saved list " + stream.Length);
            foreach (var item in lookups)
            {
                using var itemStream = new MemoryStream();
                await MessagePackSerializer.SerializeAsync(itemStream, item.Value);
                itemStream.Position = 0;
                await client.PutObjectAsync("sky-sniper", item.Key, itemStream, itemStream.Length);
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
                Console.WriteLine("failed to load ids " + response.Length);

                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
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