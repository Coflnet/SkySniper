using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Sniper.Models;
using MessagePack;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Configuration;
using Minio;

namespace Coflnet.Sky.Sniper.Services
{
    public class MinioPersistanceManager : IPersitanceManager
    {
        private IConfiguration config;
        private SniperService service;

        public MinioPersistanceManager(IConfiguration config, SniperService service)
        {
            this.config = config;
            this.service = service;
        }

        public async Task LoadLookups()
        {
            var client = new MinioClient(config["MINIO_HOST"], config["MINIO_KEY"], config["MINIO_SECRET"]);
            List<string> items = await GetIemIds(client);
            foreach (var itemName in items)
            {
                await LoadItem(client, itemName);
            }
        }


        public async Task SaveLookups()
        {
            var client = new MinioClient(config["MINIO_HOST"], config["MINIO_KEY"], config["MINIO_SECRET"]);
            using var stream = new MemoryStream();
            await MessagePackSerializer.SerializeAsync(stream, service.Lookups.Keys.ToList());
            stream.Position = 0;
            Console.WriteLine("saving list");
            await client.PutObjectAsync("sky-sniper", "itemList", stream, stream.Length);
            Console.WriteLine("saved list " + stream.Length);
            foreach (var item in service.Lookups)
            {
                using var itemStream = new MemoryStream();
                await MessagePackSerializer.SerializeAsync(itemStream, item.Value);
                itemStream.Position = 0;
                await client.PutObjectAsync("sky-sniper", item.Key, itemStream, itemStream.Length);
            }
        }

        public async Task<List<string>> GetIemIds(MinioClient client)
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

        private async Task LoadItem(MinioClient client, string itemName)
        {
            var result = new MemoryStream();
            await client.GetObjectAsync("sky-sniper", itemName, stream =>
            {
                stream.CopyTo(result);

            });
            result.Position = 0;
            var loadedVal = await MessagePackSerializer.DeserializeAsync<PriceLookup>(result);
            service.AddLookupData(itemName,loadedVal);
        }
    }
}