using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Sniper.Services;
using Coflnet.Sky.Core.Prediction;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using MessagePack;

namespace Coflnet.Sky.Sniper.Controllers
{
    [ApiController]
    [Route("/api/[controller]")]
    public class SniperController : ControllerBase
    {
        private readonly ILogger<SniperController> _logger;
        private readonly SniperService service;
        private readonly ITokenService tokenService;
        private readonly ICraftCostService craftCostService;

        public SniperController(ILogger<SniperController> logger, SniperService service, ITokenService tokenService, ICraftCostService craftCostService)
        {
            _logger = logger;
            this.service = service;
            this.tokenService = tokenService;
            this.craftCostService = craftCostService;
        }


        [HttpGet]
        [Route("/ready")]
        public ActionResult IsReady()
        {
            if (service.State >= SniperState.Ready)
                return Ok();
            else
                return StatusCode(503);
        }

        [HttpGet]
        [Route("lookup")]
        [ResponseCache(Duration = 3600, Location = ResponseCacheLocation.Any, NoStore = false)]
        public IEnumerable<string> GetIds()
        {
            return service.Lookups.Keys;
        }

        /// <summary>
        /// Retrieve item lookup state transfer
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="Authorization"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("lookup/{itemId}")]
        public string GetLookup(string itemId, [FromHeader] string Authorization)
        {
            CountUsageAndValidate(Authorization);
            return Convert.ToBase64String(MessagePack.MessagePackSerializer.Serialize(service.Lookups[itemId]));
        }

        [HttpGet]
        [Route("lookup/group/{groupId}")]
        public string GetLookupGroup(int groupId, [FromHeader] string Authorization)
        {
            CountUsageAndValidate(Authorization);
            var grouped = S3PersistanceManager.GetGroups(service.Lookups).Where(g => g.Key == groupId);
            return Convert.ToBase64String(MessagePackSerializer.Serialize(grouped, MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4Block)));
        }

        [HttpGet]
        [Route("lookup/groups")]
        public Dictionary<int,string[]> GetLookupGroups()
        {
            var grouped = S3PersistanceManager.GetGroups(service.Lookups);
            return grouped.ToDictionary(g => g.Key, g => g.Select(l => l.Key).ToArray());
        }

        /// <summary>
        /// Get multiple itemIds at once
        /// </summary>
        /// <param name="itemIds"></param>
        /// <param name="Authorization"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("lookup")]
        public IEnumerable<string> GetLookups(IEnumerable<string> itemIds, [FromHeader] string Authorization)
        {
            CountUsageAndValidate(Authorization);
            return itemIds.Select(i => Convert.ToBase64String(MessagePack.MessagePackSerializer.Serialize(service.Lookups[i])));
        }


        [Route("dump/craftCost")]
        [HttpGet]
        public Dictionary<string, double> DumpCraftCost([FromHeader] string Authorization)
        {
            CountUsageAndValidate(Authorization);
            return craftCostService.Costs;
        }
        /// <summary>
        /// Retrieve item lookup state transfer
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="Authorization"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("dump/{itemId}")]
        public PriceLookup GetLookupJson(string itemId, [FromHeader] string Authorization)
        {
            CountUsageAndValidate(Authorization);
            return service.Lookups[itemId];
        }

        private void CountUsageAndValidate(string Authorization)
        {
            try
            {
                if (!tokenService.HasTokenAccess(Authorization))
                    throw new CoflnetException("invalid_token", "The passed access token is invalid (limit probably used up)");
            }
            catch (JWT.Exceptions.TokenExpiredException)
            {
                throw new CoflnetException("token_expired", "The passed access token is no longer valid");
            }
            catch (System.FormatException)
            {
                throw new CoflnetException("invalid_token", "The passed access token is invalid");
            }
        }

        [HttpGet]
        [Route("token")]
        public string Token()
        {
            return tokenService.CreateToken();
        }


        /// <summary>
        /// Retrieve lookup references
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        [HttpPost]
        [Route("references")]
        public IEnumerable<long> GetLookups(SaveAuction auction)
        {
            return service.GetReferenceUids(auction);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="auctions"></param>
        /// <returns></returns>
        [Route("price")]
        [HttpPost]
        public IEnumerable<PriceEstimate> GetPrices(IEnumerable<ApiSaveAuction> auctions)
        {
            if (auctions == null)
                return new List<PriceEstimate>();
            return auctions.Select(a =>
            {
                try
                {
                    Console.WriteLine("a: " + JsonConvert.SerializeObject(a));
                    return service.GetPrice(a);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "getting price for auction");
                }
                return new PriceEstimate();
            });
        }

        /// <summary>
        /// Auction array as base64 encoded LZ4MessagePack
        /// </summary>
        /// <param name="auctions"></param>
        /// <returns></returns>
        [Route("prices")]
        [HttpPost]
        public IEnumerable<PriceEstimate> GetPrices([FromBody] string data)
        {
            var options = MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4Block);
            var auctions = MessagePackSerializer.Deserialize<IEnumerable<ApiSaveAuction>>(Convert.FromBase64String(data), options);
            return auctions.Select(a =>
            {
                try
                {
                    return service.GetPrice(a);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "getting price for auction");
                }
                return new PriceEstimate();
            });
        }

        [Route("similar/{tag}/{auctionId}")]
        [HttpGet]
        public IEnumerable<object> SimilarKeys(string tag, string auctionId)
        {
            var firstKey = Search(tag, auctionId).FirstOrDefault();
            if (firstKey == default)
                return new List<object>() { new { Key = "not found" } };
            return service.FindClosest(service.Lookups[tag].Lookup, firstKey, tag)
                                .Take(10)
                                .Select(v => new
                                {
                                    Key = v.Key.ToString(),
                                    Price = v.Value.Price,
                                    asJson = JsonConvert.SerializeObject(v.Key),
                                    msgPack = MessagePack.MessagePackSerializer.Serialize(v.Key)
                                })
                                .ToList();
        }

        [Route("similar/{tag}/lbin")]
        [HttpPost]
        public IEnumerable<object> SimilarKeysLbin(string tag, [FromBody] AuctionKey key)
        {
            return service.ClosestLbinKeys(tag, key).Select(v => new
            {
                Key = v.Item1.ToString(),
                v.lbin
            });
        }

        [Route("migrate")]
        [HttpPost]
        public async Task Migrate()
        {
            foreach (var item in service.Lookups)
            {
                var startCount = item.Value.Lookup.Count;
                foreach (var lookup in item.Value.Lookup)
                {
                    var key = new AuctionKey(lookup.Key)
                    {
                        Enchants = lookup.Key.Enchants?.OrderBy(e => e.Type).ToList().AsReadOnly(),
                        Modifiers = lookup.Key.Modifiers?.OrderBy(m => m.Key).ToList().AsReadOnly()
                    };
                    if (key.Modifiers == null)
                        continue;
                    if (key.Enchants == null)
                        continue;
                    if (key == lookup.Key)
                        continue;
                    // move reference to other key
                    var newBucket = item.Value.Lookup.GetOrAdd(key, k => new());
                    foreach (var reference in lookup.Value.References)
                    {
                        if (!newBucket.References.Contains(reference) && newBucket.References.Select(r => r.Day).DefaultIfEmpty((short)0).Min() < reference.Day)
                        {
                            newBucket.References.Enqueue(reference);
                        }
                    }
                    _logger.LogInformation("migrated reference from {oldKey} to {newKey}", lookup.Key, key);
                    await Task.Delay(10);
                    // remove old bucket
                    item.Value.Lookup.TryRemove(lookup.Key, out _);
                    service.UpdateMedian(newBucket);
                }
                _logger.LogInformation("migrated {count} buckets for {tag}", startCount - item.Value.Lookup.Count, item.Key);
                if (startCount != item.Value.Lookup.Count)
                {
                    await Task.Delay(3000);
                }
            }
        }

        [Route("search/{tag}/{itemId}")]
        [HttpGet]
        public IEnumerable<AuctionKey> Search(string tag, string itemId)
        {
            if (!long.TryParse(itemId, out long uid))
                uid = AuctionService.Instance.GetId(itemId);
            foreach (var bucket in service.Lookups[tag].Lookup)
            {
                foreach (var item in bucket.Value.References)
                {
                    if (item.AuctionId == uid)
                        yield return bucket.Key;
                }
            }
        }
        [Route("reassign")]
        [HttpPost]
        public async Task<List<Result>> Reassign(string tag, string value)
        {
            var toChange = Search(tag, value).ToList();
            var toCheck = service.Lookups[tag].Lookup.Where(l => toChange.Contains(l.Key)).SelectMany(l => l.Value.References.Select(r => (l.Key, r))).ToDictionary(r => r.r.AuctionId, r => r);
            List<SaveAuction> auctions = null;
            using (var context = new HypixelContext())
            {
                var uids = toCheck.Keys.Select(k => k).ToList();
                auctions = await context.Auctions.Where(a => uids.Contains(a.UId)).Include(a => a.NbtData).Include(a => a.Enchantments).ToListAsync();
            }

            var result = new List<Result>();
            foreach (var item in auctions)
            {
                var key = service.KeyFromSaveAuction(item);
                var actual = toCheck[item.UId];
                if (key == actual.Key)
                    continue;
                // yikes
                result.Add(new()
                {
                    New = key,
                    Old = actual.Key,
                    Reference = actual.r
                });

                service.Move(tag, actual.r.AuctionId, actual.Key, key);
            }
            return result;
        }

        [Route("prices/clean")]
        [HttpGet]
        [ResponseCache(Duration = 1800, Location = ResponseCacheLocation.Any, NoStore = false)]
        public Dictionary<string, long> CleanPrices()
        {
            var dictionary = service.Lookups.SelectMany(l => l.Value.Lookup
                .Where(l => l.Value.Price > 0)
                .GroupBy(i =>
                {
                    var tier = i.Key.Tier;
                    if (i.Key.Modifiers.Any(m => m.Value == SniperService.TierBoostShorthand))
                        tier = SniperService.ReduceRarity(tier);
                    if (l.Key.StartsWith("PET_") && !l.Key.StartsWith("PET_ITEM_") && !l.Key.StartsWith("PET_SKIN_"))
                        return $"{l.Key}_{tier}_{i.Key.Modifiers?.FirstOrDefault(m => m.Key == "exp").Value switch
                        {
                            "7" => 100,
                            "6" => 100,
                            "5" => 90,
                            _ => 0,
                        }}";
                    if (l.Key.StartsWith("RUNE_"))
                        return $"{l.Key}{(i.Key.Modifiers.Count > 0 ? $"_{i.Key.Modifiers.First().Value}" : "")}";
                    if (i.Key.Modifiers.Any(m => m.Key == "virtual"))
                        return $"{l.Key}+{i.Key.Modifiers.First(m => m.Key != "virtual").Key};1";
                    var modifiers = i.Key.Modifiers?.Where(m => Constants.AttributeKeys.Contains(m.Key)).ToList();
                    if (modifiers.Count > 0)
                        return $"{l.Key}+{string.Join("_", modifiers.Select(m => m.Key))}";
                    return l.Key;
                })
                .Select(g => (g.Key, g.OrderBy(l => l.Value.Price / Math.Min(l.Value.Volume, 1))
                    .Select(l => l.Value.Price / (l.Key.Count == 0 ? 1 : l.Key.Count))
                    .FirstOrDefault())
                ))
                .Where(l => l.Item2 > 0)
                .ToDictionary(l => l.Key, l => l.Item2);
            // expand group(s)
            var hyperion = dictionary["HYPERION"];
            foreach (var item in SniperService.HyperionGroup)
            {
                dictionary[item] = hyperion;
            }
            return dictionary;
        }

        public class Result
        {
            public AuctionKey Old { get; set; }
            public AuctionKey New { get; set; }
            public ReferencePrice Reference { get; set; }
        }

        /// <summary>
        /// Retrieve lookup references
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        [Route("export")]
        [ResponseCache(Duration = 3600 * 6, Location = ResponseCacheLocation.Any, NoStore = false)]
        public async Task<IEnumerable<float[]>> Export(int page = 0)
        {
            var batches = await Mapper.Instance.GetBatch(page, 30000);
            return batches.Select(p =>
            {
                var numbers = new List<float>();
                numbers.Add(p.HighestBid);
                numbers.Add(p.ItemId);
                numbers.Add(p.Rarity);
                //AddDate(numbers,  default(DateTime));
                numbers.Add(0f);
                numbers.Add(0f);
                numbers.Add(0f);
                numbers.Add(0f);
                AddDate(numbers, p.End);

                var enchants = Enum.GetValues<Coflnet.Sky.Core.Enchantment.EnchantmentType>().Select(t => 0f).ToArray();
                foreach (var item in p.Enchantments)
                {
                    enchants[item.Item1] = ((float)item.Item2) / 10;
                }
                numbers.AddRange(enchants);

                var nbt = new float[300];
                foreach (var item in p.NbtData)
                {
                    nbt[item.Item1] = (float)item.Item2 / int.MaxValue;
                }
                numbers.AddRange(nbt);


                return numbers.ToArray();
            });

            //return "ok";
        }

        private static void AddDate(List<float> numbers, DateTime date)
        {
            numbers.Add((float)date.TimeOfDay.TotalSeconds / 60 / 60 / 24);
            numbers.Add((float)date.DayOfWeek / 7);
            numbers.Add((float)date.DayOfYear / 365);
            numbers.Add((float)date.Day / 31);
        }
    }
}
