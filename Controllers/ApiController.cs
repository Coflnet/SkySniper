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

namespace Coflnet.Sky.Sniper.Controllers
{
    [ApiController]
    [Route("/api/[controller]")]
    public class SniperController : ControllerBase
    {
        private readonly ILogger<SniperController> _logger;
        private SniperService service;

        public SniperController(ILogger<SniperController> logger, SniperService service)
        {
            _logger = logger;
            this.service = service;
        }

        [HttpGet]
        [ResponseCache(Duration = 3600, Location = ResponseCacheLocation.Any, NoStore = false)]
        public IEnumerable<string> GetIds()
        {
            return service.Lookups.Keys;
        }

        /// <summary>
        /// Retrieve item lookup state transfer
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("{itemId}")]
        public PriceLookup GetLookup(string itemId, string token)
        {
            return service.Lookups[itemId];
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
        public IEnumerable<PriceEstimate> GetPrices(IEnumerable<SaveAuction> auctions)
        {
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
