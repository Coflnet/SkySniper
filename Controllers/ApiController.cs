using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Sniper.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace SkySniper.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ApiController : ControllerBase
    {
        private readonly ILogger<ApiController> _logger;
        private SniperService service;

        public ApiController(ILogger<ApiController> logger, SniperService service)
        {
            _logger = logger;
            this.service = service;
        }

        [HttpGet]
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
    }
}
