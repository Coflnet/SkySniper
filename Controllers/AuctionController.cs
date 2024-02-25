using System.Linq;
using Coflnet.Sky.Sniper.Models;
using Coflnet.Sky.Sniper.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;

namespace Coflnet.Sky.Sniper.Controllers;
[ApiController]
[Route("/api/[controller]")]
public class AuctionController : ControllerBase
{
    private readonly ILogger<AuctionController> _logger;
    private readonly SniperService service;
    private readonly HypixelContext db;

    public AuctionController(ILogger<AuctionController> logger, SniperService service, HypixelContext db)
    {
        _logger = logger;
        this.service = service;
        this.db = db;
    }

    [Route("auction/{auctionUuid}/key")]
    [HttpGet]
    public AuctionKey GetKey(string auctionUuid)
    {
        var uid = AuctionService.Instance.GetId(auctionUuid);
        var auction = db.Auctions.Include(a => a.NbtData).Include(a => a.Enchantments).FirstOrDefault(a => a.UId == uid);
        return service.KeyFromSaveAuction(auction);
    }
}

