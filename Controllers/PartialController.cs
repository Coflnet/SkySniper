using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Sniper.Services;
using Microsoft.AspNetCore.Mvc;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using System.Threading;

namespace Coflnet.Sky.Sniper.Controllers;
[ApiController]
[Route("api/[controller]")]
public class PartialController
{
    private InternalDataLoader inter;
    private PartialCalcService partialCalcService;

    public PartialController(InternalDataLoader inter, PartialCalcService partialCalcService)
    {
        this.inter = inter;
        this.partialCalcService = partialCalcService;
    }

    [HttpPost]
    [Route("partial/{tag}")]
    public async Task<Dictionary<string, Dictionary<object, double>>> LoadFor(string tag, CancellationToken token)
    {
        return await inter.PartialAnalysis(tag, token);
    }
    [HttpGet]
    [Route("partial/{uuid}")]
    public async Task<PartialCalcService.PartialResult> GetBreakDown(string uuid)
    {
        var uid = AuctionService.Instance.GetId(uuid);
        using var context = new HypixelContext();
        var auction = await context.Auctions.Where(a => a.UId == uid).Include(a => a.NbtData).Include(a => a.Enchantments).FirstOrDefaultAsync();
        return partialCalcService.GetPrice(auction, true);
    }
}
