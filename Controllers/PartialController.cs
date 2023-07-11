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
        var res = await inter.PartialAnalysis(tag, token);

        await partialCalcService.Save();
        return res;
    }
    [HttpGet]
    [Route("tag/{itemTag}")]
    public Dictionary<string, Dictionary<object, double>> GetAttributeCosts(string itemTag)
    {
        return partialCalcService.GetAttributeCosts(itemTag);
    }
    [HttpPost]
    [Route("tag/{itemTag}/correct")]
    public async Task<Dictionary<string, Dictionary<object, double>>> Correct(string itemTag, [FromBody] Dictionary<string, Dictionary<object, double>> corrections)
    {
        partialCalcService.Correct(itemTag, corrections);
        await partialCalcService.Save();
        return partialCalcService.GetAttributeCosts(itemTag);
    }
    [HttpGet]
    [Route("uuid/{uuid}")]
    public async Task<PartialCalcService.PartialResult> GetBreakDown(string uuid)
    {
        var uid = AuctionService.Instance.GetId(uuid);
        using var context = new HypixelContext();
        var auction = await context.Auctions.Where(a => a.UId == uid).Include(a => a.NbtData).Include(a => a.Enchantments).FirstOrDefaultAsync();
        return partialCalcService.GetPrice(auction, true);
    }
}
