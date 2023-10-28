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
    private readonly InternalDataLoader inter;
    private readonly PartialCalcService partialCalcService;
    private readonly RetrainService retrainService;

    public PartialController(InternalDataLoader inter, PartialCalcService partialCalcService, RetrainService retrainService)
    {
        this.inter = inter;
        this.partialCalcService = partialCalcService;
        this.retrainService = retrainService;
    }

    [HttpPost]
    [Route("partial/{tag}")]
    public async Task<Dictionary<string, Dictionary<string, double>>> LoadFor(string tag, CancellationToken token)
    {
        var res = await inter.PartialAnalysis(tag, token);

        await partialCalcService.Save();
        return res;
    }
    [HttpPost]
    [Route("partial/{tag}/shedule")]
    public void SheduleLoadFor(string tag)
    {
        retrainService.SheduleRetrain(tag);
    }
    [HttpGet]
    [Route("tag/{itemTag}")]
    public Dictionary<string, List<(string, double, string)>> GetAttributeCosts(string itemTag)
    {
        return partialCalcService.GetAttributeCosts(itemTag).ToDictionary(a => a.Key, a => a.Value.Select(b => (b.Key, b.Value, b.Key.GetType().Name)).ToList());
    }
    [HttpPost]
    [Route("tag/{itemTag}/correct")]
    public async Task<Dictionary<string, Dictionary<string, double>>> Correct(string itemTag, [FromBody] Dictionary<string, Dictionary<string, double>> corrections)
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
