using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Sniper.Services;

public interface IAttributeFlipService
{
    Task Update();
    ConcurrentDictionary<AuctionKey, AttributeFlip> Flips { get; }
}

public class AttributeFlipService : IAttributeFlipService
{
    SniperService sniperService;
    ILogger<AttributeFlipService> logger;
    private readonly PropertyMapper mapper = new();


    public ConcurrentDictionary<AuctionKey, AttributeFlip> Flips { get; } = new();
    Channel<PotentialCraftFlip> potentialCraftFlips = Channel.CreateBounded<PotentialCraftFlip>(200);

    public AttributeFlipService(SniperService sniperService, ILogger<AttributeFlipService> logger)
    {
        this.sniperService = sniperService;
        sniperService.CappedKey += c =>
        {
            potentialCraftFlips.Writer.TryWrite(c);
        };
        sniperService.OnSold += (c) =>
        {
            var auction = c.Item1;
            var key = c.Item2;
            if (Flips.TryRemove(key, out var flip))
            {
                logger.LogInformation($"Sold attribute craft flip {auction} for {flip.Target}");
            }
        };
        sniperService.OnSummaryUpdate += () =>
        {
            Task.Run(Update);
        };
        this.logger = logger;
    }

    public async Task Update()
    {
        while (await potentialCraftFlips.Reader.WaitToReadAsync())
        {
            if (potentialCraftFlips.Reader.TryRead(out var flip))
            {
                try
                {
                    await CheckPotential(flip);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Error while checking potential flip");
                }
            }
        }
        foreach (var item in Flips.Keys.ToList())
        {
            if (Flips.TryGetValue(item, out var flip) && flip.FoundAt.AddHours(3) < DateTime.UtcNow)
            {
                Flips.TryRemove(item, out _);
            }
        }
    }

    private async Task CheckPotential(PotentialCraftFlip flip)
    {
        var key = flip.FullKey;
        var cheapest = flip.Cheapest;
        var modifierSum = flip.ModifierSum;
        var lookup = flip.Lookup;
        var medianPrice = flip.MedianPrice;
        if (!lookup.Lookup.TryGetValue(key.Key, out var matchingBaucket))
            return;
        if (matchingBaucket.Volume < 3)
            return;
        var cheapestLbin = lookup.Lookup.Where(l => l.Value.Lbin.AuctionId != default).MinBy(l => l.Value.Lbin.Price);
        if (cheapestLbin.Value.Lbin.Price > cheapest)
        {
            return;
        }
        if(Flips.TryGetValue(cheapestLbin.Key, out var existingFlip) && existingFlip.FoundAt.AddMinutes(2) > DateTime.UtcNow)
        {
            return;
        }
        logger.LogInformation($"Found potential flip for {flip.tag} {cheapestLbin.Key} to {key.Key} with {cheapestLbin.Value.Lbin.Price}");
        using var context = new HypixelContext();
        var auction = await context.Auctions.Where(a => a.UId == cheapestLbin.Value.Lbin.AuctionId).Select(u => u.Uuid).FirstOrDefaultAsync();
        Flips[cheapestLbin.Key] = new AttributeFlip()
        {
            AuctionToBuy = auction,
            Ingredients = key.ValueBreakdown.SelectMany(b => NewMethod(b)).ToList(),
            StartingKey = cheapestLbin.Key,
            EndingKey = key.Key,
            Target = medianPrice,
            EstimatedCraftingCost = modifierSum,
            Tag = flip.tag
        };
    }

    private IEnumerable<AttributeFlip.Ingredient> NewMethod(SniperService.RankElem b)
    {
        if (b.Enchant.Type != 0)
        {
            yield return new AttributeFlip.Ingredient()
            {
                AttributeName = $"{b.Enchant.Type} Enchant Lvl {b.Enchant.Lvl}",
                ItemId = null,
                Amount = 1,
                Price = b.Value
            };
            yield break;
        }
        if(b.Reforge != ItemReferences.Reforge.None)
        {
            yield return new AttributeFlip.Ingredient()
            {
                AttributeName = $"{b.Reforge}",
                ItemId = null,
                Amount = 1,
                Price = b.Value
            };
            yield break;
        }
        if (mapper.TryGetIngredients(b.Modifier.Key, b.Modifier.Value, null, out var ingredients))
        {
            foreach (var ingredient in ingredients.GroupBy(s => s))
            {
                var amount = ingredient.Count();
                yield return new AttributeFlip.Ingredient()
                {
                    AttributeName = $"{b.Modifier} {b.Modifier.Value}",
                    ItemId = ingredient.Key,
                    Amount = amount,
                    Price = sniperService.GetPriceForItem(ingredient.Key) * amount
                };
            }
        }
    }
}

public record PotentialCraftFlip(string tag, KeyWithValueBreakdown FullKey, long Cheapest, long ModifierSum, PriceLookup Lookup, long MedianPrice);


public class AttributeFlip
{
    public string Tag { get; set; } 
    public string AuctionToBuy { get; set; }
    public List<Ingredient> Ingredients { get; set; }
    public AuctionKey StartingKey { get; set; }
    public AuctionKey EndingKey { get; set; }
    public long Target { get; set; }
    public long EstimatedCraftingCost { get; set; }
    public DateTime FoundAt { get; set; } = DateTime.UtcNow;

    public class Ingredient
    {
        public string ItemId { get; set; }
        public string AttributeName { get; set; }
        public int Amount { get; set; }
        public double Price { get; set; }
    }
}
#nullable disable
