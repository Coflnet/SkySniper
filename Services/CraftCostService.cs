using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Crafts.Client.Api;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System;

namespace Coflnet.Sky.Sniper.Services;

public interface ICraftCostService
{
    bool TryGetCost(string itemId, out double cost);
    Dictionary<string, double> Costs { get; }
}

public class CraftCostService : BackgroundService, ICraftCostService
{
    private readonly ICraftsApi craftsApi;
    private readonly ILogger<CraftCostService> logger;
    public Dictionary<string, double> Costs { get; private set; } = new Dictionary<string, double>();
    private HashSet<string> vanilla = ["acacia_door", "acacia_fence", "acacia_fence_gate", "acacia_stairs", "activator_rail", "air", "anvil", "apple", "armor_stand", "arrow", "baked_potato", "banner", "barrier", "bed", "beef", "birch_door", "birch_fence", "birch_fence_gate", "birch_stairs", "blaze_powder", "blaze_rod", "boat", "bone", "book", "bookshelf", "bow", "bowl", "bread", "brewing_stand", "brick", "brick_block", "brick_stairs", "brown_mushroom", "brown_mushroom_block", "bucket", "cactus", "cake", "carpet", "carrot", "carrot_on_a_stick", "cauldron", "chainmail_boots", "chainmail_chestplate", "chainmail_helmet", "chainmail_leggings", "chest", "chest_minecart", "chicken", "clay", "clay_ball", "clock", "coal", "coal_block", "coal_ore", "cobblestone", "cobblestone_wall", "command_block", "command_block_minecart", "comparator", "compass", "cooked_beef", "cooked_chicken", "cooked_fish", "cooked_mutton", "cooked_porkchop", "cooked_rabbit", "cookie", "crafting_table", "dark_oak_door", "dark_oak_fence", "dark_oak_fence_gate", "dark_oak_stairs", "daylight_detector", "deadbush", "detector_rail", "diamond", "diamond_axe", "diamond_block", "diamond_boots", "diamond_chestplate", "diamond_helmet", "diamond_hoe", "diamond_horse_armor", "diamond_leggings", "diamond_ore", "diamond_pickaxe", "diamond_shovel", "diamond_sword", "dirt", "dispenser", "double_plant", "dragon_egg", "dropper", "dye", "egg", "emerald", "emerald_block", "emerald_ore", "enchanted_book", "enchanting_table", "end_portal_frame", "end_stone", "ender_chest", "ender_eye", "ender_pearl", "experience_bottle", "farmland", "feather", "fence", "fence_gate", "fermented_spider_eye", "filled_map", "fire_charge", "firework_charge", "fireworks", "fish", "fishing_rod", "flint", "flint_and_steel", "flower_pot", "furnace", "furnace_minecart", "ghast_tear", "glass", "glass_bottle", "glass_pane", "glowstone", "glowstone_dust", "gold_block", "gold_ingot", "gold_nugget", "gold_ore", "golden_apple", "golden_axe", "golden_boots", "golden_carrot", "golden_chestplate", "golden_helmet", "golden_hoe", "golden_horse_armor", "golden_leggings", "golden_pickaxe", "golden_rail", "golden_shovel", "golden_sword", "grass", "gravel", "gunpowder", "hardened_clay", "hay_block", "heavy_weighted_pressure_plate", "hopper", "hopper_minecart", "ice", "iron_axe", "iron_bars", "iron_block", "iron_boots", "iron_chestplate", "iron_door", "iron_helmet", "iron_hoe", "iron_horse_armor", "iron_ingot", "iron_leggings", "iron_ore", "iron_pickaxe", "iron_shovel", "iron_sword", "iron_trapdoor", "item_frame", "jukebox", "jungle_door", "jungle_fence", "jungle_fence_gate", "jungle_stairs", "ladder", "lapis_block", "lapis_ore", "lava", "lava_bucket", "lead", "leather", "leather_boots", "leather_chestplate", "leather_helmet", "leather_leggings", "leaves", "leaves2", "lever", "light_weighted_pressure_plate", "lit_pumpkin", "log", "log2", "magma_cream", "map", "melon", "melon_block", "melon_seeds", "milk_bucket", "minecart", "mob_spawner", "monster_egg", "mossy_cobblestone", "mushroom_stew", "mutton", "mycelium", "name_tag", "nether_brick", "nether_brick_fence", "nether_brick_stairs", "nether_star", "nether_wart", "netherbrick", "netherrack", "noteblock", "oak_stairs", "obsidian", "packed_ice", "painting", "paper", "piston", "planks", "poisonous_potato", "porkchop", "potato", "potion", "prismarine", "prismarine_crystals", "prismarine_shard", "pumpkin", "pumpkin_pie", "pumpkin_seeds", "quartz", "quartz_block", "quartz_ore", "quartz_stairs", "rabbit", "rabbit_foot", "rabbit_hide", "rabbit_stew", "rail", "record_11", "record_13", "record_blocks", "record_cat", "record_chirp", "record_far", "record_mall", "record_mellohi", "record_stal", "record_strad", "record_wait", "record_ward", "red_flower", "red_mushroom", "red_mushroom_block", "red_sandstone", "red_sandstone_stairs", "redstone", "redstone_block", "redstone_lamp", "redstone_ore", "redstone_torch", "reeds", "repeater", "rotten_flesh", "saddle", "sand", "sandstone", "sandstone_stairs", "sapling", "sea_lantern", "shears", "sign", "skull", "slime", "slime_ball", "snow", "snow_layer", "snowball", "soul_sand", "spawn_egg", "speckled_melon", "spider_eye", "sponge", "spruce_door", "spruce_fence", "spruce_fence_gate", "spruce_stairs", "stained_glass", "stained_glass_pane", "stained_hardened_clay", "stick", "sticky_piston", "stone", "stone_axe", "stone_brick_stairs", "stone_button", "stone_hoe", "stone_pickaxe", "stone_pressure_plate", "stone_shovel", "stone_slab", "stone_slab2", "stone_stairs", "stone_sword", "stonebrick", "string", "sugar", "tallgrass", "tnt", "tnt_minecart", "torch", "trapdoor", "trapped_chest", "tripwire_hook", "vine", "water", "water_bucket", "waterlily", "web", "wheat", "wheat_seeds", "wooden_axe", "wooden_button", "wooden_door", "wooden_hoe", "wooden_pickaxe", "wooden_pressure_plate", "wooden_shovel", "wooden_slab", "wooden_sword", "wool", "writable_book", "written_book", "yellow_flower", "Minecraft ID Name"];

    public CraftCostService(ICraftsApi craftsApi, ILogger<CraftCostService> logger)
    {
        this.craftsApi = craftsApi;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            List<Crafts.Client.Model.ProfitableCraft> all = null;
            try
            {
                all = await craftsApi.CraftsAllGetAsync();
            }
            catch (System.Exception e)
            {
                logger.LogError(e, "Error while fetching crafts");
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                continue;
            }

            if (all == null)
            {
                logger.LogError("Crafts api returned null");
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                continue;
            }
            foreach (var craft in all)
            {
                Costs[craft.ItemId] = craft.CraftCost;
                if (craft.ItemId.EndsWith("GROWTH_5"))
                    logger.LogInformation("Cost for " + craft.ItemId + " is " + craft.CraftCost);
            }
            logger.LogInformation("Updated craft costs for " + all.Count + " items");
            await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
        }
    }


    public bool TryGetCost(string itemId, out double cost)
    {
        if (itemId.Contains(':') || vanilla.Contains(itemId.Replace("_ITEM", "").ToLower()))
        {
            cost = 10;
            return true;
        }
        return Costs.TryGetValue(itemId, out cost);
    }
}
#nullable disable
