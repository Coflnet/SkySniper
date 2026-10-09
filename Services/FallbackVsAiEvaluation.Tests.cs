using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Services;
using Coflnet.Sky.Sniper.Models;
using dev;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// Offline comparison of the closest-reference fallback (<see cref="SniperService.GetPrice"/>) with the AI estimate
    /// (<see cref="SelfLearningFlipFinderService"/>) on heavily upgraded items that have no exact reference bucket.
    ///
    /// <para>
    /// Not a regression test: it needs captured inputs and is ignored without them.
    /// <c>SNIPER_EVAL_ARTIFACTS</c> is a ':' separated list of per item pricing-state captures (gzip json) and
    /// <c>SNIPER_EVAL_BAZAAR</c> a json file <c>{"products":{"TAG":{"sell":1,"buy":2,"buyOrders":3}}}</c>.
    /// </para>
    ///
    /// <para>
    /// Each of the most upgraded priced buckets is held out in turn: every other captured sale is ingested through
    /// <see cref="SniperService.AddSoldItem"/> and used to train the AI model through the production feature path
    /// (<see cref="SaveAuctionExtensions.ToComplicatedFlip"/>), then both estimate the held out item and are compared
    /// with the median it actually sold for. The AI model is therefore one trained on the capture, not the deployed one.
    /// </para>
    /// </summary>
    public class FallbackVsAiEvaluation
    {
        private const int CasesPerItem = 6;
        private const int MinSalesPerCase = 4;
        private static readonly Regex KeyFormat = new(@"^(\S*) (\S+) (.*) (\S+) (\d+)$", RegexOptions.Compiled);
        private static readonly Regex ModifierFormat = new(@"\[([^,\]]+), ([^\]]*)\]", RegexOptions.Compiled);
        private static readonly Sale Unsold = new(0, 0, 1, 1);
        private static long idCounter = 1000;

        private record Sale(long Price, int Day, short Seller, short Buyer);
        private record Bucket(string Key, long Price, List<Sale> Sales, SaveAuction Item);

        private class CraftCostMock : ICraftCostService
        {
            public Dictionary<string, double> Costs { get; } = new();
            public ConcurrentDictionary<string, Category> ItemCategories { get; } = new();
            public void AddCostForSpecialItems() { }
            public bool TryGetCost(string itemId, out double cost) => Costs.TryGetValue(itemId, out cost);
        }

        private class NoPersistence : IPersitanceManager
        {
            public Task LoadLookups(SniperService service) => Task.CompletedTask;
            public Task SaveLookup(ConcurrentDictionary<string, PriceLookup> lookups) => Task.CompletedTask;
            public Task<ConcurrentDictionary<string, AttributeLookup>> GetWeigths() => Task.FromResult(new ConcurrentDictionary<string, AttributeLookup>());
            public Task SaveWeigths(ConcurrentDictionary<string, AttributeLookup> lookups) => Task.CompletedTask;
            public Task<List<KeyValuePair<string, PriceLookup>>> LoadGroup(int groupId) => Task.FromResult(new List<KeyValuePair<string, PriceLookup>>());
            public Task<Dictionary<string, double>> LoadCraftCost() => Task.FromResult(new Dictionary<string, double>());
            public Task SaveBlob(string key, Stream data) => Task.CompletedTask;
            public Task<Stream> LoadBlob(string key) => throw new FileNotFoundException();
            public Task FlushDueGroups(ConcurrentDictionary<string, PriceLookup> lookups, TimeSpan maxAge, CancellationToken cancellationToken = default) => Task.CompletedTask;
        }

        [Test]
        public async Task CompareOnHeldOutUpgradedBuckets()
        {
            var artifacts = Environment.GetEnvironmentVariable("SNIPER_EVAL_ARTIFACTS");
            var bazaarFile = Environment.GetEnvironmentVariable("SNIPER_EVAL_BAZAAR");
            if (string.IsNullOrEmpty(artifacts) || string.IsNullOrEmpty(bazaarFile))
                Assert.Ignore("needs SNIPER_EVAL_ARTIFACTS and SNIPER_EVAL_BAZAAR");
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            SniperService.StartTime = new DateTime(2021, 9, 25);
            var bazaar = ReadBazaar(bazaarFile);
            foreach (var path in artifacts.Split(':', StringSplitOptions.RemoveEmptyEntries))
                await EvaluateItem(ReadPricingState(path), bazaar);
        }

        private static async Task EvaluateItem(JObject state, BazaarPull bazaar)
        {
            var tag = state.Value<string>("item_tag");
            var cleanPrice = ((JObject)state["clean_price_per_tier"]).Properties().Select(p => (double)p.Value).DefaultIfEmpty(0).Min();
            var (parser, _) = await CreateService(tag, cleanPrice, bazaar);
            var all = state["lookup"].Children<JProperty>().Select(p => ParseBucket(tag, p)).Where(b => b.Sales.Count > 0).ToList();
            var replayable = all.Where(b => b.Item != null && parser.KeyFromSaveAuction(b.Item).ToString() == b.Key).ToList();
            var cases = replayable.Where(b => b.Price > 0 && b.Sales.Count >= MinSalesPerCase)
                .OrderByDescending(b => parser.ValueKeyForTest(b.Item).ValueBreakdown.Sum(v => v.IsEstimate ? 0 : v.Value))
                .Take(CasesPerItem).ToList();
            Console.WriteLine($"EVAL {tag} clean={cleanPrice:F0} buckets={all.Count} replayable={replayable.Count} sales={replayable.Sum(b => b.Sales.Count)} cases={cases.Count}");
            foreach (var held in cases)
                await EvaluateCase(tag, cleanPrice, bazaar, replayable, held);
        }

        private static async Task EvaluateCase(string tag, double cleanPrice, BazaarPull bazaar, List<Bucket> replayable, Bucket held)
        {
            var (service, craftCost) = await CreateService(tag, cleanPrice, bazaar);
            var lastDay = replayable.Max(b => b.Sales.Max(s => s.Day));
            var sales = replayable.Where(b => b != held).SelectMany(b => b.Sales.Select(s => Sold(b.Item, s, lastDay))).ToList();
            foreach (var sale in sales)
                service.AddSoldItem(sale);
            service.FinishedUpdate();

            var fallbackWatch = Stopwatch.StartNew();
            var fallback = service.GetPrice(Sold(held.Item, Unsold, 0));
            fallbackWatch.Stop();

            using var ai = new SelfLearningFlipFinderService(NullLogger<SelfLearningFlipFinderService>.Instance, new NoPersistence());
            await ai.TrainBatchAsync(sales.Select(s => s.ToComplicatedFlip(true, service, null, craftCost)));
            await ai.EnsureTrainedModelAsync(tag);
            var aiWatch = Stopwatch.StartNew();
            var estimate = await ai.EstimateAsync(Sold(held.Item, Unsold, 0).ToComplicatedFlip(true, service, null, craftCost));
            aiWatch.Stop();

            var soldMedian = held.Sales.Select(s => s.Price).OrderBy(p => p).ElementAt(held.Sales.Count / 2);
            Console.WriteLine($"EVAL {tag} | {held.Key} | sales={held.Sales.Count} soldMedian={soldMedian} bucketPrice={held.Price}"
                + $" | fallback={fallback.Median} ({Percent(fallback.Median, soldMedian)}) via {fallback.MedianKey} {fallbackWatch.Elapsed.TotalMilliseconds:F1}ms"
                + $" | ai={estimate?.EstimatedValue:F0} ({Percent(estimate?.EstimatedValue ?? 0, soldMedian)}) ready={estimate?.ModelReady} samples={estimate?.SampleCount} {aiWatch.Elapsed.TotalMilliseconds:F1}ms");
        }

        private static string Percent(double estimate, long actual) => $"{(estimate - actual) / actual * 100:+0;-0}%";

        private static async Task<(SniperService, CraftCostMock)> CreateService(string tag, double cleanPrice, BazaarPull bazaar)
        {
            // the capture has no craft costs, the clean price stands in for the item's craft cost ("cleancost" feature)
            var craftCost = new CraftCostMock { Costs = { [tag] = cleanPrice } };
            var service = new SniperService(new HypixelItemService(null, NullLogger<HypixelItemService>.Instance), null, NullLogger<SniperService>.Instance, craftCost);
            await service.Init();
            service.UpdateBazaar(bazaar);
            return (service, craftCost);
        }

        /// <summary>
        /// The captured short seller and buyer ids are kept so sales are deduplicated as in production
        /// </summary>
        private static SaveAuction Sold(SaveAuction item, Sale sale, int lastDay) => new(item)
        {
            Uuid = Interlocked.Increment(ref idCounter).ToString("x8"),
            UId = Interlocked.Increment(ref idCounter),
            AuctioneerId = ((ushort)sale.Seller).ToString("x4").PadRight(8, '0'),
            Bids = [new SaveBids { Bidder = ((ushort)sale.Buyer).ToString("x4").PadRight(8, '0'), Amount = sale.Price }],
            FlatenedNBT = new(item.FlatenedNBT),
            Enchantments = new(item.Enchantments),
            HighestBidAmount = sale.Price,
            StartingBid = sale.Price,
            End = DateTime.UtcNow.AddDays(sale.Day - lastDay).AddHours(-1)
        };

        private static Bucket ParseBucket(string tag, JProperty bucket)
        {
            var sales = bucket.Value["references"].Where(r => r.Value<long>("price") > 0)
                .Select(r => new Sale(r.Value<long>("price"), r.Value<int>("day"), r.Value<short>("seller"), r.Value<short>("buyer"))).ToList();
            return new Bucket(bucket.Name, bucket.Value.Value<long>("price"), sales, ParseItem(tag, bucket.Name));
        }

        /// <summary>
        /// Rebuilds an item from the display form of its bucket key, null if it can not be expressed as one
        /// </summary>
        private static SaveAuction ParseItem(string tag, string key)
        {
            var parts = KeyFormat.Match(key);
            if (!parts.Success || !Enum.TryParse<Tier>(parts.Groups[4].Value, out var tier) || !Enum.TryParse<ItemReferences.Reforge>(parts.Groups[2].Value, out var reforge))
                return null;
            var enchants = new List<Enchantment>();
            foreach (var enchant in parts.Groups[1].Value.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                var pair = enchant.Split('=');
                if (pair.Length != 2 || !Enum.TryParse<Enchantment.EnchantmentType>(pair[0], out var type) || !byte.TryParse(pair[1], out var level))
                    return null;
                enchants.Add(new Enchantment(type, level));
            }
            var modifiers = ModifierFormat.Matches(parts.Groups[3].Value).ToDictionary(m => m.Groups[1].Value, m => m.Groups[2].Value);
            if (modifiers.Remove("hotpc", out var potatoBooks))
                modifiers["hpc"] = potatoBooks switch { "1" => "15", "0.1" => "12", _ => "10" };
            return new SaveAuction
            {
                Tag = tag,
                Tier = tier,
                // created after gemstone slots had to be unlocked, so none are assumed
                ItemCreatedAt = new DateTime(2024, 1, 1),
                Reforge = reforge,
                Enchantments = enchants,
                FlatenedNBT = modifiers,
                Count = int.Parse(parts.Groups[5].Value)
            };
        }

        private static JObject ReadPricingState(string path)
        {
            using var reader = new StreamReader(new GZipStream(File.OpenRead(path), CompressionMode.Decompress));
            return (JObject)JObject.Parse(reader.ReadToEnd())["pricing_state"];
        }

        private static BazaarPull ReadBazaar(string path)
        {
            var products = (JObject)JObject.Parse(File.ReadAllText(path))["products"];
            return new BazaarPull
            {
                Timestamp = DateTime.UtcNow,
                Products = products.Properties().Select(p => new ProductInfo
                {
                    ProductId = p.Name,
                    SellSummary = Orders<SellOrder>(p.Value.Value<double>("sell"), price => new() { PricePerUnit = price }),
                    BuySummery = Orders<BuyOrder>(p.Value.Value<double>("buy"), price => new() { PricePerUnit = price }),
                    QuickStatus = new() { BuyOrders = p.Value.Value<int>("buyOrders") }
                }).ToList()
            };
        }

        private static List<T> Orders<T>(double price, Func<double, T> create) => price > 0 ? [create(price)] : [];
    }
}
