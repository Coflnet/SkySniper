using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Sniper.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R2 Phase 0 / F3 bit-exactness oracle for the <b>valuation finders</b>
    /// (<c>benchmarks/COMPUTE_FLOOR_SPEC_R2.md</c> §4-F3 + §5 R2-A/R2-B/R2-D).
    ///
    /// <para><b>Two kinds of gate, because the targets differ in how they can be observed:</b></para>
    /// <list type="number">
    ///   <item><b>Golden-output capture (this file):</b> <see cref="GetCleanItemPrice"/> is a pure read over the
    ///     populated lookup (no side effects, returns a <c>long</c>), so its current behavior is captured verbatim here
    ///     as <see cref="GetCleanItemPriceReference"/> and the de-alloc rewrite (R2-D) is gated against it directly,
    ///     on real populated services — the strongest possible contract. <see cref="GetCleanItemPriceForTest"/> exposes
    ///     both the live production method and the reference to the test harness without weakening production access.</item>
    ///   <item><b>Snipe-set capture (see <c>ValuationFinders.Tests.cs</c>):</b> <see cref="CheckCombined"/> and
    ///     <see cref="PotentialSnipe"/> are private, stateful, and emit through <c>FoundAFlip</c> -&gt; the
    ///     <c>FoundSnipe</c> event rather than returning a value. Their behavioral contract is therefore the resulting
    ///     <b>snipe set</b> (the ordered list of <c>(finder, target price)</c> emitted) for a populated service driven
    ///     through the public <c>TestNewAuction</c>. A future rewrite of those finders is proven correct iff it emits
    ///     the identical snipe set.</item>
    /// </list>
    ///
    /// <see cref="GetCleanItemPriceReference"/> is a <b>verbatim copy</b> of <see cref="GetCleanItemPrice"/> as of this
    /// commit; it lives on the same <c>partial class SniperService</c> so it reaches the same private helpers
    /// (<c>ReduceRarity</c>, <c>CanHaveGems</c>, <c>DropUnderlistings</c>, <c>IsMidas</c>) the production method uses, and
    /// reads the same shared service state. It is referenced only by the test harness; never called from production.
    /// </summary>
    public partial class SniperService
    {
        /// <summary>
        /// Test-only accessor: returns (live production result, reference snapshot result) for the same inputs so the
        /// harness can assert byte-equality without reflecting into the private finder. Pure read; no state mutation.
        /// </summary>
        internal (long live, long reference) GetCleanItemPriceForTest(string tag, KeyWithValueBreakdown key, PriceLookup lookup, bool force = false)
            => (GetCleanItemPrice(tag, key, lookup, force), GetCleanItemPriceReference(tag, key, lookup, force));

        /// <summary>
        /// R6 WS-CMB2 test seam: when <c>true</c>, <see cref="CheckCombined"/> dispatches to
        /// <see cref="CheckCombinedReference"/> (the verbatim pre-R6 LINQ). Default <c>false</c> in production (the
        /// de-LINQ'd path); flipped only by the fuzz harness for the new-vs-reference snipe-set A/B. A single bool read
        /// on the alternate-finders path (NOT the per-auction hot dispatch loop).
        /// </summary>
        internal bool UseCombinedReference = false;

        /// <summary>
        /// Verbatim copy of <see cref="CheckCombined"/> as of the R6 WS-CMB2 snapshot (the full-LINQ implementation),
        /// before the de-LINQ. The R6 de-LINQ rewrite is gated against this via the snipe-set A/B fuzz
        /// (<c>Combined_DeLinq_BitExact_Fuzz</c>): both implementations are driven over the same randomized heavy-tie
        /// lookups and must emit the identical snipe set. Lives on the same partial class so it reaches the same private
        /// helpers (<c>GetReduced</c>, <c>GetAuctionGroupTag</c>, <c>GetFullKey</c>, <c>ComparisonValueForKey</c>,
        /// <c>GetBucketDomKey</c>, <c>GetMedian</c>, <c>CapAtCraftCost</c>, <c>FindFlip</c>) the production method uses.
        /// </summary>
        private void CheckCombinedReference(SaveAuction auction, PriceLookup lookup, double lbinPrice, double medPrice, KeyWithValueBreakdown longKey, RankElem topAttrib)
        {
            var topKey = longKey.GetReduced(0);
            var targetVolume = 11;
            if (lookup.Lookup.TryGetValue(topKey, out var topBucket) && topBucket.References.Count >= targetVolume)
            {
                return; // enough references in previous check
            }
            var groupTag = GetAuctionGroupTag(auction.Tag);
            var l = lookup.Lookup;
            var similar = l.Where(e => topAttrib.Modifier.Key != default && !e.Key.Modifiers.Any(m => m.Key == "virtual") || e.Key.Enchants.Contains(topAttrib.Enchant)).ToList();
            if (similar.Count == 1)
            {
                // include all if no match otherwise
                similar = l.ToList();
            }
            var fullKey = GetFullKey(auction);
            var queryDomCmb = DominatorIndex.BuildDomKey(fullKey, scoreInterner);
            ulong qProvCmb = queryDomCmb.ProvidedMask;
            bool petSpiritCmb = auction.Tag == "PET_SPIRIT";
            var relevant = similar.Where(e => (e.Key.Reforge == topKey.Reforge || topKey.Reforge == ItemReferences.Reforge.Any)
                        && CmbDominates(e.Value, e.Key))
                .Select(e => (e, value: Math.Max(e.Value.Volume, 0.5) * Math.Pow(ComparisonValueForKey(groupTag.tag, e.Key).Sum(s => s.Value), 1.8)))
                .OrderByDescending(e => e.value)
                .ToList();

            bool CmbDominates(ReferenceAuctions candBucket, AuctionKey candKey)
            {
                var candDom = GetBucketDomKey(candBucket, candKey);
                if ((candDom.RequiredMask & qProvCmb) != candDom.RequiredMask)
                    return false; // sound presence prefilter (candidate is the base side)
                return DominatorIndex.Dominates(in candDom, in queryDomCmb, petSpiritCmb);
            }
            if (relevant.Count < 2)
            {
                return; // makes only sense if there is something combined
            }
            // get enough relevant to build a median and try to get highest value (most enchantments and modifiers)
            var combined = relevant.SelectMany(r => r.e.Value.References.Select(ri => (ri, relevancy: r.value * (ri.Day - GetDay() + 12) * Math.Log10(ri.Price + 1))))
                                .OrderByDescending(r => r.relevancy).Select(r => r.ri).Take(targetVolume).ToList();
            if (combined.Count == 0)
            {
                return;
            }
            var lbinBucket = relevant.Select(r => r.e.Value.Lbin).Where(r => r.Price != default).DefaultIfEmpty().MinBy(r => r.Price);
            var newestRef = combined.OrderByDescending(c => c.Day).Skip(1).FirstOrDefault().Day; // short term median
            var age = GetDay() - newestRef;
            var virtualBucket = new ReferenceAuctions()
            {
                Lbins = [lbinBucket],
                References = new(combined),
                Price = (combined.Count < 4 ? 0 : GetCappedMedian(auction, longKey, combined) * 98 / 100) * (age > 10 ? (10 - age / 9) : 10) / 10, // older items may have dropped in value
                OldestRef = (short)(newestRef - 2),
                Volatility = 123// mark as risky
            };
            // mark with extra value -3
            var foundAndAbort = FindFlip(auction, lbinPrice, medPrice, virtualBucket, topKey, lookup, longKey, MIN_TARGET == 0 ? 0 : -3, props =>
            {
                var total = 0;
                props.Add("combined", string.Join(",", relevant.TakeWhile(c => (total += c.e.Value.References.Count) < targetVolume)
                    .Select(c => c.e.Key.ToString() + ":" + c.e.Value.References.Count)));
                props.Add("breakdown", JsonConvert.SerializeObject(longKey.ValueBreakdown));
                if (logger?.IsEnabled(LogLevel.Information) ?? false)
                    logger.LogInformation($"Combined {longKey} {auction.Uuid} {virtualBucket.Price} {virtualBucket.Lbin.Price} keys: {string.Join(",", relevant.Select(r => r.e.Key))}");
            });

            long GetCappedMedian(SaveAuction auction, KeyWithValueBreakdown fullKey, List<ReferencePrice> combined)
            {
                var median = GetMedian(combined, []);
                var shortTerm = GetMedian(combined.Take(5).ToList(), new());
                median = CapAtCraftCost(groupTag.tag, Math.Min(median, shortTerm), fullKey, 0);
                return median;
            }
        }

        /// <summary>
        /// Verbatim copy of <see cref="GetCleanItemPrice"/> as of the R2 Phase 0 snapshot — the oracle R2-D
        /// (clean-price de-alloc + the tier-key fix) is gated against.
        /// </summary>
        private long GetCleanItemPriceReference(string tag, KeyWithValueBreakdown key, PriceLookup lookup, bool force = false)
        {
            var tier = key.Key.Tier;
            if (key.Key.Modifiers.Any(m => m.Value == TierBoostShorthand || m.Key == "rarity_upgrades"))
                tier = ReduceRarity(tier);
            if (!force && lookup.CleanPricePerTier.TryGetValue(tier, out var cleanPrice))
            {
                return cleanPrice;
            }
            var matchRarity = tag == "THEORETICAL_HOE_WHEAT_3";
            var minRarity = matchRarity ? key.Key.Tier : key.Key.Tier - 1;
            var select = (NBT.IsPet(tag) ?
                            lookup.Lookup.Where(v => key.Key.Tier == v.Key.Tier && !v.Key.Modifiers.Any(m => m.Value == TierBoostShorthand)).Select(v => v.Value) :
                             lookup.Lookup.Where(v => minRarity <= v.Key.Tier && !v.Key.Modifiers.Any(m => m.Key == "pgems" || Constants.AttributeKeys.Contains(m.Key))).Select(l => l.Value)).ToList();
            var count = select.Count;
            var all = select.SelectMany(v => v.References).ToList();

            if (NBT.IsPet(tag) || matchRarity)
                DropUnderlistings(all, 18);
            var size = (int)Math.Min(Math.Max(lookup.Volume * 10, 50), all.Count);
            var sample = all.OrderByDescending(a => a.Day).ThenBy(l => l.Price)
                .Take(size).OrderBy(r => r.Price);
            var devider = matchRarity ? 10 : 30;

            if (CanHaveGems(tag) && tag != "MELON_DICER_3")
                devider = Math.Min(14, devider);
            var target = sample.Skip(size / devider + 1).FirstOrDefault();
            if (IsMidas(tag))
                return target.Price + 80_000_000; // midas gets undersold very very often
            return target.Price;
        }
    }
}
