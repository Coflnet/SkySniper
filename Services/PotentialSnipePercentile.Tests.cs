using System;
using AwesomeAssertions;
using NUnit.Framework;

namespace Coflnet.Sky.Sniper.Services
{
    /// <summary>
    /// R8 WS-CHURN-A (partial selection) bit-exactness gate for the divided-price 25th-percentile order statistic.
    ///
    /// <para>
    /// <b>What is under test.</b> <see cref="SniperService.PotentialSnipeQuarterPercentile(long[],int,long)"/> selects
    /// the <c>count/4</c>-th smallest VALUE of the thread-static divided-price buffer filled by
    /// <c>PotentialSnipeHigherValueScan</c>. WS-CHURN-A replaces the original full <see cref="Array.Sort"/> with an
    /// in-place quickselect (nth_element). Because the result is a scalar order statistic over a <c>long[]</c>, the
    /// k-th smallest value is uniquely defined regardless of duplicates, so the partial selection must return the
    /// <b>identical</b> long as the full-sort oracle
    /// <see cref="SniperService.PotentialSnipeQuarterPercentileReference(long[],int,long)"/> for every input.
    /// </para>
    ///
    /// <para>
    /// <b>Why the heavy-tie emphasis.</b> A median-of-three Lomuto quickselect is the place divergence would hide: on
    /// dense ties / all-equal / already-sorted / reverse-sorted inputs a naive partition can mis-place the boundary or
    /// degrade. This fuzz therefore drives <b>50,000</b> seeds across narrow value ranges, all-equal buffers, many
    /// duplicates, negatives, and counts NOT divisible by 4, over lengths 0..2000, and asserts 0 mismatches.
    /// The buffer is also over-provisioned with a randomized stale tail beyond <c>count</c> (mirroring the reused
    /// <c>[ThreadStatic]</c> field) to prove only <c>[0, count)</c> is consulted.
    /// </para>
    /// </summary>
    public class PotentialSnipePercentileTests
    {
        [Test]
        public void Fuzz_QuickSelect_BitExact_VsFullSortReference_HeavyTies()
        {
            const int seeds = 50_000;
            int mismatches = 0;
            string first = "";
            long maxLen = 0;
            int heavyTieCases = 0;     // narrow-range / all-equal cases actually generated
            int notDivBy4Cases = 0;    // count % 4 != 0 cases (the floor of count/4 boundary)
            int negativeCases = 0;     // buffers that contained at least one negative

            for (int seed = 1; seed <= seeds; seed++)
            {
                var rng = new Random(seed);
                int count = rng.Next(0, 2001); // length 0..2000

                // Pick a generation mode so we span all-equal -> narrow ties -> wide-distinct, with negatives.
                int mode = rng.Next(7);
                long[] data = new long[count];
                bool sawNegative = false;
                bool narrowOrEqual = false;
                switch (mode)
                {
                    case 0: // all equal (heaviest tie)
                        {
                            long v = rng.Next(-50, 50);
                            for (int i = 0; i < count; i++) data[i] = v;
                            narrowOrEqual = true;
                            sawNegative = v < 0;
                            break;
                        }
                    case 1: // tiny range, includes negatives (dense ties)
                        for (int i = 0; i < count; i++) { data[i] = rng.Next(-3, 3); if (data[i] < 0) sawNegative = true; }
                        narrowOrEqual = true;
                        break;
                    case 2: // small range, many duplicates
                        for (int i = 0; i < count; i++) data[i] = rng.Next(0, 5);
                        narrowOrEqual = true;
                        break;
                    case 3: // already sorted ascending (quickselect adversary)
                        for (int i = 0; i < count; i++) { data[i] = i - count / 2; if (data[i] < 0) sawNegative = true; }
                        break;
                    case 4: // reverse sorted (quickselect adversary)
                        for (int i = 0; i < count; i++) { data[i] = (count - i) - count / 2; if (data[i] < 0) sawNegative = true; }
                        break;
                    case 5: // wide distinct-ish, full long span (includes negatives)
                        for (int i = 0; i < count; i++)
                        {
                            data[i] = ((long)rng.Next(int.MinValue, int.MaxValue) << 1) ^ rng.Next();
                            if (data[i] < 0) sawNegative = true;
                        }
                        break;
                    default: // medium range with negatives
                        for (int i = 0; i < count; i++) { data[i] = rng.Next(-100_000, 100_000); if (data[i] < 0) sawNegative = true; }
                        break;
                }

                // Over-provision the buffer beyond `count` with a randomized stale tail: the selection must only ever
                // read/permute [0, count) so this tail must not affect the result (mirrors the reused ThreadStatic).
                int cap = count + rng.Next(0, 6);
                long[] live = new long[cap];
                long[] oracle = new long[cap];
                for (int i = 0; i < count; i++) { live[i] = data[i]; oracle[i] = data[i]; }
                for (int i = count; i < cap; i++) { long t = rng.Next(-9, 9); live[i] = t; oracle[i] = t; }

                long fallback = rng.Next(-1_000, 1_000);

                long got = SniperService.PotentialSnipeQuarterPercentile(live, count, fallback);
                long want = SniperService.PotentialSnipeQuarterPercentileReference(oracle, count, fallback);

                if (got != want)
                {
                    if (mismatches == 0)
                        first = $"seed={seed} count={count} mode={mode} got={got} want={want}";
                    mismatches++;
                }

                if (count > maxLen) maxLen = count;
                if (narrowOrEqual && count >= 8) heavyTieCases++;
                if (count % 4 != 0) notDivBy4Cases++;
                if (sawNegative) negativeCases++;
            }

            // The generator must actually have exercised the regimes this gate exists to protect.
            heavyTieCases.Should().BeGreaterThan(1_000, "the heavy-tie generator must produce many dense-tie buffers");
            notDivBy4Cases.Should().BeGreaterThan(1_000, "many counts must NOT be divisible by 4 (floor boundary)");
            negativeCases.Should().BeGreaterThan(1_000, "negatives must be exercised");
            maxLen.Should().BeGreaterThan(1_800, "the fuzz must cover large buffers up to ~2000");

            mismatches.Should().Be(0,
                $"quickselect 25th-percentile must be bit-exact with the full-sort reference over {seeds} heavy-tie "
              + $"seeds (maxLen={maxLen}). First mismatch: {first}");
        }

        [Test]
        public void EdgeCases_Match_Reference()
        {
            var cases = new (long[] buf, int count, long fallback)[]
            {
                (Array.Empty<long>(), 0, 777),                       // empty -> fallback
                (new long[] { 5 }, 1, 0),                            // single element, k=0
                (new long[] { 5, 5, 5, 5 }, 4, 0),                   // all equal, k=1
                (new long[] { 9, 1, 9, 1, 9 }, 5, 0),               // ties, count%4 != 0 (k=1)
                (new long[] { -3, -1, -2, 0, 4, 2 }, 6, 0),         // negatives, k=1
                (new long[] { 3, 2, 1 }, 3, 0),                      // reverse, k=0
                (new long[] { 1, 2, 3 }, 3, 0),                      // sorted, k=0
                (new long[] { 7, 7 }, 2, 0),                         // two equal, k=0
                (new long[] { 10, 20, 30, 40, 50, 60, 70, 80 }, 8, 0), // distinct, k=2
                (new long[] { 5, 4, 3, 2, 1, 0, 0, 0, 0 }, 9, 0),   // heavy zeros tail, k=2
            };

            foreach (var (buf, count, fallback) in cases)
            {
                var live = (long[])buf.Clone();
                var oracle = (long[])buf.Clone();
                long got = SniperService.PotentialSnipeQuarterPercentile(live, count, fallback);
                long want = SniperService.PotentialSnipeQuarterPercentileReference(oracle, count, fallback);
                got.Should().Be(want, $"count={count}");
            }
        }
    }
}
