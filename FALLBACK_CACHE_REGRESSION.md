Fallback cache correctness
==========================

The fallback median and LBin caches now retain reference selection, rather than
mutable final estimates. Every request applies its own removable value, modifier
and attribute corrections, breakdown adjustments, and final combined-item/event
adjustments. The existing tag/key cache identity and expiry sweep remain intact.
The top median candidate and lower-value bound are cached; ordered candidates are
materialized lazily only if the top candidate cannot produce a positive median.
Full and stripped variants still share candidate-selection cache entries.

Evidence and scope
------------------

`Services/FallbackCache.Tests.cs` preserves every supplied purchase field for both
Helix cases and the drill case from `cache-evidence.json`, including exact NBT,
enchantments, tier, reforge, category, count, BIN status, bid amounts and times.
No production identities are retained. The attachment supplies no bid list or
additional-coins field; none was invented. Supplemental pet, attribute and starred
fixtures are synthetic branch tests, not additional production cases.

The tests use controlled reference buckets and component prices to isolate cache
correctness. Dependencies are the real auction parser, gem/removable-item mapping,
seeded Bazaar and component lookups, reference metadata, attribute adjustment,
fragment pricing and the existing craft-cost mock. They do not replay captured
production prices, depend on current external market state, or infer a target from
an absent finder valuation. No pricing-state artifact is needed for this cache
invariance comparison.

The base reproduces upgraded medians leaking into stripped requests, missing warm
metadata, repeated starred adjustments, and fallback LBin reusing an attribute
correction computed from another variant's median. The dropped-value case checks
fresh-equivalence and metadata; it does not establish a missing dropped-value
premium or change the existing valuation formula. Exact LBin removable-value and
higher-value cap behavior also remain unchanged.

Validation
----------

Pinned base: `4aba08f1ce9e98e082023d9b797c320a25909833`.
Local dependency: HypixelSkyblock `3db54ac02b76273db1be38bff3755a8ebef57055`.
The repository expects this dependency at `../dev`; local checks use a scratch
copy with that layout, without changing project files.

Focused command:
`dotnet test SkySniper.csproj -p:IsTestProject=true --filter FullyQualifiedName~FallbackCacheTests --verbosity quiet`

The reserved `.coflnet-regression.json` supplies this command to the host. The
maintainer explicitly requested a local base-fails comparison, which takes
precedence over the curated guide's host-only comparison advice. Only the pinned
production source was substituted in the scratch build; identical tests ran on
both sides. All nine new regressions fail behaviorally on base and pass patched.
The six existing `GetPriceGoldenTests` also pass with `ci.runsettings` and an
explicit focused filter. No full suite was run locally; the required
`docker build --pull --tag coflnet-sky-sniper-test .` and sealed regression gate
remain host validation steps.

The historical auction-finder path's use of this cache remains unproven. This
patch fixes the current read-path defect without attributing historical misses,
changing finding policy, tuning market prices, deploying, or migrating data.
