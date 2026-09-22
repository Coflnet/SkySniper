# Helix sold-reference component accounting

The correction prevents a second deduction of Perfect gemstones when `AddSoldItem`
builds a sold reference. It changes neither HELIX_CHOPPER weights nor caller-specific
caching. Existing persisted references are not rewritten by this patch.

## Evidence and normalized comparison

The supplied sanitized attachments were inspected directly. `observe auction` also
confirmed the canonical purchase and all three independent comparables below as
completed, positive-price, single-item BIN sales. All share Legendary Moonglade,
Absorb 8 / Ultimate First Impression 5 / Arcane 6, recombination and five stars.
Amounts below are millions of coins, before selling taxes.

| Sale (UTC) | Price | Gems / wood | Absorb progress | logs_cut | Other differences from purchase |
| --- | ---: | --- | ---: | ---: | --- |
| Sep 9 22:38:52, canonical | 45 | Perfect pair / 1 | 3,118,325 | 342,986 | Four level-1 boosts; two unknown level-1 enchants |
| Sep 8 21:28:01 | 59.9 | Fine pair / 0 | 5,010,458 | 200,000 | Same boosts; no unknown enchants |
| Sep 9 13:25:59 | 89.99 | Perfect pair / 0 | 4,239,357 | 200,000 | Hunting wisdom replaces fighting; adds Smelting Touch 1 |
| Sep 10 14:08:31 | 69.999 | Fine pair / 0 | 6,918,714 | 200,000 | No fighting boost; hindsight only |

Public sale provenance, in table order:
`df249dbc092a47b7995eb922151e980d`,
`d368bda6591b44b49b2b71874e06efb4`,
`0972e707cb914c8ebae991df31f60ae2`,
`758dd7341e3f433a95594df7e1aea81d`.
The unchanged 59M resale is an outcome, not another comparable. The two explicitly
identified unsold overview entries are excluded.

The historical snapshot actually returned **22:38:00.991Z**, before listing at
22:38:02Z, has demand for ten Perfect Citrines at 12,000,000.3 each. Two gems yield
24,000,000.6 gross and 23,000,000.6 after the code's 500k extraction fee per gem.
At a sale-tax fraction `t`, proceeds are `23,000,000.6 - 24,000,000.6*t`.
The remaining purchase cost is approximately 22M, including the applied Wood
Singularity. Selling that residual axe is a separate liquidity assumption.

The 22:38:41.015Z snapshot instead gives two-gem demand of 12,000,000.2 plus
11,333,539.7: 22,333,539.9 net extraction before tax. It is after listing and is
not used for the earlier detection or regression. Its 7.98M Wood Singularity
instant-buy quote supplies a replacement-cost sensitivity, not recoverable cash.
The Fine pair's approximately 0.050M gross demand is also from this later snapshot;
it is only a small comparison sensitivity, not a pre-listing executable quote.
No negative Fine extraction value is subtracted.

For a bounded comparison, assume the subject's embedded gem contribution equals
23M net removal value, the Fine pair contributes 0–0.050M, and applied wood earns
`W` between 0 and its 7.98M replacement quote. Shared upgrades are already included.
Let `P` denote the comparable's net progression advantage and `D` its other net
advantages (enchantments/secondary boosts), both uncertain rather than fitted:

| Reference | Normalized subject range before P and D | Sensitivity |
| --- | --- | --- |
| Earlier Fine | 82.850–90.880M | subtract P and unknown-enchantment adjustment |
| Earlier Perfect | 89.990–97.970M | subtract P and hunting/fighting/Smelting adjustment |
| Later Fine, hindsight | 92.949–100.979M | subtract P; account for missing fighting boost |

For example, even the earlier Fine comparison's lower endpoint exceeds 45M by
37.85M before uncertain differences. This is a break-even sensitivity, not a
measured progression premium. A different embedded gem premium changes the Fine
comparison one-for-one; identical Perfect gems cancel in the Perfect comparison.
Absorb progress and logs_cut are distinct counters. The current 8M estimated
logs_cut bucket premium is not independent market evidence and is not substituted
for P. The official `wiki.hypixel.net/Helix_Chopper` request returned HTTP 403;
the operator directed that this branch remain unchanged absent reviewed mechanics.

## Source path and demonstrated defect

`TestNewAuctionInternal` builds `DetailedKeyFromSaveAuction` and tries reduced keys
0–3. `SelectValuable` filters enchants and normalizes modifiers. Both supplied
unknown level-1 enchants are retained in the fixture but filtered below the default
minimum level 6. Efficiency 5 is also below its key minimum. `logs_cut=200000`
normalizes to `200k`, while 342986 is ignored; Absorb progress is not that counter.

`CapKeyLength` calls `HandleGems`, which removes priced Perfect gems from the key
and adds their **gross** value to `ValueSubstract`. Remaining modifier/enchant
ranks, clean value, price-relative reductions, rarity changes and reduced-key
compensation remain unchanged. Missing gem quotes leave Perfect gems embedded.
Wood Singularity is an embedded modifier, not an entry in `RemovableItems`.

Sold ingestion follows `AddSoldItem -> GetBucketForAuction -> KeyFromSaveAuction`
and `AddAuctionToBucket -> CreateReferenceFromAuction`. Previously it passed both
`key.ValueSubstract` and `GetExtraValue` to reference construction. The latter
included **net** Perfect gem value again, so storage deducted gross plus net.
At 45M, `CreateReferenceFromAuction`'s negative-price guard discards the second
deduction. At the verified 89.99M price it does not: the reference is 23M too low.
This price-dependent guard explains why a cheap canonical-only test misses the bug.

The patch deducts only other removable items and Flawless gems in that second
storage argument. Perfect gem accounting remains in `HandleGems`. Fine gems retain
zero removable credit. `GetExtraValue` still returns all net removable value for
retrieval: `GetPriceInternal -> AssignMedian` and finder `FindFlip` add it to the
bucket. Their existing missing-value compensation and caps are unchanged, as are
LBIN storage and retrieval. This fixes a reference-building input to the actual
finder, without claiming the exact reference selected by the historical finder.

## Regression and limits

`Services/HelixReferenceAccounting.Tests.cs` contains identifier-free purchase,
earlier Fine, and earlier Perfect fixtures with full pricing-relevant NBT,
enchantments, tier/reforge, count, BIN/bid amounts and timestamps. No additional
coins are reported; `additional_coins` is absent. Bidder/seller/auction/item IDs
are absent. Deserialization uses replacement semantics so the computed NBT getter
does not silently discard the fixture dictionary.

The component input is the supplied **pre-listing demand**, injected through
`UpdateBazaar`; per-unit integer conversion explains 24M gross / 23M net in code.
This intentionally isolates executable demand, not the production Bazaar midpoint
or historical smoothed median. The immutable HELIX_CHOPPER capture at
2026-09-10T17:48:20.952384Z (SHA-256
`fe80ceede2afd54b50b843670ff15fd85c3a00ffbd4ad51563a70091a7e8288d`)
was consumed programmatically. Only its 6.3M Epic clean-price aggregate is used in
the test. An exact main-enchant/logs/stars bucket had one reference and zero price;
this is current state evidence, not the earlier comparable's historical bucket.

The test leaves cross-item enchant/reforge/wood prices, craft costs and item catalog
unpopulated. It preserves their auction fields and checks the accounting invariant
against whatever non-gem value the real key path removes, without fitting these
missing prices. It does not reproduce full historical caps or reference selection.
The optional FLAWLESS_CITRINE_GEM capture failed with backend InternalError; no
Flawless price or atomic dependency capture is claimed or used. Existing removable
helper tests cover the behavior-preserving extraction of non-gem valuation.

`observe valuation` returned no saved contexts. Original log timestamps may be
flush times; current keys and a zero saved target are not historical measurements.
The demonstrated 23M storage error is not proof it alone caused this specific miss.

Base is `4aba08f1ce9e98e082023d9b797c320a25909833`; mutable validation uses an
executable task-scratch source copy with sibling HypixelSkyblock at
`3db54ac02b76273db1be38bff3755a8ebef57055`. Repository project references are unchanged.
The exact focused command is in the reserved **untracked** `.coflnet-regression.json`.
On base, the sale-restoration assertion expected 89,990,000 but got 66,990,000;
the retrieval assertion expected at least 88,990,000 but got 65,990,000.
The final focused command ran nine tests: base failed two and passed seven;
the patch passed all nine, including both previously failing assertions. No compiler warnings were emitted on changed lines; the two CS0219 warnings
refer to unchanged base statements in key construction.

Independent review and the host-recorded `regression_base_fail_patch_pass` comparison
remain required. The trusted host owns the full Docker suite after sealing.
No production data writes, deployment, publication or workflow changes were made.
