# Lookup Load Rebuild Behavior

## What happens on load

When persisted lookup data is merged through `SniperService.AddLookupData`, SkySniper keeps most saved bucket medians as-is.
It only rebuilds a bucket immediately if the persisted price is clearly out of line with recent references for that same key.

## Rebuild rules

- The bucket must have at least 6 references before the stale check runs.
- After anti-manipulation cleanup and underlisting removal, there must still be at least 4 references from the last 30 days.
- The recomputed recent median must differ by more than 25% from the persisted `bucket.Price`.
- The absolute delta must also exceed 1.5m coins.
- Clean buckets only repair stale-high saved medians.
- Buckets with kill modifiers such as `eman_kills` also repair stale-low saved medians.

## Why not rebuild every bucket on load

- Startup stays cheaper than a full `RefreshLookup` over every persisted bucket.
- Persisted medians remain stable when recent samples are small or noisy.
- Restarting the process does not reshuffle prices unless the saved median is clearly wrong.

## Full refresh versus load repair

`RefreshLookup` is still the explicit full recomputation path.
The load-time rebuild is only a targeted repair step for clearly stale persisted prices.