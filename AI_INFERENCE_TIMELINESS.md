# Timely AI estimates and listing-backed snipes

For auction `ae6b705d8ac24ed8a5b05a65f64c3c6f` (Maxor boots), the saved
SNIPER target was 50,958,299 coins; the saved AI estimate was 31,893,528.
The SNIPER context had `reference=0`, `mVal=0`, and `refCount=0`.
`refCount` is the deduplicated count, not the number of raw sales.

## Timing evidence (2026-09-16, UTC)

Both saved contexts used upstream time 18:27:24.

| Saved finder pass | Upstream to receive (`frec`) | Upstream to producer call (`fsend`) | Tracker saved at |
| --- | ---: | ---: | --- |
| SNIPER | 0.1179733 s | 0.1187441 s | 18:27:28.562 |
| AI | 423.7729675 s | 423.7846157 s | 18:34:28.449 |

The recorded AI pass was late before it received the auction; receive to
producer call was 11.6482 ms. This does not measure broker acknowledgement.
Tracker overwrites contexts keyed by auction/finder, so this record cannot
prove that there was no earlier AI emission. The AI context omitted the worker
name, preventing attribution to a particular replica.

## Mechanism and change

Each replica consumes new auctions independently. Previously the consumer
called `EstimateAsync` synchronously after its SNIPER batch. Its global read
lock could wait behind model fitting, evaluation, and storage; cold estimates
could also load and refit inline. A blocked callback prevents consuming the
next batch. The synchronous retraining callback acquired the same training
lock through `ItemKeys`/`GetModelStats`.

Live ingestion now calls `EstimateReady`: a published engine, frozen feature
mapping, training sample count, metrics, and label cap are swapped together.
Training and storage use a different lock. Missing/unready/nonfinite estimates
abstain. The retraining callback checks item relevance without enumerating
training state. Existing synchronous Kafka/Redis calls remain, so this is not
an end-to-end latency guarantee. Historical timing establishes delayed intake,
not which blocking episode caused it.

AI runs before the batch offset is committed, preserving supported SNIPER
priority. `NO_PRODUCE` now also disables AI publication. New AI messages carry
`server` so future receive/send timings can be attributed to a replica.

SNIPER now requires a positive-price listing with a nonzero auction identity
other than the candidate itself, from its own or a qualifying higher-variant
bucket. It reports the listing actually used. Craft costs or borrowed sales
alone cannot authorize SNIPER publication. Median, STONKS, and CraftCost finder
rules are unchanged. This removes the unsupported SNIPER emission rather than
trying to overwrite an already-delivered flip through client deduplication.

## Regression coverage

`ReferenceBackedAi.Tests.cs` uses the reported pricing components and a
controlled sparse state, not a historical state snapshot. It rejects zero/self
references, retains supported own/higher listings, and checks AI ingestion,
readiness, invalid predictions, worker metadata, and `NO_PRODUCE`.
`SelfLearningReadyInference.Tests.cs` exercises inference with the training lock
held, blocked persistence, feature expansion, and persisted-model restoration.
`RetrainScheduling.Tests.cs` checks the actual registered callback and relevant
versus unsupported items without Kafka, Redis, or training-state access.

## Validation

Compared against upstream `ded6961` using .NET SDK 10.0.111, the existing core
project at `3db54ac02b76273db1be38bff3755a8ebef57055`, and the same local NEU
constants in both runs. `reforgestones.json` SHA-256:
`9fa8ff7f75f17e5cc55a8beff59b8867b51700d2b02bd69b9ce10cf78b44318e`.

Command: `dotnet test --no-restore --settings ci.runsettings -p:GeneratePackageOnBuild=false`.
With the 19 new regression cases overlaid, unchanged upstream had 446 passing,
16 failing, and 2 skipped tests. The patch had 461 passing, 1 failing, and 2
skipped tests. All 19 new cases pass; 15 fail on unchanged upstream. The sole
remaining failure is the unchanged `MedianCalcTests.DedupsBuyer` assertion
(its computed price is 2,000 versus the fixture's expected 3,000).
This is not a green full-suite result or a deployed-production latency check.
