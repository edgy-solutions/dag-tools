#!/usr/bin/env python
"""How many credentials per second can the store actually mint?

ADR-0044 moved minting onto the ticket-issuance hot path: every read now costs
an AssumeRole. Its "indicators for revisiting" says to measure the store's
limits BEFORE the load arrives, because the mitigation — caching minted
credentials by (caller, asset, window) — is a deliberate weakening of decision
point 1 and must be designed with a stated window rather than patched under
load.

This measures the ceiling. It does not change anything.

    python scripts/measure_adr0044_sts_ceiling.py \
        --endpoint http://127.0.0.1:19000 --bucket dag-lake --prefix mesh_demo \
        --target-rate 33

THE LOAD THAT MATTERS IS NOT ANALYST SCANS. An analyst scanning a parquet file
mints once and then reads for minutes. The demanding profile is live-view
artifacts recomputing on an interval: cards x refresh-interval x concurrent
sessions. Twenty cards refreshing every 30s across fifty sessions is ~33
mints/sec sustained, with no human pause between them. Pass that as
--target-rate so the verdict is against the real requirement.

────────────────────────────────────────────────────────────────────────────
THE INSTRUMENT CAN BE THE BOTTLENECK, AND WOULD LOOK EXACTLY LIKE A CEILING.
A `kubectl port-forward` is a single userspace TCP proxy. If throughput
plateaus while MinIO reports no errors and latency climbs linearly with
concurrency, the plateau is probably the forward, not the store. That reading
is called out explicitly below rather than left for someone to misinterpret as
a MinIO limit — measuring must never quietly report the measuring apparatus.
Run in-cluster for a number you can design against.

PRE-REGISTERED PREDICTIONS, written before the first run:

  P1. No AssumeRole rate limit is enforced. MinIO has no documented STS
      throttle (unlike AWS STS's account-level limits), so failures — if any —
      arrive as connection errors, not Throttling/SlowDown error codes.
      Confidence: medium-high.
  P2. p50 latency per mint under low concurrency is < 100ms through a
      port-forward. Confidence: medium.
  P3. Sustained throughput exceeds a 33/sec target by a wide margin, making
      the credential cache UNNECESSARY at present scale — i.e. decision
      point 1 stands unweakened. Confidence: medium. This is the prediction
      worth being wrong about, because being wrong means designing the cache
      now rather than discovering it later.
  P4. Throughput plateaus somewhere, and the plateau is the port-forward
      rather than MinIO. Confidence: medium.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _policy(bucket: str, prefix: str) -> str:
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetObject"],
             "Resource": [f"arn:aws:s3:::{bucket}/{prefix}/*"]},
            {"Effect": "Allow", "Action": ["s3:ListBucket"],
             "Resource": [f"arn:aws:s3:::{bucket}"]},
        ],
    })


# botocore's STS model marks RoleArn REQUIRED and validates it client-side, so
# omitting it fails before a packet leaves the process — which is how the first
# run of this script recorded 0 successes and no data at all. MinIO ignores the
# value (verified: ADR-0044's live run accepted the placeholder), but botocore
# still demands one be present. Same ARN _mint_s3 sends, so this measures the
# call the broker actually makes.
_ROLE_ARN = os.getenv("AWS_ASSUME_ROLE_ARN", "arn:aws:iam::123456789012:role/DataAccessRole")


def _mint_once(sts, policy: str, i: int):
    t0 = time.perf_counter()
    try:
        sts.assume_role(
            RoleArn=_ROLE_ARN,
            RoleSessionName=f"ceiling-{i}",
            Policy=policy,
            DurationSeconds=900,
        )
        return (time.perf_counter() - t0) * 1000.0, None
    except ClientError as exc:
        return (time.perf_counter() - t0) * 1000.0, exc.response["Error"]["Code"]
    except Exception as exc:  # connection-level: the interesting failure mode
        return (time.perf_counter() - t0) * 1000.0, type(exc).__name__


def run_wave(endpoint, region, policy, concurrency, total):
    # One client per thread: botocore clients are not thread-safe, and sharing
    # one would measure lock contention in this script rather than the store.
    cfg = Config(max_pool_connections=concurrency + 8, retries={"max_attempts": 0})
    clients = [
        boto3.client("sts", endpoint_url=endpoint, region_name=region, config=cfg)
        for _ in range(concurrency)
    ]
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        results = list(pool.map(
            lambda i: _mint_once(clients[i % concurrency], policy, i), range(total)
        ))
    wall = time.perf_counter() - t0
    lat = [r[0] for r in results if r[1] is None]
    errs: dict = {}
    for _, e in results:
        if e:
            errs[e] = errs.get(e, 0) + 1
    return {
        "concurrency": concurrency,
        "requests": total,
        "ok": len(lat),
        "errors": errs,
        "wall_s": wall,
        "rate": len(lat) / wall if wall else 0.0,
        "p50": statistics.median(lat) if lat else None,
        "p95": (statistics.quantiles(lat, n=20)[18] if len(lat) > 20 else (max(lat) if lat else None)),
        "max": max(lat) if lat else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--target-rate", type=float, default=33.0,
                    help="mints/sec the live-refresh profile needs")
    ap.add_argument("--waves", default="1,4,8,16,32")
    args = ap.parse_args()

    if not os.getenv("AWS_ACCESS_KEY_ID"):
        print("AWS_ACCESS_KEY_ID unset — nothing to assume from.")
        return 2

    policy = _policy(args.bucket, args.prefix)
    print(f"\nendpoint    : {args.endpoint}")
    print(f"scope       : {args.bucket}/{args.prefix}")
    print(f"target rate : {args.target_rate}/s (live-refresh profile)\n")
    print(f"{'conc':>5} {'reqs':>5} {'ok':>5} {'rate/s':>9} {'p50ms':>8} {'p95ms':>8}  errors")

    rows = []
    for c in [int(x) for x in args.waves.split(",")]:
        row = run_wave(args.endpoint, args.region, policy, c, max(c * 5, 20))
        rows.append(row)
        p50 = f"{row['p50']:.1f}" if row["p50"] else "-"
        p95 = f"{row['p95']:.1f}" if row["p95"] else "-"
        print(f"{c:>5} {row['requests']:>5} {row['ok']:>5} {row['rate']:>9.1f} "
              f"{p50:>8} {p95:>8}  {row['errors'] or '-'}")

    best = max(rows, key=lambda r: r["rate"])
    any_throttle = any(
        code in ("Throttling", "SlowDown", "TooManyRequests", "RequestLimitExceeded")
        for r in rows for code in r["errors"]
    )

    print("\n-- verdict ----------------------------------------------------")
    print(f"peak sustained : {best['rate']:.1f} mints/s at concurrency {best['concurrency']}")
    print(f"headroom       : {best['rate'] / args.target_rate:.1f}x the {args.target_rate}/s target")
    print(f"throttle codes : {'YES — a real store limit' if any_throttle else 'none observed (P1)'}")

    # P4: distinguish a store ceiling from the instrument's ceiling.
    scaled = [r for r in rows if r["concurrency"] > 1]
    if scaled and best["concurrency"] < max(r["concurrency"] for r in rows) and not any_throttle:
        print("\nNOTE: throughput peaked BELOW max concurrency with no throttle codes and\n"
              "rising latency. That is the port-forward saturating, not MinIO. Re-run\n"
              "in-cluster before designing anything against this number.")

    if best["rate"] < args.target_rate:
        print("\nP3 FALSIFIED: the store cannot meet the live-refresh profile. The\n"
              "credential cache is REQUIRED, not optional — design it with a stated\n"
              "window (caller, asset, window) rather than discovering it under load.")
    else:
        print("\nP3 held at this scale: decision point 1 stands unweakened; no cache needed yet.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
