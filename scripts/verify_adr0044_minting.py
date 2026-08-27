#!/usr/bin/env python
"""Does the broker's minting path actually work against a real store?

ADR-0044 made the mesh-publishing path reach ``_mint_s3``. That code had never
executed against a live store — the protocol path short-circuited past it, so
it was dead. Unit tests prove the ROUTING (we call it) and the POLICY SHAPE
(what we ask for). They cannot prove MinIO accepts the call, because the STS
server in those tests is a stub we wrote.

So this exercises the REAL function against a REAL store and then does the one
thing that matters: **uses the minted credential to attempt a write, and
demands a refusal.**

    python scripts/verify_adr0044_minting.py \
        --endpoint http://minio-svc.d4-sandbox.svc.cluster.local:9000 \
        --bucket publog-lake --prefix publog/p_cage

Caller credentials come from the ambient AWS chain (AWS_ACCESS_KEY_ID /
AWS_SECRET_ACCESS_KEY) — the same chain the broker pod uses, so running this
in that pod tests the broker's real identity rather than yours.

────────────────────────────────────────────────────────────────────────────
PRE-REGISTERED PREDICTIONS. Written before the first run, so the result can
disagree. An experiment whose prediction is recorded after the fact cannot
surprise anyone.

  P1. The AssumeRole call FAILS on the first attempt, because
      AWS_ASSUME_ROLE_ARN still defaults to the placeholder
      `arn:aws:iam::123456789012:role/DataAccessRole` inherited from the old
      fallback path, and MinIO rejects a RoleArn it has no configuration for.
      Confidence: high.
  P2. Dropping RoleArn entirely succeeds, because MinIO's AssumeRole derives
      the session from the CALLER's own policy intersected with the inline
      session policy, and does not require a configured role.
      Confidence: medium-high.
  P3. If a session is issued, GetObject on the asset's prefix SUCCEEDS and
      PutObject to the same prefix is REFUSED. Confidence: high — but this is
      the prediction whose failure would matter most, because a session that
      can write is the defect ADR-0044 exists to remove, reappearing with a
      minted credential's clean bill of health.
  P4. GetObject on a DIFFERENT bucket is refused. Confidence: high.

Record what actually happened next to these in the plan item.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dag_tools.domain_broker.main import _mint_s3, _s3_scope_from_uri  # noqa: E402


def _p(label: str, ok: bool | None, detail: str = "") -> None:
    mark = {True: "PASS", False: "FAIL", None: "INFO"}[ok]
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True, help="MinIO S3/STS endpoint (FQDN, please)")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="", help="key prefix the asset lives under")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--other-bucket", default="", help="a bucket the ticket must NOT reach")
    args = ap.parse_args()

    physical_uri = f"s3://{args.bucket}/{args.prefix}".rstrip("/") + "/"
    scope = _s3_scope_from_uri(physical_uri)
    coordinates = {"endpoint_url": args.endpoint, "region": args.region}

    print(f"\nendpoint        : {args.endpoint}")
    print(f"physical_uri    : {physical_uri}")
    print(f"derived scope   : {scope}")
    print(f"role arn (env)  : {os.getenv('AWS_ASSUME_ROLE_ARN', '<placeholder default>')}")
    print(f"ttl (env)       : {os.getenv('BROKER_CREDENTIAL_TTL_SEC', '900')}s")
    print(f"caller key      : {os.getenv('AWS_ACCESS_KEY_ID', '<unset — will fail>')}\n")

    if not os.getenv("AWS_ACCESS_KEY_ID"):
        _p("caller credentials present", False,
           "AWS_ACCESS_KEY_ID unset; boto3 has no identity to assume FROM")
        return 2

    # ── 1. the real function, unmodified ───────────────────────────────────
    print("1. minting via dag_tools.domain_broker.main._mint_s3")
    minted = None
    try:
        minted = _mint_s3(scope, "urn:verify:adr0044", coordinates)
        _p("assume_role accepted", True,
           f"key={minted['aws_access_key_id'][:8]}… token={len(minted.get('aws_session_token',''))}B")
    except Exception as exc:
        _p("assume_role accepted", False, f"{type(exc).__name__}: {exc}")
        print("\n   P1 was that this fails on the placeholder RoleArn. Retrying without it,\n"
              "   which is P2 — MinIO deriving the session from the caller's own policy.\n")

        # Exercise P2 directly. NOT a fix — a measurement that tells us what
        # the fix should be.
        try:
            import json as _json
            sts = boto3.client("sts", endpoint_url=args.endpoint, region_name=args.region)
            policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Action": ["s3:GetObject"],
                     "Resource": [f"arn:aws:s3:::{scope['bucket']}/{scope['prefix']}/*"]},
                    {"Effect": "Allow", "Action": ["s3:ListBucket"],
                     "Resource": [f"arn:aws:s3:::{scope['bucket']}"]},
                ],
            }
            resp = sts.assume_role(
                RoleSessionName="verify-adr0044",
                Policy=_json.dumps(policy),
                DurationSeconds=900,
            )
            c = resp["Credentials"]
            minted = {
                "aws_access_key_id": c["AccessKeyId"],
                "aws_secret_access_key": c["SecretAccessKey"],
                "aws_session_token": c["SessionToken"],
            }
            _p("assume_role WITHOUT RoleArn", True,
               "P2 confirmed — _mint_s3 must omit RoleArn for MinIO")
        except Exception as exc2:
            _p("assume_role WITHOUT RoleArn", False, f"{type(exc2).__name__}: {exc2}")
            print("\n   Both forms failed. The minting design needs revisiting for this store\n"
                  "   BEFORE step 3 is called done — do not paper over this by restoring the\n"
                  "   producer credential, which is the defect.\n")
            return 1

    # ── 2. what the minted credential can actually do ──────────────────────
    print("\n2. exercising the minted credential (the acceptance criterion)")
    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        region_name=args.region,
        aws_access_key_id=minted["aws_access_key_id"],
        aws_secret_access_key=minted["aws_secret_access_key"],
        aws_session_token=minted.get("aws_session_token"),
    )

    # READ — must succeed.
    try:
        listing = s3.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix, MaxKeys=3)
        keys = [o["Key"] for o in listing.get("Contents", [])]
        _p("can list its own prefix", True, f"{len(keys)} key(s): {keys[:2]}")
        if keys:
            s3.get_object(Bucket=args.bucket, Key=keys[0])
            _p("can read its own object", True, keys[0])
    except ClientError as exc:
        _p("can read its own prefix", False, exc.response["Error"]["Code"])

    # WRITE — must be REFUSED. This is the whole point.
    probe_key = f"{args.prefix.rstrip('/')}/_adr0044_write_probe_{uuid.uuid4().hex[:8]}"
    try:
        s3.put_object(Bucket=args.bucket, Key=probe_key, Body=b"should not exist")
        _p("write is refused", False,
           f"WROTE {probe_key} — the ticket credential CAN WRITE. ADR-0044 is not "
           f"satisfied. DELETE THIS OBJECT.")
    except ClientError as exc:
        _p("write is refused", True, exc.response["Error"]["Code"])

    # CROSS-ASSET — must be refused.
    if args.other_bucket:
        try:
            s3.list_objects_v2(Bucket=args.other_bucket, MaxKeys=1)
            _p("other bucket is refused", False,
               f"reached {args.other_bucket} — scope is wider than the asset")
        except ClientError as exc:
            _p("other bucket is refused", True, exc.response["Error"]["Code"])
    else:
        _p("other bucket is refused", None, "skipped — pass --other-bucket to test")

    print("\nRecord these results against the pre-registered predictions in\n"
          "docs/plans/broker-advertises-unminted-credential.md\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
