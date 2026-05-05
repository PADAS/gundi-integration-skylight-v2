#!/usr/bin/env python3
"""
Explore Skylight V2 API fields.

Phase 1 — Schema introspection: discover all fields available on each
          event detail type and vessel type.
Phase 2 — Live data: fetch a sample of the last 30 days across all event
          types and print one full record per type so we can see what is
          actually populated.

Usage (from repo root):
    python scripts/explore_skylight_fields.py
"""
import os
import sys
from pathlib import Path

# Load .env BEFORE any gundi imports — settings are baked at import time.
_env = Path(__file__).parent.parent / ".env"
if _env.exists():
    for _line in _env.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

import asyncio
import json
from datetime import datetime, timedelta, timezone

from gql import Client as GQLClient, gql
from gql.transport.exceptions import TransportQueryError
from gql.transport.httpx import HTTPXTransport
from gundi_client_v2.client import GundiClient

SKYLIGHT_URL = "https://api.skylight.earth/graphql"
INTEGRATION_ID = "a239c435-8717-4daf-ad56-790047653acb"

ALL_EVENT_TYPES = [
    "fishing_activity_history",
    "dark_rendezvous",
    "standard_rendezvous",
    "speed_range",
    "aoi_visit",
    "viirs",
    "sar_sentinel1",
    "eo_sentinel2",
    "eo_landsat_8_9",
]

DETAIL_TYPES = [
    "FishingEventDetails",
    "DarkRendezvousEventDetails",
    "StandardRendezvousEventDetails",
    "SpeedRangeEventDetails",
    "AoiVisitEventDetails",
    "ImageryMetadataEventDetails",
]

VESSEL_TYPE_CANDIDATES = [
    "VesselsV2", "VesselV2", "VesselsType", "VesselDetailV2",
    "VesselDetails", "VesselInfo", "VesselData",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_client(headers=None):
    transport = HTTPXTransport(url=SKYLIGHT_URL, headers=headers or {}, verify=True)
    return GQLClient(transport=transport, execute_timeout=60, fetch_schema_from_transport=False)


def skylight_auth(username: str, password: str) -> str:
    client = make_client()
    q = gql("""
        query($u: String!, $p: String!) {
            getToken(username: $u, password: $p) {
                access_token
                token_type
            }
        }
    """)
    r = client.execute(q, variable_values={"u": username, "p": password})
    t = r["getToken"]
    return f"{t['token_type']} {t['access_token']}"


def introspect_type(client: GQLClient, type_name: str):
    q = gql("""
        query($n: String!) {
            __type(name: $n) {
                name
                fields {
                    name
                    type { name kind ofType { name kind } }
                }
            }
        }
    """)
    r = client.execute(q, variable_values={"n": type_name})
    return r.get("__type")


async def fetch_credentials() -> tuple[str, str]:
    async with GundiClient(
        base_url=os.environ["GUNDI_API_BASE_URL"],
        keycloak_client_id=os.environ["KEYCLOAK_CLIENT_ID"],
        keycloak_client_secret=os.environ["KEYCLOAK_CLIENT_SECRET"],
        keycloak_issuer=os.environ["KEYCLOAK_ISSUER"],
        keycloak_audience=os.environ.get("KEYCLOAK_AUDIENCE", "cdip-admin-portal"),
    ) as gundi:
        integration = await gundi.get_integration_details(INTEGRATION_ID)

    for conf in integration.configurations:
        if hasattr(conf, "action") and conf.action.value == "auth":
            return conf.data["username"], conf.data["password"]

    raise RuntimeError("No auth config found on integration")


def build_data_query(schema: dict, include_vessel1: bool = True) -> str:
    fragments = ""
    for type_name, fields in schema.items():
        field_list = "\n                    ".join(f["name"] for f in fields)
        fragments += f"""
                    ... on {type_name} {{
                        {field_list}
                    }}"""

    vessel1_block = ""
    if include_vessel1:
        vessel1_block = "\n                    vessel1 { name mmsi imo countryCode trackId category type class length }"

    return f"""
    query exploreFields(
        $types: [EventTypeV2!]!
        $start: String
        $end: String
        $limit: Int
    ) {{
        searchEventsV2(input: {{
            eventTypes: $types
            startTime: $start
            endTime: $end
            limit: $limit
        }}) {{
            records {{
                eventId
                eventType
                createdAt
                updatedAt
                start {{ point {{ lat lon }} time }}
                end {{ point {{ lat lon }} time }}
                vessels {{
                    vessel0 {{ name mmsi imo countryCode trackId category type class length }}{vessel1_block}
                }}
                eventDetails {{{fragments}
                }}
            }}
            total
        }}
    }}
    """


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    print("Fetching Skylight credentials from Stage Gundi...")
    username, password = await fetch_credentials()
    print(f"  username: {username}\n")

    print("Authenticating with Skylight...")
    auth_header = skylight_auth(username, password)
    print("  OK\n")

    client = make_client({"Authorization": auth_header})

    # ------------------------------------------------------------------
    # PHASE 1: Schema introspection
    # ------------------------------------------------------------------
    print("=" * 60)
    print("PHASE 1: SCHEMA INTROSPECTION")
    print("=" * 60)

    schema = {}
    for type_name in DETAIL_TYPES:
        info = introspect_type(client, type_name)
        if info and info.get("fields"):
            schema[type_name] = info["fields"]
            print(f"\n{type_name}:")
            for f in info["fields"]:
                t = f["type"]
                type_str = t.get("name") or (t.get("ofType") or {}).get("name", "?")
                print(f"  {f['name']}: {type_str}")
        else:
            print(f"\n{type_name}: NOT FOUND in schema")

    print("\n--- Vessel types ---")
    found_vessel_types = []
    for candidate in VESSEL_TYPE_CANDIDATES:
        info = introspect_type(client, candidate)
        if info and info.get("fields"):
            found_vessel_types.append(candidate)
            print(f"\n{candidate}:")
            for f in info["fields"]:
                t = f["type"]
                type_str = t.get("name") or (t.get("ofType") or {}).get("name", "?")
                print(f"  {f['name']}: {type_str}")
    if not found_vessel_types:
        print("  (none of the candidates found — vessel type may be anonymous)")

    # ------------------------------------------------------------------
    # PHASE 2: Live data
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("PHASE 2: LIVE DATA SAMPLE (last 30 days, limit 200)")
    print("=" * 60)

    end_dt = datetime.now(tz=timezone.utc)
    start_dt = end_dt - timedelta(days=30)
    params = {
        "types": ALL_EVENT_TYPES,
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "limit": 200,
    }

    records = None
    total = 0

    for include_vessel1 in (True, False):
        query_str = build_data_query(schema, include_vessel1=include_vessel1)
        label = "with vessel1" if include_vessel1 else "without vessel1 (schema may not support it)"
        print(f"\nRunning query ({label})...")
        try:
            result = client.execute(gql(query_str), variable_values=params)
            records = result["searchEventsV2"]["records"]
            total = result["searchEventsV2"]["total"]
            if not include_vessel1:
                print("  NOTE: vessel1 is NOT in the schema — it was excluded from this query")
            break
        except (TransportQueryError, Exception) as e:
            if include_vessel1:
                print(f"  Failed with vessel1: {e}")
                print("  Retrying without vessel1...")
            else:
                print(f"  Query failed: {e}")
                sys.exit(1)

    print(f"\nTotal available: {total} | Fetched: {len(records)}\n")

    by_type = {}
    for rec in records:
        by_type.setdefault(rec.get("eventType", "unknown"), []).append(rec)

    for et in ALL_EVENT_TYPES:
        recs = by_type.get(et, [])
        print(f"\n{'=' * 55}")
        print(f"  {et}  ({len(recs)} records in window)")
        print("=" * 55)
        if recs:
            print(json.dumps(recs[0], indent=2, default=str))
        else:
            print("  (no records in the last 30 days)")


if __name__ == "__main__":
    asyncio.run(main())
