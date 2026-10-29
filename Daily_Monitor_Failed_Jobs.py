import csv
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from azure.identity import ClientSecretCredential

# ============================================================
# CONFIGURATION
# ============================================================

SYNAPSE_ENDPOINT = os.environ["SYNAPSE_ENDPOINT"].rstrip("/")
TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]

IST = ZoneInfo("Asia/Kolkata")
API_VERSION = "2020-12-01"
REPORT_FILE = "synapse_pipeline_results.csv"

NOW_UTC = datetime.now(timezone.utc)
WINDOW_START_UTC = NOW_UTC - timedelta(hours=24)

FIELDNAMES = [
    "Pipeline Name",
    "Run ID",
    "Run Start (IST)",
    "Run End (IST)",
    "Duration",
    "Status",
    "Triggered By",
    "Error / Message",
]


# ============================================================
# AUTHENTICATION
# ============================================================

def get_access_token():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )

    token = credential.get_token(
        "https://dev.azuresynapse.net/.default"
    )
    return token.token


# ============================================================
# FETCH ALL PIPELINE RUNS
# ============================================================

def fetch_pipeline_runs(token):
    url = f"{SYNAPSE_ENDPOINT}/queryPipelineRuns"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    all_runs = []
    continuation_token = None

    while True:
        request_body = {
            "lastUpdatedAfter": (
                WINDOW_START_UTC - timedelta(days=7)
            ).isoformat().replace("+00:00", "Z"),
            "lastUpdatedBefore": (
                NOW_UTC + timedelta(minutes=5)
            ).isoformat().replace("+00:00", "Z"),
        }

        if continuation_token:
            request_body["continuationToken"] = continuation_token

        response = requests.post(
            url,
            headers=headers,
            params={"api-version": API_VERSION},
            json=request_body,
            timeout=60,
        )

        if not response.ok:
            raise RuntimeError(
                f"Synapse API error: HTTP {response.status_code} - "
                f"{response.text}"
            )

        data = response.json()
        all_runs.extend(data.get("value", []))

        continuation_token = data.get("continuationToken")

        if not continuation_token:
            break

    return all_runs


# ============================================================
# FORMAT DATETIME
# ============================================================

def parse_datetime(value):
    if not value:
        return None

    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# ============================================================
# FILTER FAILED RUNS
# ============================================================

def get_failed_runs(runs):
    failed_runs = []

    for run in runs:
        if str(run.get("status", "")).lower() != "failed":
            continue

        start_time = parse_datetime(run.get("runStart"))

        if start_time is None:
            continue

        if not (WINDOW_START_UTC <= start_time <= NOW_UTC):
            continue

        end_time = parse_datetime(run.get("runEnd"))
        duration_ms = run.get("durationInMs")

        if duration_ms is not None:
            duration_seconds = float(duration_ms) / 1000
        elif end_time is not None:
            duration_seconds = (end_time - start_time).total_seconds()
        else:
            duration_seconds = None

        duration_text = (
            str(timedelta(seconds=int(duration_seconds)))
            if duration_seconds is not None
            else ""
        )

        invoked_by = run.get("invokedBy") or {}
        error_message = run.get("message", "")

        failed_runs.append({
            "Pipeline Name": run.get("pipelineName", ""),
            "Run ID": run.get("runId", ""),
            "Run Start (IST)": start_time.astimezone(IST).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "Run End (IST)": (
                end_time.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
                if end_time
                else ""
            ),
            "Duration": duration_text,
            "Status": run.get("status", ""),
            "Triggered By": invoked_by.get("name", ""),
            "Error / Message": error_message,
        })

    failed_runs.sort(
        key=lambda row: row["Run Start (IST)"],
        reverse=True,
    )

    return failed_runs


# ============================================================
# DISPLAY RESULTS AS A TABLE
# ============================================================

def display_table(rows):
    print("\n" + "=" * 160)
    print("DAILY SYNAPSE FAILED JOBS REPORT")
    print("=" * 160)

    if not rows:
        print("No failed pipeline runs found in the last 24 hours.")
        print("=" * 160)
        return

    # Print all requested column headers in table format.
    print(" | ".join(FIELDNAMES))
    print("-" * 160)

    for row in rows:
        values = []

        for field in FIELDNAMES:
            value = str(row.get(field, "") or "")
            value = value.replace("\n", " ").replace("\r", " ")

            # Keep individual table cells readable in GitHub Actions logs.
            if len(value) > 80:
                value = value[:77] + "..."

            values.append(value)

        print(" | ".join(values))

    print("-" * 160)
    print(f"Total failed pipeline runs: {len(rows)}")
    print("=" * 160)


# ============================================================
# EXPORT CSV
# ============================================================

def write_csv(rows):
    with open(
        REPORT_FILE,
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=FIELDNAMES,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


# ============================================================
# MAIN
# ============================================================

def main():
    print("Starting daily Synapse failed jobs monitoring.")
    print(f"Reporting window start (UTC): {WINDOW_START_UTC.isoformat()}")
    print(f"Reporting window end   (UTC): {NOW_UTC.isoformat()}")

    token = get_access_token()
    pipeline_runs = fetch_pipeline_runs(token)

    print(f"\nPipeline runs retrieved: {len(pipeline_runs)}")

    failed_runs = get_failed_runs(pipeline_runs)

    # Export the report and display the results.
    write_csv(failed_runs)
    display_table(failed_runs)

    print(f"\nCSV report created: {REPORT_FILE}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"ERROR: Daily monitoring failed: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
