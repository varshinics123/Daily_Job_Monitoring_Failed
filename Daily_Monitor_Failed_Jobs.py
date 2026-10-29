import csv
import json
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

# Capture one consistent reporting window for the entire run.
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


def utc_iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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
# SYNAPSE API REQUEST
# ============================================================

def synapse_post(token, endpoint, body):
    url = f"{SYNAPSE_ENDPOINT}/{endpoint.lstrip('/')}"

    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        params={"api-version": API_VERSION},
        json=body,
        timeout=120,
    )

    if not response.ok:
        raise RuntimeError(
            f"Synapse API request failed for {endpoint}. "
            f"HTTP {response.status_code}: {response.text}"
        )

    return response.json()


# ============================================================
# FETCH ALL PIPELINE RUNS WITH PAGINATION
# ============================================================

def fetch_pipeline_runs(token):
    endpoint = "queryPipelineRuns"

    base_body = {
        # Search a wider update window so runs that started in the
        # last 24 hours but were updated later are not missed.
        "lastUpdatedAfter": utc_iso(
            WINDOW_START_UTC - timedelta(days=7)
        ),
        "lastUpdatedBefore": utc_iso(
            NOW_UTC + timedelta(minutes=5)
        ),
        "filters": [],
        "orderBy": [
            {
                "orderBy": "RunStart",
                "order": "DESC",
            }
        ],
    }

    all_runs = []
    continuation_token = None
    page_number = 0

    while True:
        body = dict(base_body)

        if continuation_token:
            body["continuationToken"] = continuation_token

        data = synapse_post(token, endpoint, body)
        page_runs = data.get("value", [])

        all_runs.extend(page_runs)
        page_number += 1

        print(
            f"Pipeline runs page {page_number}: "
            f"{len(page_runs)} records"
        )

        continuation_token = data.get("continuationToken")

        if not continuation_token:
            break

    # Run ID identifies an individual pipeline execution.
    # Deduplicate only accidental repeats of the same Run ID,
    # never different runs of the same pipeline.
    unique_runs = {}
    for run in all_runs:
        run_id = run.get("runId")
        if run_id:
            unique_runs[run_id] = run

    return list(unique_runs.values())


# ============================================================
# FETCH ALL ACTIVITY RUNS FOR ONE PIPELINE RUN
# ============================================================

def fetch_activity_runs(token, pipeline_run_id):
    endpoint = "queryActivityRuns"

    base_body = {
        "lastUpdatedAfter": utc_iso(
            WINDOW_START_UTC - timedelta(days=7)
        ),
        "lastUpdatedBefore": utc_iso(
            NOW_UTC + timedelta(minutes=5)
        ),
        "filters": [
            {
                "operand": "PipelineRunId",
                "operator": "Equals",
                "values": [pipeline_run_id],
            }
        ],
    }

    all_activities = []
    continuation_token = None

    while True:
        body = dict(base_body)

        if continuation_token:
            body["continuationToken"] = continuation_token

        data = synapse_post(token, endpoint, body)
        all_activities.extend(data.get("value", []))

        continuation_token = data.get("continuationToken")

        if not continuation_token:
            break

    return all_activities


# ============================================================
# DATETIME HELPERS
# ============================================================

def parse_datetime(value):
    if not value:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def format_ist(value):
    if value is None:
        return ""

    return value.astimezone(IST).strftime("%d-%m-%Y %H:%M:%S")


# ============================================================
# COMPLETE ERROR DETAILS
# ============================================================

def fetch_error_message(token, run):
    run_id = run.get("runId", "")
    errors = []

    try:
        activities = fetch_activity_runs(token, run_id)

        for activity in activities:
            if str(activity.get("status", "")).lower() != "failed":
                continue

            activity_name = activity.get(
                "activityName",
                "Unknown activity",
            )

            error = activity.get("error")

            if isinstance(error, dict):
                error_details = json.dumps(
                    error,
                    ensure_ascii=False,
                    indent=2,
                )
            elif error is not None:
                error_details = str(error)
            else:
                error_details = (
                    "Activity failed, but Synapse returned no "
                    "activity error details."
                )

            errors.append(
                f"Activity: {activity_name}\n"
                f"Error: {error_details}"
            )

    except Exception as exc:
        print(
            f"WARNING: Could not retrieve activity details for "
            f"run {run_id}: {exc}",
            file=sys.stderr,
        )

    if errors:
        return "\n\n".join(errors)

    # Fall back to the pipeline-level message when no activity
    # error was returned.
    pipeline_message = run.get("message")

    if pipeline_message:
        return str(pipeline_message)

    return (
        "Pipeline status is Failed, but no pipeline-level or "
        "activity-level error message was returned by Synapse."
    )


# ============================================================
# BUILD FAILED-RUN REPORT
# ============================================================

def build_failed_report(token, runs):
    results = []

    for run in runs:
        status = str(run.get("status", "")).strip().lower()

        if status != "failed":
            continue

        start_time = parse_datetime(run.get("runStart"))
        end_time = parse_datetime(run.get("runEnd"))

        if start_time is None:
            print(
                f"WARNING: Skipping failed run without a start time: "
                f"{run.get('runId', '')}",
                file=sys.stderr,
            )
            continue

        # Include every failed run that started in the rolling
        # 24-hour reporting window.
        if not (WINDOW_START_UTC <= start_time <= NOW_UTC):
            continue

        duration_ms = run.get("durationInMs")

        if duration_ms is not None:
            duration_seconds = max(0, int(float(duration_ms) / 1000))
        elif end_time is not None:
            duration_seconds = max(
                0,
                int((end_time - start_time).total_seconds()),
            )
        else:
            duration_seconds = None

        duration_text = (
            str(timedelta(seconds=duration_seconds))
            if duration_seconds is not None
            else ""
        )

        invoked_by = run.get("invokedBy") or {}

        error_message = fetch_error_message(token, run)

        results.append({
            "Pipeline Name": run.get("pipelineName", ""),
            "Run ID": run.get("runId", ""),
            "Run Start (IST)": format_ist(start_time),
            "Run End (IST)": format_ist(end_time),
            "Duration": duration_text,
            "Status": run.get("status", ""),
            "Triggered By": invoked_by.get("name", ""),
            "Error / Message": error_message,
        })

    # Newest failed runs first. Repeated pipeline names are retained.
    results.sort(
        key=lambda row: row["Run ID"],
        reverse=True,
    )

    return results


# ============================================================
# EXPORT CSV WITHOUT TRUNCATING ERROR MESSAGES
# ============================================================

def export_csv(rows):
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
            quoting=csv.QUOTE_ALL,
        )

        writer.writeheader()
        writer.writerows(rows)


# ============================================================
# DISPLAY TABLE IN GITHUB ACTIONS LOG
# ============================================================

def display_table(rows):
    print("\nDAILY SYNAPSE FAILED JOBS REPORT")
    print(f"Window start (IST): {format_ist(WINDOW_START_UTC)}")
    print(f"Window end   (IST): {format_ist(NOW_UTC)}")
    print(f"Failed runs found: {len(rows)}")
    print()

    print(" | ".join(FIELDNAMES))
    print("-" * 180)

    if not rows:
        print("No failed pipeline runs found in the last 24 hours.")
        return

    for row in rows:
        values = []

        for field in FIELDNAMES:
            value = str(row.get(field, "") or "")
            value = value.replace("\r", " ").replace("\n", " ")

            # Console output only: CSV retains the complete value.
            if len(value) > 100:
                value = value[:97] + "..."

            values.append(value)

        print(" | ".join(values))

    print("-" * 180)
    print(f"Total failed pipeline runs exported: {len(rows)}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("Starting daily Synapse failed jobs monitoring.")
    print(f"Synapse workspace endpoint: {SYNAPSE_ENDPOINT}")
    print(f"Reporting window start (UTC): {utc_iso(WINDOW_START_UTC)}")
    print(f"Reporting window end   (UTC): {utc_iso(NOW_UTC)}")

    token = get_access_token()

    all_runs = fetch_pipeline_runs(token)

    print(f"\nUnique pipeline runs retrieved: {len(all_runs)}")

    failed_runs = build_failed_report(token, all_runs)

    # Always create the CSV, including when there are no failures.
    export_csv(failed_runs)
    display_table(failed_runs)

    print(f"\nCSV report created: {REPORT_FILE}")
    print(f"Rows exported: {len(failed_runs)}")

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
