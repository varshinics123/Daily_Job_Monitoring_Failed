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
# API HELPERS
# ============================================================

def make_synapse_request(token, endpoint, body):
    url = f"{SYNAPSE_ENDPOINT}/{endpoint.lstrip('/')}"

    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        params={"api-version": API_VERSION},
        json=body,
        timeout=60,
    )

    if not response.ok:
        raise RuntimeError(
            f"Synapse API error for {endpoint}: "
            f"HTTP {response.status_code} - {response.text}"
        )

    return response.json()


# ============================================================
# FETCH PIPELINE RUNS
# ============================================================

def fetch_pipeline_runs(token):
    endpoint = "queryPipelineRuns"

    request_body = {
        "lastUpdatedAfter": (
            WINDOW_START_UTC - timedelta(days=7)
        ).isoformat().replace("+00:00", "Z"),
        "lastUpdatedBefore": (
            NOW_UTC + timedelta(minutes=5)
        ).isoformat().replace("+00:00", "Z"),
    }

    all_runs = []
    continuation_token = None

    while True:
        body = dict(request_body)

        if continuation_token:
            body["continuationToken"] = continuation_token

        data = make_synapse_request(token, endpoint, body)

        all_runs.extend(data.get("value", []))
        continuation_token = data.get("continuationToken")

        if not continuation_token:
            break

    return all_runs


# ============================================================
# FETCH ACTIVITY RUNS FOR ERROR DETAILS
# ============================================================

def fetch_activity_errors(token, pipeline_run_id):
    """
    Retrieve activity runs for a pipeline run.

    Return complete error details available from the activity-run API.
    """
    endpoint = "queryActivityRuns"

    body = {
        "lastUpdatedAfter": (
            WINDOW_START_UTC - timedelta(days=7)
        ).isoformat().replace("+00:00", "Z"),
        "lastUpdatedBefore": (
            NOW_UTC + timedelta(minutes=5)
        ).isoformat().replace("+00:00", "Z"),
        "filters": [
            {
                "operand": "PipelineRunId",
                "operator": "Equals",
                "values": [pipeline_run_id],
            }
        ],
    }

    data = make_synapse_request(token, endpoint, body)
    activity_runs = data.get("value", [])

    errors = []

    for activity in activity_runs:
        if str(activity.get("status", "")).lower() != "failed":
            continue

        activity_name = activity.get("activityName", "Unknown activity")
        error = activity.get("error")

        if error:
            if isinstance(error, dict):
                error_text = json.dumps(
                    error,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            else:
                error_text = str(error)

            errors.append(f"{activity_name}: {error_text}")
        else:
            errors.append(
                f"{activity_name}: Activity failed, but no error details "
                f"were returned by the API."
            )

    return "\n".join(errors)


# ============================================================
# DATE AND ERROR HELPERS
# ============================================================

def parse_datetime(value):
    if not value:
        return None

    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_pipeline_error(token, run):
    """
    Prefer complete activity-level errors.
    Fall back to the pipeline-level message if no activity error exists.
    """
    run_id = run.get("runId", "")
    pipeline_message = run.get("message", "")

    try:
        activity_error = fetch_activity_errors(token, run_id)

        if activity_error:
            return activity_error

    except Exception as exc:
        print(
            f"Warning: Could not retrieve activity error details "
            f"for run {run_id}: {exc}",
            file=sys.stderr,
        )

    if pipeline_message:
        return str(pipeline_message)

    return (
        "Pipeline status is Failed, but the API did not return "
        "an error message or activity-level error details."
    )


# ============================================================
# FILTER FAILED RUNS AND BUILD REPORT
# ============================================================

def get_failed_runs(token, runs):
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

        # Get complete available activity-level failure details.
        error_message = get_pipeline_error(token, run)

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
# EXPORT COMPLETE CSV REPORT
# ============================================================

def write_csv(rows):
    """
    Write complete error messages to CSV.

    No truncation is applied to the CSV fields.
    """
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
# DISPLAY RESULTS AS A READABLE TABLE
# ============================================================

def display_table(rows):
    print("\n" + "=" * 160)
    print("DAILY SYNAPSE FAILED JOBS REPORT")
    print("=" * 160)

    if not rows:
        print("No failed pipeline runs found in the last 24 hours.")
        print("=" * 160)
        return

    print(" | ".join(FIELDNAMES))
    print("-" * 160)

    for row in rows:
        values = []

        for field in FIELDNAMES:
            value = str(row.get(field, "") or "")
            value = value.replace("\n", " ").replace("\r", " ")

            # Truncation is only for the console display.
            # The CSV retains the complete message.
            if len(value) > 80:
                value = value[:77] + "..."

            values.append(value)

        print(" | ".join(values))

    print("-" * 160)
    print(f"Total failed pipeline runs: {len(rows)}")
    print("=" * 160)


# ============================================================
# MAIN
# ============================================================

def main():
    print("Starting daily Synapse failed jobs monitoring.")
    print(f"Window start UTC: {WINDOW_START_UTC.isoformat()}")
    print(f"Window end UTC:   {NOW_UTC.isoformat()}")

    token = get_access_token()

    pipeline_runs = fetch_pipeline_runs(token)
    print(f"Pipeline runs retrieved: {len(pipeline_runs)}")

    failed_runs = get_failed_runs(token, pipeline_runs)

    # Always create a CSV, including when no failures are found.
    write_csv(failed_runs)
    display_table(failed_runs)

    print(f"\nCSV report created: {REPORT_FILE}")
    print(f"Failed runs exported: {len(failed_runs)}")

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
