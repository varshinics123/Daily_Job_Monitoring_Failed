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


def get_access_token():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )
    return credential.get_token(
        "https://dev.azuresynapse.net/.default"
    ).token


def make_synapse_request(token, endpoint, body):
    response = requests.post(
        f"{SYNAPSE_ENDPOINT}/{endpoint.lstrip('/')}",
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
            f"HTTP {response.status_code}: {response.text}"
        )

    return response.json()


def fetch_pipeline_runs(token):
    all_runs = []
    continuation_token = None

    while True:
        body = {
            "lastUpdatedAfter": (
                WINDOW_START_UTC - timedelta(days=7)
            ).isoformat().replace("+00:00", "Z"),
            "lastUpdatedBefore": (
                NOW_UTC + timedelta(minutes=5)
            ).isoformat().replace("+00:00", "Z"),
        }

        if continuation_token:
            body["continuationToken"] = continuation_token

        data = make_synapse_request(
            token, "queryPipelineRuns", body
        )

        all_runs.extend(data.get("value", []))
        continuation_token = data.get("continuationToken")

        if not continuation_token:
            break

    return all_runs


def parse_datetime(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch_activity_errors(token, pipeline_run_id):
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

    data = make_synapse_request(token, "queryActivityRuns", body)
    errors = []

    for activity in data.get("value", []):
        if str(activity.get("status", "")).lower() != "failed":
            continue

        activity_name = activity.get("activityName", "Unknown activity")
        error = activity.get("error")

        if error:
            errors.append(
                f"{activity_name}: {error}"
            )
        else:
            errors.append(
                f"{activity_name}: Activity failed; "
                "no error details were returned."
            )

    return "\n".join(errors)


def get_error_message(token, run):
    run_id = run.get("runId", "")

    try:
        activity_error = fetch_activity_errors(token, run_id)
        if activity_error:
            return activity_error
    except Exception as exc:
        print(
            f"Warning: Could not retrieve activity errors "
            f"for run {run_id}: {exc}",
            file=sys.stderr,
        )

    return run.get("message") or (
        "Pipeline failed, but no detailed error message was returned."
    )


def get_failed_runs(token, runs):
    failed_runs = []

    for run in runs:
        if str(run.get("status", "")).lower() != "failed":
            continue

        start_time = parse_datetime(run.get("runStart"))

        if start_time is None:
            continue

        if not WINDOW_START_UTC <= start_time <= NOW_UTC:
            continue

        end_time = parse_datetime(run.get("runEnd"))
        duration_ms = run.get("durationInMs")

        if duration_ms is not None:
            duration_seconds = float(duration_ms) / 1000
        elif end_time:
            duration_seconds = (end_time - start_time).total_seconds()
        else:
            duration_seconds = None

        duration = (
            str(timedelta(seconds=int(duration_seconds)))
            if duration_seconds is not None
            else ""
        )

        invoked_by = run.get("invokedBy") or {}

        failed_runs.append({
            "Pipeline Name": run.get("pipelineName", ""),
            "Run ID": run.get("runId", ""),
            "Run Start (IST)": start_time.astimezone(IST).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "Run End (IST)": (
                end_time.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
                if end_time else ""
            ),
            "Duration": duration,
            "Status": run.get("status", ""),
            "Triggered By": invoked_by.get("name", ""),
            "Error / Message": get_error_message(token, run),
        })

    failed_runs.sort(
        key=lambda row: row["Run Start (IST)"],
        reverse=True,
    )
    return failed_runs


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
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        writer.writerows(rows)


def display_table(rows):
    print("\nDAILY SYNAPSE FAILED JOBS REPORT")
    print(" | ".join(FIELDNAMES))
    print("-" * 160)

    if not rows:
        print("No failed pipeline runs found in the last 24 hours.")
        return

    for row in rows:
        values = []

        for field in FIELDNAMES:
            value = str(row.get(field, "") or "")
            value = value.replace("\n", " ").replace("\r", " ")

            # Shorten only console display, never the CSV.
            if len(value) > 80:
                value = value[:77] + "..."
            values.append(value)

        print(" | ".join(values))

    print(f"\nTotal failed runs: {len(rows)}")


def main():
    print("Starting daily Synapse failure monitoring.")
    print(f"Window start UTC: {WINDOW_START_UTC.isoformat()}")
    print(f"Window end UTC: {NOW_UTC.isoformat()}")

    token = get_access_token()
    runs = fetch_pipeline_runs(token)

    print(f"Pipeline runs retrieved: {len(runs)}")

    failed_runs = get_failed_runs(token, runs)
    write_csv(failed_runs)
    display_table(failed_runs)

    print(f"\nCSV created: {REPORT_FILE}")
    print(f"Failed runs exported: {len(failed_runs)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: Daily monitoring failed: {exc}", file=sys.stderr)
        sys.exit(1)
