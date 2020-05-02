import patch

import validata_core
import requests
import yaml

import functools
from urllib.parse import urlencode
from collections import defaultdict
import csv
import datetime
import sys
import json


@functools.lru_cache()
def schemas_details():
    with requests.get("https://schema.data.gouv.fr/schemas/schemas.yml") as response:
        response.raise_for_status()
        return yaml.safe_load(response.content)


def get_schema_url(slug):
    schemas = schemas_details()[slug]["schemas"]
    assert len(schemas) == 1
    return schemas[0]["latest_url"]


def get_schema_version(slug):
    return schemas_details()[slug]["latest_version"]


def get_details(dataset_id, slug):
    response = requests.get(f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/")
    response.raise_for_status()

    dataset_url = response.json()["resources"][0]["url"]
    schema_url = get_schema_url(slug)

    return {
        "schema_url": schema_url,
        "schema_slug": slug,
        "schema_version": get_schema_version(slug),
        "dataset_id": dataset_id,
        "name": response.json()["title"],
        "dataset_url": dataset_url,
        "report_url": f"https://go.validata.fr/table-schema?input=url&schema_url={schema_url}&url={dataset_url}&repair=true",
    }


def enrich_report(report, columns):
    count_col_code = {}
    for error in report["tables"][0]["errors"]:
        if error["tag"] != "value":
            continue
        col = columns[(error["column-number"] - 1)]
        if col not in count_col_code:
            count_col_code[col] = defaultdict(int)
        count_col_code[col][error["code"]] += 1

    report["tables"][0]["error-stats"]["value-errors"][
        "count-by-col-and-code"
    ] = count_col_code

    return report


def validate(source, schema):
    report = validata_core.validate(source, schema)
    columns = report["tables"][0]["headers"]

    return enrich_report(report, columns)


def build_report(report):
    def badge_url(nb_errors, color):
        query = urlencode(
            {
                "label": "Consolidation",
                "message": f"{nb_errors} erreurs",
                "color": color,
                "style": "flat-square",
            }
        )
        # See documentation on https://shields.io
        return f"https://img.shields.io/static/v1?{query}"

    percentage = int(report["nb_errors"] * 100 / report["nb_rows"])
    if percentage == 0:
        status, color = "ok", "green"
    elif percentage <= 10:
        status, color = "warning", "orange"
    else:
        status, color = "invalid", "red"

    return {
        **report,
        **{
            "status": status,
            "error_percentage": percentage,
            "badge_url": badge_url(report["nb_errors"], color),
        },
    }


def build_details(details, report):
    errors = report["tables"][0]["error-stats"]

    return {
        "date": str(datetime.date.today()),
        "dataset_id": details["dataset_id"],
        "name": details["name"],
        "schema_slug": details["schema_slug"],
        "schema_version": details["schema_version"],
        "file_url": details["dataset_url"],
        "report_url": details["report_url"],
        "nb_rows": report["tables"][0]["row-count"],
        "nb_errors": errors["count"],
        "nb_rows_with_errors": errors["value-errors"]["rows-count"],
        "errors_report": json.dumps(errors),
    }


daily_data = []
json_report = {}
for slug, data in schemas_details().items():
    if data["consolidation"] and data["consolidation"]["dataset_id"]:
        dataset_id = data["consolidation"]["dataset_id"]
        details = get_details(dataset_id, slug)

        report = validate(details["dataset_url"], details["schema_url"])
        details = build_details(details, report)
        daily_data.append(details)

        json_report[dataset_id] = build_report(details)

# Write today's data to a JSON file
with open("data/report.json", "w") as f:
    json.dump(json_report, f, indent=2, ensure_ascii=False)

# Append daily data to a CSV file
with open("data/data.csv", "a") as f:
    writer = csv.DictWriter(f, daily_data[0].keys(), lineterminator="\n")
    if f.tell() == 0:
        writer.writeheader()
    writer.writerows(daily_data)
