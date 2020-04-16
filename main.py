import patch

import validata_core
import requests

from collections import defaultdict
import csv
import datetime
import sys
import json


def get_details(dataset_id, schema):
    response = requests.get(f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/")
    response.raise_for_status()

    return {
        "dataset_id": dataset_id,
        "name": response.json()["title"],
        "url": response.json()["resources"][0]["url"],
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


def build_report(source, schema):
    report = validata_core.validate(source, schema)
    columns = report["tables"][0]["headers"]

    return enrich_report(report, columns)


def build_details(details, report):
    errors = report["tables"][0]["error-stats"]

    return {
        "date": datetime.date.today(),
        "dataset_id": details["dataset_id"],
        "name": details["name"],
        "file_url": details["url"],
        "nb_rows": report["tables"][0]["row-count"],
        "nb_errors": errors["count"],
        "nb_rows_with_errors": errors["value-errors"]["rows-count"],
        "errors_report": json.dumps(errors),
    }


data = [
    (
        "5d6eaffc8b4c417cdc452ac3",
        "https://schema.data.gouv.fr/schemas/etalab/schema-lieux-covoiturage/latest/schema.json",
    ),
    (
        "5448d3e0c751df01f85d0572",
        "https://schema.data.gouv.fr/schemas/etalab/schema-irve/latest/schema.json",
    ),
]

res = []
for dataset_id, schema in data:
    details = get_details(dataset_id)
    report = build_report(details["url"], schema)
    res.append(build_details(details, report))

with open("data.csv", "a") as f:
    writer = csv.DictWriter(f, res[0].keys(), lineterminator="\n")
    if f.tell() == 0:
        writer.writeheader()
    writer.writerows(res)
