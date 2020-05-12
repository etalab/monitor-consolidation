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
import os
import textwrap

CSV_PATH = "data/data.csv"


@functools.lru_cache()
def schemas_details():
    with requests.get("https://schema.data.gouv.fr/schemas/schemas.yml") as response:
        response.raise_for_status()
        return yaml.safe_load(response.content)


@functools.lru_cache()
def existing_data():
    with open(CSV_PATH, "r") as f:
        return [d for d in csv.DictReader(f)]


def file_is_new(file_url):
    same_line = [row for row in existing_data() if file_url == row["file_url"]]
    return len(same_line) == 0


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
        "date": datetime.date.today().isoformat(),
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


def post_comment(details):
    def plural(count, word):
        if count != 1:
            return f"{count} {word}s"
        return f"{count} {word}"

    schema_doc_url = f"https://schema.data.gouv.fr/{details['schema_slug']}/latest.html"

    comment = f"""\
    Bonjour,

    [Le fichier]({details["file_url"]}) que vous venez de publier ou mettre à jour comporte {plural(details["nb_errors"], "erreur")} sur un total de {plural(details["nb_rows"], "ligne")} par rapport au [schéma de référence]({schema_doc_url}).

    Vous pouvez consulter le [dernier rapport de validation]({details["report_url"]}) pour vous aider à corriger les erreurs.
    """

    requests.post(
        "https://www.data.gouv.fr/api/1/discussions/",
        headers={
            "X-API-KEY": os.environ["DATAGOUV_API_KEY"],
            "User-Agent": "https://github.com/etalab/monitor-consolidation",
        },
        json={
            "title": "Conformité au schéma",
            "comment": textwrap.dedent(comment),
            "subject": {"id": details["dataset_id"], "class": "Dataset"},
        },
    ).raise_for_status()


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

        # If the file is new, post a comment on the dataset
        # to report the validation's result
        if file_is_new(details["file_url"]):
            post_comment(details)

# Write today's data to a JSON file
with open("data/report.json", "w") as f:
    json.dump(json_report, f, indent=2, ensure_ascii=False)

# Append daily data to a CSV file
with open(CSV_PATH, "a") as f:
    writer = csv.DictWriter(f, daily_data[0].keys(), lineterminator="\n")
    if f.tell() == 0:
        writer.writeheader()
    writer.writerows(daily_data)
