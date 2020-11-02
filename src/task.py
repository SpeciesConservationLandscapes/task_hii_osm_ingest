import argparse
import csv
import os
import shutil
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import gdal
import requests
from google.cloud.storage import Client
from task_base import HIITask

import config

os.environ["OGR_INTERLEAVED_READING"] = "YES"
os.environ["OSM_CONFIG_FILE"] = "/app/osm.ini"


class ConversionException(Exception):
    pass


class HIIOSMIngest(HIITask):
    """

    Process:

    1. Fetch OSM pbf file
    2. Convert PBF file -> CSV files (for each layer) using OGR
    3. Filter and Combine CSV files
    4. Upload filtered CSV file to Google Cloud Storage
    5. Using earthengine CLI load CSV as table (temporary) in EE

    """

    ee_osm_root = "osm"
    google_creds_path = "/.google_creds"

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._args = kwargs
        if "osm_file" in self._args:
            self.osm_file = self._args["osm_file"]

        self.osm_url = self._args.get("osm_url") or os.environ["OSM_DATA_SOURCE"]
        self.csv_file = self._args.get("csv_file")

        creds_path = Path(self.google_creds_path)
        if creds_path.exists() is False:
            with open(str(creds_path), "w") as f:
                f.write(self.service_account_key)

    def _unique_file_name(self, ext: str, prefix: Optional[str] = None) -> str:
        name = f"{uuid.uuid4()}.{ext}"
        if prefix:
            name = f"{prefix}-{name}"

        return name

    def download_osm(self) -> str:
        file_path = self._unique_file_name(ext="pbf")

        with requests.get(self.osm_url, stream=True) as r:
            with open(file_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        return file_path

    def _upload_to_cloudstorage(self, src_path: str) -> str:
        targ_path = Path(src_path).name
        client = Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        blob = bucket.blob(targ_path)
        blob.upload_from_filename(src_path)

        return targ_path

    def osm_to_csv(self, osm_file_path: str) -> str:
        tags = config.tags
        where_args = [f"{k}='{v}'" for k, v in tags]
        options = gdal.VectorTranslateOptions(
            format="CSV",
            where=" or ".join(where_args),
            layerCreationOptions=["GEOMETRY=AS_WKT"],
        )
        csv_path = self._unique_file_name(ext="csv")
        gdal.VectorTranslate(csv_path, osm_file_path, options=options)

        return csv_path

    def _remove_from_cloudstorage(self, path: str):
        client = Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        bucket.delete_blob(path)

    def import_csv_to_ee_table(self, local_path: str) -> str:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds_path
        blob_path = self._upload_to_cloudstorage(local_path)
        uri = f"gs://{os.environ['HII_OSM_BUCKET']}/{blob_path}"
        try:
            return self._cp_storage_to_ee(uri)
        finally:
            self._remove_from_cloudstorage(blob_path)

    def _cp_storage_to_ee(self, blob_uri: str) -> str:
        asset_id = f"projects/{self.ee_project}/_temp_osm_{self._args['taskdate']}"
        try:
            cmd = [
                "/usr/local/bin/earthengine",
                f"--service_account_file={self.google_creds_path}",
                "upload table",
                "--primary_geometry_column WKT",
                "--wait 7200",
                f"--asset_id={asset_id}",
                blob_uri,
            ]

            subprocess.check_output(" ".join(cmd), stderr=subprocess.STDOUT, shell=True)
            return asset_id
        except subprocess.CalledProcessError as err:
            raise ConversionException(err.stdout)

    def _match_attribute_tag(self, row):
        result = [
            (attribute, tag) for attribute, tag in config.tags if row[attribute] == tag
        ]
        if result:
            return result[0]
        return None

    def _write_to_file(self, src_csv_path, output_csv_file):
        n = 0
        with open(src_csv_path) as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_ALL)
            for row in reader:
                result = self._match_attribute_tag(row)
                if result is None:
                    continue

                output_csv_file.writerow([row["WKT"], result[0], result[1], "1"])
                n += 1
        return n

    def combine_csv_files(self, csv_path: str):
        output_path = self._unique_file_name(ext="csv")
        _csv_path = Path(csv_path)
        layers = [
            _csv_path,
            Path(_csv_path.parent, "points.csv"),
            Path(_csv_path.parent, "lines.csv"),
            Path(_csv_path.parent, "multilinestrings.csv"),
            Path(_csv_path.parent, "multipolygons.csv"),
            Path(_csv_path.parent, "other_relations.csv"),
        ]

        n = 0
        with open(output_path, "w") as output:
            w = csv.writer(output, quoting=csv.QUOTE_ALL)
            w.writerow(["WKT", "attribute", "tag", "burn"])

            for layer in layers:
                if layer.exists() is False:
                    continue

                n += self._write_to_file(str(layer), w)

        if n == 0:
            Path(output_path).unlink(missing_ok=True)
            output_path = None

        return output_path

    def calc(self):
        # - download full-world osm pbf
        # - use ogr2ogr to convert just tags we need to csv
        # - remove extraneous columns
        # - combine multiple geometries (?)
        # - ingest into ee

        if self.csv_file is None:
            if self.osm_file is None:
                self.osm_file = self.download_osm()

            if self.csv_file is None:
                csv_path = self.osm_to_csv(self.osm_file)
                self.csv_file = self.combine_csv_files(csv_path)

        if self.csv_file:
            self.import_csv_to_ee_table(self.csv_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate", default=datetime.now(timezone.utc).date())
    parser.add_argument(
        "-f",
        "--osm_file",
        type=str,
        help=(
            "Add local path to OSM source file."
            " If not provided, file will be downloaded"
        ),
    )
    parser.add_argument(
        "-u",
        "--osm_url",
        type=str,
        help="Set a different source url to download OSM pbf file",
    )

    parser.add_argument(
        "-c",
        "--csv_file",
        type=str,
        help="CSV file to upload to Earth Engine.  Format: WKT,attribute,tag,burn",
    )

    options = parser.parse_args()
    task = HIIOSMIngest(**vars(options))
    task.run()
