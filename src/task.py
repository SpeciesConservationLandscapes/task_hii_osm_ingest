import argparse
import os
import shutil
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import ee  # type: ignore
import gdal  # type: ignore
import requests
from google.cloud.storage import Client  # type: ignore
from task_base import HIITask  # type: ignore

import config

os.environ["OGR_INTERLEAVED_READING"] = "YES"
os.environ["OSM_CONFIG_FILE"] = "/app/osm.ini"
os.environ["OSM_MAX_TMPFILE_SIZE"] = "1024"


class ConversionException(Exception):
    pass


class HIIOSMIngest(HIITask):
    """

    Process:

    1. Fetch OSM pbf file
    2. Convert PBF file -> CSV files (for each layer) using OGR
    3. Upload filtered CSV file to Google Cloud Storage
    4. Using earthengine CLI load CSV as table (temporary) in EE

    """

    ee_osm_root = "osm"
    scale = 100
    google_creds_path = "/.google_creds"

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._args = kwargs
        if "osm_file" in self._args:
            self.osm_file = self._args["osm_file"]

        self.osm_url = self._args.get("osm_url") or os.environ["OSM_DATA_SOURCE"]
        self.csv_file = self._args.get("csv_file")
        self.ee_osm_table = self._args.get("ee_osm_table")
        self.overwrite = self._args.get("overwrite")

        creds_path = Path(self.google_creds_path)
        if creds_path.exists() is False:
            with open(str(creds_path), "w") as f:
                f.write(self.service_account_key)

    def _unique_file_name(self, ext: str, prefix: Optional[str] = None) -> str:
        name = f"{uuid.uuid4()}.{ext}"
        if prefix:
            name = f"{prefix}-{name}"

        return name

    def _get_asset_id(self, attribute, tag, task_date):
        root = f"projects/{self.ee_project}/{self.ee_osm_root}"
        return f"{root}/{attribute}/{tag}/{tag}_{task_date}"

    def _upload_to_cloudstorage(self, src_path: str) -> str:
        targ_path = Path(src_path).name
        client = Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        blob = bucket.blob(targ_path)
        blob.upload_from_filename(src_path)

        return targ_path

    def _remove_from_cloudstorage(self, path: str):
        client = Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        bucket.delete_blob(path)

    def _cp_storage_to_ee(self, blob_uri: str) -> str:
        asset_id = f"projects/{self.ee_project}/_temp_osm_{self.taskdate}"
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

    def download_osm(self) -> str:
        file_path = self._unique_file_name(ext="pbf")

        with requests.get(self.osm_url, stream=True) as r:
            with open(file_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        return file_path

    def osm_to_csv(self, osm_file_path: str) -> str:
        layer_names = [
            "points",
            "lines",
            "multilinestrings",
            "multipolygons",
            "other_relations",
        ]

        where = []
        attributes = []
        for attribute, tags in config.tags.items():
            attributes.append(attribute)
            quoted_tags = ",".join([f"'{tag}'" for tag in tags])
            where.append(f"{attribute} in ({quoted_tags})")

        sqls = []
        where_clause = " OR ".join(where)
        select_tag = f"COALESCE({','.join(attributes)})"
        for layer_name in layer_names:
            sqls.append(
                f"SELECT GEOMETRY, {select_tag} AS tag, 1 AS burn \
                FROM {layer_name} WHERE {where_clause}"
            )

        options = gdal.VectorTranslateOptions(
            format="CSV",
            SQLStatement=" UNION ALL ".join(sqls),
            SQLDialect="SQLITE",
            layerCreationOptions=["GEOMETRY=AS_WKT"],
        )
        csv_path = self._unique_file_name(ext="csv")
        gdal.VectorTranslate(csv_path, osm_file_path, options=options)

        return csv_path

    def asset_exists(self, asset_id: str) -> bool:
        try:
            return ee.data.getAsset(asset_id) is not None
        except ee.ee_exception.EEException:
            return False

    def import_csv_to_ee_table(self, local_path: str) -> str:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds_path
        blob_path = self._upload_to_cloudstorage(local_path)
        uri = f"gs://{os.environ['HII_OSM_BUCKET']}/{blob_path}"
        try:
            return self._cp_storage_to_ee(uri)
        finally:
            self._remove_from_cloudstorage(blob_path)

    def rasterize_table(self, table_asset_id: str, attribute: str, tag: str) -> str:
        table = ee.FeatureCollection(table_asset_id)

        image = table.filter(ee.Filter.eq("tag", tag)).reduceToImage(
            properties=["burn"], reducer=ee.Reducer.first()
        )
        image = image.reproject(crs="EPSG:4326", scale=self.scale)
        image = image.reduceResolution(ee.Reducer.max())

        asset_path = f"{self.ee_osm_root}/{attribute}/{tag}"
        return self.export_image_ee(
            image, asset_path, image_collection=True, pyramiding={".default": "max"}
        )

    def delete_asset(self, asset_id):
        ee.data.deleteAsset(asset_id)

    def import_osm_pbf(self):
        if self.ee_osm_table:
            return self.ee_osm_table

        if self.csv_file is None:
            self.osm_file = self.osm_file or self.download_osm()
            self.csv_file = self.osm_to_csv(self.osm_file)

        return self.import_csv_to_ee_table(self.csv_file)

    def calc(self):
        self.ee_osm_table = self.import_osm_pbf()

        for attribute, tags in config.tags.items():
            for tag in tags:
                asset_id = self._get_asset_id(attribute, tag, self.taskdate)
                asset_exists = self.asset_exists(asset_id)

                if self.overwrite is False and asset_exists is True:
                    continue

                if asset_exists:
                    self.delete_asset(asset_id)

                self.rasterize_table(self.ee_osm_table, attribute, tag)

    def clean_up(self, **kwargs):
        if self.status == self.FAILED:
            return

        self.delete_asset(self.ee_osm_table)


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
        help="CSV file to upload to Earth Engine.  Format: WKT,tag,burn",
    )

    parser.add_argument(
        "-t", "--ee_osm_table", type=str, help="Asset id of OSM table ingested into EE",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing HII tag images for task date",
    )

    options = parser.parse_args()
    task = HIIOSMIngest(**vars(options))
    task.run()
