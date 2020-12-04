import argparse
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import ee  # type: ignore
from google.cloud import storage  # type: ignore
from task_base import HIITask  # type: ignore

import config
from timer import timing


class ConversionException(Exception):
    pass


class HIIOSMIngest(HIITask):
    """

    Process:

    1. Using earthengine CLI load CSV as table (temporary) in EE
    2. Rasterize EE table for each attribute and tag
    3. Clean up EE table and Cloud Storage

    """

    ee_osm_root = "osm"
    scale = 100
    google_creds_path = "/.google_creds"

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._args = kwargs
        self.csv_uri = self._args.get("csv_uri")
        self.ee_osm_table = self._args.get("ee_osm_table")
        self.overwrite = self._args.get("overwrite")

        creds_path = Path(self.google_creds_path)
        if creds_path.exists() is False:
            with open(str(creds_path), "w") as f:
                f.write(self.service_account_key)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds_path

    def _get_asset_id(self, attribute: str, tag: str, task_date: str) -> str:
        root = f"projects/{self.ee_project}/{self.ee_osm_root}"
        return f"{root}/{attribute}/{tag}/{tag}_{task_date}"

    def _upload_to_cloudstorage(self, src_path: str) -> str:
        targ_path = Path(src_path).name
        client = storage.Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        blob = bucket.blob(targ_path)
        blob.upload_from_filename(src_path)

        return targ_path

    def _remove_from_cloudstorage(self, path: str):
        client = storage.Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        bucket.delete_blob(path)

    @timing
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

    def asset_exists(self, asset_id: str) -> bool:
        try:
            return ee.data.getAsset(asset_id) is not None
        except ee.ee_exception.EEException:
            return False

    @timing
    def rasterize_table(self, table_asset_id: str, attribute: str, tag: str) -> str:
        table = ee.FeatureCollection(table_asset_id)

        image = table.filter(
            ee.Filter.stringContains("tag", f"{attribute}={tag}")
        ).reduceToImage(properties=["burn"], reducer=ee.Reducer.first())
        image = image.reproject(crs="EPSG:4326", scale=self.scale)
        image = image.reduceResolution(ee.Reducer.max())

        asset_path = f"{self.ee_osm_root}/{attribute}/{tag}"
        return self.export_image_ee(
            image, asset_path, image_collection=True, pyramiding={".default": "max"}
        )

    def delete_asset(self, asset_id: str):
        ee.data.deleteAsset(asset_id)

    def _csv_exists(self, uri):
        client = storage.Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        return storage.Blob(bucket=bucket, name=Path(uri).name).exists(client)

    def _get_csv_uri(self):
        return f"gs://{os.environ['HII_OSM_BUCKET']}/{self.taskdate}.csv"

    def calc(self):
        if self.ee_osm_table is None:
            self.csv_uri = self.csv_uri or self._get_csv_uri()
            if self._csv_exists(self.csv_uri) is False:
                raise FileNotFoundError("Missing CSV file")

            self.ee_osm_table = self._cp_storage_to_ee(self.csv_uri)

        for attribute, tag in config.tags:
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

        # TODO: Create a new table of highway attributes from table before getting rid of table

        # self.delete_asset(self.ee_osm_table)
        self._remove_from_cloudstorage(self._get_csv_uri())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate", default=datetime.now(timezone.utc).date())

    parser.add_argument(
        "-c",
        "--csv_uri",
        type=str,
        help="URI to source CSV uri path in Google Cloud Storage.  Format: wkt,tag,burn",
    )

    parser.add_argument(
        "-t",
        "--ee_osm_table",
        type=str,
        help="Asset id of OSM table ingested into EE",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing HII tag images for task date",
    )

    options = parser.parse_args()
    task = HIIOSMIngest(**vars(options))
    task.run()
