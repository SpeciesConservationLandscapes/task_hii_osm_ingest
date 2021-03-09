import argparse
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

import ee  # type: ignore
from google.cloud import storage  # type: ignore
from task_base import HIITask  # type: ignore

import config
from timer import Timer


class ConversionException(Exception):
    pass


class HIIOSMIngest(HIITask):
    """

    Process:

    1. For each attribute/tag upload table to EE
    2. Poll upload tasks and as they finish spin of rasterization task
    3. Create "roads" table
    4. Clean up CSV files in Google Storage
    5. Clean up temp tables in EE

    """

    ee_osm_root = "osm"
    scale = 100
    google_creds_path = "/.google_creds"
    project_id = "hii3-246517"
    EESUCCEEDED = "SUCCEEDED"
    _asset_prefix = f"projects/{HIITask.ee_project}/{ee_osm_root}"

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._args = kwargs

        creds_path = Path(self.google_creds_path)
        if creds_path.exists() is False:
            with open(str(creds_path), "w") as f:
                f.write(self.service_account_key)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds_path

    def _read_merged_image_metadata(self, blob_uri: str) -> Path:
        client = storage.Client()
        bucket = client.bucket(os.environ["HII_OSM_BUCKET"])
        blob = bucket.blob(blob_uri)
        return json.loads(blob.download_as_text())

    def _parse_task_id(self, output: Union[str, bytes]) -> Optional[str]:
        if isinstance(output, bytes):
            text = output.decode("utf-8")
        else:
            text = output

        task_id_regex = re.compile(r"(?<=ID: ).*", flags=re.IGNORECASE)
        try:
            matches = task_id_regex.search(text)
            if matches is None:
                return None
            return matches[0]
        except TypeError:
            return None

    def _cp_storage_to_ee_image(self, blob_uri: str, image_asset_id: str) -> str:
        try:
            cmd = [
                "/usr/local/bin/earthengine",
                f"--service_account_file={self.google_creds_path}",
                "upload image",
                "--nodata_value=0",
                f"--asset_id={image_asset_id}",
                blob_uri,
            ]
            output = subprocess.check_output(
                " ".join(cmd), stderr=subprocess.STDOUT, shell=True
            )
            task_id = self._parse_task_id(output)
            if task_id is None:
                raise TypeError("task_id is None")
            return task_id
        except subprocess.CalledProcessError as err:
            raise ConversionException(err.stdout)

    def _get_image_asset_id(self, attribute: str, tag: str, task_date: str):
        return f"{self._asset_prefix}/{attribute}/{tag}/{tag}_{task_date}"

    # Step 1
    def import_image_to_ee(
        self, blob_uri: str, image_asset_id: Optional[str] = None
    ) -> str:
        if image_asset_id is None:
            image_asset_id = f"{self._asset_prefix}/osm_raster-{self.taskdate}"

        task_id = self._cp_storage_to_ee_image(blob_uri, image_asset_id)
        self.ee_tasks[task_id] = {}

        self.wait()

        return image_asset_id

    # Step 2
    def import_roads_to_ee(
        self, blob_uri: str, roads_asset_id: Optional[str] = None
    ) -> str:
        print("Not implemented")

    # Step 3
    def split_image_bands(self, image_metadata_uri: str, image_asset_id: str):
        image_metadata = self._read_merged_image_metadata(image_metadata_uri)
        image = ee.Image(image_asset_id)

        attribute_tags = set([f"{a}-{t}" for a, t in config.tags])

        for metadata in image_metadata.values():
            attribute = metadata["attribute"]
            tag = metadata["tag"]

            if f"{attribute}-{tag}" not in attribute_tags:
                continue

            image_asset_id = self._get_image_asset_id(attribute, tag, self.taskdate)
            bands = image.select(metadata["bands"])
            split_img = ee.ImageCollection(bands).or() # noqa
            self.export_image_ee(split_img, image_asset_id)

        self.wait()

    # Step 4
    def clean_assets(self, assets):
        if self.skip_cleanup:
            return

        for asset in assets:
            self.rm_ee(asset)

    def calc(self):
        image_uri = self._args.get("image")
        metadata_uri = self._args.get("metadata")
        _assets_to_clean = []

        try:
            _base_gs_uri = f"gs://{os.environ['HII_OSM_BUCKET']}/{self.taskdate}/"

            if image_uri is None:
                image_uri = f"{_base_gs_uri}/merged-{self.taskdate}.tif"

            if metadata_uri is None:
                metadata_uri = f"{_base_gs_uri}/merged-{self.taskdate}.json"

            with Timer("Import multi-band image Storage to EE"):
                image_asset_id = self.import_image_to_ee(image_uri)
                _assets_to_clean.append(image_asset_id)

            with Timer("Import roads table Storage to EE multi-band image"):
                self.import_roads_to_ee(None, None)

            with Timer("Split multi-band image"):
                self.split_image_bands(metadata_uri, image_asset_id)

        finally:
            with Timer("Clean up"):
                self.clean_assets(_assets_to_clean)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate", default=datetime.now(timezone.utc).date())

    parser.add_argument(
        "-i",
        "--image",
        type=str,
        help="Google cloud storage uri for multi-band image.",
    )

    parser.add_argument(
        "-m",
        "--metadata",
        type=str,
        help="Google cloud storage uri for multi-band image json metadata file.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing HII tag images for task date",
    )

    parser.add_argument(
        "--skip_cleanup",
        action="store_true",
        help="Skip cleaning up temporary task files",
    )

    options = parser.parse_args()
    task = HIIOSMIngest(**vars(options))
    task.run()
