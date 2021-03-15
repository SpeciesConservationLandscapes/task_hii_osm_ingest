import argparse
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union, List

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
        self.service_account_key = os.environ["SERVICE_ACCOUNT_KEY"]
        if creds_path.exists() is False:
            with open(creds_path, "w") as f:
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
    
    def _cp_storage_to_ee_table(self, blob_uri: str, table_asset_id: str) -> str:
        try:
            cmd = [
                "/usr/local/bin/earthengine",
                f"--service_account_file={self.google_creds_path}",
                "upload table",
                "--primary_geometry_column wkt",
                f"--asset_id={table_asset_id}",
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
    def import_images_to_ee(
        self, metadata: dict, image_asset_id: Optional[str] = None
    ) -> List[str]:
        
        image_uris = metadata.get("images") or []
        if not image_uris:
            return []
        
        ee_dir = f"{self._asset_prefix}/{self.taskdate}"
        self._prep_asset_id(ee_dir, image_collection=False)

        image_asset_ids = []
        for image_uri in image_uris:
            image_asset_id = f"{ee_dir}/{Path(image_uri).name}"
            task_id = self._cp_storage_to_ee_image(image_uri, image_asset_id)
            self.ee_tasks[task_id] = {}
            image_asset_ids.append(image_asset_id)

        self.wait()

        return image_asset_ids

    # Step 2
    def import_roads_to_ee(
        self, blob_uri: str, roads_asset_id: Optional[str] = None
    ) -> str:
        task_id = self._cp_storage_to_ee_table(blob_uri, roads_asset_id)
        self.ee_tasks[task_id] = {}

    # Step 3
    # def split_image_bands(self, image_asset_ids: List[str], metadata: dict):
    #     image_metadata = metadata["images"]

    #     image = ee.Image(image_asset_id)

    #     attribute_tags = set([f"{a}-{t}" for a, t in config.tags])

    #     for metadata in image_metadata.values():
    #         attribute = metadata["attribute"]
    #         tag = metadata["tag"]

    #         if f"{attribute}-{tag}" not in attribute_tags:
    #             continue

    #         image_asset_id = self._get_image_asset_id(attribute, tag, self.taskdate)
    #         bands = image.select(metadata["bands"])
    #         split_img = ee.ImageCollection(bands).Or()
    #         self.export_image_ee(split_img, image_asset_id)

    #     self.wait()
    def split_image_bands(self, stacked_image_asset_ids: List[str], metadata: dict):
        image_metadata = metadata["bands"]

        attribute_tags = set([f"{a}-{t}" for a, t in config.tags])
        for meta in image_metadata.values():
            attribute = meta["attribute"]
            tag = meta["tag"]
            band_indices = meta["bands"]

            if f"{attribute}-{tag}" not in attribute_tags:
                continue

            bands = []
            for stacked_img_asset_id in stacked_image_asset_ids:
                image = ee.Image(stacked_img_asset_id)
                bands.append(image.select(band_indices))
            
            split_img = ee.ImageCollection(bands).Or()
            image_asset_id = self._get_image_asset_id(attribute, tag, self.taskdate)
            self.export_image_ee(split_img, image_asset_id)

        # self.wait()

    # Step 4
    def clean_assets(self, assets):
        if self.skip_cleanup:
            return

        print("Not implemented")

        # for asset in assets:
        #     self.rm_ee(asset)

    def calc(self):
        metadata_uri = self._args.get("metadata")
        _assets_to_clean = []

        try:
            _base_gs_uri = f"gs://{os.environ['HII_OSM_BUCKET']}/{self.taskdate}/"

            if metadata_uri is None:
                metadata_uri = f"{_base_gs_uri}/metadata.json"

            metadata = self._read_merged_image_metadata(metadata_uri)

            with Timer("Import multi-band images Storage to EE"):
                image_asset_ids = self.import_images_to_ee(metadata)
                _assets_to_clean.extend(image_asset_ids)

            with Timer("Split multi-band image"):
                self.split_image_bands(image_asset_ids, metadata)

            with Timer("Import roads table Storage to EE table"):
                roads_asset_id = f"{self._asset_prefix}/roads/roads_{self.taskdate}"
                self.import_roads_to_ee(metadata["roads"], roads_asset_id)
            

            self.wait()
        finally:
            with Timer("Clean up"):
                self.clean_assets(_assets_to_clean)
    
    def clean_up(self):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate", default=datetime.now(timezone.utc).date())

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
