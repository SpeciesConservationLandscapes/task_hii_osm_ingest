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
from task_base import HIITask, PROJECTS  # type: ignore

import config
from timer import Timer


class ConversionException(Exception):
    pass


class HIIOSMIngest(HIITask):
    """

    Process:

    1. Import split images from Google Storage into Earth Engine
    2. Import roads from Google Storage into Earth Engine
    3. Group bands from imported images into a single image
    4. Clean split images

    """

    ee_osm_root = "osm"
    scale = 100
    google_creds_path = "/.google_creds"
    project_id = "hii3-246517"

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._args = kwargs
        self.skip_cleanup = self._args["skip_cleanup"]

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
        text = output.decode("utf-8") if isinstance(output, bytes) is True else output
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
                f"--service_account_file {self.google_creds_path}",
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
                f"--service_account_file {self.google_creds_path}",
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
        return f"{self.ee_osm_root}/{attribute}/{tag}/{tag}_{task_date}"

    # Step 1
    def import_images_to_ee(
        self, metadata: dict, image_asset_id: Optional[str] = None
    ) -> List[str]:
        
        image_uris = metadata.get("images") or []
        if not image_uris:
            return []
        
        ee_dir = f"{self.ee_osm_root}/{self.taskdate}"
        self._prep_asset_id(ee_dir, image_collection=False)

        image_asset_ids = []
        for image_uri in image_uris:
            image_asset_id = f"{PROJECTS}/{self.ee_project}/{ee_dir}/{Path(os.path.splitext(image_uri)[0]).name}"
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
    def group_bands(self, image_asset_ids: List[str], metadata: dict) -> str:
        band_names = [f"{at['attribute']}_{at['tag']}" for at in metadata["bands"].values()]
        image_stack = ee.ImageCollection.fromImages([ee.Image(i) for i in image_asset_ids])
        bands = ee.List(list(metadata["bands"].values()))
        projection = ee.Image(image_asset_ids[0]).projection()

        def band_merge(attr_tag_meta):
            atm = ee.Dictionary(attr_tag_meta)
            _bands = ee.List(atm.get("bands"))
            attribute = atm.get("attribute")
            tag = atm.get("tag")

            band_indices = _bands.map(lambda x: ee.Number(x).subtract(ee.Number(1)))
            band_name = ee.String(attribute).cat(ee.String("_").cat(tag))
            return image_stack.select(band_indices) \
                .mosaic() \
                .reduce(ee.Reducer.max()) \
                .rename(band_name) \
                .reproject(projection)

        img_col = ee.ImageCollection(bands.map(band_merge))
        img = img_col.toBands().rename(band_names)
        asset_path = f"{self.ee_osm_root}/osm_image"
        self.export_image_ee(img, asset_path, image_collection=True)

        return asset_path
    
    # Step 4
    def clean_assets(self, assets):
        if self.skip_cleanup:
            return

        for asset in assets:
            self.rm_ee(asset)

    def calc(self):
        metadata_uri = self._args.get("metadata")
        _assets_to_clean = []

        try:
            if metadata_uri is None:
                metadata_uri = f"{self.taskdate}/metadata.json"

            metadata = self._read_merged_image_metadata(metadata_uri)

            with Timer("Import multi-band images Storage to EE"):
                image_asset_ids = self.import_images_to_ee(metadata)
                _assets_to_clean.extend(image_asset_ids)

            with Timer("Group image bands"):
                self.group_bands(image_asset_ids, metadata)

            # with Timer("Import roads table Storage to EE table"):
            #     osm_roads_dir = f"{self.ee_osm_root}/roads"
            #     roads_dir = f"{PROJECTS}/{self.ee_project}/{osm_roads_dir}"
            #     self._prep_asset_id(osm_roads_dir)
            #     roads_asset_id = f"{roads_dir}/roads_{self.taskdate}"
            #     self.import_roads_to_ee(metadata["road"], roads_asset_id)

            self.wait()
        finally:
            with Timer("Clean up"):
                self.clean_assets(_assets_to_clean)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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
