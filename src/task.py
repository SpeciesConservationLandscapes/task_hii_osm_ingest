import argparse
import json
import os
from pathlib import Path
from typing import Optional, List

import ee  # type: ignore
from google.cloud import storage  # type: ignore
from task_base import HIITask, PROJECTS  # type: ignore

from timer import Timer


class HIIOSMIngest(HIITask):
    """

    Process:

    1. Import split images from Google Storage into Earth Engine
    2. Import roads from Google Storage into Earth Engine
    3. Group bands from imported images into a single image
    4. Clean split images

    """

    DEFAULT_BUCKET = os.environ["HII_OSM_BUCKET"]
    ee_osm_root = "osm"
    project_id = "hii3-246517"

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self.metadata_uri = (
            kwargs.get("metadata")
            or os.environ.get("metadata")
            or f"{self.taskdate}/metadata.json"
        )
        self.skip_cleanup = (
            kwargs.get("skip_cleanup") or os.environ.get("skip_cleanup") or False
        )
        self.output_image = (
            kwargs.get("output_image") or os.environ.get("output_image") or "osm_image"
        )

    def _read_merged_image_metadata(self, blob_uri: str) -> Path:
        bucket = self.gcsclient.get_bucket(self.DEFAULT_BUCKET)
        blob = bucket.blob(blob_uri)
        return json.loads(blob.download_as_text())

    def _get_image_asset_id(self, attribute: str, tag: str, task_date: str):
        return f"{self.ee_osm_root}/{attribute}/{tag}/{tag}_{task_date}"

    # Step 1
    def import_images_to_ee(self, metadata: dict) -> List[str]:

        image_uris = metadata.get("images") or []
        if not image_uris:
            return []

        ee_dir = f"{self.ee_osm_root}/{self.taskdate}"
        self._prep_asset_id(ee_dir, image_collection=False)

        image_asset_ids = []
        for image_uri in image_uris:
            image_asset_id = f"{PROJECTS}/{self.ee_project}/{ee_dir}/{Path(os.path.splitext(image_uri)[0]).name}"
            task_id = self.storage2image(image_uri, image_asset_id, nodataval=0)
            image_asset_ids.append(image_asset_id)

        self.wait()

        return image_asset_ids

    # Step 2
    def import_roads_to_ee(
        self, blob_uri: str, roads_asset_id: Optional[str] = None
    ) -> str:
        task_id = self.storage2table(blob_uri, roads_asset_id, geometry_column="wkt")
        self.wait()

    # Step 3
    def group_bands(self, image_asset_ids: List[str], metadata: dict) -> str:
        band_names = [
            f"{at['attribute']}_{at['tag']}" for at in metadata["bands"].values()
        ]
        image_stack = ee.ImageCollection.fromImages(
            [ee.Image(i) for i in image_asset_ids]
        )
        bands = ee.List(list(metadata["bands"].values()))
        projection = ee.Image(image_asset_ids[0]).projection()

        def band_merge(attr_tag_meta):
            atm = ee.Dictionary(attr_tag_meta)
            _bands = ee.List(atm.get("bands"))
            attribute = atm.get("attribute")
            tag = atm.get("tag")

            band_indices = _bands.map(lambda x: ee.Number(x).subtract(ee.Number(1)))
            band_name = ee.String(attribute).cat(ee.String("_").cat(tag))
            return (
                image_stack.select(band_indices)
                .mosaic()
                .reduce(ee.Reducer.max())
                .rename(band_name)
                .reproject(projection)
            )

        img_col = ee.ImageCollection(bands.map(band_merge))
        img = img_col.toBands().rename(band_names)
        asset_path = f"{self.ee_osm_root}/{self.output_image}"
        self.export_image_ee(img, asset_path, image_collection=True)

        return asset_path

    # Step 4
    def clean_assets(self, assets):
        if self.skip_cleanup:
            return

        for asset in assets:
            self.rm_ee(asset)

    def calc(self):
        _assets_to_clean = []

        try:
            metadata = self._read_merged_image_metadata(self.metadata_uri)

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

        finally:
            with Timer("Clean up"):
                self.clean_assets(_assets_to_clean)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-d", "--taskdate")
    parser.add_argument(
        "-m",
        "--metadata",
        type=str,
        help="Google cloud storage uri for multi-band image json metadata file.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite existing outputs instead of incrementing",
    )
    parser.add_argument(
        "--skip_cleanup",
        action="store_true",
        help="Skip cleaning up temporary task files",
    )
    parser.add_argument(
        "--output_image",
        type=str,
        help="Custom output EE image name",
    )

    options = parser.parse_args()
    task = HIIOSMIngest(**vars(options))
    task.run()
