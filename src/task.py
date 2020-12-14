import argparse
import json
import os
import re
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any, Dict, Optional, Union

import ee  # type: ignore
from google.cloud import storage  # type: ignore
from task_base import HIITask  # type: ignore

import config


class ConversionException(Exception):
    pass


@dataclass
class Task:
    id: str
    attribute: str
    tag: str
    is_done: bool = False
    asset_path: Optional[str] = None
    operation_type: Optional[str] = None
    state: Optional[str] = None
    error: Optional[dict] = None

    def __str__(self):
        return self.id

    def update(self, details: Dict[str, Any]):
        self.asset_path = details.get("asset_path")
        self.operation_type = details.get("type")
        self.is_done = True if details.get("is_done") is True else False
        self.state = details.get("state")

        if self.state == self.EEFAILED:
            self.error = details.get("error")

    def to_json(self):
        return json.dumps(asdict(self))


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

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        self._args = kwargs
        self.skip_cleanup = self._args.get("skip_cleanup")
        self.storage_directory = self._args.get("storage_directory")
        self.ee_directory = self._args.get("ee_directory")
        self.overwrite = self._args.get("overwrite")
        self.bucket = os.environ["HII_OSM_BUCKET"]

        creds_path = Path(self.google_creds_path)
        if creds_path.exists() is False:
            with open(str(creds_path), "w") as f:
                f.write(self.service_account_key)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds_path

    @property
    def path_prefix(self):
        return f"{self.ee_osm_root}/tables/{self.taskdate}"

    def _get_table_root_dir(self, task_date: Optional[str] = None) -> str:
        if self.ee_directory is not None:
            return self.ee_directory
        elif task_date is None:
            raise ValueError("task_date is required when ee_directory is not defined.")
        else:
            return f"projects/{self.ee_project}/{self.ee_osm_root}/tables/{task_date}"

    def _get_table_asset_id(
        self, attribute: str, tag: str, task_date: Optional[str] = None
    ) -> str:
        root = self._get_table_root_dir(task_date)
        return str(Path(root, f"{attribute}_{tag}"))

    def _get_image_asset_id(self, attribute: str, tag: str, task_date: str):
        root = f"projects/{self.ee_project}/{self.ee_osm_root}"
        return f"{root}/{attribute}/{tag}/{tag}_{task_date}"

    def _get_csv_uri(self, attribute: str, tag: str) -> str:
        if self.storage_directory:
            root = self.storage_directory
            if root.endswith("/") is True:
                root = root[0:-1]
        else:
            root = f"gs://{self.bucket}/{self.taskdate}"

        return f"{root}/{attribute}_{tag}.csv"

    def _upload_to_cloudstorage(self, src_path: str) -> str:
        targ_path = Path(src_path).name
        client = storage.Client()
        bucket = client.bucket(self.bucket)
        blob = bucket.blob(targ_path)
        blob.upload_from_filename(src_path)

        return targ_path

    def _remove_from_cloudstorage(self, path: str):
        client = storage.Client()
        bucket = client.bucket(self.bucket)
        bucket.delete_blob(path)

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

    def _cp_storage_to_ee_table(self, blob_uri: str, table_asset_id: str) -> str:
        try:
            cmd = [
                "/usr/local/bin/earthengine",
                f"--service_account_file={self.google_creds_path}",
                "upload table",
                "--primary_geometry_column WKT",
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

    def asset_exists(self, asset_id: str) -> bool:
        try:
            return ee.data.getAsset(asset_id) is not None
        except ee.ee_exception.EEException:
            return False

    def rasterize_table(self, attribute: str, tag: str) -> str:
        table_asset_id = self._get_table_asset_id(attribute, tag, self.taskdate)
        table = ee.FeatureCollection(table_asset_id)

        image = table.reduceToImage(properties=["burn"], reducer=ee.Reducer.first())
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
        bucket = client.bucket(self.bucket)
        return storage.Blob(bucket=bucket, name=Path(uri).name).exists(client)

    def _parse_operation_response(
        self, response: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        if isinstance(response, dict) is False:
            return None

        name = response.get("name") or ""
        is_done = response.get("done")
        metadata = response.get("metadata") or dict()
        operation_type = metadata.get("type")
        description = metadata.get("description") or ""
        state = metadata.get("state") or None

        # fmt: off
        task_id = name[name.rfind("/") + 1:]
        # fmt: on

        asset_path_regex = re.compile(
            f'(?<=[/])projects/{self.ee_project}.*(?=["])', flags=re.IGNORECASE
        )
        asset_path = None
        try:
            matches = asset_path_regex.search(description)
            if matches:
                asset_path = matches[0]
        except TypeError:
            asset_path = None

        return dict(
            asset_path=asset_path,
            task_id=task_id,
            type=operation_type,
            is_done=is_done,
            state=state,
            operation=response,
        )

    def _update_tasks_details(self, tasks: Dict[str, Task]):
        operations = ee.data.listOperations(project=f"projects/{self.project_id}")
        for operation_dict in operations:
            operation = self._parse_operation_response(operation_dict) or dict()
            task_id = operation["task_id"]
            if task_id not in tasks:
                continue
            tasks[task_id].update(operation)

    def _rasterize_completed_table_imports(self, tasks: Dict[str, Task]):
        _tasks = dict()
        for task in tasks.values():
            if task.is_done is True and task.state == self.EESUCCEEDED:
                print(task.to_json())
                self.rasterize_table(task.attribute, task.tag)
                continue

            elif task.is_done is True:
                print(task.to_json())
                continue

            print(task.to_json())
            _tasks[task.id] = task

        return _tasks

    def _check_for_rasterization_ready_tables(self):
        if self.tasks:
            self._update_tasks_details(tasks=self.tasks)

        n = 2
        max_n = 10
        while True:
            if self.tasks is None or len(self.tasks) == 0:
                break

            self.tasks = self._rasterize_completed_table_imports(self.tasks)
            if n > max_n:
                n = max_n

            sleep(n ** 2)
            n += 1

            self._update_tasks_details(tasks=self.tasks)

    def _start_csv_import(
        self, attribute: str, tag: str, taskdate: str
    ) -> Optional[Task]:
        if (
            self.asset_exists(self._get_image_asset_id(attribute, tag, taskdate))
            is True
        ):
            return None

        table_asset_id = self._get_table_asset_id(attribute, tag, taskdate)
        if self.asset_exists(table_asset_id) is True:
            self.rasterize_table(attribute, tag)
            return None

        task_id = self._cp_storage_to_ee_table(
            self._get_csv_uri(attribute, tag),
            table_asset_id,
        )
        return Task(id=task_id, attribute=attribute, tag=tag)

    def start_csv_imports(self) -> Dict[str, Task]:
        tasks = dict()
        taskdate = self.taskdate
        attribute_tags = config.tags

        # Use _prep_asset_id to create folders
        self._prep_asset_id(self.path_prefix, False)

        for attribute, tag in attribute_tags:
            task = self._start_csv_import(attribute, tag, taskdate)
            if task is None:
                continue

            tasks[task.id] = task
        return tasks

    def _rasterize_attributes_tags(self):
        attribute_tags = config.tags
        for attribute, tag in attribute_tags:
            self.rasterize_table(attribute, tag)

    def remove_existing_data(self):
        attribute_tags = config.tags
        taskdate = self.taskdate

        if self.ee_directory is None:
            # TODO: Is there a check to see if directories exist.
            table_directory = self._get_table_root_dir(taskdate)
            # self.rm_ee(table_directory)

        for attribute, tag in attribute_tags:
            image_asset_id = self._get_image_asset_id(attribute, tag, taskdate)
            if self.asset_exists(image_asset_id) is True:
                self.rm_ee(image_asset_id)
                pass

    def generate_road_table(self):
        road_tags = config.road_tags
        feature_collections = []
        for attribute, tag in road_tags:
            feature_collections.append(
                ee.FeatureCollection(
                    self._get_table_asset_id(attribute, tag, self.taskdate)
                )
            )

        merged_feature_collection = ee.FeatureCollection(feature_collections)

        asset_path = f"{self.ee_osm_root}/roads"
        self.export_fc_ee(merged_feature_collection, asset_path)

    def calc(self):
        if self.overwrite is True:
            self.remove_existing_data()

        if self.ee_directory:
            self._rasterize_attributes_tags()
        else:
            self.tasks = self.start_csv_imports()
            self._check_for_rasterization_ready_tables()

        self.generate_road_table()

    def clean_up(self, **kwargs):
        if self.status == self.FAILED or self.skip_cleanup:
            return

        # TODO: Delete cloudstorage tables
        # self._remove_from_cloudstorage(self._get_csv_uri())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate", default=datetime.now(timezone.utc).date())

    parser.add_argument(
        "-s",
        "--storage_directory",
        type=str,
        help="URI to source CSV directory in Google Cloud Storage,"
        " overrides default storage directory path based on taskdate.",
    )

    parser.add_argument(
        "-e",
        "--ee_directory",
        type=str,
        help="EE directory path where import OSM tables are located.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing HII tag images for task date",
    )

    parser.add_argument(
        "--skip_cleanup",
        type=bool,
        help="Skip cleaning up temporary task files",
    )

    options = parser.parse_args()
    task = HIIOSMIngest(**vars(options))
    task.run()
