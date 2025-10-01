HII OSM Ingest
--------------

## What does this task do?

1. Import split images from Google Storage into Earth Engine
2. Import roads from Google Storage into Earth Engine
3. Group bands from imported images into a single image
4. Clean split images

## Environment Variables

```
SERVICE_ACCOUNT_KEY=<GOOGLE SERVICE ACCOUNT KEY>
DEFAULT_BUCKET = os.environ.get("HII_OSM_BUCKET", "hii-osm")
ee_osm_root = "osm"
project_id = "hii3-246517"
```

## Usage

*All parameters may be specified in the environment as well as the command line.*

```
/app # python task.py --help
usage: task.py [-h] [-d TASKDATE] [-m METADATA] [--overwrite] [--skip_cleanup] [--output_image OUTPUT_IMAGE]

optional arguments:
  -h, --help            show this help message and exit
  -d TASKDATE, --taskdate TASKDATE
  -m METADATA, --metadata METADATA
                        Google cloud storage uri for multi-band image json metadata file. (default: None)
  --overwrite           overwrite existing outputs instead of incrementing (default: False)
  --skip_cleanup        Skip cleaning up temporary task files (default: False)
  --output_image OUTPUT_IMAGE
                        Custom output EE image name (default: None)
```

## Google Cloud VM Setup
1. In the Cloud Console navigate to Compute Engine > VM Instances

2. Find the VM named 'task-hii-osm-ingest' and click 'SSH' to enter into the VM in your browswer

3. Once inside the VM, check for necessary installed software that should be already installed. Copy and paste this block of commands into the terminal and hit enter

```bash
git --version
docker --version
make --version
```

5. Copy and paste this block of commands into terminal and hit enter:

```bash
git clone --branch pgeo-maintenance https://github.com/SpeciesConservationLandscapes/task_hii_osm_ingest.git
cd task_hii_osm_ingest
sudo make build
```
6. To enter into the task's docker container, it needs an environment file (.env) to pass in. Save-as the [.env.example](.env.example) file to a new file called .env. You will need to paste in the google cloud service account key info for the service account that runs the code on our behalf. See ... for more details.

7. Once your .env file is filled out, run:

```bash
sudo make shell
```

8. You should now be inside the built docker container. Finally, run a task for your chosen task date, i.e.

```python
python task.py -d 2025-12-31 -m gs://hii-osm/2025-12-31/metadata.json
```

### License
Copyright (C) 2022 Wildlife Conservation Society
The files in this repository  are part of the task framework for calculating 
Human Impact Index and Species Conservation Landscapes (https://github.com/SpeciesConservationLandscapes) 
and are released under the GPL license:
https://www.gnu.org/licenses/#GPL
See [LICENSE](./LICENSE) for details.
