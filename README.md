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
HII_OSM_BUCKET=hii-osm
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