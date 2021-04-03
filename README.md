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

```
/app # python task.py --help
usage: task.py [-h] [-d TASKDATE] [-m METADATA] [--overwrite] [--skip_cleanup]

optional arguments:
  -h, --help            show this help message and exit
  -d TASKDATE, --taskdate TASKDATE
  -m METADATA, --metadata METADATA
                        Google cloud storage uri for multi-band image json metadata file.
  --overwrite           Replace existing HII tag images for task date
  --skip_cleanup        Skip cleaning up temporary task files
```