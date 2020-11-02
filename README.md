# task_base
Base docker/python setup for running tasks and Earth Engine tasks

This repo contains the framework for use by all HII and SCL repos:
1. base Dockerfile to be inherited
2. base Python classes to be subclassed 

It is not meant to be run or instantiated directly. An example for how to use this repo can be found at  
https://github.com/SpeciesConservationLandscapes/task_hii_popdens  
Based on this example, to create a new task:
1. create a new repo using the convention of starting names with `task_hii_` or `task_scl_`
2. create a new Dockerfile in its root that can be more or less a copy of  
https://github.com/SpeciesConservationLandscapes/task_hii_popdens/blob/master/Dockerfile  
3. within a src/ dir, create your python files

To be available for cloud deployment, create a docker repo at  
https://cloud.docker.com/u/scl3/repository/list  
and configure an automated build using the `:latest` tag of the new git repo.

## Running locally
To run locally, copy into your root either:  
a) a `.config` dir containing your Earth Engine credentials file, or  
b) a .env file  containing stringified GCP service account authentication details

Example commands below use `task_hii_popdens` as an example inheriting from this repo.
- To build docker image:  
`docker build --pull --no-cache -t scl3/task_hii_popdens .`
- To run with your personal ee credentials stored in a .config dir that you've copied from your user dir:  
`docker run -it -v $PWD/.config:/root/.config scl3/task_hii_popdens python task/hii_popdens.py`
- To run with GCP service account credentials:  
`docker run -it --env-file ./.env scl3/task_hii_popdens python task/hii_popdens.py`
- To additionally map host code dir to container app dir for development, running `python task/hii_popdens.py` within
 container  
`docker run -it --env-file ./.env -v $PWD/src:/app scl3/task_hii_popdens sh`

## Classes
- `Task`: base class defining `taskdate` and other key properties - use for basic pipeline tasks not involving Earth
 Engine
- `EETask`: base Earth Engine task - sufficient for all non-species-specific EE tasks
- `SCLTask`: use for species-specific EE tasks










export OGR_INTERLEAVED_READING=YES; \
export OSM_CONFIG_FILE=~/projects/task_hii_osm/src/osm.ini; \
time ogr2ogr \
  -f "CSV" \
  osm.csv \
  -where "aeroway='aerodrome' or aeroway='apron' or aeroway='hangar' or aeroway='helipad' or aeroway='heliport' or aeroway='runway' or aeroway='spaceport' or aeroway='taxiway' or aeroway='terminal' or amenity='aerialway' or amenity='alpinecampwild' or leisure='beach_resort' or amenity='fuel' or leisure='golf_course' or leisure='marina' or leisure='pitch' or amenity='sanitary_dump_station' or barrier='city_wall' or barrier='ditch' or barrier='hedge' or barrier='retaining_wall' or barrier='wall' or landuse='basin' or landuse='cemetery' or landuse='industrial' or landuse='landfill' or landuse='quarry' or landuse='salt_pond' or landuse='village_green' or man_made='adit' or man_made='beacon' or man_made='breakwater' or man_made='chimney' or man_made='communications_tower' or man_made='dyke' or man_made='embankment' or man_made='gasometer' or man_made='groyne' or man_made='lighthouse' or man_made='mast' or man_made='mineshaft' or man_made='observatorytelescope' or man_made='petroleum_well' or man_made='pier' or man_made='pipeline' or man_made='pumping_station' or man_made='reservoir_covered' or man_made='silo' or man_made='snow_fence' or man_made='storage_tank' or man_made='tower' or man_made='wastewater_plant' or man_made='watermill' or man_made='water_tower' or man_made='water_well' or man_made='water_works' or man_made='windmill' or man_made='works' or military='airfield' or military='ammunition' or military='barracks' or military='bunker' or military='checkpoint' or military='danger_area' or military='naval_base' or military='nuclear_explosion_site' or military='range' or military='trench' or power='cable' or power='heliostat' or power='line' or power='substation' or power='xbio' or power='xcoal' or power='xhydro' or power='xnuclear' or power='xoil' or power='xother' or power='xsolar' or power='xwaste' or power='xwind' or waterway='canal' or waterway='dam' or waterway='ditch' or waterway='drain' or waterway='lock_gate' or waterway='weir' or highway='bridleway' or highway='bus_guideway' or highway='cycleway' or highway='elevator' or highway='escape' or highway='footway' or highway='living_street' or highway='mini_roundabout' or highway='motorway' or highway='motorway_link' or highway='path' or highway='pedestrian' or highway='primary' or highway='primary_link' or highway='raceway' or highway='residential' or highway='rest_area' or highway='road' or highway='secondary' or highway='secondary_link' or highway='service' or highway='steps' or highway='tertiary' or highway='tertiary_link' or highway='track' or highway='trunk' or highway='trunk_link' or highway='turning_circle' or highway='unclassified' or railway='abandoned' or railway='disused' or railway='funicular' or railway='halt' or railway='light_rail' or railway='miniature' or railway='monorail' or railway='narrow_gauge' or railway='platform' or railway='preserved' or railway='rail' or railway='station' or railway='subway' or railway='tram'" \
  planet-201012.osm.pbf
