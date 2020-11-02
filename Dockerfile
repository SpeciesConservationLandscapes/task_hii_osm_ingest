FROM scl3/task_base:latest

RUN pip install git+https://github.com/SpeciesConservationLandscapes/task_base.git
RUN apk add gdal-tools gdal-dev
RUN pip install requests pytest gdal==3.1.2

WORKDIR /app
COPY $PWD/src .