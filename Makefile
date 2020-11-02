IMAGE=scl3/task_hii_osm_ingest


build:
	docker build --no-cache -t $(IMAGE) .

run:
	docker run --env-file=.env -v `pwd`/src:/app --rm -it --entrypoint python $(IMAGE) task.py -f osm.pbf -c c39cd233-a051-420e-8ec1-892d3488e32b.csv

shell:
	docker run --env-file=.env -v `pwd`/src:/app --rm -it --entrypoint sh $(IMAGE)
