IMAGE=scl3/task_hii_osm_ingest


build:
	docker build --no-cache -t $(IMAGE) .

run:
	docker run --env-file=.env -v `pwd`/src:/app --rm -it --entrypoint python $(IMAGE) task.py

shell:
	docker run --env-file=.env -v `pwd`/src:/app --rm -it --entrypoint sh $(IMAGE)
