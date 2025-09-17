from task import HIIOSMIngest
options = {
    "taskdate": "2012-12-31", # setting this further back to test self.population_density property bug
    "metadata": "gs://hii-osm/2012-12-31/metadata.json", # this is what actually controls what OSM data you're processing
    # "skip_cleanup": True,
    "output_image": "osm_kyle_test_guatemala2"
}
task = HIIOSMIngest(**options)
task.run()