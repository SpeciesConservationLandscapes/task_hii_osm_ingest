# allows to run HIIOSMIngest task with a python debugger for future maintenance..
# if you know you know
from task import HIIOSMIngest
options = {
    "taskdate": "2021-12-31", # setting this further back to test self.population_density property bug
    "metadata": "gs://hii-osm/2021-12-31/metadata.json", # this is what actually controls what OSM data you're processing
    # "skip_cleanup": True,
    "import_roads": True,
    "output_image": "osm_kyle_test_belize"
}
task = HIIOSMIngest(**options)
task.run()