# Using this to stop uploading to twine every time (make sure mdml_client is not installed in the environment)
# import sys
# sys.path.insert(1, "..")

import time
import mdml_client as mdml 

# Create connection to the MDML
exp = mdml.experiment("TEST", "test", "testtest", "merfpoc.egs.anl.gov")

# Initialize an experiment
exp.add_config(experiment_run_id="auto_gen_config_testing", auto=True)
exp.send_config() # Send configuration to MDML

time.sleep(1) # Give it a second to be ingested

# Start sending data
exp.publish_data("NEW_DEVICE_1", {"temp":94, "air_flow": 25}, add_device=True)

time.sleep(5) # Sleeping for debugging purposes

exp.reset() # End experiment with MDML
exp.disconnect() # Disconnect from MDML