##################################  READ ME  ##################################
# Some devices generate data that can be stored better than the default       #
# format MDML allows. For example, with spectrometry data there may be a few  #
# thousand intensities for a range of wavelength values. Rather than creating #
# headers for every possible wavelength value and sending the corresponding   #
# intensity, use publish_vector_data(). By publishing vector data, the        #
# configuration only needs two headers: wavelength and intensity. This        #
# requires one addition to the configuration in adding any entry for          #
# "influx_tags" in the device's configuration. "influx_tags" should be a list #
# containing the header of the "X" variable - in this case wavelength.        #
# Generally when publishing vector data, "influx_tags" must include the       #
# variables that are known even before the data collection takes place like   #
# wavelength here (we expect to measure the same wavelengths each time).      #
# Using publish_vector_data is also the only way to create graphs in Grafana  #
# such that the X-axis is not time.                                           #
###############################################################################

import time
import json
import random
import mdml_client as mdml # pip install mdml_client #

print("**************************************************************************")
print("*** This example will run indefinitely.                                ***")
print("*** Press Ctrl+C to stop sending data and send the MDML reset message. ***")
print("*** Press Ctrl+C again to stop the debugger and end the example.       ***")
print("**************************************************************************")
time.sleep(5)

Exp_ID = 'TEST'
host = 'merf.egs.anl.gov'
username = 'test'
password = 'testtest'

# Connect to MDML
My_MDML_Exp = mdml.experiment(Exp_ID, username, password, host)

# Receive events about your experiment from MDML
def user_func(msg):
    print("MDML MESSAGE: "+ msg)
My_MDML_Exp.set_debug_callback(user_func)
My_MDML_Exp.start_debugger()

# Sleep to let debugger thread set up
time.sleep(1)

# Create a configuration for the experiment
config = {
    "experiment": {
        "experiment_id": "TEST",
        "experiment_notes": "Publishing vector data example",
        "experiment_devices": ["VECTOR"]
    },
    "devices": [
        {
            "device_id": "VECTOR",
            "device_name": "Wavelength/Intensity Vectors",
            "device_output": "spectrometry data of wavelengths and corresponding intensities",
            "device_output_rate": 2, # in Hertz
            "device_data_type": "vector",
            "device_notes": "wavelengths may vary",
            "headers" : [
                "wavelength",
                "intensity"
            ],
            "data_types" : [
                "numeric",
                "numeric"
            ],
            "data_units" : [
                "nanometers",
                "intensity"
            ],
            "influx_tags": ["wavelength"]
        }
    ]
}

# Add and validate a configuration for the experiment
My_MDML_Exp.add_config(config, 'vector_data_example')

# Send configuration file to the MDML
My_MDML_Exp.send_config()

# Gather data to send
dat = {}
# Notice this data stays the same here but it does not need to 
dat['wavelength'] = list(range(100,201,1)) 

def random_data(size):
    dat = []
    for _ in range(size):
        dat.append(str(random.random()))
    return dat

try:
    while True:
        # Create random data
        dat['intensity'] = random_data(101)

        # Send data to MDML
        My_MDML_Exp.publish_vector_data('VECTOR', dat, mdml.unix_time())

        # Sleep to publish data every three seconds
        time.sleep(3)
except KeyboardInterrupt:
    print("Ending MDML experiment")
finally:
    My_MDML_Exp.reset()

# Make sure to reset the MDML to end your experiment!
time.sleep(10)
My_MDML_Exp.reset()
