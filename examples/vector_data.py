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

print("***************************************************************************")
print("*** This example will publish data once every 3 seconds for 30 seconds. ***")
print("*** Press Ctrl+C to stop the example.                                   ***")
print("***************************************************************************")
time.sleep(5)

Exp_ID = 'TEST'
host = 'merf.egs.anl.gov'
username = 'test'
password = 'testtest'

# Connect to MDML
My_MDML_Exp = mdml.experiment(Exp_ID, username, password, host)

# Receive events about your experiment from MDML
def user_func(msg):
    msg_obj = json.loads(msg)
    if msg_obj['type'] == "NOTE":
        print(f'MDML NOTE: {msg_obj["message"]}')
    elif msg_obj['type'] == "ERROR":
        print(f'MDML ERROR: {msg_obj["message"]}')
    elif msg_obj['type'] == "RESULTS":
        print(f'MDML ANALYSIS RESULT FOR "{msg_obj["analysis_id"]}": {msg_obj["message"]}')
    else:
        print("ERROR WITHIN MDML, CONTACT ADMINS.")
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
My_MDML_Exp.add_config('./examples_config.json', 'mdml_examples')
# NOTE: The config variable created earlier is to illustrate the 
# relevant configuration information for this example. The actual configuration
# sent to the MDML contains devices for all examples so that different examples can 
# be run together. However, it is not recommended due to MDML details that are
# explained in the multiple_clients example scripts.
# Using the line below is also valid 
# My_MDML_Exp.add_config(config, 'mdml_examples')

# Send configuration file to the MDML
My_MDML_Exp.send_config()
time.sleep(1)

# Gather data to send
dat = {}
# Notice this data stays the same here but it does not need to 
dat['wavelength'] = list(range(100,201,1)) 

def random_data(size):
    dat = []
    for _ in range(size):
        dat.append(str(random.random()))
    return dat

reset = False
try:
    i = 0
    while True:
        while i < 10: # publish data 10 times
            # Create random data
            dat['intensity'] = random_data(101)

            # Send data to MDML
            My_MDML_Exp.publish_vector_data('VECTOR', dat, mdml.unix_time())

            # Sleep to publish data every three seconds
            time.sleep(3)
            i += 1
        if not reset:
            print("Ending MDML experiment")
            My_MDML_Exp.reset()
            reset = True
except KeyboardInterrupt:
    if not reset:
        My_MDML_Exp.reset()
    My_MDML_Exp.stop_debugger()
