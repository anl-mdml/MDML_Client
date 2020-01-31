import time
import json
import numpy as np
import cv2
import random
from base64 import b64encode

import mdml_client as mdml # pip install mdml_client #

print("****************************************************************************")
print("*** This example publish an image 10 times a second for 30 seconds.      ***")
print("*** Press Ctrl+C to stop the example.                                    ***")
print("****************************************************************************")
time.sleep(5)

# Approved experiment ID (supplied by MDML administrators - will not work otherwise)
Exp_ID = 'TEST'
# MDML message broker host
host = "merfpoc.egs.anl.gov"
# MDML username and password
username = 'test'
password = 'testtest'

# Create MDML experiment
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

num_points = 12000
headers = []
for i in range(num_points):
    headers.append("var"+str(i))

config = {
    "experiment": {
        "experiment_id": "TEST",
        "experiment_notes": "Example image streaming with the MDML",
        "experiment_devices": ["LARGE_DATA"]
    },
    "devices": [
        {
            "device_id": "LARGE_DATA",
            "device_name": "Example images",
            "device_output": "Random images",
            "device_output_rate": 0.01, # in Hertz
            "device_data_type": "image",
            "device_notes": "Random Images",
            "headers": headers,
            "data_types": headers,
            "data_units": headers
        }
    ]
}

# Add and validate a configuration for the experiment
My_MDML_Exp.add_config(config, 'mdml_examples')

# Send configuration file to the MDML
My_MDML_Exp.send_config() # this starts the experiment

# Sleep to let MDML ingest the configuration
time.sleep(2)

reset = False

def random_data(size):
    dat = []
    for _ in range(size):
        dat.append(str(random.random()))
    return dat

try:
    i = 0
    dat = '\t'.join(random_data(num_points))
    while True:
        while i < 100000000:
            # Generating data
            My_MDML_Exp.publish_data('LARGE_DATA', dat, "\t", timestamp=mdml.unix_time())
            print(i)
            time.sleep(0.05)
            i += 1
        if not reset:
            print("Ending MDML experiment")
            My_MDML_Exp.reset()
            reset = True
except KeyboardInterrupt:
    if not reset:
        My_MDML_Exp.reset()
    My_MDML_Exp.stop_debugger()
finally:
    My_MDML_Exp.disconnect()
