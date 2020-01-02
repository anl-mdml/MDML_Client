import time
import json
import numpy as np
import cv2
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
host = 'merf.egs.anl.gov'
# MDML username and password
username = 'test'
password = 'testtest'

# Creating experiment configuration
config = {
    "experiment": {
        "experiment_id": "TEST",
        "experiment_notes": "Example image streaming with the MDML",
        "experiment_devices": ["IMAGE"]
    },
    "devices": [
        {
            "device_id": "IMAGE",
            "device_name": "Example images",
            "device_output": "Random images",
            "device_output_rate": 10, # in Hertz
            "device_data_type": "image",
            "device_notes": "Random Images",
            "headers": [
                "PLIF"
            ],
            "data_types": [
                "Image"
            ],
            "data_units": [
                "Image"
            ]
        }
    ]
}


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
My_MDML_Exp.send_config() # this starts the experiment

# Sleep to let MDML ingest the configuration
time.sleep(2)

reset = False

try:
    i = 1
    while True:
        while i < 300:
            # Generating random images
            random_image = np.random.randint(255, size=(400,500,3), dtype=np.uint8)    
            _, img = cv2.imencode('.jpg', random_image)
            img_bytes = img.tobytes()
            img_b64bytes = b64encode(img_bytes)
            img_byte_string = img_b64bytes.decode('utf-8')
            
            # Publish image
            My_MDML_Exp.publish_image('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time())

            # Sleep to publish data once a second
            time.sleep(.1)
            i += 1
        if not reset:
            print("Ending MDML experiment")
            My_MDML_Exp.reset()
            reset = True
except KeyboardInterrupt:
    if not reset:
        My_MDML_Exp.reset()
    My_MDML_Exp.stop_debugger()
