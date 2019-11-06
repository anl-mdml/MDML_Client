import time
import json
import numpy as np
import cv2
from base64 import b64encode

import mdml_client as mdml # pip install mdml_client #

print("****************************************************************************")
print("*** This example will run indefinitely.                                  ***")
print("*** Press Ctrl+C to stop sending images and send the MDML reset message. ***")
print("*** Press Ctrl+C again to stop the example.                              ***")
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
        "experiment_devices": ["EXAMPLE"]
    },
    "devices": [
        {
            "device_id": "EXAMPLE",
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
    print("MDML MESSAGE: "+ msg)
My_MDML_Exp.set_debug_callback(user_func)
My_MDML_Exp.start_debugger()

# Sleep to let debugger thread set up
time.sleep(1)

# Add and validate a configuration for the experiment
My_MDML_Exp.add_config(config, 'streaming_image_example')

# Send configuration file to the MDML
My_MDML_Exp.send_config() # this starts the experiment

# Sleep to let MDML ingest the configuration
time.sleep(2)

try:
    i = 1
    while True:
        # Generating random images
        random_image = np.random.randint(255, size=(400,500,3), dtype=np.uint8)    
        _, img = cv2.imencode('.jpg', random_image)
        img_bytes = img.tobytes()
        img_b64bytes = b64encode(img_bytes)
        img_byte_string = img_b64bytes.decode('utf-8')
        
        # Publish image
        My_MDML_Exp.publish_image('EXAMPLE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time())

        # Sleep to publish data once a second
        time.sleep(.1)
except KeyboardInterrupt:
    print("Ending MDML experiment")
finally:
    My_MDML_Exp.reset()
