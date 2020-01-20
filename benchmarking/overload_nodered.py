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
#host = 'merf.egs.anl.gov'
host = "146.137.10.50"
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

import os
print(len(f"\x00{os.urandom(16)}\x00"))
print(f"\x00{os.urandom(4)}\x00")
print(len(np.random.bytes(16)))
print(np.random.bytes(16))

print(len(b64encode(np.random.bytes(750)).decode('utf-8'))) # 1KB
print(len(b64encode(np.random.bytes(1500)).decode('utf-8'))) # 2KB
print(len(b64encode(np.random.bytes(3750)).decode('utf-8'))) # 5KB
print(len(b64encode(np.random.bytes(7500)).decode('utf-8'))) # 10KB
print(len(b64encode(np.random.bytes(18750)).decode('utf-8'))) # 25KB
print(len(b64encode(np.random.bytes(37500)).decode('utf-8'))) # 50KB
print(len(b64encode(np.random.bytes(75000)).decode('utf-8'))) # 100KB
print(len(b64encode(np.random.bytes(187500)).decode('utf-8'))) # 250KB
print(len(b64encode(np.random.bytes(375000)).decode('utf-8'))) # 500KB
print(len(b64encode(np.random.bytes(750000)).decode('utf-8'))) # 1MB
print(len(b64encode(np.random.bytes(1500000)).decode('utf-8'))) # 2MB
print(len(b64encode(np.random.bytes(3750000)).decode('utf-8'))) # 5MB
print(len(b64encode(np.random.bytes(7500000)).decode('utf-8'))) # 10MB



try:
    i = 0
    while True:
        while i < 10000:
            if i < 500:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(750)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '1KB')

            elif i < 1000:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(1500)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '2KB')

            elif i < 1500:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(3750)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '5KB')
                time.sleep(.1)

            elif i < 2000:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(7500)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '10KB')

            elif i < 2500:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(18750)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '25KB')

            elif i < 3000:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(37500)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '50KB')
                time.sleep(1)

            elif i < 3500:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(75000)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '100KB')

            elif i < 4000:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(187500)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '250KB')

            elif i < 4500:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(375000)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '500KB')

            elif i < 5000:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(750000)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '1MB')

            elif i < 5500:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(1500000)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '2MB')

            elif i < 6000:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(3750000)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '5MB')

            elif i < 6500:
                # Generating random images
                img_byte_string = b64encode(np.random.bytes(7500000)).decode('utf-8')
                My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time(), '10MB')

            # elif i < 7000:
            # elif i < 7500:
            # elif i < 8000:
            # elif i < 8500:
            # elif i < 9000:
            # elif i < 9500:
            # elif i < 10000:

            i += 1
        if not reset:
            print("Ending MDML experiment")
            My_MDML_Exp.reset()
            reset = True
except KeyboardInterrupt:
    if not reset:
        My_MDML_Exp.reset()
    My_MDML_Exp.stop_debugger()

# try:
#     i = 0
#     while True:
#         while i < 10000:
#             if i < 500:
#                 # Generating random images
#                 random_image = np.random.randint(255, size=(200,400,3), dtype=np.uint8) # 125 KB
#                 _, img = cv2.imencode('.jpg', random_image)
#                 img_bytes = img.tobytes()
#                 img_b64bytes = b64encode(img_bytes)
#                 img_byte_string = img_b64bytes.decode('utf-8')
                
#                 # Publish image
#                 start = mdml.unix_time()
#                 print(f'{start}: {len(img_byte_string)}')
#                 My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', start, 125)
#                 time.sleep(.25)
#             elif i < 1000:
#                 # Generating random images
#                 random_image = np.random.randint(255, size=(400,400,3), dtype=np.uint8) # 250 KB
#                 _, img = cv2.imencode('.jpg', random_image)
#                 img_bytes = img.tobytes()
#                 img_b64bytes = b64encode(img_bytes)
#                 img_byte_string = img_b64bytes.decode('utf-8')
                
#                 # Publish image
#                 start = mdml.unix_time()
#                 print(f'{start}: {len(img_byte_string)}')
#                 My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', start, 250)
#                 time.sleep(.5)
#             elif i < 1500:
#                 # Generating random images
#                 random_image = np.random.randint(255, size=(800,800,3), dtype=np.uint8) # 1 MB
#                 _, img = cv2.imencode('.jpg', random_image)
#                 img_bytes = img.tobytes()
#                 img_b64bytes = b64encode(img_bytes)
#                 img_byte_string = img_b64bytes.decode('utf-8')
                
#                 # Publish image
#                 start = mdml.unix_time()
#                 print(f'{start}: {len(img_byte_string)}')
#                 time.sleep(.5)
#                 My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', start, 1000)
#             elif i < 2000:
#                 # Generating random images
#                 random_image = np.random.randint(255, size=(1600,800,3), dtype=np.uint8) # 2 MB
#                 _, img = cv2.imencode('.jpg', random_image)
#                 img_bytes = img.tobytes()
#                 img_b64bytes = b64encode(img_bytes)
#                 img_byte_string = img_b64bytes.decode('utf-8')
                
#                 # Publish image
#                 start = mdml.unix_time()
#                 print(f'{start}: {len(img_byte_string)}')
#                 My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', start, 2000)
#                 time.sleep(.75)
#             elif i < 2500:
#                 # Generating random images
#                 random_image = np.random.randint(255, size=(1600,1600,3), dtype=np.uint8) # 4 MB
#                 _, img = cv2.imencode('.jpg', random_image)
#                 img_bytes = img.tobytes()
#                 img_b64bytes = b64encode(img_bytes)
#                 img_byte_string = img_b64bytes.decode('utf-8')
                
#                 # Publish image
#                 start = mdml.unix_time()
#                 print(f'{start}: {len(img_byte_string)}')
#                 My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', start, 4000)
#                 time.sleep(1)
#             elif i < 3000:
#                 # Generating random images
#                 random_image = np.random.randint(255, size=(3200,1600,3), dtype=np.uint8) # 8 MB
#                 _, img = cv2.imencode('.jpg', random_image)
#                 img_bytes = img.tobytes()
#                 img_b64bytes = b64encode(img_bytes)
#                 img_byte_string = img_b64bytes.decode('utf-8')
                
#                 # Publish image
#                 start = mdml.unix_time()
#                 print(f'{start}: {len(img_byte_string)}')
#                 My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', start, 8000)
#                 time.sleep(1)
#             elif i < 3500:
#                 # Generating random images
#                 random_image = np.random.randint(255, size=(3200,3200,3), dtype=np.uint8) #  MB
#                 _, img = cv2.imencode('.jpg', random_image)
#                 img_bytes = img.tobytes()
#                 img_b64bytes = b64encode(img_bytes)
#                 img_byte_string = img_b64bytes.decode('utf-8')
                
#                 # Publish image
#                 start = mdml.unix_time()
#                 print(f'{start}: {len(img_byte_string)}')
#                 My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', start, 8000)
#                 time.sleep(1)
#             elif i < 4000:
#                 # Generating random images
#                 random_image = np.random.randint(255, size=(8,8,3), dtype=np.uint8) # 1 KB
#                 _, img = cv2.imencode('.jpg', random_image)
#                 img_bytes = img.tobytes()
#                 img_b64bytes = b64encode(img_bytes)
#                 img_byte_string = img_b64bytes.decode('utf-8')
                
#                 # Publish image
#                 start = mdml.unix_time()
#                 print(f'{start}: {len(img_byte_string)}')
#                 My_MDML_Exp._publish_image_benchmarks('IMAGE', img_byte_string, 'random_image_' + str(i) + '.JPG', start, 8000)
#                 time.sleep(1)
#             # elif i < 4500:
#             # elif i < 5000:
#             # elif i < 5500:
#             # elif i < 6000:
#             # elif i < 6500:
#             # elif i < 7000:
#             # elif i < 7500:
#             # elif i < 8000:
#             # elif i < 8500:
#             # elif i < 9000:
#             # elif i < 9500:
#             # elif i < 10000:

#             i += 1
#         if not reset:
#             print("Ending MDML experiment")
#             My_MDML_Exp.reset()
#             reset = True
# except KeyboardInterrupt:
#     if not reset:
#         My_MDML_Exp.reset()
#     My_MDML_Exp.stop_debugger()
