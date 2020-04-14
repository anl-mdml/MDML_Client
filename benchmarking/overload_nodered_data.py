import time
import json
import cv2
import random
import argparse
import numpy as np
from base64 import b64encode

import mdml_client as mdml # pip install mdml_client #

parser = argparse.ArgumentParser(description='MDML Benchmarking')
parser.add_argument('--host', dest='host', required=True, help='MDML hostname.')
parser.add_argument('--send_images', type=bool, default=False, dest='IMAGES', help='True to send images, False to send text/numeric.')
parser.add_argument('--test_throughput', type=bool, default=False, dest='THROUGHPUT', help='True for throughput, False for processing speed.')
parser.add_argument('--messages', type=int, dest='NUM_MESSAGES', help='Number of messages to send.')
args = parser.parse_args()

# Approved experiment ID (supplied by MDML administrators - will not work otherwise)
Exp_ID = 'TEST'
# MDML message broker host
host = args.host
# MDML username and password
username = 'test'
password = 'testtest'

# Setting variables
IMAGES = bool(args.IMAGES)
print(args.THROUGHPUT)
THROUGHPUT = bool(args.THROUGHPUT)
NUM_MESSAGES = args.NUM_MESSAGES
if IMAGES:
    POINTS = [(10,10,3), (30,20,3), (60,40,3), (80,80,3), (160,80,3), (160,160,3), (320,160,3), (320,320,3), (400,400,3), (600,530,3), (800,600,3), (800,800,3), (1000,800,3), (1000,1000,3), (1200,1200,3), (1600,1600,3), (2000,2000,3), (2400,2400,3), (2800,2800,3)]
    DELAYS = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1.5,1.5,1.5]
    assert len(POINTS) == len(DELAYS)
    if THROUGHPUT:
        RUN = 'IMAGE_THROUGHPUT'
    else:
        RUN = 'IMAGE_PROCESSING'
else:
    POINTS = [6,50,100,250,500,750,1000,2000,2500,3000,4000,5000,10000,11000,12000,13000,14000,15000,20000,30000,40000,50000,60000,70000,80000,90000,100000]
    DELAYS = [0.5,0.5,0.5,0.5,0.5,0.5,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1.5,1.5,1.5,1.5]
    assert len(POINTS) == len(DELAYS)
    if THROUGHPUT:
        RUN = 'TEXT_THROUGHPUT'
    else:
        RUN = 'TEXT_PROCESSING'


# Create MDML experiment
My_MDML_Exp = mdml.experiment(Exp_ID, username, password, host)

for j, num_points in enumerate(POINTS):
    # Creating configuration files
    if IMAGES:
        device = f'IMAGE_{num_points[0]}_{num_points[1]}'
        config = {
            "experiment": {
                "experiment_id": "TEST",
                "experiment_notes": "Example image streaming with the MDML",
                "experiment_devices": [device]
            },
            "devices": [
                {
                    "device_id": device,
                    "device_name": "Example images",
                    "device_output": "Random images",
                    "device_output_rate": 0.01, # in Hertz
                    "device_data_type": "image",
                    "device_notes": "Random Images",
                    "headers": ["Image"],
                    "data_types": ["Image"],
                    "data_units": ["Image"]

                }
            ]
        }
        run_id = f'{RUN}_{num_points[0]}_{num_points[1]}'
    else:
        headers = []
        for i in range(num_points):
            headers.append("var"+str(i))
        device = f'TEXTNUMERIC_{num_points}'
        config = {
            "experiment": {
                "experiment_id": "TEST",
                "experiment_notes": "Example image streaming with the MDML",
                "experiment_devices": [device]
            },
            "devices": [
                {
                    "device_id": device,
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
        run_id = f'{RUN}_{num_points}'

    # Add and validate a configuration for the experiment
    My_MDML_Exp.add_config(config, run_id)

    # Send configuration file to the MDML
    My_MDML_Exp.send_config() # this starts the experiment

    # Sleep to let MDML ingest the configuration
    time.sleep(7)

    def random_data(size):
        dat = []
        for _ in range(size):
            dat.append(str(random.random()))
        return dat

    i = 0
    # Generating data
    if IMAGES:
        random_image = np.random.randint(255, size=num_points, dtype=np.uint8)    
        _, img = cv2.imencode('.jpg', random_image)
        img_bytes = img.tobytes()
        img_b64bytes = b64encode(img_bytes)
        img_byte_string = img_b64bytes.decode('utf-8')
    else:
        dat = '\t'.join(random_data(num_points))

    try:
        while i < NUM_MESSAGES:
            if IMAGES:
                My_MDML_Exp.publish_image(device, img_byte_string, 'random_image_' + str(i) + '.JPG', mdml.unix_time())
                print(THROUGHPUT)
                if not THROUGHPUT:
                    time.sleep(DELAYS[j])
            else:
                # Generating data
                My_MDML_Exp.publish_data(device, dat, "\t", timestamp=mdml.unix_time())
                if not THROUGHPUT:
                    time.sleep(DELAYS[j])
            i += 1
    except KeyboardInterrupt:
        print("Ending MDML experiment early")
    finally:
        if THROUGHPUT:
            time.sleep(120)
        else:
            time.sleep(5)
        My_MDML_Exp.reset()
        time.sleep(30)

My_MDML_Exp.disconnect()