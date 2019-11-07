import time
import json
import random
import mdml_client as mdml # pip install mdml_client #

print("**************************************************************************")
print("*** This example will run indefinitely.                                ***")
print("*** Press Ctrl+C to stop sending data and send the MDML reset message. ***")
print("*** Press Ctrl+C again to stop the example.                            ***")
print("**************************************************************************")
time.sleep(5)

# Approved experiment ID (supplied by MDML administrators - will not work otherwise)
Exp_ID = 'TEST'
# MDML message broker host
host = '146.137.10.50'
# MDML username and password
username = 'test'
password = 'testtest'

# Create a configuration for your experiment
config = {
    "experiment": {
        "experiment_id": "TEST",
        "experiment_notes": "example.py file for MDML python package",
        "experiment_devices": ["DEVICE_A"]
    },
    "devices": [
        {
            "device_id": "DEVICE_A",
            "device_name": "Test device A",
            "device_output": "random data",
            "device_output_rate": 1, # in Hertz
            "device_data_type": "text/numeric",
            "device_notes": "Random data generated and streamed for example purposes",
            "headers" : [
                "variable1",
                "variable2",
                "variable3",
                "variable4",
                "variable5"
            ],
            "data_types" : [
                "numeric",
                "numeric",
                "numeric",
                "numeric",
                "numeric"
            ],
            "data_units" : [
                "NA",
                "NA",
                "NA",
                "NA",
                "NA"
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

# Login for using FuncX
My_MDML_Exp.globus_login()

# Add and validate a configuration for the experiment
My_MDML_Exp.add_config(config, 'funcx_analysis_example')

# Send configuration file to the MDML
My_MDML_Exp.send_config()

# Sleep to let MDML ingest the configuration
time.sleep(2)

def random_data(size):
    dat = []
    for _ in range(size):
        dat.append(str(random.random()))
    return dat

try:
    # Init vars for funcx analysis
    i = 1
    queries = [
        {
            "device": "DEVICE_A",
            "variables": [],
            "last" : 1
        }
    ]
    # FuncX endpoint id and function id
    funcx_endp_id = "a5c5f716-610e-40e1-9b9c-05c4b9a0a102"
    funcx_func_id = "ca4ca1a5-abb1-49b1-a5c8-8ce4d3db4138"
    # Send data and run analyses indefinitely
    while True:
        # Create random data
        deviceA_data = '\t'.join(random_data(5))
        
        # Send data
        My_MDML_Exp.publish_data('DEVICE_A', deviceA_data, '\t', influxDB=True)

        # run funcx analysis
        if i % 5 == 0:
            # Send message to start analysis
            My_MDML_Exp.publish_analysis(queries, funcx_func_id, funcx_endp_id)
        
        # Sleep to send data once a second
        i += 1
        if i == 6:
            raise KeyboardInterrupt
        time.sleep(.9)
except KeyboardInterrupt:
    print("Ending MDML experiment")
finally:
    time.sleep(3)
    My_MDML_Exp.reset()