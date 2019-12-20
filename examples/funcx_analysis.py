import time
import json
import random
import mdml_client as mdml # pip install mdml_client #

print("**************************************************************************")
print("*** This example will publish data 5 times and then call an analysis.  ***")
print("*** Press Ctrl+C to stop the example.                                  ***")
print("**************************************************************************")
time.sleep(5)

# Approved experiment ID (supplied by MDML administrators - will not work otherwise)
Exp_ID = 'TEST'
# MDML message broker host
host = 'merf.egs.anl.gov'
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

# Sleep to let MDML ingest the configuration
time.sleep(2)

def random_data(size):
    dat = []
    for _ in range(size):
        dat.append(str(random.random()))
    return dat

# Init vars for funcx analysis
queries = [
    {
        "device": "DEVICE_A",
        "variables": [],
        "last" : 1
    }
]
# FuncX endpoint id and function id
funcx_endp_id = "4b116d3c-1703-4f8f-9f6f-39921e5864df" # public tutorial endpoint
funcx_func_id = "b5d83b0a-2c1e-48fc-a61f-fb7eae58c960" # sums variables 1 thru 5

# # The function below was registered with funcx to get the above func_id
# def sum_vars(data):
#     var_sum = float(data[0][0]['variable1']) + float(data[0][0]['variable2']) + float(data[0][0]['variable3']) + float(data[0][0]['variable4']) + float(data[0][0]['variable5'])
#     return str(var_sum)
#
# # The input parameter from MDML looks like this:
# [[{
#   'time': '2019-12-20T18:23:09.883Z', 
#   'variable1': 0.7148689571386346, 
#   'variable2': 0.3303284415100972, 
#   'variable3': 0.7029252964954437, 
#   'variable4': 0.5739044292459075, 
#   'variable5': 0.09692214917245678
# }]]
#
# # The return value is a string, for example:
# '2.4817439194'

reset = False
try:
    i = 1
    while True:
        # Send 5 datapoints and then an analyses
        while i < 6:
            # Create random data
            deviceA_data = '\t'.join(random_data(5))
            
            # Send data
            My_MDML_Exp.publish_data('DEVICE_A', deviceA_data, '\t', influxDB=True)

            # run funcx analysis
            if i % 5 == 0:
                # Send message to start analysis
                My_MDML_Exp.publish_analysis("ANALYSIS", queries, funcx_func_id, funcx_endp_id)
            
            # Sleep to send data once a second
            i += 1
            time.sleep(.9)
        if not reset:
            time.sleep(2)
            print("Ending MDML experiment")
            My_MDML_Exp.reset()
            reset = True
except KeyboardInterrupt:
    if not reset:
        My_MDML_Exp.reset()
    My_MDML_Exp.stop_debugger()
