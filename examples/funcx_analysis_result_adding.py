import time
import json
import random
import multiprocessing
import mdml_client as mdml # pip install mdml_client #

print("**************************************************************************")
print("*** This example will publish the number 1 then call a FuncX function  ***")
print("*** that simply adds 1. The result is returned through the debugger    ***")
print("*** and that new value is publish as the next data point. This example ***")
print("*** is used to illustrate how analysis results can be used to guide an ***")
print("*** experiment.                                                        ***")
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
        "experiment_devices": ["DEVICE_F", "ADD1"]
    },
    "devices": [
        {
            "device_id": "DEVICE_F",
            "device_name": "Test device F",
            "device_output": "integer",
            "device_output_rate": 1, # in Hertz
            "device_data_type": "text/numeric",
            "device_notes": "Integer increasing by 1",
            "headers" : [
                "count"
            ],
            "data_types" : [
                "numeric"
            ],
            "data_units" : [
                "NA"
            ]
        },
        {
            "device_id": "ADD1",
            "device_name": "Example function",
            "device_output": "Add 1 to a number",
            "device_output_rate": 0.01,
            "device_data_type": "text/numeric",
            "device_notes": "Add 1 to something",
            "analysis_results": True,
            "estimated_runtime_ms": 100,
            "headers": [
                "sum"
            ],
            "data_types": [
                "sum"
            ],
            "data_units": [
                "sum"
            ]
        }
    ]
}

# Create MDML experiment
My_MDML_Exp = mdml.experiment(Exp_ID, username, password, host)

# Receive events about your experiment from MDML
# results returned and publishing logic will be in separate processes
add1_result_queue = multiprocessing.Manager().Queue() # passes results between processes

def user_func(msg):
    msg_obj = json.loads(msg)
    if msg_obj['type'] == "NOTE":
        print(f'MDML NOTE: {msg_obj["message"]}')
    elif msg_obj['type'] == "ERROR":
        print(f'MDML ERROR: {msg_obj["message"]}')
    elif msg_obj['type'] == "RESULTS":
        print(f'MDML ANALYSIS RESULT FOR "{msg_obj["analysis_id"]}": {msg_obj["message"]}')
        if msg_obj['analysis_id'] == "ADD1":
            add1_result_queue.put(msg_obj["message"])
    else:
        print("ERROR WITHIN MDML, CONTACT ADMINS.")

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

# Init vars for funcx analysis
queries = [
    {
        "device": "DEVICE_F",
        "variables": [],
        "last" : 1
    }
]
# FuncX endpoint id and function id
funcx_endp_id = "4b116d3c-1703-4f8f-9f6f-39921e5864df" # public tutorial endpoint
funcx_func_id = "c966b543-1611-4534-9e14-599f456e1a45" # Adds 1 to the value supplied

# # The function below was registered with funcx to get the above func_id
# def add1(data):
#     val = data[0][0]['count']
#     val += 1
#     return str(val)
#
# # The input parameter from MDML looks like this:
# [[{
#   'time': '2019-12-20T18:23:09.883Z', 
#   'count': 1
# }]]
#
# # The return value is a string: '2'

reset = False
CURR_DATA = 0 # integer to publish first
i = 0 # loop index
try:
    while True:
        while i < 6:
            # Do not update CURR_DATA the first time (there would be no results anyway)
            if i != 0:
                CURR_DATA = add1_result_queue.get() # .get() is blocking
            # Send data
            My_MDML_Exp.publish_data('DEVICE_F', str(CURR_DATA), '\t', influxDB=True)

            # Send message to start analysis
            My_MDML_Exp.publish_analysis("ADD1", queries, funcx_func_id, funcx_endp_id)
            
            # Inc loop index
            i += 1
        if not reset:
            time.sleep(5)
            print("Ending MDML experiment")
            My_MDML_Exp.reset()
            reset = True
except KeyboardInterrupt:
    if not reset:
        My_MDML_Exp.reset()
    My_MDML_Exp.stop_debugger()
