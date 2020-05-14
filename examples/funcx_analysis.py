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
host = 'merfpoc.egs.anl.gov'
# MDML username and password
username = 'test'
password = 'testtest'

# Create a configuration for your experiment
config = {
    "experiment": {
        "experiment_id": "TEST",
        "experiment_notes": "example.py file for MDML python package",
        "experiment_devices": ["DEVICE_A", "ANALYSIS"]
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
        },
        {
            "device_id": "ANALYSIS",
            "device_name": "Example analysis",
            "device_output": "Sum of 5 numbers",
            "device_output_rate": 0.01,
            "device_data_type": "text/numeric",
            "device_notes": "Sums variable1-variable5",
            "analysis_results": True,
            "estimated_runtime_ms": 1000,
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

# FuncX endpoint id and function id
func_id = "5481e03d-a8dc-4289-8574-9e5735062165" # sums variables 1 thru 5
endp_id = "a62a830a-5cd1-42a8-a4a8-a44fa552c899" # merf.egs.anl.gov endpoint


reset = False
try:
    i = 1
    while True:
        # Send 5 datapoints and then an analyses
        while i < 600:
            # Create random data
            deviceA_data = '\t'.join(random_data(5))
            
            # Send data
            My_MDML_Exp.publish_data('DEVICE_A', deviceA_data, '\t', influxDB=True)

            # run funcx analysis
            time.sleep(.2)
            # Send message to start analysis
            params = {
                "experiment_id": "TEST",
                "host": "merfpoc.egs.anl.gov"
            }
            My_MDML_Exp.publish_analysis("ANALYSIS", func_id, endp_id, params)
            
            # Sleep to send data once a second
            i += 1
            time.sleep(0.7)
        if not reset:
            time.sleep(10)
            print("Ending MDML experiment")
            My_MDML_Exp.reset()
            reset = True
except KeyboardInterrupt:
    if not reset:
        My_MDML_Exp.reset()
    My_MDML_Exp.stop_debugger()
