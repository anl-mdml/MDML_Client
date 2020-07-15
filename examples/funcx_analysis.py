import time
import json
import random
import mdml_client as mdml # pip install mdml_client #
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--register", help="register the function with FuncX",
                    action="store_true")
parser.add_argument("--local", help="run the function locally",
                    action="store_true")
args = parser.parse_args()

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
        print(f'MDML RESULTS: {msg_obj["message"]}')
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

# # The function below was registered with funcx to get the above func_id
def sum_vars(params):
    import mdmlclient
    import mdml_client as mdml
    query = [{
        "device": "DEVICE_A",
        "variables": [],
        "last" : 1
    }]
    exp = mdml.experiment('TEST','test','testtest','merfpoc.egs.anl.gov')
    dat = exp.query(query)
    print(dat)
    row = dat['DEVICE_A'][0]
    var_sum = float(row['variable1']) + float(row['variable2']) + float(row['variable3']) + float(row['variable4']) + float(row['variable5'])
    return str(var_sum)

if args.register:
    from funcx.sdk.client import FuncXClient
    fxc = FuncXClient()
    funcx_func_uuid = fxc.register_function(sum_vars,
        description="Sum 5 variables")
    print(f'FuncX function UUID: {funcx_func_uuid}')
else:
    funcx_func_uuid = '2b5b472c-8f04-4dec-bc03-fe7ed0717cfa'

funcx_endp_id = "a62a830a-5cd1-42a8-a4a8-a44fa552c899" # merf.egs.anl.gov endpoint
# funcx_endp_id = "2895306b-569f-4ec9-815a-bcab73ea32f7" # 146.137.10.50 endpoint
# funcx_endp_id = "4b116d3c-1703-4f8f-9f6f-39921e5864df" # public tutorial endpoint

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
            time.sleep(.2)
            if args.local:
                # Run locally
                print(sum_vars({}))
                #Send message to FuncX directly
                # from funcx.sdk.client import FuncXClient
                # fxc = FuncXClient()
                # res = fxc.run({}, endpoint_id=funcx_endp_id, function_id=funcx_func_uuid)
                # print(res)
                # time.sleep(50)
                # print(fxc.get_result(res))
            else:
                # Send message to start analysis
                My_MDML_Exp.publish_analysis("ANALYSIS", funcx_func_uuid, funcx_endp_id)
            # Sleep to send data once a second
            i += 1
            time.sleep(.7)
        if not reset:
            time.sleep(10)
            print("Ending MDML experiment")
            My_MDML_Exp.reset()
            reset = True
except KeyboardInterrupt:
    if not reset:
        My_MDML_Exp.reset()
    My_MDML_Exp.stop_debugger()
