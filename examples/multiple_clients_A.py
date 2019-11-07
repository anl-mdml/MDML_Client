##################################  READ ME  ##################################
# When running multiple clients, it is important that all data streaming      #
# happens between the configuration message and the reset message. Any data   #
# messages sent to the MDML before a configuration file is received will be   #
# lost. The same goes for data messages sent after an experiment's reset      #
# message. It is recommended that only one client be responsible for sending  #
# the configuration and reset messages. In this example, that client is the   #
# one in multiple_clients_A.py. This means that multiple_clients_B.py should  #
# should start after and end before multiple_clients_A.py. Any additional     #
# clients created will have the same limitations to ensure data is not lost.  #
# There is no limit to the number of clients streaming data.                  #
###############################################################################

import time
import json
import random
import mdml_client as mdml # pip install mdml_client #

print("**************************************************************************")
print("*** This example will run indefinitely.                                ***")
print("*** Remember to stop all other clients first!                          ***")
print("*** Press Ctrl+C to stop sending data and send the MDML reset message. ***")
print("*** Press Ctrl+C again to stop the debugger and end the example.       ***")
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
        "experiment_devices": ["CLIENT_A", "CLIENT_B"]
    },
    "devices": [
        {
            "device_id": "CLIENT_A",
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
            "device_id": "CLIENT_B",
            "device_name": "Test device B",
            "device_output": "random data",
            "device_output_rate": 1, # in Hertz
            "device_data_type": "text/numeric",
            "device_notes": "Random data generated and streamed for example purposes",
            "headers" : [
                "variable1",
                "variable2",
                "variable3"
            ],
            "data_types" : [
                "numeric",
                "numeric",
                "numeric"
            ],
            "data_units" : [
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

# Add and validate a configuration for the experiment
My_MDML_Exp.add_config(config, 'multiple_clients_example')

# Send configuration file to the MDML
My_MDML_Exp.send_config() # this starts the experiment

# Sleep to let MDML ingest the configuration
time.sleep(2)

def random_data(size):
    dat = []
    for _ in range(size):
        dat.append(str(random.random()))
    return dat

try:
    while True:
        # Create random data
        deviceA_data = '\t'.join(random_data(5))
        
        # Send data
        My_MDML_Exp.publish_data('CLIENT_A', deviceA_data, '\t', influxDB=True)

        # Sleep to publish data once a second
        time.sleep(1)
except KeyboardInterrupt:
    print("Ending MDML experiment")
finally:
    My_MDML_Exp.reset()
