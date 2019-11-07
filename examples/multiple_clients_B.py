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
print("*** Press Ctrl+C to stop sending data.                                 ***")
print("*** Press Ctrl+C again to stop the example.                            ***")
print("**************************************************************************")
time.sleep(5)

# Approved experiment ID (supplied by MDML administrators - will not work otherwise)
Exp_ID = 'TEST'
# MDML message broker host
host = 'merf.egs.anl.gov'
# MDML username and password
username = 'test'
password = 'testtest'

# Connect to MDML
My_MDML_Exp = mdml.experiment(Exp_ID, username, password, host)

# Generate random data
def random_data(size):
    dat = []
    for _ in range(size):
        dat.append(str(random.random()))
    return dat

try:
    while True:
        # Create random data
        deviceB_data = '\t'.join(random_data(3))
        
        # Send data        
        My_MDML_Exp.publish_data('CLIENT_B', deviceB_data, '\t', influxDB=True)

        # Sleep to publish data once a second
        time.sleep(1)
except KeyboardInterrupt:
    print("Stop sending CLIENT_B data")
