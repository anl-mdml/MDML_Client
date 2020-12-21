# Parameters to run the example
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--host", help="MDML instance host",
                        required=True)
parser.add_argument("--username", help="MDML username",
                        required=True)
parser.add_argument("--password", help="MDML password",
                        required=True)
args = parser.parse_args()

import time
import mdml_client as mdml

exp = mdml.experiment("TEST", args.username, args.password, args.host)
exp.add_config(auto=True)
exp.send_config()
time.sleep(1)

curr_time = mdml.unix_time(True) # True for integer return instead of string
print(curr_time)

# Data type of dict required for the first .publish_data call when using auto configurations. 
data = {
    'wavelength': [300, 400, 500, 600],
    'intensity': [1200, 1500, 2100, 1750]
}
exp.publish_vector_data(device_id = "VECTOR", data = data, timestamp = curr_time+2, add_device = True, tags=["wavelength"])

try:
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    print("Quitting")
finally:
    exp.reset()
    time.sleep(1)
    exp.disconnect()
