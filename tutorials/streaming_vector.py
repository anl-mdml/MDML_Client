import sys # using local mdml_client
sys.path.insert(1, '../')

import time
import mdml_client as mdml

exp = mdml.experiment("TEST", "test", "testtest", "merfpoc.egs.anl.gov")
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
