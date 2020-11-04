import sys # using local mdml_client
sys.path.insert(1, '../')

import time
import mdml_client as mdml

exp = mdml.experiment("TEST", "test", "testtest", "merf.egs.anl.gov")
exp.add_config(auto=True)
exp.send_config()
time.sleep(1)

curr_time = mdml.unix_time(True) # True for integer return instead of string
print(curr_time)

# Data type of dict required for the first .publish_data call when using auto configurations. 
data = {'time': mdml.unix_time(True), 'temperature': 56, 'humidity':58, 'note': 'Temperature and humidity values'}
exp.publish_data(device_id = "DATA1", data = data, timestamp = curr_time+2, add_device = True)

time.sleep(3)

data = [mdml.unix_time(True), 57, 59, 'Temperature and humidity values']
exp.publish_data(device_id = "DATA1", data = data, timestamp = curr_time+2)

time.sleep(3)

data = f'{mdml.unix_time(True)}\t54\t61\tTemperature and humidity values'
exp.publish_data(device_id = "DATA1", data = data, data_delimiter='\t', timestamp = curr_time+2)

try:
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    print("Quitting")
finally:
    exp.reset()
    time.sleep(1)
    exp.disconnect()
