import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--register", help="register the function with FuncX",
			action="store_true")
parser.add_argument("--run",
			choices=['local', 'funcx', 'mdml'],
			help="Run the function in 1 of three ways: 'local', 'funcx', or 'mdml'.")
args = parser.parse_args()

### FIRST, YOUR FUNCX FUNCTION MUST BE CREATED AND THEN REGISTERED. ###
def basic_analysis(params):
    import mdml_client as mdml
    # Connect to the MDML to query for data
    exp = mdml.experiment(params['experiment_id'], params['user'], params['pass'], params['host'])
    # Grabbing the latest temperature value
    query = [{
        "device": "DATA1",
        "variables": ["temperature"],
        "last": 1
    }]
    res = exp.query(query, verify_cert=False) # Running the query
    tempF = res['DATA1'][0]['temperature']
    tempC = (tempF-32)*(5/9)
    return {'time': mdml.unix_time(True), 'tempC': tempC}

# Registering the function
if args.register:
	from funcx.sdk.client import FuncXClient
	fxc = FuncXClient()
	funcx_func_uuid = fxc.register_function(basic_analysis,
		description="Temperature conversion")
	print(f'funcX UUID: {funcx_func_uuid}')
else: # Use the most recent function funcx ID (manually put here after running --register once)
	funcx_func_uuid = "1712a2fc-cc40-4b2c-ae44-405d58f78c5d" # Sept 16th 2020


# Now that the function is ready for use, we need to start an experiment to use it with
import sys
import time 
sys.path.insert(1, '../') # using local mdml_client
import mdml_client as mdml

exp = mdml.experiment("TEST", "test", "testtest", "merfpoc.egs.anl.gov")
exp.add_config(auto=True)
exp.send_config()
time.sleep(1)

# Send some data to analyze 
data = {'time': mdml.unix_time(True), 'temperature': 212, 'humidity':58, 'note': 'Temperature and humidity values'}
exp.publish_data(device_id = "DATA1", data = data, add_device = True)

# Now, run the analysis
# Define function parameters 
params = {
	'experiment_id': 'TEST',
	'user': 'test',
  	'pass': 'testtest',
	'host': 'merfpoc.egs.anl.gov'
    # Other function specific params here.
}
funcx_endp_id = "a62a830a-5cd1-42a8-a4a8-a44fa552c899" # merf.egs.anl.gov endpoint


# Running the function locally only
if args.run == 'local':
	basic_analysis(params)

# Use funcX to run the function
elif args.run == 'funcx':
	if not args.register:
		from funcx.sdk.client import FuncXClient
		fxc = FuncXClient()
	task_id = fxc.run(params, endpoint_id=funcx_endp_id, function_id=funcx_func_uuid)
	import time
	print("Sleeping for the computation.")
	time.sleep(2)
	result = fxc.get_result(task_id)
	print(result)

# Use funcX through MDML to run the function
elif args.run == 'mdml':
	exp.globus_login()
	exp.publish_analysis('TEMP_ANALYSIS', funcx_func_uuid, funcx_endp_id, params, trigger=['DATA1']) # Change the analysis device ID
	time.sleep(4)
	data = {'time': mdml.unix_time(True), 'temperature': 32, 'humidity':59, 'note': 'Temperature and humidity values'}
	exp.publish_data(device_id = "DATA1", data = data)

try:
    print("Entering infinite loop... Crtl+C to exit and end the experiment.")
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    print("Quitting...")
finally:
    print("Reseting...")
    exp.reset()
    time.sleep(1)
    print("Disconnecting...")
    exp.disconnect()