import time
import json
import mdml_client as mdml # pip install mdml_client #

# Approved experiment ID (supplied by MDML administrators - will not work otherwise)
Exp_ID = 'TEST'
# MDML message broker host
host = '146.137.10.50'
#host = '127.0.0.1'
# MDML username and password
username = 'test'
password = 'testtest'


# Create a configuration for your experiment
config = {
    "experiment": {
        "experiment_id": "TEST",
        "experiment_notes": "example.py file for MDML python package",
        "experiment_devices": ["OES_VECTOR", "DEVICE_J"]
    },
    "devices": [
        {
            "device_id": "OES_VECTOR",
            "device_name": "Wavelength/Intensity Vectors",
            "device_output": "spectrometry data of wavelengths and corresponding intensities",
            "device_output_rate": 0.5,
            "device_data_type": "vector",
            "device_notes": "wavelengths may vary",
            "headers" : [
                "wavelength",
                "intensity"
            ],
            "data_types" : [
                "numeric",
                "numeric"
            ],
            "data_units" : [
                "nanometers",
                "intensity"
            ],
            "influx_tags": ["wavelength"]
        },
        {
            "device_id": "DEVICE_J",
            "device_name": "Test device",
            "device_version": "1",
            "device_output": "Random data for testing",
            "device_output_rate": 0.1,
            "device_data_type": "text/numeric",
            "device_notes": "Nothing here",
            "headers": [
                "time",
                "row",
                "experimentor_id",
                "Temperature 1",
                "Temperature 2",
                "Temperature 3",
                "Note"
            ],
            "data_types": [
                "int",
                "int",
                "int",
                "int",
                "int",
                "int",
                "string"
            ],
            "data_units": [
                "nanoseconds",
                "count",
                "NA",
                "degrees C",
                "degrees C",
                "degrees C",
                "text"
            ],
            "save_tsv": True
        }
    ]
}

# Create MDML experiment
My_MDML_Exp = mdml.experiment(Exp_ID, username, password, host)

# Login with Globus
My_MDML_Exp.globus_login()

# Receive events about your experiment from MDML
My_MDML_Exp.start_debugger()

# Sleep to let debugger thread set up
time.sleep(1)

# Add and validate a configuration for the experiment
My_MDML_Exp.add_config(config, 'funcx_test')
# You can also input a filepath to a file containing the configuration
# The contents of the file must be a dict after json.loads()

# Send configuration file to the MDML
My_MDML_Exp.send_config() # this starts the experiment


# Creating example data to publish
data = '1\t4\t30\t1630\t64\tExperiment running according to plan.'
device_id = 'DEVICE_J' # Should match one of the devices in config file
data_delimiter = '\t'
use_influxdb = True#False#
# Appending unix time to data for InfluxDB
data = mdml.unix_time() + data_delimiter + data
# Publishing data - do this as much and as often as required by your experiment
My_MDML_Exp.publish_data(device_id,\
    data,\
    data_delimiter,\
    use_influxdb)


# Gather data to send
with open('new_oes.json') as nf:
    dat = json.loads(nf.read())
dat['wavelength'] = dat['wavelength'].split('\t')
dat['intensity'] = dat['intensity'].split('\t')
times = mdml.unix_time()
print(times)
# Send data to MDML
My_MDML_Exp.publish_vector_data('OES_VECTOR', dat, times)


# Creating funcx analysis
queries = [
    { # SELECT intensity, wavelength FROM TEST_VECTOR1 WHERE (wavelength <= 250.1 ) GROUP BY wavelength_tag order by time desc limit 2;
        "device": "OES_VECTOR",
        "variables": ["intensity", "wavelength"],
        "last": 1
    },
    { # SELECT * FROM TEST_DEVICE_J ORDER BY time DESC LIMIT 1;
        "device": "DEVICE_J",
        "variables": [],
        "last" : 2
    }
]
# FuncX endpoint id and function id
funcx_endp_id = "a5c5f716-610e-40e1-9b9c-05c4b9a0a102"
funcx_func_id = "ca4ca1a5-abb1-49b1-a5c8-8ce4d3db4138"
# Send message to start analysis
My_MDML_Exp.publish_analysis(queries, funcx_func_id, funcx_endp_id)


time.sleep(10)
# Make sure to reset the MDML to end your experiment!
My_MDML_Exp.reset()
