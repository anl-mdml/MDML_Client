from MDML_client import MDML_experiment, MDML_debugger

# Approved experiment ID (supplied by MDML administrators - will not work otherwise)
Exp_ID = 'TEST'

# Create a subscriber on the MDML message broker to receive events while using MDML  
MDML_debugger(Exp_ID)

# Create a configuration for your experiment
config = {
    "experiment": {
        "experiment_id": "TEST",
        "experiment_number": "2",
        "experiment_notes": "example.py file for MDML python package",
        "experiment_devices": ["DEVICE_J"]
    },
    "devices": [
        {
            "device_id": "DEVICE_J",
            "device_name": "Test device",
            "device_version": "1",
            "device_output": "Random data for testing",
            "device_output_rate": 0.1,
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
My_MDML_Exp = MDML_experiment(Exp_ID, config)

# You can also input a filepath to a file containing the configuration
# The contents of the file must be a dict after json.loads()
### My_MDML_Exp = MDML_experiment(Exp_ID, './test_config.json')


# Validate configuration file
My_MDML_Exp.validate_config()

# Send configuration file
My_MDML_Exp.send_config() # this starts the experiment

# Creating data to send
data = '1\t4\t30\t1430\t64\tExperiment running according to plan.'
device_id = 'DEVICE_J' # Should match one of the devices in config file
data_delimiter = '\t'
influx_measurement = True#False#
# Appending unix time to data for InfluxDB
data = My_MDML_Exp.unix_time() + data_delimiter + data 

# Publishing data
My_MDML_Exp.publish_data(data,\
    device_id,\
    data_delimiter,\
    influx_measurement)

# Reset MDML to end the experiment! VERY IMPORTANT!!!
My_MDML_Exp.reset()