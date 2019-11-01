# MDML Client

Create a client to easily access the features of the Manufacturing Data & Machine Learning Layer (MDML).

## Installation
```bash
    pip install mdml_client
```

## Usage

### Experiment Class
  * Provides methods to connect to the MDML message broker, start an experiment, publish data, terminate an experiment, and receive updates from the MDML. Your experiment ID, username, password, and the MDML host IP will be provided by an admin.
  ```python
    import mdml_client as mdml

    # Create an MDML experiment
    My_MDML_Exp = mdml.experiment("EXPERIMENT_ID", "USERNAME", "PASSWORD", "HOST.IP.ADDRESS")

    # Start the debugger - prints messages from the MDML about your experiment
    My_MDML_Exp.start_debugger()

    # Validate and locally add a configuration to your experiment
    My_MDML_Exp.add_config({"See Configuration section for details"})

    # Send the configuration to the MDML
    My_MDML_Exp.send_config()

    # Creating example data to publish
    data = '1\t4\t30\t1630\t64\tExperiment running according to plan.'
    device_id = 'EXAMPLE_DEVICE'
    data_delimiter = '\t'
    use_influxDB = False

    # Appending unix time to the data string for more accurate time-keeping (see Time section)
    data = mdml.unix_time() + data_delimiter + data

    # Publishing data - do this as much and as often as required by your experiment
    My_MDML_Exp.publish_data(device_id, data, data_delimiter, use_influxDB)

    # After publishing everything, make sure to reset the MDML to end your experiment!
    My_MDML_Exp.reset()
  ```

## Important Notes

### Configuration
The configuration of an experiment serves as metadata for each device/sensor generating data and for the experiment itself. The configuration also allows the MDML to warn you if any bad data is being published. We highly recommend taking the time to craft a detailed configuration so that if used in the future, any researcher would be able to understand your experiment and its data.

The add_config() method of an experiment object can accept one of two things. A dictionary containing the configuration itself or a string of the path to a file containing the configuration. The file's contents will be run through json.loads() and must return a dictionary. 

Below is the experiment configuration used in the example above. Every key shown in the configuration's nested dictionaries is required of all configurations. More keys can be added as needed as long as the additional keys' names do not collide with required keys.

```python

config = {
    "experiment": {
        "experiment_id": "TEST",
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
            "device_data_type": "text/numeric",
            "device_notes": "Nothing to note here",
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

```

### Time
This package includes a helper function "unix_time()" which outputs the current unix time in nanoseconds. This can be used to append a timestamp to your data - like in the example above. In the experiment's configuration, the corresponding data header must be "time" which ensures that InfluxDB (MDML's time-series database) will use it properly. Without it, the timestamp will be created by InfluxDB and represent when the data was stored, not when the data was actually generated.