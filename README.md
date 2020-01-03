# MDML Client
[![Documentation Status](https://readthedocs.org/projects/mdml-client/badge/?version=latest)](https://mdml-client.readthedocs.io/en/latest/?badge=latest)

Create a client to easily access the features of the Manufacturing Data & Machine Learning Layer (MDML).

## Installation
```bash
    pip install mdml_client
```

## Documentation

Check out our [Read the Docs](https://mdml-client.readthedocs.io/en/latest/index.html)


## Basic Usage

  * The MDML client uses a class named ```experiment``` that provides methods for connecting to the MDML message broker, starting an experiment, publishing data, running analyses, terminating an experiment, and receiving event notifications. Below is a simplified example of using the MDML. 
  ```python
    import mdml_client as mdml

    # Create an MDML experiment
    My_MDML_Exp = mdml.experiment("EXPERIMENT_ID", "USERNAME", "PASSWORD", "HOST.IP.ADDRESS")

    # Start the debugger - prints messages from the MDML about your experiment
    def user_func(msg):
        print("MDML MESSAGE: "+ msg)
    My_MDML_Exp.set_debug_callback(user_func)
    My_MDML_Exp.start_debugger()

    # Login to allow FuncX usage. A link will be printed in the console window for authentication. 
    My_MDML_Exp.globus_login()
    # This method logs the user in using Globus' authentication. It is only 
    # required if FuncX analyses will be run. Upon running, a link will be 
    # printed in the console window. Clicking it will open a web browser where
    # you will log in to your globus account and be provided a token. Copy and
    # paste this token in the console window to finish the login.

    # Validate and locally add a configuration to your experiment
    My_MDML_Exp.add_config({"Your configuration file here"}, "optional_run_id")
    # This method adds your configuration file to your experiment object - it 
    # has not been sent to the MDML yet. The config parameter is explained in 
    # detail below. The second parameter is the run ID for the experiment 
    # about to be started. A valid run ID can only contain letters and 
    # underscores. Reusing a previous run ID will treat the data as if it came
    # from the past experiment regardless of the time elapsed - data files will
    # be appended to where they left off.
  

    # Send the configuration to the MDML
    My_MDML_Exp.send_config()
    # This message sends the configuration you added with .add_config() to the
    # MDML. If a debugger has been started, you should see a message regarding
    # the configuration.

    # Publishing data - do this as much and as often as required by your experiment
    My_MDML_Exp.publish_data(device_id, data, data_delimiter, use_influxDB)

    # Analyze data
    My_MDML_Exp.publish_analysis(queries, function_uuid, endpoint_uuid)
    # A description of the data to send to the FuncX function using the <a href="#queries_syntax">syntax below</a>

    # Make sure to reset the MDML to end your experiment!
    My_MDML_Exp.reset()
    # This method must be called in order to end an experiment. A message
    # is sent to the MDML backend that finishes sending data messages and 
    # begins archiving all data files for storage. 
  ```


<div id="config_syntax"></div>

### MDML Configuration Syntax


### Configuration Documentation
Every experiment run through the MDML needs to first have a configuration file. This serves to give the MDML context to your data and provide meaningful metadata for your experiments, processes, and data-generating devices. Information in the configuration file should answer questions that the data itself does not. Things like, what units are the data in, what kind of device generated the data, or was an analysis done before sending your data to the MDML? Providing as much information as possible not only increases the data's value for scientific purposes but also minimizes future confusion when you or another researcher want to use the data.

The configuration of an experiment serves as metadata for each device/sensor generating data and for the experiment itself. The configuration also allows the MDML to warn you and prevent any bad data from being published. We highly recommend taking the time to craft a detailed configuration so that if used in the future, any researcher would be able to understand your experiment and data.

The configuration file must be a [valid JSON file](https://en.wikipedia.org/wiki/JSON). It consist of two parts, an `experiment` section and a `devices` section. The experiment section is for general experiment notes and the list of devices that will generate data. The devices section contains an entry for each device listed in the experiment section. In each section, there are required fields and optional fields that control the MDML's behavior while streaming data. Furthermore, it is possible to create any additional fields you wish as long as the field's name is not already used by a required or optional field. Below is an in depth description of the configuration file.


###     Experiment Section

#### Required Fields: 
     
* experiment_id
    - Experiment ID provided by the MDML administrators
* experiment_notes
    - Any important notes about your experiment that you would like to remain with the data 
* experiment_devices
    - A list of devices that will be generating and sending data. These will be described in the `Devices` section

#### Optional Fields:

* experiment_run_id
    * Experiment run ID (Defaults to 1 and increases for each new experiment) Will be added automatically if the second parameter in .add_config() is specified. This is different than `experiment_id`.

###     Devices Section

#### Required Fields:

* device_id
    * Identification string for the device. __MUST__ match a device listed in the experiment section
* device_name
    * Full name of the device
* device_output
    * Explanation of what data the device is outputting
* device\_output\_rate
    * The rate (in hertz) that this sensor will be generating data (If the rate during your experiment may vary, please use the fastest rate)
* device\_data\_type
    * Type of data being generated. Must be "text/numeric", "vector", or "image"
* device_notes
    * Any other relevant information to provide that has not been listed
* headers
    * A list of headers to describe the data that will be sent
* data_types
    * A list of data types for each value (__MUST__ correspond to the `headers` field)
* data_units
    * A list of the units for each value (__MUST__ correspond to the `headers` field)

#### Optional Fields:

* melt_data - Contains more data on how to melt the data (see the melting data section below) 
    * keep
        * List of variables to keep the same (must have been listed in the `headers` field)
    * var_name
        * Name of the new variable that is created with all the values from headers that are not included in `keep`
    * var_val
        * Name of the new variable that is created with the values corresponding to the original headers 
* influx_tags
    * List of variables that should be used as tags - __MUST__ correspond to values in the `headers` field (Tags are specific to InfluxDB. See the Software Stack section below for details.)


###     Experiment Configuration Example
```
{
    "experiment": {
      "experiment_id": "FSP",
      "experiment_notes": "Flame Spray Pyrolysis Experiment",
      "experiment_devices": [
        "OES",
        "DATA_LOG",
        "PLIF"
      ]
    },
    "devices": [
      {
        "device_id": "OES",
        "device_name": "ANDOR Kymera328",
        "device_output": "2048 intensity values in the 250-700nm wavelength range",
        "device_output_rate": 0.01,
        "device_data_type": "text/numeric",
        "device_notes": "Points directly at the flame in 8 different locations",
        "headers": [
          "time",
          "Date",
          "Channel",
          "188.06",
          "188.53"
        ],
        "data_types": [
          "time",
          "date",
          "numeric",
          "numeric",
          "numeric"
        ],
        "data_units": [
          "nanoseconds",
          "date",
          "number",
          "dBm/nm",
          "dBm/nm"
        ],
        "melt_data": {
          "keep": [
            "time",
            "Date",
            "Channel",
          ],
          "var_name": "wavelength",
          "var_val": "intensity"
        },
        "influx_tags": ["Channel", "wavelength"]
      },
      {
        "device_id": "DATA_LOG",
        "device_name": "ANDOR Kymera328",
        "device_output": "2048 intensity values in the 250-700nm wavelength range",
        "device_output_rate": 0.9,
        "device_data_type": "text/numeric",
        "device_notes": "Points directly at the flame in 8 different locations",
        "headers": [
          "time",
          "Sample #",
          "Date",
          "SOL#",
          "Vol remaining [ml]",
          "Exhaust Flow",
          "Pressure"
        ],
        "data_types": [
          "time",
          "numeric",
          "date",
          "numeric",
          "numeric",
          "numeric",
          "numeric"
        ],
        "data_units": [
          "nanoseconds",
          "number",
          "date",
          "number",
          "milliliters",
          "liters/hour",
          "atm"
        ]
      },
      {
        "device_id": "PLIF",
        "device_name": "Planar Laser Induced Fluorescence",
        "device_output": "Image of flames showing specific excited species.",
        "device_output_rate": 10,
        "device_data_type": "image",
        "device_notes": "Points down, directly at the flame",
        "headers": [
          "PLIF"
        ],
        "data_types": [
          "image"
        ],
        "data_units": [
          "image"
        ]
      }
    ]
  }
```


-------------------------------
<div id="queries_syntax"></div>

### MDML Query Syntax
The query syntax is used to specify what data should be sent to a FuncX function. Using this syntax, the MDML builds and executes queries for InfluxDB to gather all data that neeeds to be sent to the FuncX function. For each device to be queried, an dictionary should be created with the following three keys:
* device - value is the device ID specified in the configuration
* variables - value is a list of variables to be sent to FuncX. An empty list will grab all variables for the given device
* last - value is the number of lines to return (most recent lines)
* where - value is an object. Keys are the variable name and the corresponding value is what the variable should equal in the query
* time_start - value is an integer for the unix time in nanoseconds where the returned points should start 
* time_end - value is an integer for the unix time in nanoseconds where the returned points should stop
 
Below is an example of the syntax.

```
[
  {
    "device": "OES_VECTOR",
    "variables": ["intensity", "wavelength"],
    "last": 1
    "where": {
      "wavelength": 250
    }
  },
  {
    "device": "DEVICE_J",
    "variables": [],
    "last" : 2
    "time_start": 1577426400000000000,
    "time_end": 1577426500000000000

  }
]
```

```
{
  "OES_VECTOR": [
    {row 1}
  ],
  "DEVICE_J": [
    {row 2}
  ]
}
```
-------------------------------
<div id="funcx_payload"></div>

### MDML-to-FuncX Payload Syntax
When running a real-time analysis with the MDML, data is fed to FuncX using the above query syntax. This section will help you understand the structure of the data returned by a query. This understanding is needed in order to properly write a function to use with FuncX. 

In short, the structure of the data that is returned from a query is an object where the keys correspond to device IDs that were queried and the values are lists containing objects for each point sorted newest to oldest. 

For example, a query like:
```json
[
  {
    "device": "OES_VECTOR",
    "variables": ["intensity", "wavelength"],
    "last": 1,
    "where": {
      "wavelength": 250
    }
  },
  {
    "device": "DEVICE_J",
    "variables": [],
    "last" : 2,
    "time_end": 1577426558192850000

  }
]
```

Would return a data structure like:
```json
{
  "OES_VECTOR": [
    {"time": 1577426558193450000, "intensity": 1023, "wavelength": 250}
  ],
  "DEVICE_J": [
    {"time": 1577426558192850000, "variable1": 0.48275, "variable2": 0.49021, ...},
    {"time": 1577426558192830000, "variable1": 0.90321, "variable2": 0.98281, ...}
  ]
}

```

Things to Note:
* The list for Device J contains 2 elements because the query parameter `last` was set to 2.
* The `...` in the Device J data points are to inllustrate that all variables have been returned since the `variables` query parameter was set to `[]`