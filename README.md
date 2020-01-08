# MDML Client
[![Documentation Status](https://readthedocs.org/projects/mdml-client/badge/?version=latest)](https://mdml-client.readthedocs.io/en/latest/?badge=latest)

Create a client to easily access the features of the Manufacturing Data & Machine Learning Layer (MDML).

## Installation
```bash
    pip install mdml_client
```

## Documentation

Check out the [Read the Docs](https://mdml-client.readthedocs.io/en/latest/index.html)


## Basic Usage

There are several examples in the examples folder which demonstrate how to use the MDML. The `streaming_data.py` example is the best place to start. Each example file highlights a different feature of the MDML.  

The MDML's Python client uses a class named ```experiment``` to provides methods for connecting to the MDML message broker, starting an experiment, publishing data, running analyses, terminating an experiment, and receiving event notifications. 
  

<div id="config_syntax"></div>

### MDML Configuration Syntax


### Configuration Documentation
Every experiment run through the MDML needs to first have a configuration file. This serves to give the MDML context to your data and provide meaningful metadata for your experiments, processes, and data-generating devices. Information in the configuration file should answer questions that the data itself does not. Things like, what units are the data in, what kind of device generated the data, or was an analysis done before sending your data to the MDML? Providing as much information as possible not only increases the data's value for scientific purposes but also minimizes future confusion when you or another researcher want to use the data again.

The configuration of an experiment serves as metadata for each device/sensor generating data and for the experiment itself. The configuration also allows the MDML to warn you and prevent bad data from being published. We highly recommend taking the time to craft a detailed configuration so that if used in the future, any researcher will be able to understand your experiment and data.

The configuration file must be a [valid JSON file](https://en.wikipedia.org/wiki/JSON). It consist of two parts, an `experiment` section and a `devices` section. The experiment section is for general experiment notes and the list of devices and analyses that will generate data. The devices section contains an entry for each device or analysis listed in the experiment section. In each section, there are required fields and optional fields that control the MDML's behavior while streaming data. Furthermore, it is possible to create any additional fields you wish as long as the field's name is not already used by a required or optional field. Below is an in-depth description of the configuration file.


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

* store_results - true if the MDML should save the analysis results, false otherwise 
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
<div id="real_time_analysis"></div>

### Real-Time Analysis
The MDML enables real-time analysis of experimental data through the FuncX service. Analyses are described in the devices section of a configuration. An extra key, `store_results`, set to `true` can be added so that results are saved for immediate dashboard access or later retrieval in experiment files. If this option is used, the output of the function must be a tab-delimited string. The string's data must correspond to the headers listed in its device section.


-------------------------------
<div id="queries_syntax"></div>

### MDML Query Syntax
This query syntax is used to specify what data should be sent to a FuncX analysis function. Using this syntax, the MDML builds and executes queries for InfluxDB to gather all data that neeeds to be sent to the FuncX function. For each device to be queried, a dictionary should be created with the following keys:
* device - value is the device ID specified in the configuration
* variables - value is a list of variables to be sent to FuncX. An empty list will grab all variables for the given device
* last - value is the number of lines to return (most recent lines)
* where (optional) - value is an object. Keys are the variable name and the corresponding value is what the variable should equal in the query
* time_start (optional) - value is an integer for the unix time in nanoseconds where the returned points should start 
* time_end (optional) - value is an integer for the unix time in nanoseconds where the returned points should stop
 
Below is an example of the syntax. This structure is supplied to the `queries` parameter of the .publish_analysis() method. 

```json
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

If a device that you would like to query produces images, only use the keys: device, variables, and last. The variables value should be an empty list. Support for the other keys is on the way. 

```json
{
  "device": "IMAGE_DEVICE",
  "variables": [],
  "last": 5
}

-------------------------------
<div id="funcx_payload"></div>

### MDML-to-FuncX Payload Syntax
When running a real-time analysis with the MDML, data is fed to FuncX using the above query syntax. This section will help you understand the structure of the data returned by a query. This understanding is essential in order to properly write a function for FuncX that will be compatible with the MDML. 

In short, the structure of the data that is returned from an MDML query is an object where the keys correspond to device IDs that were queried and the values are lists of objects for each point - sorted newest to oldest. Each object contains the queried variables as well as the timestamp (Unix time in nanoseconds) for that entry. The example below will help visualize this.

A query like this...
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

will return a data structure like this...
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


