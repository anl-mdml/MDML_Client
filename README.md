# MDML Client

Create a client to easily access the features of the Manufacturing Data & Machine Learning Layer (MDML).

## Installation
```bash
    pip install mdml_client
```

## Usage

  * The MDML client uses a class named ```experiment``` that provides methods for connecting to the MDML message broker, starting an experiment, publishing data, running analyses, terminating an experiment, and receiving event notifications. Below is an example of a standard use case for the MDML. 
  ```python
    import mdml_client as mdml

    # Create an MDML experiment
    My_MDML_Exp = mdml.experiment("EXPERIMENT_ID", "USERNAME", "PASSWORD", "HOST.IP.ADDRESS")

    # Start the debugger - prints messages from the MDML about your experiment
    My_MDML_Exp.start_debugger()

    # Login to allow FuncX usage. A link will be printed in the console window for authentication. 
    My_MDML_Exp.globus_login()

    # Validate and locally add a configuration to your experiment
    My_MDML_Exp.add_config({"Your configuration file here"}, "optional_run_id")

    # Send the configuration to the MDML
    My_MDML_Exp.send_config()

    # Publishing data - do this as much and as often as required by your experiment
    My_MDML_Exp.publish_data(device_id, data, data_delimiter, use_influxDB)

    # Analyze data
    My_MDML_Exp.publish_analysis(queries, function_uuid, endpoint_uuid)

    # Make sure to reset the MDML to end your experiment!
    My_MDML_Exp.reset()
  ```

## Documentation

-------------------------------
  ```python 
  My_MDML_Exp = mdml.experiment(experiment_id, username, passwd, host)
  ```
  Parameters:   
  * experiment_id (str) - MDML experiment ID, given to you by an MDML admin
  * username (str) - MDML username
  * passwd (str) - MDML password
  * host (str) - IP address of the MDML host, given to you by an MDML admin
  
  Returns - experiment object 

  This is the first step in interacting with the MDML. ```mdml.experiment()``` creates an experiment object through which methods for interacting with the MDML are accessed. This line also creates a connection to the MDML that will be used later. All input parameters specified here will be given to you by an MDML admin. From here on out, all methods should be called on the variable created by ```mdml.experiment()``` - in this case `My_MDML_Exp`. Since it is possible that your experiment may need to send data from multiple different places, multiple connections to the MDML can be made with this line of code.


-------------------------------
  ```python
  My_MDML_Exp.start_debugger()
  ```
  Starting the debugger allows you to receive event notifications from the MDML. These notifications will be automatically printed to the console window.

-------------------------------
  ```python
  My_MDML_Exp.globus_login()
  ```
  This method logs the user in using Globus' authentication. It is only required if FuncX analyses will be run. Upon running, a link will be printed in the console window. Clicking it will open a web browser where you will log in to your globus account and be provided a token. Copy and paste this token in the console window to finish the login.

-------------------------------
  ```python
  My_MDML_Exp.add_config(config, experiment_run_id)
  ```
  Parameters:
  * config (str | dict) - string to a file path or a dict containing the configuration
  * experiment_run_id (str) - string of only letters and underscores to identify the experiment run
  
  This method adds your configuration file to your experiment object - it has not been sent to the MDML yet. The config parameter is explained in detail below. The second parameter is the run ID for the experiment about to be started. A valid run ID can only contain letters and underscores. Reusing a previous run ID will treat the data as if it came from the past experiment regardless of the time elapsed - data files will be appended to where they left off.
  

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
```python
My_MDML_Exp.send_config()
```
This message sends the configuration you added with .add_config() to the MDML. If a debugger has been started, you should see a message regarding the configuration.


    # Send the configuration to the MDML
    My_MDML_Exp.send_config()

    # Publishing data - do this as much and as often as required by your experiment
    My_MDML_Exp.publish_data(device_id, data, data_delimiter, use_influxDB)

    # Analyze data
    My_MDML_Exp.publish_analysis(queries, function_uuid, endpoint_uuid)

    # Make sure to reset the MDML to end your experiment!
    My_MDML_Exp.reset()

-------------------------------
  ```python
  My_MDML_Exp.publish_data(device_id, data, data_delimiter='null', influxDB = False)
  ```
  Parameters:
  * device_id (str) - device id string that corresponds to a device in the configuration file
  * data (str) - delimited string of data
  * data_delimiter (str) - delimiter used in the data parameter
  * influxDB (bool) - true if data should be stored in InfluxDB, false otherwise

-------------------------------
  ```python
  My_MDML_Exp.publish_analysis(device_id, img_byte_string, timestamp = 0)
  ```
  Parameters:
  * device_id (str) - device id string that corresponds to a device in the configuration file
  * img_byte_string (str) - string of bytes for the image (output from mdml_client.read_image())
  * timestamp (str) - unix time in nanseconds when the photo was taken 
  

-------------------------------
```python
My_MDML_Exp.reset()
```
This method must be called in order to end an experiment. A message is sent to the MDML backend that finishes sending data messages and begins archiving all data files for storage. 

-------------------------------

### Time
This package includes a helper function "unix_time()" which outputs the current unix time in nanoseconds. This can be used to append a timestamp to your data - like in the example above. In the experiment's configuration, the corresponding data header must be "time" which ensures that InfluxDB (MDML's time-series database) will use it properly. Without it, the timestamp will be created by InfluxDB and represent when the data was stored, not when the data was actually generated.
