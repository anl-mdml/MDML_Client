# MDML Client

Create a client to easily access the features of the Manufacturing Data & Machine Learning Layer (MDML).

## Installation
```bash
    pip install mdml_client
```

## Usage
There are two classes in this package: experiment and debugger.

### Experiment
  * Provides the functionality to connect to the MDML messsage broker, start an experiment, publish data, and terminate an experiment.
  ```python
    import mdml_client as mdml

    # Experiment ID - supplied by MDML admins for your project
    Exp_ID = 'TEST'
    
    # MDML message broker host
    host = 'Some.IP.Address'
    
    # Variable for the experiment's configutation
    config = {"See configuration section for details"}
    
    # Create an MDML experiment
    My_MDML_Exp = mdml.experiment(Exp_ID, host)

    # Add and validate a configuration for the experiment
    My_MDML_Exp.add_config(config)

    # Send the configuration to the MDML
    My_MDML_Exp.send_config()

    # Creating example data to publish
    data = '1\t4\t30\t1630\t64\tExperiment running according to plan.'
    device_id = 'EXAMPLE_DEVICE'
    data_delimiter = '\t'

    # Appending unix time to data for InfluxDB (see unix_time() section)
    data = mdml.unix_time() + data_delimiter + data 

    # Publishing data
    My_MDML_Exp.publish_data(device_id, data, data_delimiter)

    # Reset MDML to end the experiment! VERY IMPORTANT!!!
    My_MDML_Exp.reset()
  ```
### Debugger
* Listens to the MDML message broker for any updates pertaining to your experiment (error, status, etc).
    ```python
    import mdml_client as mdml
    
    # Create a subscriber on the MDML message broker to receive events while using MDML  
    mdml.debugger(Exp_ID, host)
    ```

## Important Notes

### Configuration
The configuration of an experiment serves as metadata for each device/sensor generating data and for the experiment itself. The configuration also gives the MDML an idea of what data to expect in order to warn you if bad data is being published. We highly recommend taking the time to craft a detailed configuration so that your experiment and its data can be easily understood and reused by someone else if needed.

### Time
This package includes a helper function "unix_time()". It outputs the current unix time in nanoseconds. This can be used to append a timestamp to your data - like in the example above. In the experiment's configuration, the corresponding data header must be "time" which ensures that InfluxDB (MDML's time-series database) will use it properly. Without it, the timestamp used will represent when the data was saved in InfluxDB, not when the data was actually generated.s