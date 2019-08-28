import json
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import time
from threading import Thread


def on_message(client, userdata, message):
    print("******************************************************************************")
    print("******************************** MDML MESSAGE ********************************")
    print("******************************************************************************\n")
    print("%s %s" % (message.topic, message.payload))
    print()


class MDML_debugger:
    """
    This class allows users to retrieve error messages or other important 
    events when running an experiment with the MDML.

    """

    def __init__(self, experiment_id, host, port=1883):
        """
        Init an MDML debugger

        ...

        Parameters
        ----------
        experiment_id : str
            MDML experiment ID, this should have been given to you by an administrator
        host : str
            string for the MDML host running the MQTT message broker
        port : int
            port number used by the MDML MQTT message broker (default is 1883)
        """

        debug = Thread(target=subscribe.callback,\
            kwargs={\
                'callback': on_message,\
                'topics': "MDML_DEBUG/" + experiment_id.upper(),\
                'hostname':host\
            })
        debug.setDaemon(False)
        debug.start()


class MDML_experiment:
    """
    This class allows users to run an experiment with the MDML.
    This includes submiting a configuration, sending data and stopping 
    the experiment. Also included are some basic helper methods: 
    unix_time() provides the unix time in nanoseconds (MDML's InfluxDB 
    needs it this way). 
    ...

    Attributes
    ----------
    device_config : dict
        example configuration for one device. Used in the  
        'devices' array of the experiment's config file
    configuration : dict
        example configuration dictionary used for an experiment

    Methods
    -------
    validate_config()
        Validates that nothing is wrong with the supplied config file
    send_config()
        Sends the configuration dict to MDML to start an experiment
    publish_data(data, device_id, data_delimiter='null', influx_measurement=False)
        Publishes data to the MDML message broker
    reset()
        Sends a reset message to MDML to trigger the end of an experiment
    unix_time()
        Gets unix time in nanoseconds 
    """

    device_config = {
        'device_id': 'INSERT device_id',
        'device_name': 'INSERT device_name',
        'device_version': 'INSERT device_version',
        'device_output': 'INSERT device_output',
        'device_output_rate': 0,
        'device_notes': 'INSERT device_notes',
        'headers': ['INSERT headers ARRAY'],
        'data_types': ['INSERT data_types ARRAY'],
        'data_units': ['INSERT data_units ARRAY'],
        'save_tsv': True
    }
    configuration = {
        'experiment': {
            'experiment_id': 'INSERT APPROVED experiment_id',
            'experiment_notes': 'INSERT experiment_notes',
            'experiment_devices': ['INSERT ARRAY CORRESPONDING TO EACH device_id IN devices']
        },
        'devices': [
            device_config,
            device_config
        ]
    }

    def __init__(self, experiment_id, config, host, port):
        """
        Init an MDML experiment

        ...

        Parameters
        ----------
        experiment_id : str
            MDML experiment ID, this should have been given to you by an administrator
        config_json_filepath : str or dict
            If string, contains the filepath to a json file with the experiment's configuration
            If dict, the dict will be the experiment's configuration
        host : str
            string for the MDML host running the MQTT message broker
        port : int
            port number used by the MDML MQTT message broker (default is 1883)
        """
        
        self.experiment_id = experiment_id.upper()
        
        if type(config) == str:
            with open(config, 'r') as config_file:
                config_str = config_file.read()
            try:
                self.config = json.loads(config_str)
            except:
                print("json.loads() call on the file contents does not return a dict")
                return
        elif type(config) == dict:
            self.config = config
        else:
            print("Supplied configuration type is not supported. Must be of type str or type dict.")
        # Creating connection to MQTT broker
        client = mqtt.Client()
        
        # Connect to Mosquitto broker
        client.connect(host, port, 60)
        
        # Storing
        self.client = client


    def validate_config(self):
        """
        Validate the configuration supplied during MDML_experiment init

        Returns
        -------
        boolean
            True for a valid configuration, False otherwise
          
        """

        config_keys = self.config.keys()

        # Validating top level section
        if 'experiment' not in config_keys or 'devices' not in config_keys:
            print("""Highest level of configuration json must be a dictionary 
            with the keys: 'experiment' and 'devices'""")
            return False
        
        # Validating experiment section
        experiment_keys = self.config['experiment']
        if 'experiment_id' not in experiment_keys or\
            'experiment_number' not in experiment_keys or\
            'experiment_notes' not in experiment_keys or\
            'experiment_devices' not in experiment_keys:
            print("""Missing required fields in the 'experiment' section of your
            configuration""")
            return False

        # Validating devices section
        devices = self.config['devices']
        for device_keys in devices:
            if 'device_id' not in device_keys or\
                'device_name' not in device_keys or\
                'device_version' not in device_keys or\
                'device_output' not in device_keys or\
                'device_output_rate' not in device_keys or\
                'device_notes' not in device_keys or\
                'headers' not in device_keys or\
                'data_types' not in device_keys or\
                'data_units' not in device_keys or\
                'save_tsv' not in device_keys:
                print("""Missing required fields in the 'devices' section of your 
                configuration""")
                return False

        # Return to string to prepare for sending to MDML
        self.config = json.dumps(self.config)
        print("Valid configuration!")
        return True

    def send_config(self):
        """
        Send experiment configuration to MDML

        Returns
        -------
        boolean
            True for success, False otherwise

        """

        topic = "MDML/" + self.experiment_id + "/CONFIG"
        # Publishing experiment configuration
        try:
            self.client.publish(topic, self.config)
            print(topic)
            self.config_sent = True
            return True
        except: # Warn if something goes wrong
            self.config_sent = False
            print("Error sending config.")
            return False


    def publish_data(self, data, device_id, data_delimiter='null', influx_measurement=False):
        """
        Publish data to MDML
        
        ...

        Parameters
        ----------
        data : str
            String containing the data to send
        device_id : str
            Unique string identifying the device this data originated from.
            This should correspond with the experiment's configuration
        data_delimiter : str
            String containing the delimiter of the data  (default is 'null', no delimiter)
        influx_measurement : boolean
            True is the data should be stored in InfluxDB, False otherwise (default is False)
        """

        if self.config_sent:
            # Creating MQTT topic
            topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
            # Base payload
            payload = {
                'data': data
            }
            # Optional parameters 
            if data_delimiter != 'null':
                payload['data_delimiter'] = data_delimiter
            if influx_measurement:
                payload['influx_measurement'] = device_id.upper()
            
            # Send data via MQTT
            self.client.publish(topic, json.dumps(payload))
        else:
            print("""No configuration has been sent to MDML yet. Have you run 
            validate_config() and send_config()?""")

            
    def reset(self):
        """
        Publish a reset message on the MDML message broker to reset
        your current experiment.
        """
        topic = "MDML/" + self.experiment_id + "/RESET"
        self.client.publish(topic, '{"reset": 1}')

    def unix_time(self):
        """
        Get unix time and convert to nanoseconds to match MDML's
        InfluxDB time resolution.
        """
        unix_time = format(time.time() * 1_000_000_000, '.0f')
        return unix_time



#######################################################
################# TESTING & DEBUGGING #################
#######################################################

# # Return data or error messages
# debugger = MDML_debugger('TEST') # loops forever

# # Testing the classes
# experiment = MDML_experiment('TEST', './real_config.json')
# experiment.validate_config()
# experiment.send_config()
# time.sleep(2)
# experiment.publish_data('some_data_here', 'SENSOR1', influx_measurement=True)
# time.sleep(1)
# experiment.reset()
# print(experiment.unix_time())