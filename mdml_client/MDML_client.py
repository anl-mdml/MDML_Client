import json
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import time
from mdml_client.config import CLIENT_ID
from threading import Thread
from fair_research_login.client import NativeClient

def on_MDML_message(client, userdata, message):
    print("******************************** MDML MESSAGE ********************************\n")
    print("%s  :  %s" % (message.topic, message.payload.decode('utf-8')))
    print()

def unix_time():
    """
    Get unix time and convert to nanoseconds to match the 
    time resolution in the MDML's InfluxDB.
    """
    unix_time = format(time.time() * 1_000_000_000, '.0f')
    return unix_time

class experiment:
    """
    This class allows users to run an experiment with the MDML.
    This includes submiting a configuration, sending data and stopping 
    the experiment. Also included are some basic helper methods: 
    unix_time() provides the unix time in nanoseconds (MDML's InfluxDB 
    needs it this way). 
    ...

    Attributes
    ----------
    example_device_config : dict
        example configuration for one device. Device configs are used 
        in the 'experiment_devices' array in the experiment section 
        of the config file
    example_config : dict
        example configuration used to start an experiment

    Methods
    -------
    add_config(config)
        Validates that nothing is wrong with the supplied config file
    send_config()
        Sends the configuration dict to MDML to start an experiment
    publish_data(device_id, data, data_delimiter='null', influx_measurement=False)
        Publishes data to the MDML message broker
    publish_image(device_id, img_bytes)
        Publishes image to the MDML message broker
    reset()
        Sends a reset message to MDML to trigger the end of an experiment
    start_debugger()
        Creates a separate thread to print messages from MDML regarding
        your experiment
    """

    example_device_config = {
        "device_id": "EXAMPLE_DEVICE",
        "device_name": "Test device",
        "device_version": "1",
        "device_output": "Random data for testing",
        "device_output_rate": 0.1,
        "device_notes": "Researcher notes go here",
        "headers": [
            "time",
            "data_row",
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

    example_config = {
        "experiment": {
            "experiment_id": "TEST",
            "experiment_number": "1",
            "experiment_notes": "example.py file for MDML python package",
            "experiment_devices": ["EXAMPLE_DEVICE"]
        },
        "devices": [
            example_device_config
        ]
    }

    def __init__(self, experiment_id, username, passwd, host, port=1883):
        """
        Init an MDML experiment

        ...

        Parameters
        ----------
        experiment_id : str
            MDML experiment ID, this should have been given to you by an MDML admin
        username : str
            MDML username
        passwd : str
            Password for the supplied MDML username
        host : str
            string for the MDML host running the MQTT message broker
        port : int
            port number used by the MDML MQTT message broker (default is 1883)
        """
        
        self.experiment_id = experiment_id.upper()
        self.username = username
        self.password = passwd
        self.host = host
        self.port = port
        self.tokens = None

        # Creating connection to MQTT broker
        client = mqtt.Client()
        print(client)        
        try:
            # Set authorization parameters
            print(self.username)
            print(host)
            print(port)
            print("Connecting...")
            client.username_pw_set(self.username, self.password)
            # Connect to Mosquitto broker
            client.connect(host, port, 10)
            self.client = client
            print("Successfully connected to the message broker.")
        except ConnectionRefusedError: 
            print("Broker connection was refused. This may be caused by an incorrect username or password.")
        except:
            print("Error! Could not connect to MDML's message broker. Verify you have the correct host. Contact jelias@anl.gov if the problem persists.")


    def login(self):
        """
        Perform a Globus login to acquire auth tokens.
        """
        cli = NativeClient(client_id=CLIENT_ID)
        self.tokens = cli.login(refresh_tokens=True, no_local_server=True, no_browser=True)
        print(self.tokens) 

    def add_config(self, config):
        """
        Add a configuration to the experiment

        ...

        Parameters
        ----------
        config : str or dict
            If string, contains the filepath to a json file with the experiment's configuration
            If dict, the dict will be the experiment's configuration

        Returns
        -------
        boolean
            True for a valid configuration, False otherwise

        """

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
            return True
        except: # Warn if something goes wrong
            print("Error sending config.")
            return False

    def publish_data(self, device_id, data, data_delimiter='null', influxDB=False):
        """
        Publish data to MDML
        
        ...

        Parameters
        ----------
        device_id : str
            Unique string identifying the device this data originated from.
            This should correspond with the experiment's configuration
        data : str
            String containing the data to send
        data_delimiter : str
            String containing the delimiter of the data  (default is 'null', no delimiter)
        influxDB : boolean
            True is the data should be stored in InfluxDB, False otherwise (default is False)
        """

        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
        # Base payload
        payload = {
            'data': data
        }
        # Add auth if set
        if self.tokens:
            payload['globus_token'] = self.tokens['auth.globus.org']['access_token']

        # Optional parameters 
        if data_delimiter != 'null':
            payload['data_delimiter'] = data_delimiter
        if influxDB:
            payload['influx_measurement'] = device_id.upper()
        
        # Send data via MQTT
        self.client.publish(topic, json.dumps(payload))

    def publish_image(self, device_id, img_bytes):
        """
        Publish an image to MDML

        ...

        Parameters
        ----------
        device_id : str
            Unique string identifying the device this data originated from.
            This must correspond with the experiment's configuration
        img_bytes : bytes
            bytes of the image you want to send
        """

        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
        # Data checks
        if type(img_bytes) == bytes:
            # Publish it
            self.client.publish(topic, img_bytes)
        else:
            print("Data supplied was not of type bytes")
            
    def reset(self):
        """
        Publish a reset message on the MDML message broker to reset
        your current experiment.
        """
        topic = "MDML/" + self.experiment_id + "/RESET"
        self.client.publish(topic, '{"reset": 1}')
    
    def start_debugger(self):
        """
        Init an MDML debugger to retrieve error messages or other important 
        events when running an experiment.

        """

        debug = Thread(target=subscribe.callback,\
            kwargs={\
                'callback': on_MDML_message,\
                'topics': "MDML_DEBUG/" + self.experiment_id,\
                'hostname':self.host,\
                'auth': {'username': self.username, 'password': self.password}
            })
        debug.setDaemon(False)
        debug.start()
