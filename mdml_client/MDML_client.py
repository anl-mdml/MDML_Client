import os
import re
import cv2
import json
import time
import tarfile
import numpy as np
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
from base64 import b64encode
from threading import Thread


def on_MDML_message(client, userdata, message):
    print("******************************** MDML MESSAGE ********************************\n")
    print("%s  :  %s" % (message.topic, message.payload.decode('utf-8')))
    print()

def on_MDML_connect(client, userdata, flags, rc):
    print("Connecting to the message broker...")
    if rc == 5:
        print("Error connecting to broker - check username and password")
        client.loop_stop(force=True)
    # print("MDML_DEBUG/"+userdata)
    # client.subscribe("MDML_DEBUG/"+userdata)

def unix_time(ret_int=False):
    """
    Get unix time and convert to nanoseconds to match the 
    time resolution in the MDML's InfluxDB.

    ...

    Parameters
    ----------
    ret_int : bool
        True to return the value as type integer. False to 
        return the value as type string. 
    """
    unix_time = format(time.time() * 1000000000, '.0f') #1,000,000,000
    if ret_int:
        return int(unix_time)
    else:
        return unix_time

def read_image(file_name, resize_x=0, resize_y=0):
    """
    Read image from a local file and convert to bytes from sending
    over the MDML.
    ...

    Parameters
    ----------
    file_name : string
        file name of the file to be opened
    resize_x : int
        horizontal size of the resized image
    resize_y : int
        vertical size of the resized image

    Returns
    -------
    string
        String of bytes that can be used as the second argument in 
        experiment.publish_image()
    """
    with open(file_name, 'rb') as open_file:
            image_bytes = open_file.read()
    npimg = np.fromstring(image_bytes, dtype=np.uint8)
    source = cv2.imdecode(npimg, 1)
    if resize_x != 0 and resize_y != 0:
        source = cv2.resize(source, (resize_x, resize_y))
    # img_bytes = cv2.imencode('.jpg', img_small)[1].tobytes()
    # processed_queue.put(img_bytes)
    _, img = cv2.imencode('.jpg', source)
    img_bytes = img.tobytes()
    img_b64bytes = b64encode(img_bytes)
    img_byte_string = img_b64bytes.decode('utf-8')
    return img_byte_string

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
        "device_data_type":"text/numeric",
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
        
        # Creating connection to MQTT broker
        client = mqtt.Client()
        client.on_connect = on_MDML_connect
        client.on_message = on_MDML_message
        try:
            # Set authorization parameters
            client.username_pw_set(self.username, passwd)
            # Set experiment ID
            client.user_data_set(self.experiment_id)
            # Connect to Mosquitto broker
            client.connect(host, port, 60)
            client.loop_start()
            self.client = client
        except ConnectionRefusedError:
            print("Broker connection was refused. This may be caused by an incorrect username or password.")
        except:
            print("Error! Could not connect to MDML's message broker. Verify you have the correct host. Contact jelias@anl.gov if the problem persists.")


    def add_config(self, config, experiment_run_id=""):
        """
        Add a configuration to the experiment

        ...

        Parameters
        ----------
        config : str or dict
            If string, contains the filepath to a json file with the experiment's configuration
            If dict, the dict will be the experiment's configuration

        experiment_run_id : str
            String containing the desired experiment run id


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
                print("Error in json.loads() call on the config file contents.")
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
            # 'experiment_number' not in experiment_keys or\
        if 'experiment_id' not in experiment_keys or\
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
                'device_output' not in device_keys or\
                'device_output_rate' not in device_keys or\
                'device_data_type' not in device_keys or\
                'device_notes' not in device_keys or\
                'headers' not in device_keys or\
                'data_types' not in device_keys or\
                'data_units' not in device_keys:
                print("""Missing required fields in the 'devices' section of your 
                configuration""")
                return False

        if experiment_run_id != "":
            self.config['experiment']['experiment_run_id'] = experiment_run_id

        # Return to string to prepare for sending to MDML
        self.config = json.dumps(self.config)
        print("Valid configuration found, now use .send_config() to send it to the MDML.")
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

    def publish_vector_data(self, device_id, data, timestamp='none', data_delimiter='\t', influxDB=True):
        """
        Publish vector data to MDML. The data will be 

        ...

        Parameters
        ----------
        device_id : str
            Unique string identifying the device this data originated from.
            This should correspond with the experiment's configuration
        data : dict
            Dictionary where keys are the headers for the data device and values are
            tab delimited strings of data values
        timestamp : str
            1 of 3 options: 
                'none' - influxdb creates timestamp
                'many' - different timestamp for each data point
                unix time in nanosecond (as string) - one timestamp for all data points
        data_delimiter : str
            String containing the delimiter of the data  (default is 'null', no delimiter)
        influxDB : boolean
            True if the data should be stored in InfluxDB, False otherwise (default is False)
        """
        if type(data) != dict:
            print("Parameter data is not a dictionary.")
            return

        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
        # Base payload
        payload = {
            'data': data,
            'data_type': 'vector',
            'timestamp': timestamp
        }
        # Optional parameters 
        payload['data_delimiter'] = data_delimiter
        if influxDB:
            payload['influx_measurement'] = device_id.upper()
        if timestamp != 'none':
            payload['timestamp'] = timestamp
        
        # Send data via MQTT
        self.client.publish(topic, json.dumps(payload))
      

    def publish_data(self, device_id, data, data_delimiter='null', influxDB = False):
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
            True if the data should be stored in InfluxDB, False otherwise (default is False) 
        """

        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
        # Base payload
        payload = {
            'data': data
        }
        # Optional parameters 
        if data_delimiter != 'null':
            payload['data_delimiter'] = data_delimiter
        if influxDB:
            payload['influx_measurement'] = device_id.upper()
        
        # Send data via MQTT
        self.client.publish(topic, json.dumps(payload))
      
    def publish_image(self, device_id, img_byte_string, timestamp = 0):
        """
        Publish an image to MDML

        ...

        Parameters
        ----------
        device_id : str
            Unique string identifying the device this data originated from.
            This must correspond with the experiment's configuration
        img_byte_string : bytes
            byte string of the image you want to send. Can be supplied by the
            read_image() function in this package
        timestamp : int
            Unix time in nanoseconds. Can be supplied by the unix_time()
            function in this package
        """

        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
        # Data checks
        if timestamp == 0:
            timestamp = unix_time()
        # Base payload
        payload = {
            'timestamp': timestamp,
            'data': img_byte_string,
            'data_type': 'image'
        }
        payload = json.dumps(payload)
        # Publish it
        self.client.publish(topic, payload)
                
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

    def replay_experiment(self, filename):
        """
        Replay an old experiment by specifying a tar file output from MDML
        
        ...
        
        Parameters
        ----------
        filename : str
            absolute filepath of the tar file for the experiment you would like to replay.
            this is format dependent so the file must have been output from the MDML 
        """
        # Validate file
        try:
            if not tarfile.is_tarfile(filename):
                print("File supplied is not a tar file")
                return
        except:
            print("File not found.")
            return
        self.start_debugger()
        # Opening experiment tar file
        exp_tar = tarfile.open(filename)
        exp_tar.extractall()
        exp_dir = exp_tar.getnames()[0]
        # Load experiment configuration
        with open(exp_dir + '/config.json', 'r') as config_file:
            config = config_file.read()
            config = json.loads(config)
        self.add_config(config)
        self.send_config()
        # Get data devices and their data types
        devices = config['experiment']['experiment_devices']
        device_data_types = []
        # Find timestamps for simulation
        first_timestamps = []
        last_timestamps = []
        valid_devices = []
        for d in devices:
            data_type = [dev['device_data_type'] for dev in config['devices'] if dev['device_id'] == d][0]
            try:
                with open(exp_dir + '/' + d, 'r') as open_file:
                    data = open_file.readlines()
                    first_timestamps.append(int(re.split('\t', data[1])[0]))
                    last_timestamps.append(int(re.split('\t', data[len(data)-1])[0]))
                    valid_devices.append(d)
                    device_data_types.append(data_type)
            except:
                print("Device listed in configuration has no data file. Continuing...")
                continue
        # Setting start times of experiment
        exp_start_time = int(min(first_timestamps))
        exp_end_time = int(max(last_timestamps))
        exp_duration = (exp_end_time - exp_start_time)/60000000000 # nansecs to mins 60,000,000,000
        print("Experiment replay will take " + str(exp_duration) + " minutes.")
        sim_start_time = unix_time(ret_int=True)
        
        try:
            # Start simulations
            for i in range(len(valid_devices)):
                tmp = Thread(target=self._replay_file, args=(valid_devices[i], \
                                                        exp_dir, \
                                                        device_data_types[i], \
                                                        exp_start_time, \
                                                        sim_start_time,))
                tmp.setDaemon(True)
                tmp.start()
            time.sleep(4)
        except KeyboardInterrupt:
            print("Ending experiment with MDML.")
            self.reset()
            return

    def _replay_file(self, device_id, file_dir, data_type, exp_start_time, sim_start_time):
        with open(file_dir + '/' + device_id) as data_file:
            _ = data_file.readline()
            data = data_file.readlines()
            data = [line.strip('\n') for line in data]
        while True:
            # Get next timestamp TODO delimiter needs to come from experiment configuration file
            next_dat_time = int(re.split('\t', data[0])[0])
            exp_delta = next_dat_time - exp_start_time
            sim_delta = unix_time(ret_int=True) - sim_start_time

            if (sim_delta >= exp_delta):
                if data_type == "text/numeric":
                    new_time = str(sim_start_time + exp_delta)
                    next_row = data[0].split('\t')
                    next_row[0] = new_time
                    next_row = '\t'.join(next_row)
                    self.publish_data(device_id, next_row, data_delimiter='\t', influxDB=True)
                elif data_type == "image":
                    img_filename = file_dir + '/' + re.split('\t', data[0])[1]
                    img_byte_string = read_image(img_filename)
                    self.publish_image(device_id, img_byte_string, timestamp=sim_start_time + exp_delta)
                else:
                    print("DATA_TYPE IN CONFIGURATION NOT SUPPORTED")
                del data[0]
                if len(data) == 0:
                    break
