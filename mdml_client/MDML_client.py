import cv2
import json
import numpy as np
import os
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import queue
import re
import sys
import tarfile
import threading
import time
import requests
import multiprocessing
from base64 import b64encode
from fair_research_login.client import NativeClient
from mdml_client.config import CLIENT_ID


def print_MDML_message(client, userdata, message):
    # self.file_queue.put(message)
    print("******************************** MDML MESSAGE ********************************\n")
    print("%s  :  %s" % (message.topic, message.payload.decode('utf-8')))
    print()

def on_MDML_message(client, userdata, message):
    userdata.put(message.payload.decode('utf-8'))

def on_MDML_connect(client, userdata, flags, rc):
    print("Connecting to the message broker...")
    if rc == 5:
        print("ERROR! Broker connection was refused. This may be caused by an incorrect username or password.")
        client.loop_stop(force=True)
    # print("MDML_DEBUG/"+userdata)
    # client.subscribe("MDML_DEBUG/"+userdata)

def unix_time(ret_int=False):
    """
    Get unix time and convert to nanoseconds to match the 
    time resolution of the MDML's time-series database, InfluxDB.


    Parameters
    ----------
    ret_int : bool
        True to return time as an integer. False to 
        return as a string.

    Returns
    -------
    string or int
        Unix time in nanoseconds 
    """
    unix_time = format(time.time() * 1000000000, '.0f') #1,000,000,000
    if ret_int:
        return int(unix_time)
    else:
        return unix_time

def _read_image_file(file_name, resize_x=0, resize_y=0, rescale_pixel_intensity=False):
    """
    Read image from a local file and convert to bytes from sending
    over the MDML.
    

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

def read_image(file_name, resize_x=0, resize_y=0, rescale_pixel_intensity=False):
    """
    Read image from a local file and convert to bytes from sending
    over the MDML.


    Parameters
    ----------
    file_name : string
        file name of the file to be opened
    resize_x : int
        horizontal size of the resized image
    resize_y : int
        vertical size of the resized image
    rescale_pixel_intensity : bool
        scale the pixel intensities between 0-255 according to the min and max pixel values.

    Returns
    -------
    string
        String of bytes that can be used as the second argument in 
        experiment.publish_image()
    """
    source = cv2.imread(file_name, cv2.IMREAD_ANYCOLOR | cv2.IMREAD_ANYDEPTH) 
    if resize_x != 0 and resize_y != 0:
        source = cv2.resize(source, (resize_x, resize_y))
    if rescale_pixel_intensity:
        source = np.nan_to_num(source)
        min_val = np.min(source)
        max_val = np.max(source)
        source = (source - min_val) * (255/max_val)
    _, img = cv2.imencode('.jpg', source)
    img_bytes = img.tobytes()
    img_b64bytes = b64encode(img_bytes)
    img_byte_string = img_b64bytes.decode('utf-8')
    return img_byte_string

def GET_images(image_metadata, experiment_id, host):
    """
    Retrieves images from the MDML using a GET request. Created for
    use in FuncX functions that analyze images.


    Parameters
    ----------
    image_metadata : list
        list of dicts of image metadata. From the query value for the image-generating device
    host : string
        Hostname/IP of the MDML host
    experiment_id : string
        MDML Experiment ID

    Returns
    -------
    list
        List of bytes, one element for each image from the metadata
    """
    imgs = []
    for img in image_metadata:
        resp = requests.get(f"http://{host}:1880/image?path={img['filepath']}&experiment_id={experiment_id}")
        imgs.append(resp.content)
    return imgs


class experiment:
    """
    This is the first step in interacting with the MDML. 
    
    Parameters
    ----------
    experiment_id : str
        MDML experiment ID, this will be given to you by an MDML admin
    username : str
        MDML username
    passwd : str
        Password for the supplied MDML username
    host : str
        MDML host,  this will be given to you by an MDML admin
    
    """

    def __init__(self, experiment_id, username, passwd, host):
        
        self.experiment_id = experiment_id.upper()
        self.username = username
        self.password = passwd
        self.host = host
        self.port = 1883
        self.tokens = None
        self.msg_queue = None
        self.debugger = None
        self.debug_callback = None
        

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
            client.connect(self.host, self.port, 60)
            client.loop_start()
            self.client = client
        except ConnectionRefusedError:
            print("ERROR! Broker connection was refused. This may be caused by an incorrect username or password.")
        except:
            print("ERROR! Could not connect to the MDML's message broker. Verify you have the correct host. Contact jelias@anl.gov if the problem persists.")

    def globus_login(self):
        """
        Perform a Globus login to acquire auth tokens for FuncX analyses. Must be done before experiment.add_config()
        """
        scopes = ["https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"]
        cli = NativeClient(client_id=CLIENT_ID)
        self.tokens = cli.login(refresh_tokens=True, no_local_server=True, 
                                no_browser=True, requested_scopes=scopes) 

    def add_config(self, config, experiment_run_id=""):
        """
        Add a configuration to the experiment. Added only locally - use 
        experiment.send_config() to send to the MDML


        Parameters
        ----------
        config : str or dict
            If string, contains a filepath to a json file with the experiment's configuration.
            If dict, the dict will be the experiment's configuration

        experiment_run_id : str
            String containing the experiment run ID


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

        if self.tokens:
            try:
                self.config['globus_token'] = self.tokens['funcx_service']['access_token']
            except:
                print("No Auth token found. Have you run .globus_login() to create one?\
                    This can be ignored if you are not using funcX for analysis.")
                pass

        if experiment_run_id != "":
            self.config['experiment']['experiment_run_id'] = experiment_run_id
        # Check run id only contains letters and underscores
        if re.match(r"^[\w]*$", self.config['experiment']['experiment_run_id']):
            self.config = json.dumps(self.config)
            print("Valid configuration found, now use .send_config() to send it to the MDML.")
            # Return to string to prepare for sending to MDML
            return True
        else:
            print("Experiment run ID contains characters other than letters, numbers, and underscores.")
            return False

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
        Publish vector data to MDML.


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
            True if the data should be stored in InfluxDB, False otherwise (default is True)
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
      
    def publish_data(self, device_id, data, data_delimiter='null', influxDB = True):
        """
        Publish data to MDML
        

        Parameters
        ----------
        device_id : str
            Unique string identifying the device that created the data.
            This should correspond with the experiment's configuration.
        data : str
            String containing the data to send
        data_delimiter : str
            String containing the delimiter of the data  (default is 'null', no delimiter)
        influxDB : boolean
            True if the data should be stored in InfluxDB, False otherwise (default is True) 
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

    def publish_analysis(self, device_id, queries, function_id, endpoint_id, parameters={}):
        """
        Publish a message to run an analysis


        Parameters
        ----------
        device_id : string
            Device ID for storing analysis results (must match configuration file)
        queries : list
            Description of the data to send funcx. See queries format in the documentation on GitHub
        function_id : string
            From FuncX, the id of the function to run
        endpoint_id : string
            From FuncX, the id of the endpoint to run the function on
        parameters : any json serializable type
            Custom parameters to be accessed in the second element of your FuncX data parameter
        """
        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/FUNCX/" + device_id

        # Set message payload
        payload = {
            'queries': queries,
            'function_id': function_id,
            'endpoint_id': endpoint_id,
            'timestamp': unix_time(),
            'parameters': parameters
        }

        # Add auth if set
        if self.tokens:
            payload['globus_token'] = self.tokens['funcx_service']['access_token']
        else:
            print("No globus token found. You must use the .globus_login() method first.\
                    This can be ignored if you are not using funcX for analysis.")
            return

        # Send data via MQTT
        self.client.publish(topic, json.dumps(payload))
        
    def publish_image(self, device_id, img_byte_string, filename = '', timestamp = 0):
        """
        Publish an image to MDML


        Parameters
        ----------
        device_id : str
            Unique string identifying the device that created the data.
            This must correspond with the experiment's configuration
        img_byte_string : str
            byte string of the image you want to send. Can be supplied by
            mdml_client.read_image() function in this package
        filename : str
            filename to store the file in the MDML. Can only contain letters, 
            numbers, and underscores If left blank filenames are the experiment
            ID followed by an index (e.g. EXPID_1.JPG, EXPID_2.JPG...)
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
            'filename': filename,
            'data': img_byte_string,
            'data_type': 'image'
        }
        # Check for valid filename
        if filename != '':
            if re.match(r"^[\w]+\.[A-Za-z0-9]+$", filename) == None:
                print("Filename not valid. Can only contains letters, numbers, and underscores.")
                return
            else:
                payload['filename'] = filename 
        payload = json.dumps(payload)
        # Publish it
        self.client.publish(topic, payload)

    def _publish_image_benchmarks(self, device_id, img_byte_string, filename = '', timestamp = 0, size = 0):
        """
        Publish an image to MDML


        Parameters
        ----------
        device_id : str
            Unique string identifying the device that created the data.
            This must correspond with the experiment's configuration
        img_byte_string : str
            byte string of the image you want to send. Can be supplied by
            mdml_client.read_image() function in this package
        filename : str
            filename to store the file in the MDML. Can only contain letters, 
            numbers, and underscores If left blank filenames are the experiment
            ID followed by an index (e.g. EXPID_1.JPG, EXPID_2.JPG...)
        timestamp : int
            Unix time in nanoseconds. Can be supplied by the unix_time()
            function in this package
        size : string
            Size of the message being sent
        """

        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
        # Data checks
        if timestamp == 0:
            timestamp = unix_time()
        # Base payload
        payload = {
            'timestamp': timestamp,
            'filename': filename,
            'data': img_byte_string,
            'data_type': 'image',
            'size': size
        }
        # Check for valid filename
        if filename != '':
            if re.match(r"^[\w]+\.[A-Za-z0-9]+$", filename) == None:
                print("Filename not valid. Can only contains letters, numbers, and underscores.")
                return
            else:
                payload['filename'] = filename 
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
        self.debugger = multiprocessing.Process(target=subscribe.callback,\
            kwargs={\
                'callback': on_MDML_message,\
                'topics': "MDML_DEBUG/" + self.experiment_id,\
                'hostname':self.host,\
                'auth': {'username': self.username, 'password': self.password},\
                'userdata': self.msg_queue
            })
        self.debugger.start()

    def disconnect(self):
        """
        Disconnect MQTT client from the broker
        """
        self.client.disconnect()

    def set_debug_callback(self, user_func):
        """
        Set a function to run every time a message is received from the MDML debugger
        

        Parameters
        ==========
        user_func : function
            Function that takes one parameter. Each message received from 
            the MDML will trigger this function with the message string as 
            the parameter.
        """
        self.msg_queue = multiprocessing.Manager().Queue()
        def func(msg_queue):
            while True:
                msg = msg_queue.get()
                user_func(msg)
                sys.stdout.flush()
        self.debug_callback = multiprocessing.Process(target=func, args=(self.msg_queue,))
        self.debug_callback.start()

    def stop_debugger(self):
        """
        Stop an MDML debugger that has already been started
        """
        print("Stopping debugger.")
        if self.debug_callback.is_alive():
            self.debug_callback.terminate()
        if self.debugger.is_alive():
            self.debugger.terminate()
        print("Debugger stopped.")

    def replay_experiment(self, filename):
        """
        Replay an old experiment by specifying a tar file output from MDML
        
        
        Parameters
        ----------
        filename : str
            absolute filepath of the tar file for the experiment you would like to replay.
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
                tmp = threading.Thread(target=self._replay_file, args=(valid_devices[i], \
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
