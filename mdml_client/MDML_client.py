import cv2
import json
import numpy as np
import pandas as pd
import os
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import queue
import re
import sys
import tarfile
import threading
import time
import boto3
import requests
import multiprocessing
import matplotlib.image as mpimg
from base64 import b64encode
from fair_research_login.client import NativeClient
from mdml_client.config import CLIENT_ID
from uuid import uuid4
from confluent_kafka import SerializingProducer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


def on_MDML_message(client, userdata, message):
    userdata.put(message.payload.decode('utf-8'))

def on_MDML_connect(client, userdata, flags, rc):
    if rc == 5:
        print("ERROR! Broker connection was refused. This may be caused by an incorrect username or password.")
        client.loop_stop(force=True)

def unix_time(ret_int=True):
    """
    Get unix time and convert to nanoseconds


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

def random_image(width,height):
    random_image = np.random.randint(255, size=(height,width,3), dtype=np.uint8)    
    _, img = cv2.imencode('.png', random_image)
    img_bytes = img.tobytes()
    img_b64bytes = b64encode(img_bytes)
    img_byte_string = img_b64bytes.decode('utf-8')
    return img_byte_string

def read_image(file_name):
    """
    Read image from a local file and convert to bytes from sending
    over the MDML.


    Parameters
    ----------
    file_name : string
        file name of the file to be opened
    
    Returns
    -------
    string
        String of bytes that can be used as the second argument in 
        experiment.publish_image()
    """
    with open(file_name, 'rb') as buf:
        img_bytes = buf.read()
    img_b64bytes = b64encode(img_bytes)
    img_byte_string = img_b64bytes.decode('utf-8')
    return img_byte_string

def GET_images(image_metadata, experiment_id, host, verify_cert=True):
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
    verify_cert : bool
        Boolean is requests should verify the SSL cert

    Returns
    -------
    dict
        Dict where keys are filepaths and values are bytestrings
    """
    imgs = {}
    for img in image_metadata:
        resp = requests.get(f"https://{host}:1880/image?path={img['filepath']}&experiment_id={experiment_id}", verify=verify_cert)
        imgs[img['filepath']] = resp.content
    return imgs

def query_to_pandas(device, query_result, sort=True):
    """
    Pull out a device's queried data as a pandas DataFrame from the results of experiment.query().
    
    Parameters
    ----------
    device : string
        String of the device id that you want to pull out as a pandas DataFrame
    query_result : list
        Result of a experiment.query() call.

    Returns
    -------
    DataFrame
        Pandas DataFrame of the query performed
    """
    try:
        device_data = query_result[device]
        if len(device_data) == 0:
            raise Exception("No data found in the given device.")
    except KeyError:
        print("Device does not exist in this query result.")
    
    tmp = {i:[] for i in device_data[0]}
    for row in device_data:
        for d in row:
            tmp[d].append(row[d])
    device_data_pd = pd.DataFrame.from_dict(tmp)
    if sort:
        device_data_pd = device_data_pd.sort_index(axis=1)
    return device_data_pd

class kafka_mdml_producer:
    """
    Creates a serializingProducer instance for interacting with the MDML. 
    
    Parameters
    ----------
    topic : str
        Topic to send under 
    schema : dict or str
        JSON schema for the message value. If dict, value is used as the 
        schema. If string, value is used as a file path to a json file.
    kafka_host : str
        Host name of the kafka broker
    kafka_port : int
        Port used for the kafka broker
    schema_host : str
        Host name of the kafka schema registry
    schema_port : int
        Port of the kafka schema registry
    """
    def __init__(self, topic, schema, 
                kafka_host="merf.egs.anl.gov", kafka_port=9092,
                schema_host="merf.egs.anl.gov", schema_port=8081):
        # Checking topic param
        if type(topic) == str:
            if topic[0:5] != "mdml-":
                raise Exception("Error, topic must be of the form 'mdml-<experiment id>-<sensor>'")
            else:
                self.topic = topic
        else:
            raise Exception("Error, topic must be of type string.")
        # Checking schema param
        if type(schema) == dict:
            self.schema = json.dumps(schema)
        elif type(schema) == str:
            with open(schema,"r") as f:
                self.schema = f.read()
        else:
            raise Exception("Error, schema must be of type str or dict.")
        # Create schema registry config, client, and serializer
        schema_registry_conf = {
            "url": f"http://{schema_host}:{schema_port}"
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        json_serializer = JSONSerializer(self.schema, schema_registry_client)
        # Create producer and its config 
        producer_config = {
            'bootstrap.servers': f'{kafka_host}:{kafka_port}',
            'value.serializer': json_serializer
        }
        self.producer = SerializingProducer(producer_config)
    def produce(self, data):
        """
        Produce data to the supplied topic 

        Parameters
        ----------
        data : dict
            Dictionary of the data
        """
        self.producer.produce(topic = self.topic, value=data)
        self.producer.flush()

class kafka_mdml_consumer:
    """
    Creates a serializingProducer instance for interacting with the MDML. 
    
    Parameters
    ----------
    topic : str
        Topic to send under 
    group : str
        Consumer group ID. Messages are only consumed by a given group ID
        once.
    kafka_host : str
        Host name of the kafka broker
    kafka_port : int
        Port used for the kafka broker
    schema_host : str
        Host name of the kafka schema registry
    schema_port : int
        Port of the kafka schema registry
    """
    def __init__(self, topic, group, 
                kafka_host="merf.egs.anl.gov", kafka_port=9092,
                schema_host="merf.egs.anl.gov", schema_port=8081):
        # Checking topic param
        if type(topic) == str:
            if topic[0:5] != "mdml-":
                raise Exception("Error, topic must be of the form 'mdml-<experiment id>-<sensor>'")
            else:
                self.topic = topic
        else:
            raise Exception("Error, topic must be of type string.")
        # Create schema registry config, client, and serializer
        sr_config = {
            "url": f"http://{schema_host}:{schema_port}"
        }
        client = SchemaRegistryClient(sr_config)
        schema_string = client.get_latest_version(f'{topic}-value').schema.schema_str
        json_deserializer = JSONDeserializer(schema_string)
        
        consumer_conf = {
            'bootstrap.servers': f"{kafka_host}:{kafka_port}",
            'value.deserializer': json_deserializer,
            'group.id': group,
            'auto.offset.reset': "earliest"
        }
        consumer = DeserializingConsumer(consumer_conf)
        consumer.subscribe([topic])
        self.consumer = consumer

    def consume(self, timeout=1.0):
        print("Ctrl+C to break consumer loop")
        while True:
            try:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    continue
                yield msg.value()
            except KeyboardInterrupt:
                break

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
    s3_access_key : str
        S3 access key provided by an MDML admin
    s3_secret_key : str
        S3 secret key provided by an MDML admin
    
    """

    def __init__(self, experiment_id, username, passwd, host, s3_access_key=None, s3_secret_key=None):
        
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
            print("ERROR! Could not connect to the MDML's message broker. Verify you have the correct host and can reach it on port 1883.")

        if s3_access_key is not None:
            # Creating boto3 (s3) client connection
            session = boto3.session.Session()
            try:
                self.s3_client = session.client(
                    service_name='s3',
                    aws_access_key_id=s3_access_key,
                    aws_secret_access_key=s3_secret_key,
                    endpoint_url='https://s3.it.anl.gov:18082'
                )
            except:
                print("ERROR! Connection to s3 failed.")

    def s3_login(self, s3_access_key, s3_secret_key):
        """
        Create connection to BIS S3 object store for use in image streaming

        Parameters
        ----------
        s3_access_key : str
            S3 access key provided by an MDML admin
        s3_secret_key : str
            S3 secret key provided by an MDML admin
        """
        # Creating boto3 (s3) client connection
        session = boto3.session.Session()
        try:
            self.s3_client = session.client(
                service_name='s3',
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
                endpoint_url='https://s3.it.anl.gov:18082'
            )
        except:
            print("ERROR! Connection to s3 failed.")

    def globus_login(self):
        """
        Perform a Globus login to acquire auth tokens for FuncX analyses. Must be done before experiment.add_config()
        """
        fx_scope = "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
        search_scope = "urn:globus:auth:scope:search.api.globus.org:all"
        scopes = [fx_scope, search_scope, "openid"]
        cli = NativeClient(client_id=CLIENT_ID)
        self.tokens = cli.login(refresh_tokens=True, no_local_server=True, 
                                no_browser=True, requested_scopes=scopes)

    def add_config(self, config={}, experiment_run_id="", auto=False):
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
        auto : bool
            True for the MDML to auto build your configuration.
            False requires you to build the config manually


        Returns
        -------
        boolean
            True for a valid configuration, False otherwise

        """
        if auto:
            if config != {}:
                print("Warning! Using auto will discard any configuration supplied here.")
            config = {
                "experiment": {
                    "experiment_id": self.experiment_id,
                    "experiment_run_id": experiment_run_id,
                    "experiment_notes": f"{self.experiment_id} experiment. (Auto generated configuration)",
                    "delete_db_data_when_reset": True,
                    "experiment_devices": [],
                    "allow_auto_gen_devices": True
                },
                "devices": []
            }
            self.config = json.dumps(config)
        else:
            if type(config) == str:
                with open(config, 'r') as config_file:
                    config_str = config_file.read()
                try:
                    config = json.loads(config_str)
                except:
                    print("Error in json.loads() call on the config file contents.")
                    return
            elif type(config) == dict:
                config = config
            else:
                print("Supplied configuration type is not supported. Must be of type str or type dict.")

            # Validating top level section
            if 'experiment' not in config or 'devices' not in config:
                print("""Highest level of configuration json must be a dictionary 
                with the keys: 'experiment' and 'devices'""")
                return False
            
            # Validating experiment section
            experiment_section = config['experiment']
                # 'experiment_number' not in experiment_keys or\
            if 'experiment_id' not in experiment_section or\
                'experiment_notes' not in experiment_section or\
                'experiment_devices' not in experiment_section:
                print("""Missing required fields in the 'experiment' section of your
                configuration""")
                return False

            # Validating devices section
            devices = config['devices']
            for device in devices:
                if 'device_id' not in device or\
                    'device_name' not in device or\
                    'device_output' not in device or\
                    'device_output_rate' not in device or\
                    'device_data_type' not in device or\
                    'device_notes' not in device or\
                    'headers' not in device or\
                    'data_types' not in device or\
                    'data_units' not in device:
                    print("""Missing required fields in the 'devices' section of your 
                    configuration""")
                    return False

            if self.tokens:
                try:
                    config['globus_token'] = self.tokens['funcx_service']['access_token']
                except:
                    print("No Auth token found. Have you run .globus_login() to create one?\
                        This can be ignored if you are not using funcX for analysis.")
                    pass

            if experiment_run_id != "":
                # Check run id only contains letters and underscores
                if re.match(r"^[\w]*$", experiment_run_id):
                    config['experiment']['experiment_run_id'] = experiment_run_id
                    # Set config as string to prepare for sending to MDML
                    self.config = json.dumps(config)
                    print("Valid configuration found, now use .send_config() to send it to the MDML.")
                    return True
                else:
                    print("Experiment run ID contains characters other than letters, numbers, and underscores.")
                    return False
            else: # No need to add a blank run ID
                # Set config as string to prepare for sending to MDML
                self.config = json.dumps(config) 

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

    def publish_vector(self, device_id, data, timestamp, add_device=False):
        """
        Publish vector data to MDML.


        Parameters
        ----------
        device_id : str
            Unique string identifying the device this data originated from.
            This should correspond with the experiment's configuration
        data : dict
            Dictionary where keys are the headers for the data device and values are
            tab delimited strings of data values.
        timestamp : int
            unix time in nanosecond (as string) - one timestamp for all data points
        data_delimiter : str
            String containing the delimiter of the data  (default is 'null', no delimiter)
        add_device : bool
            True if the device should be automatically added to the experiment's configuration (default: False)
        """
        if type(data) != dict:
            print("Error! Data parameter is not a dictionary.")
            return
        else:
            data['time'] = timestamp
        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
        # Base payload
        payload = {
            'data': data,
            'data_type': 'vector',
            'timestamp': timestamp
        }
        # Optional parameters 
        if add_device:
            payload['add_device'] = add_device
        
        payload['sys_timestamp'] = unix_time()
        # Send data via MQTT
        self.client.publish(topic, json.dumps(payload))
      
    def publish_data(self, device_id, data, data_delimiter='', timestamp = 0, add_device=False):
        """
        Publish data to MDML
        

        Parameters
        ----------
        device_id : str
            Unique string identifying the device that created the data.
            This should correspond with the experiment's configuration.
        data : str or dict or list
            String, dictionary, or list containing the data
        data_delimiter : str
            String containing the delimiter of the data.
            Only needed when data is of type string (default: "", no delimiter)
        timestamp : int
            Unix time in nanoseconds that the data should the timestamped
        add_device : bool
            True if the device should be automatically added to the experiment's configuration (default: False) 
        """
        # Checking for misuse of the client
        if add_device and type(data) != dict:
            Exception("When using add_device, the data type must be dict \
                in order for MDML to infer variable names")
        # Figuring out data type
        if type(data) == dict:
            payload = {
                'data': data,
                'data_type': 'text/numeric',
                'data_format': 'dict'
            }
        elif type(data) == list or type(data) == np.ndarray:
            if type(data) == np.ndarray:
                try:
                    data = data.tolist()
                except:
                    print("Error! ndarray.tolist() failed.")
            payload = {
                'data': data,
                'data_type': 'text/numeric',
                'data_format': 'list'
            }
        elif type(data) == str:
            if data_delimiter == '':
                print("Warning! No data delimiter specified while supplying\
                    a string to the data parameter. The entire data parameter\
                    will be read as one value.")
            payload = {
                'data': data,
                'data_type': 'text/numeric',
                'data_format': 'string',
                'data_delimiter': data_delimiter
            }
        else:
            print("Error! Data type not supported. Must be one of (dict, list, str, np.ndarray)")
            return

        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()

        # Optional parameters 
        if add_device:
            payload['add_device'] = add_device
        # Added for system timings
        payload['timestamp'] = unix_time()
        payload['sys_timestamp'] = unix_time()
        # Send data via MQTT
        self.client.publish(topic, json.dumps(payload))

    def publish_analysis(self, device_id, function_id, endpoint_id, parameters={}, funcx_callback=None, trigger=None):
        """
        Publish a message to run an analysis


        Parameters
        ----------
        device_id : string
            Device ID for storing analysis results (must match configuration file)
        function_id : string
            From FuncX, the id of the function to run
        endpoint_id : string
            From FuncX, the id of the endpoint to run the function on
        parameters : any json serializable type
            Custom parameters to be accessed in the second element of your FuncX data parameter
        funcx_callback : dict
            Dictionary with another set of FuncX params to run with data output from the first FuncX call
        trigger : list
            List of device IDs that when data is generated will trigger this 
            analysis again with the same parameters 
        """
        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/FUNCX/" + device_id

        # Set message payload
        payload = {
            'function_id': function_id,
            'endpoint_id': endpoint_id,
            'timestamp': unix_time(),
            'parameters': parameters
        }

        # Add FuncX callback function
        if funcx_callback is not None:
            # Test format of the inputs
            assert "endpoint_uuid" in funcx_callback.keys()
            assert "function_uuid" in funcx_callback.keys()
            assert "save_intermediate" in funcx_callback.keys()
            # Add to payload
            payload['funcx_callback'] = funcx_callback

        # Add triggers
        if trigger is not None:
            assert type(trigger) is list
            payload['trigger'] = trigger

        # Add auth if set
        if self.tokens:
            payload['globus_token'] = self.tokens['funcx_service']['access_token']
        else:
            print("No globus token found. You must use the .globus_login() before starting an analysis.")
            return

        # Send data via MQTT
        self.client.publish(topic, json.dumps(payload))

    def use_dlhub(self, data, device_id, function_id, funcx_callback=None):
        """
        Publish a message to run an analysis


        Parameters
        ----------
        data : object
            Input data for the model being used.
        device_id : string
            Device ID for storing analysis results (must match configuration file)
        function_id : string
            From FuncX, the id of the function to run,
        funcx_callback : dict
            Dictionary with another set of FuncX params to run with data output from the first FuncX call
        """
        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DLHUB/" + device_id

        # Set message payload
        payload = {
            'function_id': function_id,
            'endpoint_id': '86a47061-f3d9-44f0-90dc-56ddc642c000',
            'timestamp': unix_time(),
            'parameters': {"data": data}
        }

        # Add FuncX callback function
        if funcx_callback is not None:
            # Test format of the inputs
            assert "endpoint_uuid" in funcx_callback.keys()
            assert "function_uuid" in funcx_callback.keys()
            assert "save_intermediate" in funcx_callback.keys()
            # Add to payload
            payload['funcx_callback'] = funcx_callback

        # Add auth if set
        if self.tokens:
            payload['globus_token'] = self.tokens['funcx_service']['access_token']
        else:
            print("No globus token found. You must use the .globus_login() method first.\
                    This can be ignored if you are not using funcX for analysis.")
            return

        # Send data via MQTT
        self.client.publish(topic, json.dumps(payload))

    def publish_image_s3(self, device_id, img_filename, obj_name, timestamp = 0, metadata = {}):
        """
        Publish an image to MDML


        Parameters
        ----------
        device_id : str
            Unique string identifying the device that created the data.
            This must correspond with the experiment's configuration
        img_filename : str
            filename where the image is stored
        obj_name : str
            A unique object name that will be used in the object store
        timestamp : int
            Unix time in nanoseconds. Can be supplied by the unix_time()
            function in this package
        metadata : dict
            Dictionary containing any metadata for the image. Data types of 
            the dictionary values must not be changed. Keys cannot include
            "time" or "filepath".
        """

        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
        # Base payload
        payload = {
            'filename': img_filename,
            'obj_name': obj_name,
            'data_type': 'image',
            'send_method': 's3'
        }
        # Adding metadata if necessary
        payload['metadata'] = metadata
        # # Check for valid filename
        # if img_filename != '':
        #     if re.match(r"^[\w/-]+\.[A-Za-z0-9]+$", img_filename) == None:
        #         print("Filename not valid. Can only contains letters, numbers, and underscores.")
        #         return
        #     else:
        #         payload['filename'] = img_filename

        # Adding timestamp
        if timestamp == 0:
            timestamp = unix_time()
        payload['timestamp'] = timestamp
        payload = json.dumps(payload)
        # Upload to s3
        self.s3_client.upload_file(img_filename, f'mdml-{self.experiment_id.lower()}', obj_name)
        # Publish it
        self.client.publish(topic, payload)
        
    def publish_image(self, device_id, img_byte_string, timestamp, filename = '', metadata = {}, add_device=False):
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
            numbers, underscores and must end with a valid file extension. 
            If left blank, filenames will the experiment ID followed by an
            index (e.g. EXPID_1.JPG, EXPID_2.JPG...)
        timestamp : int
            Unix time in nanoseconds. Can be supplied by the unix_time()
            function in this package
        metadata : dict
            Dictionary containing any metadata for the image. Data types of 
            the dictionary values must not be changed. Dictionary keys must 
            not include "time" or "filepath".
        add_device : bool
            True if the device should be automatically added to the experiment's configuration (default: False)
        """

        # Creating MQTT topic
        topic = "MDML/" + self.experiment_id + "/DATA/" + device_id.upper()
        # Base payload
        payload = {
            'filename': filename,
            'data': img_byte_string,
            'data_type': 'image',
            'metadata': metadata,
            'timestamp': timestamp
        }
        # Adding other params
        if add_device:
            payload['add_device'] = add_device
        payload = json.dumps(payload)
        # Publish it
        self.client.publish(topic, payload)

    def query(self, query, verify_cert=True):
        """
        Query the MDML for an example of the data structure that your query will return. This is aimed at aiding in development of FuncX functions for use with the MDML.


        Parameters
        ----------
        query : list
            Description of the data to send funcx. See queries format in the documentation on GitHub
        verify_cert : bool
            Boolean is requests should verify the SSL cert

        Returns
        -------
        list
            Experiment data from MDML
        """
        import json
        resp = requests.get(f"https://{self.host}:1880/query?query={json.dumps(query)}&experiment_id={self.experiment_id}", verify=verify_cert)
        return json.loads(resp.text)

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
                
    def reset(self, hard_reset=False):
        """
        Publish a reset message on the MDML message broker to reset
        your current experiment.


        Parameters
        ----------
        hard_reset : bool
            True if the experiment should be ended regardless of analysis 
            progress.            
        """
        topic = "MDML/" + self.experiment_id + "/RESET"
        if hard_reset:
            self.client.publish(topic, '{"reset": 1, "hard_reset": 1}')
        else:
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

    def listener(self, analysis_names=None, callback=None):
        """
        Init a listener to receive any events pertaining to the MDML experiment ID given.
        
        
        Parameters
        ----------
        analysis_names : list
            A list of MDML analyses that you would like to listen to for results.
        callback : function
            A callback function to be run on the received messages. 
            This function must have three parameters. Typically, only the
            third parameter is used because it contains the topic and message
            as attributes.
        """
        if callback is None:
            def default_func(client, userdata, message):
                print(f'TOPIC: {message.topic}\nPAYLOAD: {message.payload.decode("utf-8")}')
            callback = default_func
        topics = [f"MDML_DEBUG/{self.experiment_id}"]
        if analysis_names is not None:
            for name in analysis_names:
                topics.append(f"MDML/{self.experiment_id}/RESULTS/{name.upper()}")
        subscribe.callback(callback, topics, hostname=self.host,
            auth={'username': self.username, 'password': self.password})


    def disconnect(self):
        """
        Disconnect MQTT client from the broker
        """
        self.client.disconnect()
        print("Disconnected from MDML.")

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

    def replay_experiment_run(self, experiment_run_id):
        """
        Tell MDML to replay a past experiment.

        Parameters
        ----------
        experiment_run_id : str
            Experiment run ID supplied by the user - this is different than 
            an experiment ID. This could be an integer if no run ID was 
            supplied by the user for the original experiment.
        """
        topic = "MDML/" + self.experiment_id + "/REPLAY/" + experiment_run_id
        self.client.publish(topic, '{"replay": 1}')


    def replay_experiment(self, filename, speedup=1):
        """
        Replay an old experiment by specifying a tar file output from MDML
        
        
        Parameters
        ----------
        filename : str
            absolute filepath of the tar file for the experiment you would like to replay.
        speedup : int
            speedup of data rates
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
        print("Experiment replay will take " + str(float(exp_duration/speedup)) + " minutes.")
        sim_start_time = unix_time(ret_int=True)
        
        try:
            # Start simulations
            for i in range(len(valid_devices)):
                tmp = threading.Thread(target=self._replay_file, args=(valid_devices[i], \
                                                        exp_dir, \
                                                        device_data_types[i], \
                                                        exp_start_time, \
                                                        sim_start_time, \
                                                        speedup))
                tmp.setDaemon(True)
                tmp.start()
            time.sleep(4)
        except KeyboardInterrupt:
            print("Ending experiment with MDML.")
            self.reset()
            self.disconnect()
            return

    def _replay_file(self, device_id, file_dir, data_type, exp_start_time, sim_start_time, speedup):
        """
        Replay individual file
        """
        with open(file_dir + '/' + device_id) as data_file:
            _ = data_file.readline()
            data = data_file.readlines()
            data = [line.strip('\n') for line in data]
        while True:
            # Get next timestamp TODO delimiter needs to come from experiment configuration file
            next_dat_time = int(re.split('\t', data[0])[0])
            exp_delta = float(next_dat_time - exp_start_time)/speedup
            sim_delta = unix_time(ret_int=True) - sim_start_time

            if (sim_delta >= exp_delta):
                if data_type == "text/numeric":
                    # new_time = str(sim_start_time + exp_delta)
                    # next_row = data[0].split('\t')
                    # next_row[0] = new_time
                    # next_row = '\t'.join(next_row)
                    # self.publish_data(device_id, next_row, data_delimiter='\t')
                    self.publish_data(device_id, data[0], data_delimiter='\t')
                elif data_type == "image":
                    img_filename = file_dir + '/' + re.split('\t', data[0])[1]
                    img_byte_string = read_image(img_filename)
                    self.publish_image(device_id, img_byte_string, timestamp=sim_start_time + sim_delta)
                else:
                    print("DATA_TYPE IN CONFIGURATION NOT SUPPORTED")
                del data[0]
                if len(data) == 0:
                    break
