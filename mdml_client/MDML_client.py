import json
import math
import os
import time
import boto3
import requests
from base64 import b64encode, b64decode
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import NewTopic, AdminClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

py_type_to_schema_type = {
    str: "string",
    float: "number",
    int: "number",
    list: "array",
    dict: "object",
}

def chunk_file(fn, chunk_size, use_b64=True, encoding='utf-8', file_id=None):
    """
    Chunks a file into parts. Yields dictionaries 
    containing the file bytes encoded in base64. Base64 is used since
    the kafka Producer requires a string and some files must be opened in
    byte format.    
    
    Parameters
    ----------
    fn : str
        Path to the file 
    chunk_size : int
        Size of chunk to use
    use_b64 : bool
        True to return the file bytes as a base64 encoded string
    encoding : string
        Encoding to use to open the file if use_b64 is False  
    file_id : string
        File ID to use in the chunking process if the fn param is not suitable 
 
    Yields
    ------
    Dictionary containing a chunk of data and metadata information
    required to piece all of the chunks back together.
    """
    if use_b64:
        encoding = 'base64'
        with open(fn, 'rb') as f:
            file_bytes = f.read()
        file_dat = b64encode(file_bytes).decode('utf-8')
    else:
        with open(fn, 'r', encoding=encoding) as f:
            file_dat = f.read()
    file_len = len(file_dat)
    total_parts = math.ceil(file_len/chunk_size)
    part = 1
    while len(file_dat):
        chunk_bytes = file_dat[0:chunk_size]
        file_dat = file_dat[chunk_size:]
        dat = {
            'time': time.time(),
            'chunk': chunk_bytes,
            'part': f'{part}.{total_parts}',
            'filename': fn,
            'encoding': encoding
        }
        if file_id is not None:
            dat['filename'] = file_id
        part += 1
        yield dat

def start_experiment(id, topics, producer_kwargs={}):
    """
    Start an experiment with the MDML Experiment service.
    Messages produced on all of the specified topics will be saved
    to a file and upload to S3. 
    
    Parameters
    ----------
    id : str
        Unique ID for the experiment
    topics : list(str)
        Topics to consume from that make up the experiment
    """
    experiment_topics_schema = {
        "$schema": "http://merf.egs.anl.gov/mdml-experiment-service-schema#",
        "title": "ExperimentServiceSchema",
        "description": "Schema for Kafka MDML Experiments",
        "type": "object",
        "properties": {
            "time": {
                "description": "Sent timestamp",
                "type": "number"
            },
            "experiment_id": {
                "description": "A unique experiment ID",
                "type": "string"
            },
            "topics": {
                "description": "Topics under the experiment",
                "type": "array",
                "items": {
                    "type": "string"
                },
            },
            "status": {
                "description": "Experiment status",
                "type": "string"
            }
        },
        "required": [ "time", "experiment_id", "status" ]
    }
    producer = kafka_mdml_producer("mdml-experiment-service", schema=experiment_topics_schema, **producer_kwargs)
    producer.produce({
        "time": time.time(),
        "experiment_id": id,
        "topics": topics,
        "status": "on"
    })
    producer.flush()
    time.sleep(5)
    print("Experiment started")

def stop_experiment(id, producer_kwargs={}):
    """
    Stop a previously started experiment. Upon stopping, the experiment service
    will package all data streamed during an experiment, verify all data is
    present, and write a file to S3.
    
    Parameters
    ----------

    id : str
        Unique ID for the experiment
    producer_kwargs : dict
        Dictionary that is passed as kwargs to the underlying producer in this function
    """
    experiment_topics_schema = {
        "$schema": "http://merf.egs.anl.gov/mdml-experiment-service-schema#",
        "title": "ExperimentServiceSchema",
        "description": "Schema for Kafka MDML Experiments",
        "type": "object",
        "properties": {
            "time": {
                "description": "Sent timestamp",
                "type": "number"
            },
            "experiment_id": {
                "description": "A unique experiment ID",
                "type": "string"
            },
            "topics": {
                "description": "Topics under the experiment",
                "type": "array",
                "items": {
                    "type": "string"
                },
            },
            "status": {
                "description": "Experiment status",
                "type": "string"
            }
        },
        "required": [ "time", "experiment_id", "status" ]
    }
    producer = kafka_mdml_producer("mdml-experiment-service", schema=experiment_topics_schema, **producer_kwargs)
    producer.produce({
        "time": time.time(),
        "experiment_id": id,
        "status": "off"
    })
    producer.flush()
    print("Experiment stopped")

def upload_experiment_to_ADC(exp_id, group, ADC_SDL_TOKEN, study_id, producer_kwargs={}, consumer_kwargs={}):
    experiment_topics_schema = {
        "$schema": "http://merf.egs.anl.gov/mdml-experiment-upload-urls-schema#",
        "title": "ExperimentUploadURLSchema",
        "description": "Schema for Kafka MDML Experiment upload URL list",
        "type": "object",
        "properties": {
            "time": {
                "description": "Sent timestamp",
                "type": "number"
            },
            "name": {
                "description": "Upload name",
                "type": "string"
            },
            "user_name": {
                "description": "Upload user name",
                "type": "string"
            },
            "user_email": {
                "description": "Upload user email",
                "type": "string"
            },
            "url": {
                "description": "Upload URL",
                "type": "string"
            }
        },
        "required": [ "time", "name", "user_name", "user_email", "url" ]
    }
    url_producer = kafka_mdml_producer("mdml-experiment-upload-urls", schema=experiment_topics_schema, **producer_kwargs)
    data = []
    exp_consumer = kafka_mdml_consumer_schemaless([f"mdml-experiment-{exp_id}"], group, **consumer_kwargs)
    print("Gathering experiment data for upload.")
    for msg in exp_consumer.consume(overall_timeout=5, verbose=False):
        data.append(json.loads(msg['value']))
    if len(data) == 0:
        print("No experiment data found. You must use a unique group ID for each upload.")
        return
    for d in data:
        d['time'] = d['value']['time']
    data = sorted(data, key=lambda k: k['time'])
    # Save data messages to a JSON file
    with open(f'{exp_id}.json', 'w') as f:
        f.writelines(json.dumps(data))
    if study_id is None or ADC_SDL_TOKEN is None:
        Exception("cannot use method 'upload' without a study_id")
    else:
        from adc_sdk.client import ADCClient
        client = ADCClient(ADC_SDL_TOKEN)
        with open(f'{exp_id}.json', 'rb') as f:
            sample = client.create_sample(f,study_id,f"MDML experiment {exp_id}")
            print(type(sample))
            print(f"SAMPLE {sample} SAMPLE END")
    url_producer.produce({
        "time": time.time(),
        "name": sample['sample']['name'],
        "user_name": sample['sample']['user']['name'],
        "user_email": sample['sample']['user']['email'],
        "url": sample['sample']['url'],
    })
    url_producer.flush()
    os.remove(f"{exp_id}.json")
    return sample

def get_experiment_data(exp_id, ADC_TOKEN):
    """
    Return the data streamed during an experiment
    
    Parameters
    ----------
    exp_id : str
        Experiment ID
    ADC_TOKEN : str
        ADC SDK access token

    Return
    ------
    (url, data) : (str, list(dict))
        Tuple containing the ADC URL to the sample and a list 
        containing the data messsages streamed during the experiment
    """
    from adc_sdk.client import ADCClient
    MDML_STUDY_ID = "U3R1ZHlOb2RlOjMx" # ADC ID for MDML experiments Study
    client = ADCClient(ADC_TOKEN)
    study = client.get_study(MDML_STUDY_ID)
    exp_sample = None
    exp_url = None
    for sample in study['study']['samples']['edges']:
        # print(sample)
        if sample['node']['name'] == f"MDML experiment {exp_id}":
            exp_sample = sample
            exp_url = exp_sample['node']['url']
    if exp_sample is None:
        raise Exception("No experiment found with that ID.")
    resp = requests.get(exp_sample['node']['url'], verify=False)
    return (exp_url, resp.json())

# def replay_ADC_experiment(adc_sample_id, producer_kwargs={}):
#     """
    
#     Parameters
#     ==========
#     id : str
#         Unique ID of the experiment to replay
#     group : str
#         group to use when consuming the main experiment topic
#     replay : bool
#         Replay the experiment
#     producer_kwargs : dict
#         Dictionary of kwargs for this functions internal producer
#     """
#     experiment_replay_schema = {
#         "$schema": "http://merf.egs.anl.gov/mdml-experiment-replay-schema#",
#         "title": "ExperimentReplaySchema",
#         "description": "Schema for Kafka MDML Experiment Replays",
#         "type": "object",
#         "properties": {
#             "time": {
#                 "description": "Sent timestamp",
#                 "type": "number"
#             },
#             "adc_sample_id": {
#                 "description": "Argonne Data Cloud sample ID",
#                 "type": "string"
#             }
#         },
#         "required": [ "time", "adc_sample_id" ]
#     }
#     producer = kafka_mdml_producer("mdml-experiment-replay", schema=experiment_replay_schema, **producer_kwargs)
#     producer.produce({
#         "time": time.time(),
#         "adc_sample_id": adc_sample_id
#     })
#     producer.flush()

def replay_experiment(experiment_id, speed=1, producer_kwargs={}):
    """
    Replay an experiment - stream data back down their original topics
    
    Parameters
    ----------

    experiment_id : str
        Unique ID of the experiment to replay
    speed : int
        Speed multiplier used during the replay
    producer_kwargs : dict
        Dictionary of kwargs for this functions internal producer
    """
    experiment_replay_schema = {
        "$schema": "http://merf.egs.anl.gov/mdml-replay-service-schema#",
        "title": "ExperimentReplayServiceSchema",
        "description": "Schema for Kafka MDML Experiment Replays",
        "type": "object",
        "properties": {
            "time": {
                "description": "Sent timestamp",
                "type": "number"
            },
            "experiment_id": {
                "description": "Argonne Data Cloud sample ID",
                "type": "string"
            },
            "speed": {
                "description": "Speed to replay at",
                "type": "number"
            }
        },
        "required": [ "time", "experiment_id", "speed" ]
    }
    producer = kafka_mdml_producer("mdml-replay-service", schema=experiment_replay_schema, **producer_kwargs)
    producer.produce({
        "time": time.time(),
        "experiment_id": experiment_id,
        "speed": speed
    })
    producer.flush()

def create_schema(d, title, descr, required_keys=None, add_time=False):
    """
    Create a schema for use in a kafka_mdml_producer object.
    An example of the data object that will be produced is needed
    to create the schema.
    
    Parameters
    ----------
    d : dict
        Data object to translate into a schema
    title : str
        Title of the schema
    descr : str
        Description of the schema
    required_keys : list of str
        List of strings of the keys that are required in the schema

    Returns
    -------
    Schema dictionary compatible with kafka_mdml_producer

    """
    def get_property(key, dat, prop={}):
        try:
            dtype = py_type_to_schema_type[type(dat)]
        except:
            raise Exception("Unhandled type exception")
        if dtype == "array":
            item_type = py_type_to_schema_type[type(dat[key][0])]
            return {
                "type": "array",
                "items": {
                    "type": item_type
                }
            }
        elif dtype == "object":
            props = {}
            for k in dat:
                props[k] = get_property(k, dat[k])
            return {
                "type": "object",
                "properties": props
            }
        else:
            dtype = py_type_to_schema_type[type(dat)]
            return {
                "type": dtype
            }
    schema = {
        "$schema": f"http://merf.egs.anl.gov/mdml-{title}-auto-schema#",
        "title": title,
        "description": descr,
        "type": "object",
        "properties": {}
    }
    if required_keys is not None:
        schema['required'] = required_keys
    if add_time:
        d['mdml_time'] = time.time()
    for key in d.keys():
        schema['properties'][key] = get_property(key, d[key])
        # if dtype == "array":
        #     item_type = py_type_to_schema_type[type(d[key][0])]
        #     schema['properties'][key] = {
        #         "type": "array",
        #         "items": {
        #             "type": item_type
        #         }
        #     }
        # if dtype == "object":
        #     item_type = py_type_to_schema_type[type(d[key][0])]
        #     schema['properties'][key] = {
        #         "type": "array",
        #         "items": {
        #             "type": item_type
        #         }
        #     }
        # else:
        #     schema['properties'][key] = {
        #         "type": dtype
        #     }
    return schema

class kafka_mdml_producer_schemaless:
    """
    Creates a schemaless Producer instance for interacting with the MDML.

    Parameters
    ----------
    topic : str
        Topic to send under
    config: dict
        Confluent Kafka client config
    kafka_host : str
        Host name of the kafka broker
    kafka_port : int
        Port used for the Kafka broker
    """
    def __init__(self, topic, config=None,
                kafka_host="merf.egs.anl.gov", kafka_port=9092):
        # Checking topic param
        if type(topic) == str:
            if topic[0:5] != "mdml-":
                raise Exception("Error, topic must be of the form 'mdml-<experiment id>-<sensor>'")
            else:
                self.topic = topic
        else:
            raise Exception("Error, topic must be of type string.")
        # Create producer and its config 
        # Create producer and its config 
        if config is None:
            producer_config = {
                'bootstrap.servers': f'{kafka_host}:{kafka_port}'
            }
        else:
            producer_config = config
        self.producer = Producer(producer_config)
    def produce(self, data, key=None, partition=None):
        """
        Produce data to the supplied topic

        Parameters
        ----------
        data : dict
            Dictionary of the data
        key : string
            Key of the message (used in determining a partition) - not required
        partition : int
            Partition used to save the message - not required
        """
        if partition is None:
            self.producer.produce(topic=self.topic, value=data, key=key)
        else:
            self.producer.produce(topic=self.topic, value=data, key=key, partition=partition)
    def flush(self):
        """
        Flush (send) any messages currently waiting in the producer.
        """
        self.producer.flush()

class kafka_mdml_producer:
    """
    Creates a producer instance for producing data to an MDML instance. 
    
    Parameters
    ----------
    topic : str
        Topic to send under 
    schema : dict or str
        JSON schema for the message value. If dict, value is used as the 
        schema. If string, value is used as a file path to a json file.
    config : dict
        Confluent Kafka client config
    kafka_host : str
        Host name of the kafka broker
    kafka_port : int
        Port used for the kafka broker
    schema_host : str
        Host name of the kafka schema registry
    schema_port : int
        Port of the kafka schema registry
    """
    def __init__(self, topic, schema=None, config=None, add_time=True,
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
        schema_registry_conf = {
            "url": f"http://{schema_host}:{schema_port}"
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        # Checking schema param
        if schema is None:
            try:
                # Look up schema here
                registeredSchema = schema_registry_client.get_latest_version(f"{self.topic}-value")
                schema = registeredSchema.schema.schema_str
                self.schema = schema
            except:
                raise Exception("No schema found for the given topic. One must be supplied.")
        else:
            if type(schema) == dict:
                self.schema = json.dumps(schema)
            elif type(schema) == str:
                with open(schema,"r") as f:
                    self.schema = f.read()
            else:
                raise Exception("Error, schema must be of type str or dict.")
        json_serializer = JSONSerializer(self.schema, schema_registry_client)
        # Create producer and its config 
        if config is None:
            producer_config = {
                'bootstrap.servers': f'{kafka_host}:{kafka_port}',
                'value.serializer': json_serializer
            }
        else:
            producer_config = config
        self.add_time = add_time
        self.producer = SerializingProducer(producer_config)
    def produce(self, data, key=None, partition=None):
        """
        Produce data to the supplied topic 

        Parameters
        ----------
        data : dict
            Dictionary of the data
        key : str
            String for the Kafka assignor to use to calculate a partition
        partition : int
            Number of the partition to assign the message to
        """
        if self.add_time:
            data['mdml_time'] = time.time()
        if partition is None:
            self.producer.produce(topic=self.topic, value=data, key=key)
        else:
            self.producer.produce(topic=self.topic, value=data, key=key, partition=partition)
    def flush(self):
        """
        Flush (send) any messages currently waiting in the producer.
        """
        self.producer.flush()

class kafka_mdml_consumer:
    """
    Creates a consumer to consume messages from an MDML instance. 
    
    Parameters
    ----------
    topics : list(str)
        Topics to consume from 
    group : str
        Consumer group ID. Messages are only consumed by a given group ID
        once.
    auto_offset_reset : str
        'earliest' or 'latest'. 'earliest' is the default and will start consuming
        messages from where the consumer group left off. 'latest' will start
        consuming messages from the time that the consumer is started. 
    show_mdml_time : bool
        Indicator to show the mdml_time field of a message 
    kafka_host : str
        Host name of the kafka broker
    kafka_port : int
        Port used for the kafka broker
    schema_host : str
        Host name of the kafka schema registry
    schema_port : int
        Port of the kafka schema registry
    """
    def __init__(self, topics, group, auto_offset_reset="earliest",
                show_mdml_time=True,
                kafka_host="merf.egs.anl.gov", kafka_port=9092,
                schema_host="merf.egs.anl.gov", schema_port=8081):
        self.topics = topics
        self.group = group
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.schema_host = schema_host
        self.schema_port = schema_port
        self.deserializers = {}
        # Checking topic param
        if type(topics) == list:
            for topic in topics:
                if type(topic) == str:
                    if topic[0:5] != "mdml-":
                        raise Exception("Error, topic must be of the form 'mdml-<experiment id>-<sensor>'")
                    else:
                        # Create schema registry config, client, and serializer
                        sr_config = {
                            "url": f"http://{schema_host}:{schema_port}"
                        }
                        self.sr_client = SchemaRegistryClient(sr_config)
                        try:
                            schema_string = self.sr_client.get_latest_version(f'{topic}-value').schema.schema_str
                            self.deserializers[topic] = JSONDeserializer(schema_string)
                        except:
                            self.deserializers[topic] = None
                else:
                    raise Exception("Error, topic must be of type string.")
        else:
            raise Exception("Error, topics parameter must be a list of strings.")
        # Topic creation is needed
        AC = AdminClient({'bootstrap.servers': self.kafka_host})
        for topic in topics:
            res = AC.create_topics([NewTopic(topic, 10)])
            res = res[topic]
            if res.exception() is None:
                print("Topic created since it did not exist yet.")
                continue
            else:
                reason = res.exception().args[0].name()
                if reason == "TOPIC_ALREADY_EXISTS":
                    continue
                else:
                    print(f"ERROR creating topic {reason}") 
        # .exception().args[0].name()
        consumer_conf = {
            'bootstrap.servers': f"{kafka_host}:{kafka_port}",
            'group.id': group,
            'auto.offset.reset': auto_offset_reset,
            'allow.auto.create.topics': 'true' # prevents unknown topic error 
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe(topics)
        self.consumer = consumer
        self.show_mdml_time = show_mdml_time

    def consume(self, poll_timeout=1.0, overall_timeout=300.0, verbose=True):
        """
        Start consuming from the specified topic

        Parameters
        ----------

        poll_timeout : float
            Timeout to wait when consuming one message
        overall_timeout : float
            Timeout to wait until the consume generator is closed down.
            This timeout is restarted every time a new message is received
        verbose : bool
            Print a message with notes when the consume loop starts

        Yields
        ------
        dict
            A dictionary containing the topic and value of a single message  
        """
        if verbose:
            if overall_timeout != -1:
                print(f"Consumer loop will exit after {overall_timeout} seconds without receiving a message or with Ctrl+C")
            else:
                print(f"Consumer loop will run indefinitely until a Ctrl+C")
        timeout = 0.0
        while timeout < overall_timeout or overall_timeout == -1:
            try:
                msg = self.consumer.poll(poll_timeout)
                if msg is None:
                    timeout += poll_timeout
                    continue # no messages within timeout - poll again 
                if self.deserializers[msg.topic()] is None:
                    if "topic not available" in msg.value().decode('utf-8'):
                        continue # default message from broker the topic hasn't been created - poll again
                    else:
                        schema_string = self.sr_client.get_latest_version(f'{msg.topic()}-value').schema.schema_str
                        self.deserializers[msg.topic()] = JSONDeserializer(schema_string)
                timeout = 0.0
                val = self.deserializers[msg.topic()](msg.value(), {})
                if not self.show_mdml_time:
                    if 'mdml_time' in val:
                        del val['mdml_time']
                yield {
                    'topic': msg.topic(),
                    'value': val
                }
            except KeyboardInterrupt:
                break
        
    def consume_chunks(self, poll_timeout=1.0, overall_timeout=300.0, save_file=True, save_dir='.', passthrough=True, verbose=True):
        """
        Consume messages from a topic that contains chunked messages.
        The original file is saved to disk by default. 
        
        Parameters
        ----------
        poll_timeout : float
            Timeout for one message to reach the consumer 
        overall_timeout : float
            Time until the consumer will be shutdown if no messages 
            are received 
        save_file : bool
            True if the chunked file should be saved. False will
            return the original data contained in the file
        save_dir : str
            Directory to save files
        passthrough : bool
            If multiple topics are subscribed to and one of them is 
            not using chunking, passthrough=True will ensure those 
            messages are still yielded by the generator
        verbose : bool
            Print details regarding the consumer on start

        Yields
        ------
        tuple
            A tuple containing (timestamp, data) where timestamp is the 
            time the first chunk of the message was sent and where data 
            is either a filepath (save_file=True) or the bytes of the 
            file that was chunked and streamed (save_file=False). 
        dict 
            If passthrough=True is used and a message from a topic without
            chunking is received, a dictionary containing the topic and
            value of the message will be yielded. 

        """
        if verbose:
            if overall_timeout != -1:
                print(f"Consumer loop will exit after {overall_timeout} seconds without receiving a message or with Ctrl+C")
            else:
                print(f"Consumer loop will run indefinitely until a Ctrl+C")
        timeout = 0.0
        files = {}
        while timeout < overall_timeout or overall_timeout == -1:
            try:
                msg = self.consumer.poll(poll_timeout)
                if msg is None:
                    timeout += poll_timeout
                    continue # no messages within timeout - poll again
                if self.deserializers[msg.topic()] is None:
                    if "topic not available" in msg.value().decode('utf-8'):
                        continue # default message from broker the topic hasn't been created - poll again
                    else: 
                        schema_string = self.sr_client.get_latest_version(f'{msg.topic()}-value').schema.schema_str
                        self.deserializers[msg.topic()] = JSONDeserializer(schema_string)
                timeout = 0.0
                value = self.deserializers[msg.topic()](msg.value(), {})
                if passthrough:
                    if 'chunk' not in value:
                        if self.show_mdml_time:
                            if 'mdml_time' in value:
                                del value['mdml_time']
                            yield {
                                'topic': msg.topic(),
                                'value': value
                            }
                fn = value['filename']
                part_info = value['part'].split('.')
                if fn in files:
                    files[fn][part_info[0]] = value['chunk']
                else:
                    files[fn] = {
                        'parts': part_info[1],
                        part_info[0]: value['chunk'] 
                    }
                if int(part_info[0]) == 1:
                    files[fn]['time'] = value['time']
                if len(files[fn].keys()) == (int(files[fn]['parts']) + 2):
                    dat = ''
                    for i in range(int(files[fn]['parts'])):
                        dat += files[fn][str(i+1)]
                    if value['encoding'] == 'base64':
                        dat_bytes = b64decode(dat)
                        if save_file:
                            with open(f'{save_dir}/{os.path.basename(fn)}', 'wb') as f:
                                f.write(dat_bytes)
                            ret = os.path.basename(fn)
                        else:
                            ret = dat_bytes
                    else:
                        if save_file:
                            with open(f'{save_dir}/{os.path.basename(fn)}', 'w', encoding=value['encoding']) as f:
                                f.write(dat)
                        else:
                            ret = dat.decode(value['encoding'])                     
                    timestamp = files[fn]['time']
                    del files[fn]
                    yield timestamp, ret
            except KeyboardInterrupt:
                break
    def close(self):
        """
        Closes down the consumer. Ensures that received 
        messages have been acknowledged by Kafka.
        """
        self.consumer.close()

class kafka_mdml_consumer_schemaless:
    """
    Creates a serializingProducer instance for interacting with the MDML. 
    
    Parameters
    ----------
    topics : list(str)
        Topics to consume from 
    group : str
        Consumer group ID. Messages are only consumed by a given group ID
        once.
    kafka_host : str
        Host name of the kafka broker
    kafka_port : int
        Port used for the kafka broker

    """
    def __init__(self, topics, group, 
                kafka_host="merf.egs.anl.gov", kafka_port=9092):
        self.topics = topics
        self.group = group
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        # Checking topic param
        if type(topics) == list:
            for topic in topics:
                if type(topic) == str:
                    if topic[0:5] != "mdml-":
                        raise Exception("Error, topic must be of the form 'mdml-<experiment id>-<sensor>'")
                else:
                    raise Exception("Error, topic must be of type string.")
        else:
            raise Exception("Error, topics parameter must be a list of strings.")
        # Topic creation is needed
        AC = AdminClient({'bootstrap.servers': self.kafka_host})
        for topic in topics:
            res = AC.create_topics([NewTopic(topic, 10)])
            res = res[topic]
            if res.exception() is None:
                print("Topic created since it did not exist yet.")
                continue
            else:
                reason = res.exception().args[0].name()
                if reason == "TOPIC_ALREADY_EXISTS":
                    continue
                else:
                    print(f"ERROR creating topic {reason}") 

        consumer_conf = {
            'bootstrap.servers': f"{kafka_host}:{kafka_port}",
            'group.id': group,
            'auto.offset.reset': 'earliest',
            'allow.auto.create.topics': 'true' # prevents unknown topic error 
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe(topics)
        self.consumer = consumer

    def consume(self, poll_timeout=1.0, overall_timeout=300.0, verbose=True):
        """
        Yields
        ------
        dict
            A dictionary containing the topic and value of a single message 
        """
        if verbose:
            if overall_timeout != -1:
                print(f"Consumer loop will exit after {overall_timeout} seconds without receiving a message or with Ctrl+C")
            else:
                print(f"Consumer loop will run indefinitely until a Ctrl+C")
        timeout = 0.0
        while timeout < overall_timeout or overall_timeout == -1:
            try:
                msg = self.consumer.poll(poll_timeout)
                if msg is None:
                    timeout += poll_timeout
                    continue # no messages within timeout - poll again 
                timeout = 0.0
                yield {
                    'topic': msg.topic(),
                    'value': msg.value()
                }
            except KeyboardInterrupt:
                break
    def close(self):
        """
        Closes down the consumer. Ensures that received 
        messages have been acknowledged by Kafka.
        """
        self.consumer.close()

class kafka_mdml_s3_client:
    """
    Creates an MDML producer for sending >1MB files to an s3 location. Simultaneously, the MDML sends 
    upload information along a Kafka topic to be received by a client that can retrieve the file. 
    
    Parameters
    ----------
    topic : str
        Topic to send under
    s3_endpoint : str
        Host of the S3 service
    s3_access_key : str
        S3 access key
    s3_secret_key : str
        S3 secret key
    kafka_host : str
        Host name of the kafka broker
    kafka_port : int
        Port used for the kafka broker
    schema_host : str
        Host name of the kafka schema registry
    schema_port : int
        Port of the kafka schema registry
    schema : dict or str
        Schema of the messages sent on the supplied topic. Default schema
        sends a dictionary containing the time of upload and the location 
        for retrieval. If dict, value is used as the schema. If string, 
        value is used as a file path to a json file.
    """
    def __init__(self, topic, 
                s3_endpoint=None, s3_access_key=None, s3_secret_key=None,
                kafka_host="merf.egs.anl.gov", kafka_port=9092,
                schema_host="merf.egs.anl.gov", schema_port=8081,
                schema=None):
        # Checking topic param
        if type(topic) == str:
            if topic[0:5] != "mdml-":
                raise Exception("Error, topic must be of the form 'mdml-<experiment id>-<sensor>'")
            else:
                self.topic = topic
                # Parsing topic to determine S3 save location
                topic_parts = self.topic.split('-')
                self.bucket = f"{topic_parts[0]}-{topic_parts[1]}"
        else:
            raise Exception("Error, topic must be of type string.")
        self.schema = schema
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.schema_host = schema_host
        self.schema_port = schema_port
        if schema is None:
            self.schema = {
                "$schema": "http://merf.egs.anl.gov/mdml-s3-notification-schema#",
                "title": "MDML-S3-Upload-Notification",
                "description": "Default S3 Upload notification",
                "type": "object",
                "properties": {
                    "time": {
                        "description": "Time of upload",
                        "type": "number"
                    },
                    "s3_bucket": {
                        "description": "S3 bucket the file is stored in.",
                        "type": "string"
                    },
                    "s3_object_name": {
                        "description": "Object name/key of the file within the S3 bucket.",
                        "type": "string"
                    }
                },
                "required": [ "time", "s3_bucket", "s3_object_name" ]
            }
        else:
            self.schema = schema 
        # Creating boto3 (s3) client connection
        try:
            session = boto3.session.Session()
            self.s3_client = session.client(
                service_name='s3',
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
                endpoint_url=s3_endpoint
            )
        except Exception as e:
            print("ERROR creating connection to the S3 endpoint!")
            print(e)
        # Creating Kafka producer
        self.producer = kafka_mdml_producer(
            topic, self.schema, 
            self.kafka_host, self.kafka_port,
            self.schema_host, self.schema_port
        )

    def produce(self, filepath, obj_name, payload=None):
        """
        Produce data to supplied S3 endpoint and Kafka topic 

        Parameters
        ----------
        filepath : str
            Path of the file to upload to the S3 bucket 
        obj_name : str
            Name to store the file under  
        payload : dict
            Payload for the message sent on the Kafka topic.
            Only used when the default schema has been overridden.
        """
        # Default payload
        if payload is None:
            payload = {
                'filepath': filepath,
                'obj_name': obj_name
            }
        # Upload to s3
        self.s3_client.upload_file(filepath, self.bucket, obj_name)
        # Publish it
        self.producer.produce({
            'time': time.time(),
            's3_bucket': self.bucket,
            's3_object_name': obj_name
        })
    def consume(self, bucket, object_name, save_filepath):
        """
        Gets a file from an S3 bucket. Can return the bytes of the file 
        or save the file to a specified path.

        Parameters
        ----------
        bucket : str
            Name of the bucket the object is saved in
        object_name : str
            Name/key of the object to retrieve from the bucket
        save_filepath : str
            Path in which to save the downloaded file. Using a value of None
            will return the bytes of the file instead of saving to a file
        """
        try:
            resp = self.s3_client.get_object(Bucket=bucket, Key=object_name)
        except Exception as e:
            print("ERROR getting object!")
            print(e)
        if save_filepath is None:
            return resp['Body'].read()
        else:
            with open(save_filepath, 'wb') as f:
                f.write(resp['Body'].read())
