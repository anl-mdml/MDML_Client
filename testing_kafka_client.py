import sys
sys.path.append(".")
import time
import random
import mdml_client as mdml

producer = mdml.kafka_mdml_producer(
    topic = "mdml-example-integer",
    schema = "../kafka-mdml/json_schemas/example_integer.json")

try:
    i = 0
    while True:
        dat = {
            'id': i,
            'time': time.time(),
            'int1': random.randint(0,25),
            'int2': random.randint(25,50),
            'int3': random.randint(50,100) 
        }
        producer.produce(dat)
        time.sleep(5)
        i += 1
except KeyboardInterrupt:
    print("Quitting...")