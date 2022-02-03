import sys
sys.path.append(".")

import mdml_client as mdml

consumer = mdml.kafka_mdml_consumer(
    topic = "mdml-example-integer",
    group = "mdml-example-integer")

for msg in consumer.consume():
    print(msg)