#!/usr/bin/env python
import argparse

def main(args):
    import mdml_client as mdml
    print(args)
    pro_kwargs = {
        'kafka_host': args.host,
        'kafka_port': args.port,
        'schema_host': args.schemaHost,
        'schema_port': args.schemaPort
    }
    mdml.replay_experiment(args.exp_id, int(args.speed), producer_kwargs=pro_kwargs)
    # Starting a consumer to see the replay
    print("Starting a consumer to see the experiment replay in action.")
    consumer = mdml.kafka_mdml_consumer(
        topics = args.topics,
        group = args.group,
        kafka_host = args.host,
        kafka_port = args.port,
        schema_host = args.schemaHost,
        schema_port = args.schemaPort)
    i = 0
    for msg in consumer.consume(overall_timeout=-1):
        i += 1
        print(msg)
    consumer.close()
    print(f"{i} msgs consumed")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Admin tool run an MDML replay")
    parser.add_argument('-t', dest="topics", required=True,nargs='+',default=[],
                        help="Topic to consume")
    parser.add_argument('-g', dest="group", required=True,
                        help="Consumer group ID")
    parser.add_argument('-s', dest="host", required=True,
                        help="Hostname of the kafka broker")
    parser.add_argument('-i', dest="exp_id", required=True,
                        help="Experiment ID")
    parser.add_argument('--speed', dest="speed", default=1,
                        help="Replay speed")
    parser.add_argument('-port', dest="port", default=9092,
                        help="Kafka broker port number [default: 9092]")
    parser.add_argument('--schema-host', dest="schemaHost", default=None,
                        help="Schema registry host [defaults to -s HOST parameter]")
    parser.add_argument('--schema-port', dest="schemaPort", default=8081,
                        help="Schema registry port [default: 8081]")
    args = parser.parse_args()
    # args.schemaHost defaults to args.host
    if args.schemaHost is None:
        args.schemaHost = args.host
    main(args)
