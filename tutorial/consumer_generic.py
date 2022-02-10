#!/usr/bin/env python
import argparse

def main(args):
    import mdml_client as mdml
    # default for args.offset
    offset = 'earliest'
    if args.offset:
        offset = 'latest'
    # args.schemaHost defaults to args.host
    if args.schemaHost is None:
        args.schemaHost = args.host

    consumer = mdml.kafka_mdml_consumer(
        topics = args.topic,
        group = args.group,
        auto_offset_reset = offset,
        kafka_host = args.host,
        kafka_port = args.port,
        schema_host = args.schemaHost,
        schema_port = args.schemaPort)

    for msg in consumer.consume(overall_timeout=args.timeout):
        print(msg)
    consumer.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A generic MDML consumer for example purposes")
    parser.add_argument('-t', dest="topic", required=True,nargs='+',default=[],
                        help="Topic to consume")
    parser.add_argument('-g', dest="group", required=True,
                        help="Consumer group ID")
    parser.add_argument('--latest', dest="offset", type=bool, default=False,
                        help="Use for auto_offset_reset=latest")
    parser.add_argument('--timeout', dest="timeout", default=-1,
                        help="Consumer timeout after not receiving a message")
    parser.add_argument('-s', dest="host", required=True,
                        help="Hostname of the kafka broker")
    parser.add_argument('-port', dest="port", default=9092,
                        help="Kafka broker port number [default: 9092]")
    parser.add_argument('--schema-host', dest="schemaHost", default=None,
                        help="Schema registry host [defaults to the -s HOST argument]")
    parser.add_argument('--schema-port', dest="schemaPort", default=8081,
                        help="Schema registry port [default: 8081]")
    args = parser.parse_args()
    main(args)
