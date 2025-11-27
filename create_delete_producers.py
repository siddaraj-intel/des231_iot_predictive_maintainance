#!/usr/bin/env python3

import argparse
import csv
import json
import os
from kafka import KafkaProducer

KAFKA_MSG_BROKER = '127.0.0.189:9092, 127.0.0.199:9092, 127.0.0.38:9092'

def produce_start(producer, topic, csv_file, start, end, motor_id, cmd):
	csv_file = os.path.abspath(csv_file)
	if not os.path.exists(csv_file):
		#raise FileNotFoundError(f"CSV file '{csv_file}' does not exist")
		print(f"WARN: CSV file '{csv_file}' not found")
	else:
		if os.path.getsize(csv_file) == 0:
			raise ValueError(f"CSV file '{csv_file}' is empty")

	message = {
		'id': motor_id,
		'cmd': cmd,
		'csv_file': csv_file,
		'start': start,
		'end': end,
	}
	producer.send(topic, value=message)
	print(f"Produced: {message}")

def produce_stop(producer, topic, motor_id, cmd):
	message = {
		'id': motor_id,
		'cmd': cmd
	}
	producer.send(topic, value=message)
	print(f"Produced: {message}")

def main():
	parser = argparse.ArgumentParser(description='Kafka producer for create/delete commands.')
	parser.add_argument('--csv-file', type=str, help='CSV file with sensor data')
	parser.add_argument('--start', type=int, help='Start line number (inclusive)')
	parser.add_argument('--end', type=int, help='End line number (inclusive)')
	parser.add_argument('--id', required=True, type=str, help='Motor name (e.g., motor1)')
	parser.add_argument('--cmd', required=True, choices=['start', 'stop'], help='Command: start or stop')

	args = parser.parse_args()

	producer = KafkaProducer(
		bootstrap_servers=KAFKA_MSG_BROKER,
		value_serializer=lambda v: json.dumps(v).encode('utf-8')
	)

	topic = 'topic_create_delete'

	if args.cmd == 'start':
		if not (args.csv_file and args.start and args.end):
			parser.error('--csv-file, --start, and --end are required for start command')
		produce_start(producer, topic, args.csv_file, args.start, args.end, args.id, args.cmd)
	elif args.cmd == 'stop':
		produce_stop(producer, topic, args.id, args.cmd)

	producer.flush()
	producer.close()

if __name__ == '__main__':
	main()
