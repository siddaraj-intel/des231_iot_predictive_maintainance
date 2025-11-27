#!/usr/bin/env python3

import json
import threading
import time
import csv
import os
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewPartitions
from kafka.errors import KafkaError
from kafka.admin import NewTopic

KAFKA_BROKERS_OUT = '127.0.0.38.9092, 127.0.0.189:9092, 127.0.0.199:9092'
KAFKA_TOPIC_OUT  = 'topic_sensor_input'

KAFKA_BROKERS_IN = '127.0.0.189:9092, 127.0.0.199.9092, 127.0.0.38:9092'
KAFKA_TOPIC_IN  = 'topic_create_delete'

# Global structures to manage threads
running_threads = {}
stop_events = {}
next_partition_id = 0
motor_partition_map = {}

THREAD_LOCK = threading.Lock()
def create_partition(partition_id, topic=KAFKA_TOPIC_OUT):
	""" if the partition_id with the matching <motor-id> exists, use it.
	    Else create a new partition for the new <motor-id>
	"""

	try:
		with THREAD_LOCK:
			admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS_OUT)

			# Check if topic exists, create if it doesn't
			topics = admin_client.list_topics()
			if topic not in topics:
				rf = 2  # Set desired replication factor
				np = 1  # Start with 1 partition
				new_topic = NewTopic(name=topic, num_partitions=np, replication_factor=rf)
				admin_client.create_topics([new_topic])
				print(f"Created new topic '{topic}' with {np}-partition and replication-factor={rf}")
				time.sleep(5)  # Wait for topic creation to propagate

			# Fetch partition info
			consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKERS_OUT)
			partitions = consumer.partitions_for_topic(topic)
			consumer.close()

			if partitions and partition_id in partitions:
				print(f"Partition {partition_id} already exists on topic: {topic}. Using the existing partition.")
				return True

			# Calculate required partition count
			required_partitions = partition_id + 1
			current_partitions = len(partitions) if partitions else 1

			if required_partitions > current_partitions:
				topic_partitions = {topic: NewPartitions(total_count=required_partitions)}
				admin_client.create_partitions(topic_partitions)
				print(f"Created partition {partition_id} on topic '{topic}'")

			admin_client.close()
			return True

	except Exception as e:
		print(f"Error managing partitions: {e}")
		return False

def motor_producer_thread(motor_id, partition_id, csv_file, start_line, end_line, stop_event):
	"""Thread function to produce sensor data from CSV"""

	# Check if CSV file exists
	if not os.path.exists(csv_file):
		print(f"Error: CSV file '{csv_file}' not found")
		return

	# Create partition if needed
	#if not check_create_partition(int(motor_id)):
	#	print(f"Failed to create/verify partition for motor {motor_id}")
	#	return

	# Initialize Kafka producer
	producer = KafkaProducer(
		bootstrap_servers=KAFKA_BROKERS_OUT,
		value_serializer=lambda v: json.dumps(v).encode('utf-8')
	)

	sensor_cols_names = [
		"sensor_00","sensor_01","sensor_02","sensor_03","sensor_04","sensor_05","sensor_06","sensor_07",
		"sensor_08","sensor_09","sensor_10","sensor_11","sensor_12","sensor_13","sensor_14","sensor_16",
		"sensor_17","sensor_18","sensor_19","sensor_20","sensor_21","sensor_22","sensor_23","sensor_24",
		"sensor_25","sensor_26","sensor_27","sensor_28","sensor_29","sensor_30","sensor_31","sensor_32",
		"sensor_33","sensor_34","sensor_35","sensor_36","sensor_37","sensor_38","sensor_39","sensor_40",
		"sensor_41","sensor_42","sensor_43","sensor_44","sensor_45","sensor_46","sensor_47","sensor_48",
		"sensor_49","sensor_50"
	]
	try:
		# Read CSV file
		with open(csv_file, 'r') as f:
			csv_reader = list(csv.reader(f))

		current_line = start_line
		last_message = None

		while not stop_event.is_set():
			# Get the current row (accounting for 0-based indexing)
			if current_line < len(csv_reader):
				row = csv_reader[current_line]
				# timestamp = row[1] if len(row) > 1 else time.strftime('%Y-%m-%d %H:%M:%S')
				read_timestamp = row[1] if len(row) > 1 else None
				epoch_timestamp = read_timestamp

				# convert back timestamp from string to Linux u64 epoch
				# try:
				# 	epoch_timestamp = int(time.mktime(time.strptime(read_timestamp), '%Y-%m-%d %H:%M:%S'))
				# except ValueError:
				# 	epoch_timestamp = int(time.time())

				sensor_col_values = row[2:-1] if len(row) > 3 else []
				sensor_col_values = [float(val) for val in sensor_col_values]
				sensor_key_val = dict(zip(sensor_cols_names, sensor_col_values))

				last_message = {
					#'MID': int(motor_id),
					'MID': motor_id,
					'seq_id': current_line,
					'timestamp': str(epoch_timestamp),
				}
				last_message.update(sensor_key_val)

			elif last_message is not None:
				pass # Reuse last message if no new lines are available
			else:
				print(f"Warning: No data available at line {current_line}")
				time.sleep(1)
				continue

			# Prepare message
			message = last_message

			# Send to Kafka
			producer.send(
				KAFKA_TOPIC_OUT,
				value=message,
				key=str(KAFKA_TOPIC_OUT).encode('utf-8'),
				#partition=None,
				#partition=int(motor_id),
				partition = partition_id,
			)
			producer.flush()

			print(f"Motor {motor_id}: Sent line {current_line} to partition {partition_id}")

			# Increment line number until end_line
			if current_line < end_line:
				current_line += 1

			# Wait for 1 second
			time.sleep(1)

	except Exception as e:
		print(f"Error in motor producer thread {motor_id}: {e}")

	finally:
		producer.close()
		print(f"Motor producer thread {motor_id} stopped")

def handle_start_command(msg):
	"""Handle start command"""
	motor_id = msg['id']
	csv_file = msg['csv_file']
	start_line = msg['start']
	end_line = msg['end']

	thread_name = f"motor_prod_thread_{motor_id}"

	# Check if thread already exists
	if motor_id in running_threads and running_threads[motor_id].is_alive():
		print(f"Thread {thread_name} already running")
		return

	# Create stop event
	stop_event = threading.Event()
	stop_events[motor_id] = stop_event

	# Create partition if needed
	if motor_id in motor_partition_map:
		partition_id = motor_partition_map[motor_id]

	else:
		global next_partition_id
		partition_id = next_partition_id
		next_partition_id = partition_id + 1

		if not create_partition(partition_id):
			print(f"Failed to create/verify partition-{partition_id} for motor {motor_id}")
			return

	# Create and start thread
	thread = threading.Thread(
		target=motor_producer_thread,
		args=(motor_id, partition_id, csv_file, start_line, end_line, stop_event),
		name=thread_name
	)
	thread.daemon = True
	thread.start()

	running_threads[motor_id] = thread
	motor_partition_map[motor_id] = partition_id
	print(f"Started {thread_name} for motor-{motor_id} with Partition: {partition_id}")

def handle_stop_command(msg):
	"""Handle stop command"""
	motor_id = msg['id']
	thread_name = f"motor_prod_thread_{motor_id}"

	# Check if thread exists
	if motor_id not in running_threads or not running_threads[motor_id].is_alive():
		print(f"Info: No active thread {thread_name} found")
		return

	# Signal thread to stop
	if motor_id in stop_events:
		stop_events[motor_id].set()
		running_threads[motor_id].join(timeout=5)
		print(f"Stopped {thread_name}")

	# Cleanup
	if motor_id in running_threads:
		del running_threads[motor_id]

	if motor_id in stop_events:
		del stop_events[motor_id]

def main():
	"""Main consumer loop"""
	# Retry connecting to Kafka broker until successful
	while True:
		try:
			consumer = KafkaConsumer(
				KAFKA_TOPIC_IN,
				bootstrap_servers=KAFKA_BROKERS_IN,
				value_deserializer=lambda m: json.loads(m.decode('utf-8')),
				auto_offset_reset='latest',
				enable_auto_commit=True
			)
			print(f"Successfully connected to Kafka broker at {KAFKA_BROKERS_IN}")
			break

		except Exception as e:
			print(f"Failed to connect to Kafka broker: {e}. Retrying in 5 seconds...")
			time.sleep(5)

	print(f"Motor producer started. Listening to {KAFKA_TOPIC_IN}...")



	try:
		while True:
			msg_pack = consumer.poll(timeout_ms=1000, max_records=1)
			for tp, msgs in msg_pack.items():
				for m in msgs:
					msg = m.value
					print(f"Received message: {msg}")

					if msg.get('cmd') == 'start':
						handle_start_command(msg)
					elif msg.get('cmd') == 'stop':
						handle_stop_command(msg)
					else:
						print(f"Unknown command: {msg.get('cmd')}")

	except KeyboardInterrupt:
		print("Shutting down...")
	finally:
		# Stop all running threads
		for motor_id in list(running_threads.keys()):
			if motor_id in stop_events:
				stop_events[motor_id].set()

		consumer.close()

		try:
			admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS_OUT)
			topics = admin_client.list_topics()
			if KAFKA_TOPIC_OUT in topics:
				admin_client.delete_topics([KAFKA_TOPIC_OUT])
				print(f"Deleted topic '{KAFKA_TOPIC_OUT}'")
		except KafkaError as e:
			print(f"Error deleting topic = {KAFKA_TOPIC_OUT}: {e}")

		print("Motor producer stopped")

if __name__ == "__main__":
	main()
