#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, trim, to_json, struct, when, coalesce, lit, to_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import PipelineModel
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

# -------------------------------
# 1. Argument parser
# -------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", required=True)
parser.add_argument("--in_topic", default="sensors")
parser.add_argument("--out_topic", default="predictions")
parser.add_argument("--checkpoint", default="/tmp/spark_chk_infer")
parser.add_argument("--startingOffsets", default="latest")
parser.add_argument("--out_bootstrap", required=True)
parser.add_argument("--replication", default=1, type=int)
parser.add_argument("--partitions", default=1, type=int)
args = parser.parse_args()

status_model_path = "hdfs://10.66.245.207:9000/models/model_class"
rul_model_path    = "hdfs://10.66.245.207:9000/models/model_reg"

# -------------------------------
# 2. Spark session
# -------------------------------
spark = SparkSession.builder.appName("PredictiveMaintenance").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# 3. Sensor columns & schema
# -------------------------------
sensor_cols = [
    "sensor_00","sensor_01","sensor_02","sensor_03","sensor_04","sensor_05","sensor_06","sensor_07",
    "sensor_08","sensor_09","sensor_10","sensor_11","sensor_12","sensor_13","sensor_14","sensor_16",
    "sensor_17","sensor_18","sensor_19","sensor_20","sensor_21","sensor_22","sensor_23","sensor_24",
    "sensor_25","sensor_26","sensor_27","sensor_28","sensor_29","sensor_30","sensor_31","sensor_32",
    "sensor_33","sensor_34","sensor_35","sensor_36","sensor_37","sensor_38","sensor_39","sensor_40",
    "sensor_41","sensor_42","sensor_43","sensor_44","sensor_45","sensor_46","sensor_47","sensor_48",
    "sensor_49","sensor_50"
]

schema = StructType([StructField("MID", StringType(), True),
                     StructField("timestamp", StringType(), True)] +
                    [StructField(c, DoubleType(), True) for c in sensor_cols])

# -------------------------------
# 4. Create Kafka output topic if missing
# -------------------------------
def create_kafka_topic_confluent(bootstrap_servers, topic_name, num_partitions=1, replication_factor=1):
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic '{topic}' created successfully.")
            except KafkaException as e:
                if "Topic with same name already exists" in str(e):
                    print(f"Topic '{topic}' already exists. Skipping creation.")
                else:
                    print(f"Failed to create topic '{topic}': {e}")
    except Exception as e:
        print(f"Unexpected error while creating topic: {e}")

create_kafka_topic_confluent(args.out_bootstrap, args.out_topic, args.partitions, args.replication)

# -------------------------------
# 5. Read Kafka streaming source
# -------------------------------
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", args.bootstrap)
       .option("subscribe", args.in_topic)
       .option("startingOffsets", args.startingOffsets)
       .option("failOnDataLoss", False)
       .load())

df = raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
df = df.withColumn("timestamp_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# -------------------------------
# 6. Process batch function
# -------------------------------
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # -------------------------------
    # 6a. Replace nulls with 0 (fastest)
    # -------------------------------
    filled_df = batch_df.fillna(0.0,subset=sensor_cols)

    # -------------------------------
    # 6b. Assemble features
    # -------------------------------
    assembler = VectorAssembler(inputCols=sensor_cols, outputCol="features")
    features_df = assembler.transform(filled_df)

    # -------------------------------
    # 6c. Load models inside batch
    # -------------------------------
    STATUS_MODEL = PipelineModel.load(status_model_path)
    RUL_MODEL = PipelineModel.load(rul_model_path)

    # -------------------------------
    # 6d. Predict
    # -------------------------------
    pred_status = STATUS_MODEL.transform(features_df).withColumnRenamed("predicte_machine_failure_in_24hrs", "status_pred")
    pred_rul = RUL_MODEL.transform(features_df).withColumnRenamed("predicted_hours_remaining_healthy", "rul_pred")

    # Clean columns
    pred_status_clean = pred_status.select(
        trim(col("MID")).alias("MID_clean"),
        trim(col("timestamp")).alias("timestamp_clean"),
        "status_pred"
    )
    pred_rul_clean = pred_rul.select(
        trim(col("MID")).alias("MID_clean"),
        trim(col("timestamp")).alias("timestamp_clean"),
        "rul_pred"
    )

    # Join predictions
    pred_final = pred_status_clean.join(
        pred_rul_clean,
        on=["MID_clean", "timestamp_clean"],
        how="inner"
    ).withColumnRenamed("MID_clean", "MID") \
     .withColumnRenamed("timestamp_clean", "timestamp") \
     .withColumn("status_label", when(col("status_pred") == 1, "broken").otherwise("normal"))

    # -------------------------------
    # 6e. Write to Kafka
    # -------------------------------
    out_df = pred_final.select(to_json(struct("MID", "timestamp", "status_label", "rul_pred")).alias("value"))

    out_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.out_bootstrap) \
        .option("topic", args.out_topic) \
        .save()

# -------------------------------
# 7. Start streaming query
# -------------------------------
query = df.writeStream.foreachBatch(process_batch) \
    .option("checkpointLocation", args.checkpoint) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

