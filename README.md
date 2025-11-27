# des231_iot_predictive_maintainance
IOT Predictive Maintenance Project repository for DES231
HDFS Setup:
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /opt/hadoop

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk

Edit core-site.xml in namenode
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://<namenode-ip>:9000</value>
  </property>
</configuration>

Edit hdfs-site.xml in namenode
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>   <!-- Change to number of datanodes -->
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///hadoop/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///hadoop/datanode</value>
  </property>
</configuration>

Edit workers file
node1_ip
node2_ip

Create Storage Directories
sudo mkdir -p /hadoop/namenode
sudo mkdir -p /hadoop/datanode
sudo chown -R $USER:$USER /hadoop

Format NameNode
hdfs namenode -format

Start DFS on NameNode
start-dfs.sh

Verify HDFS
hdfs dfsadmin -report
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/hadoop
hdfs dfs -put localfile.txt /user/hadoop/
hdfs dfs -ls /user/hadoop/


Spark Setup Configurations:

cd /opt
wget https://archive.apache.org/dist/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz
tar -xzf spark-3.5.7-bin-hadoop3.tgz
mv spark-3.5.7-bin-hadoop3 spark

export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

Edit Master spark-env.sh in spark conf file with below exports
export SPARK_MASTER_HOST=<hostipaddress>
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

export SPARK_WORKER_CORES=3           # changes according number of cores to be used
export SPARK_WORKER_MEMORY=4g         # change according memory for worker
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081

Edit slaves file
<worker1 ip address>
<worker2 ip address>
<worker3 ip address>

Edit Workers spark-env.sh conf file with below exports
export SPARK_WORKER_CORES=3           # changes according number of cores to be used
export SPARK_WORKER_MEMORY=4g         # change according memory for worker
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081

To Start Cluster
Run
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slaves.sh

Download Below JARS to $SPARK_HOME/jars for Spark Kafka Inetgration
# Spark-Kafka integration
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.7/spark-token-provider-kafka-0-10_2.12-3.5.7.jar

# Kafka client
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar

# Commons pool
wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

Pre-Requisites to submit Spark Job:
pip install pyspark
pip install confluent-kafka

Command to Run Spark Inference Job:

spark-submit   --master spark://<Master IP>:7077   --jars "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar,/opt/spark/jars/kafka-clients-3.7.0.jar,/opt/spark/jars/commons-pool2-2.12.0.jar" --conf spark.dynamicAllocation.enabled=true   --conf spark.dynamicAllocation.minExecutors=1   --conf spark.dynamicAllocation.maxExecutors=6   --conf spark.dynamicAllocation.initialExecutors=1  --conf spark.dynamicAllocation.executorIdleTimeout=10 --conf spark.kafka.producer.buffer.memory=67108864 --conf spark.kafka.producer.batch.size=32768 --conf spark.kafka.producer.max.request.size=10485760 --conf spark.kafka.producer.linger.ms=50 spark_inference.py   --bootstrap <in kafka topic ip>:9092   --in_topic <input topic name> --out_topic <output_topic_name>   --checkpoint <checkpoint location>   --startingOffsets latest --out_bootstrap <out kafka broker ip> --replication=3 --partitions=5


Dashboard Pre-Requisites :

Check if Python is already installed . If not run below command
sudo yum install -y python3-pip

# confluent-kafka needs librdkafka
sudo yum install -y librdkafka

Install Python packages
pip3 install dash==2.17.1 plotly==5.23.0 confluent-kafka==2.6.0



check if all the pre-requisites are installed -  helper command to check

python3 -c "import plotly; print(f'plotly: {plotly.__version__}')"
python3 -c "import dash; print(f'dash: {dash.__version__}')"
python3 -c "import confluent_kafka; print('confluent-kafka: installed')"



To Run the Dashoard :

PORT=8051 python3 dashboard.py

Please note - the IP address of the KAKFA bootstrap is hardcoded here as per demo setup.

