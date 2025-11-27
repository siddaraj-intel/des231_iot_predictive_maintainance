# des231_iot_predictive_maintainance
IOT Predictive Maintenance Project repository for DES231




















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

