# CR1000 Fetcher Project

Featches data from a CR1000 Sensor and pushes to MQTT for ingestion

## Getting Started

### Prerequisites

You will need InfluxDB running, as well as MQTT. You will also need access to a data file with CR1000 water quality readings. You will also need docker and docker-compose if you want to use the pre-configured setups.

### Starting the CR1000 Fetcher using dockermanually

You can run the CR1000 fetcher directly from your Python virtual environment. You will need to ensure you already have the ingestion source running (eg. MQTT):

```bash
pip install -r requirements.txt

python cr1000-data-fetcher.py -c cr1000-config.yaml -i CR1000.dat --mqtt_topic topic/cr1000-sensor --mqtt_port 1883 --mqtt_host localhost --silent --sleep_for 10
```
