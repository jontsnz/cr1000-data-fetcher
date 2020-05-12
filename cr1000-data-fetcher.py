"""Fetch CR1000 Data from Source and Push to MQTT for Ingestion
"""

import os
import sys
import argparse
import yaml
from abc import ABC, abstractmethod
import csv
import json
import time
import paho.mqtt.client as mqtt

class ReadingsOutputter(ABC):

    @abstractmethod
    def output(self, readings):
        pass

class ScreenJsonOutputter(ReadingsOutputter):

    def __init__(self):
        super().__init__()
    
    def output(self, readings):
        print(json.dumps(dict(readings)))

class CSVOutputter(ReadingsOutputter):

    def __init__(self, filename):
        super().__init__()
        self._csvfile = csv.writer(open(filename, 'w'), quoting = csv.QUOTE_NONNUMERIC)
        self._first_line = True

    def output(self, readings):
        print('Writing line to CSV file...%s' % (readings[0][1]))
        if self._first_line:
            self._csvfile.writerow(t[0] for t in readings)
            self._first_line = False
        self._csvfile.writerow(t[1] for t in readings)

class MqttOutputter(ReadingsOutputter):

    def __init__(self, host, port, topic):
        super().__init__()
        self._client = mqtt.Client()
        self._topic = topic
        keepalive = 60
        self._client.connect(host,port,keepalive)
        self.silent = False
 
    def output(self, readings):
        if not self.silent:
            print('Pushing readings to MQTT...%s' % (readings[0][1]))
        self._client.publish(self._topic,json.dumps(dict(readings)))

    def __del__(self):
        self._client.disconnect()

class Station:
    def __init__(self, station_config):
        self.station_name = station_config['station_name']
        self.sensors = []
        for sensor_config in station_config['sensors']:
            self.sensors.append(Sensor(sensor_config))

def fetch_readings_from_file(config, in_file, outputter, sleep_for):
    print('Reading lines from file...')
    cnt = 0
    headers = []
    for line in in_file:
        cnt += 1
        # print('%d %s' % (cnt, line))
        if cnt == 2:
            headers = line.replace('"','')[:-1].split(',')
            print(headers)
        if cnt >= 5:
            vals = line[:-1].split(',')
            readings = []
            for i in range(len(headers)):
                val = vals[i]
                if '"' in val:
                    val = val.replace('"','')
                elif '.' in val:
                    val = float(val) 
                else:
                    val = int(val)
                if val != 'NAN':
                    readings.append((headers[i],val))
            # print(readings)
            outputter.output(readings)
        if cnt % 1000 == 0:
            print('%d readings sent to queue' % cnt)
            if sleep_for:
                time.sleep(sleep_for)

    print('Done! %d readings sent to queue' % cnt)     

def main(arguments):

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--configfile', help="Config file")
    parser.add_argument('-i', '--inputfile', help="Input file", required=False)
    parser.add_argument('--silent', action='store_true', help="Silent mode")
    parser.add_argument('--mqtt_topic', help="The MQTT topic to publish", required=False)
    parser.add_argument('--mqtt_hostname', help="The MQTT hostname", required=False, default='localhost')
    parser.add_argument('--mqtt_port', help="The MQTT port", required=False, type=int, default=1883)
    parser.add_argument('--sleep_for', help="Secodns to sleep every 1000 readinss", required=False, type=int)
    args = parser.parse_args(arguments)

    if args.configfile:
        with open(args.configfile) as config_file:
            try:
                config = yaml.safe_load(config_file)
            except yaml.YAMLError as exc:
                print(exc)
    else:
        print('Config file must be provided')

    in_file = None
    if args.inputfile:
        try:
            in_file = open(args.inputfile,'r')
        except yaml.YAMLError as exc:
            print('Input file missing')
            print(exc)

    if args.mqtt_topic:
        host = args.mqtt_hostname
        port = args.mqtt_port
        topic = args.mqtt_topic
        print('Sending output to MQTT %s:%s on %s' % (host, port, topic))
        outputter = MqttOutputter(host, port, topic)
    else:
        outputter = ScreenJsonOutputter()

    if args.silent:
        print('SILENT mode')
        outputter.silent = True

    if args.sleep_for:
        sleep_for = args.sleep_for
    else:
        sleep_for = None

    if config and in_file:
        fetch_readings_from_file(config, in_file, outputter, sleep_for)

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
