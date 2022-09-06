import os
import requests
import boto3
import csv
import ssl
import logging
import random
import paho.mqtt.client as mqtt
import re
import json
import sys

MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PWD = os.getenv("MQTT_PWD")
MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = os.getenv("MQTT_PORT", 8883)

STORAGE_USERNAME = os.getenv("STORAGE_USERNAME")
STORAGE_PWD = os.getenv("STORAGE_PWD")
STORAGE_HOST_URL = os.getenv("STORAGE_HOST_URL")

DATA_MAPPING = {}

TOPICS = []

LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
LOGGER = logging.getLogger('wis2box-subscriber')
LOGGER.setLevel(LOG_LEVEL)
logging.basicConfig(stream=sys.stdout)

def sub_connect(client, userdata, flags, rc, properties=None):
    LOGGER.info(f"on connection to subscribe: {mqtt.connack_string(rc)}")
    for topic in DATA_MAPPING:
        LOGGER.info(f'subscribing to topic: {topic}')
        client.subscribe(topic, qos=1)

def sub_on_message(client, userdata, msg):
    """
      do something with the message
    """
    if msg.topic not in TOPICS:
        LOGGER.info(f"new_topic,{msg.topic}")
        TOPICS.append(msg.topic)
    return

    # use regex to match msg.topic with subscribed-topic and get S3-folder  
    folder = ''
    for topic in DATA_MAPPING:
        pattern = topic.replace('+', '[^/]*').replace('/#', '(|/.*)')
        if re.match(pattern, msg.topic):
            folder = DATA_MAPPING[topic]['folder']
    if folder == '':
        LOGGER.warning(f"failed to find folder for msg.topic={msg.topic}")
        return
    try:
        message = json.loads(msg.payload.decode('utf-8'))
        data_url = message["links"][0]["href"]
        data_type = message["links"][0]["type"]
        LOGGER.info(f"data_url={data_url}")
        LOGGER.info(f"data_type={data_type}")
        if data_type != 'application/bufr':
            LOGGER.info(f"data_type != 'application/bufr', ignore !")
            return
        else :
            LOGGER.info(f"data_type == 'application/bufr', retrieve data !")
        try:
            resp = requests.get(data_url)
            resp.raise_for_status()
        except Exception as e:
            LOGGER.warning(f"Could not download data from: {data_url}")
            return
        session = boto3.Session(
            aws_access_key_id=STORAGE_USERNAME,
            aws_secret_access_key=STORAGE_PWD
        )
        filename = data_url.split('/')[-1]
        s3_bucket = STORAGE_HOST_URL.split('/')[-1]
        s3_url = STORAGE_HOST_URL.replace(s3_bucket,'')
        s3client = session.client('s3', endpoint_url=s3_url)
        LOGGER.info(f"Upload data, bucket={s3_bucket} key={folder}/{filename}")
        s3client.put_object(Body=resp.content, Bucket=s3_bucket, Key=f'{folder}/{filename}')
        LOGGER.info(f"Upload completed!")
    except Exception as e:
        LOGGER.error(f'Failed to upload to wis2box: {e}')


def run_wis2box_subscriber():
    r = random.Random()
    client_id = f"{__name__}_{r.randint(1,1000):04d}"
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5)
    if MQTT_PORT == 8883:
        client.tls_set(
            certfile=None,
            keyfile=None,
            cert_reqs=ssl.CERT_REQUIRED
        )
    client.username_pw_set(MQTT_USERNAME, MQTT_PWD)
    client.on_connect = sub_connect
    client.on_message = sub_on_message
    client.connect(MQTT_HOST, port=int(MQTT_PORT))
    client.loop_forever()


def main():
    print(f'Starting wis2box-subscriber with LOG_LEVEL={LOG_LEVEL}')
    LOGGER.info("run wis2box-subscriber")
    LOGGER.info(f'MQTT_HOST={MQTT_HOST}')
    LOGGER.info(f'MQTT_USERNAME={MQTT_USERNAME}')
    LOGGER.info(f'MQTT_PASSWORD={MQTT_PWD}')
    LOGGER.info(f'MQTT_PORT={MQTT_PORT}')
    LOGGER.info(f'STORAGE_HOST_URL={STORAGE_HOST_URL}')
    LOGGER.info(f'STORAGE_USERNAME={STORAGE_USERNAME}')
    LOGGER.info(f'STORAGE_PWD={STORAGE_PWD}')

    if not MQTT_HOST:
        LOGGER.error('MQTT_HOST is not defined')
        return
    if not STORAGE_HOST_URL:
        LOGGER.error('STORAGE_HOST_URL is not defined')
        return
    data_mapping_file = '/tmp/data_mapping.csv'
    print(f"Read configuration from {data_mapping_file}: ")
    with open(data_mapping_file) as file:
        reader = csv.reader(file)
        next(reader, None)
        for data in reader:
            topic = data[0].rstrip()
            incoming_folder = data[1].rstrip()
            if topic == 'mqtt_topic':
                continue
            print(f' * topic={topic}')
            print(f'   folder={incoming_folder}')
            DATA_MAPPING[topic] = {}
            DATA_MAPPING[topic]['folder'] = incoming_folder
    if len(DATA_MAPPING.keys()) > 0:
        print(f'Configuration provided {len(DATA_MAPPING.keys())} mqtt-topics')
        run_wis2box_subscriber()
    else:
        print('0 topics defined, exiting')


if __name__ == "__main__":
    main()
