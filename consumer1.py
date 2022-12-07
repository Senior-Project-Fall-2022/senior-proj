# Author: Vikram Roy

from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import requests
import sys

api_url = "https://otx.alienvault.com/api/v1/indicator/IPv4/"

consumer = KafkaConsumer('probe2', bootstrap_servers = "tier2.cadencecyber.io:9093")
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers = "tier2.cadencecyber.io:9093")

while True:
  for msg in consumer:
    my_bytes_value = msg.value
    try:
      my_json = my_bytes_value.decode('utf8').replace("'", '"')
    except:
      continue
    try:
      data = json.loads(my_json)
    except:
      continue
    src_ip = None
    if 'src_ip' in data:
      src_ip = data['src_ip']
    else:
      continue

    try:
      s = api_url + src_ip + "/geo"
      response = requests.get(s)
      if response.status_code != 200:
        continue
      s1 = api_url + src_ip + "/passive_dns"
      response1 = requests.get(s1)
      if response1.status_code != 200:
        continue
    except:
      continue
    
    try:
      json_enriched_geo_data = response.json()
      data['geolocation'] = json.dumps(json_enriched_geo_data)
      json_enriched_dns_data = response1.json()
      data['dns'] = json.dumps(json_enriched_dns_data)
    except:
      continue

    producer.send('enriched', data)

sys.exit()


