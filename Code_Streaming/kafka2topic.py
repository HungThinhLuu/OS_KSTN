import pandas as pd
import json
df = pd.read_csv('VNI-test.csv')

list_price = ["Price", "Open", "High", "Low"]
for i in list_price:
    df[i] = pd.to_numeric(df[i].apply(lambda x: x.replace(",", "")))

BOOTSTRAP_SERVER = 'localhost'
TOPIC = 'myTest'

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])

col = []
for i in range(0, 7):
    col.append(df.columns[i])

def send(tmp):
    '''s = ""
    for i in range(0,6):
        s = s + col[i] + ": " + str(tmp[i]) + "-"'''
    s = {}
    for i in range(0, 7):
        s[col[i]] = tmp[i]
    print("Message: ", s)
    producer.send(TOPIC, value = json.dumps(s).encode('utf-8'))

import time
timing = 0.5
for i in range(df.shape[0]-1, -1, -1):
    tmp = list(df.loc[i])
    send(tmp)
    time.sleep(timing)
