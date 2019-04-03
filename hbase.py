#from kafka import KafkaConsumer
import happybase
import datetime
import json_hbase

zk = 'master:2181,slave01:2181,slave02:2181,slave03:2181'
kf_svr='master:9092,slave01:9092,slave02:9092,slave03:9092'
table_name = 'sensor'
cf_name = 'data'
table_prefix_sap = ':'

#consumer = KafkaConsumer('mqtt-topic')
#consumer = KafkaConsumer('mqtt-topic',enable_auto_commit=True,bootstrap_servers=kf_svr)
#consumer = KafkaConsumer('mqtt-topic',enable_auto_commit=True,group_id="mqtt_group",bootstrap_servers=kf_svr)
#consumer = KafkaConsumer('mqtt-topic',enable_auto_commit=False,group_id="mqtt_group",bootstrap_servers=kf_svr)
connection = happybase.Connection('master', table_prefix='sensor',table_prefix_separator=table_prefix_sap)
batch = connection.table(table_name).batch()
connection.close()
def insert_hbase(raw_data, cf_name):
    now = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
    data = json_hbase.parse_jsonToHbase(json_hbase.parse_jsonarr(raw_data), cf_name)
    batch.put(now, data)
    batch.send()

#전부다 문자열 형식이어야 insert 가능
js_arr = '[{"A": "KSC01_V01", "C": "38.0", "B": "181011170000.63", "E": "2", "D": "14.9", "G": "1897", "F": "2048", "I": "02", "H": "2844", "K": "-0173", "J": "-0217", "M": "75", "L": "21601", "O": "00057", "N": "-0184", "Q": 20, "P": "00129", "S": -65535, "R": -65535, "U": "0", "T": "2596", "W": "12721.39155", "V": "3621.35120", "Y": 0, "X": "1.21", "Z": 120}]'

now = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
data = json_hbase.parse_jsonToHbase(json_hbase.parse_jsonarr(js_arr), 'data')

table = connection.table('sen_data')

batch = table.batch()
batch.put(now, data)
batch.send()

for tmp in table.scan():
    print(tmp)