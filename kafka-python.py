import happybase
import datetime
import json_hbase
import json
from kafka import KafkaConsumer

zk = 'master:2181,slave01:2181,slave02:2181,slave03:2181'
kf_svr='master:9092,slave01:9092,slave02:9092,slave03:9092'
table_name = 'sen_data'
cf_name = 'data'
table_prefix_sap = ':'

#consumer = KafkaConsumer('mqtt-topic')
consumer = KafkaConsumer('mqtt-topic',enable_auto_commit=True,group_id="mqtt_group")
#consumer = KafkaConsumer('mqtt-topic',enable_auto_commit=True,group_id="mqtt_group",bootstrap_servers=kf_svr)
#consumer = KafkaConsumer('mqtt-topic',enable_auto_commit=False,group_id="mqtt_group")

def insert_hbase(raw_data, cf_name):
    now = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
    ps_data = json_hbase.parse_jsonarr(raw_data)
    print("ps_data : "+ str(ps_data))
    data = json_hbase.parse_jsonToHbase(ps_data, cf_name)
    connection = happybase.Connection('master', table_prefix='sensor', table_prefix_separator=table_prefix_sap)
    connection.open()
    batch = connection.table(table_name).batch()
    batch.put(now, data)
    batch.send()

print('start')
while True:
    for msg in consumer:
        print(msg)
        #print(str(msg.value)[1:])
        ss = str(msg.value)[1:]
#		consumer.commit()
        insert_hbase(str(msg.value)[1:],"data")
        print("insert end")
print('end')
