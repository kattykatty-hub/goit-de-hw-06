import json
from kafka import KafkaConsumer
from .config import OUTPUT_TOPIC,python_kafka_options

def main():
    consumer=KafkaConsumer(OUTPUT_TOPIC,auto_offset_reset="latest",group_id=None,value_deserializer=lambda b:json.loads(b.decode("utf-8")),**python_kafka_options())
    print("LISTENING",OUTPUT_TOPIC,flush=True)
    try:
        for item in consumer: print(f"ALERT RECEIVED topic={item.topic} {item.value}",flush=True)
    except KeyboardInterrupt: pass
    finally: consumer.close()
if __name__=="__main__":main()
