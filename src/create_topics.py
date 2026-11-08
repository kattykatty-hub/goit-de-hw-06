from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from .config import INPUT_TOPIC, OUTPUT_TOPIC, python_kafka_options

def main():
    admin = KafkaAdminClient(client_id="hw06-topic-setup", **python_kafka_options())
    try:
        existing=set(admin.list_topics())
        for topic in (INPUT_TOPIC, OUTPUT_TOPIC):
            if topic not in existing:
                try:
                    admin.create_topics([NewTopic(name=topic,num_partitions=1,replication_factor=1)])
                    print("CREATED",topic)
                except TopicAlreadyExistsError:
                    print("EXISTS",topic)
            else: print("EXISTS",topic)
    finally: admin.close()
if __name__=="__main__": main()
