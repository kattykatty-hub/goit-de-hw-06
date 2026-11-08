import os
from dotenv import load_dotenv
load_dotenv()
BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
USERNAME = os.environ["KAFKA_USERNAME"]
PASSWORD = os.environ["KAFKA_PASSWORD"]
PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "katty")
INPUT_TOPIC = f"{PREFIX}_building_sensors"
OUTPUT_TOPIC = f"{PREFIX}_streaming_alerts"

def kafka_options():
    # Local course broker uses SASL_PLAINTEXT; never use for sensitive data.
    jaas = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{USERNAME}" password="{PASSWORD}";'
    return {"kafka.bootstrap.servers": BOOTSTRAP,
            "kafka.security.protocol": "SASL_PLAINTEXT",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": jaas}

def python_kafka_options():
    return dict(bootstrap_servers=BOOTSTRAP,security_protocol="SASL_PLAINTEXT",sasl_mechanism="PLAIN",sasl_plain_username=USERNAME,sasl_plain_password=PASSWORD)
