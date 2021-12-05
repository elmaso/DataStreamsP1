"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            ZOOKEEPER_CONNECT_CONFIG: "localhost:2181",
            BOOTSTRAP_SERVERS_CONFIG: "localhost:9092",
            SCHEMA_REGISTRY_URL_CONFIG: "http://localhost:8081",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer({
            "bootstrap.servers": self.broker_properties.get("BOOTSTRAP_SERVERS_CONFIG"),
            "schema.registry.url": self.broker_properties.get("SCHEMA_REGISTRY_URL_CONFIG"),
        },
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        
        # Only Create new topic if topic_name does not already exist
        if self.topic_name not in Producer.existing_topics:
            client = AdminClient({"bootstrap.servers": self.broker_properties.get("BOOTSTRAP_SERVERS_CONFIG")})
            
            future = client.create_topics(
                [NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config = {"cleanup.policy": "compact",
                              "compression.type": "lz4",
                              "delete.retention.ms": "6000",
                              "file.delete.delay.ms": "60000",}
                )]
            )
            
            for topic, f in future.items():
                try:
                    f.result()
                    print("Topic {} created".format(topic))
                except Exception as e:
                    print("Failed to create topic {}: {}".format(topic, e))
        
        # We leve a log of all existing topics retries
        if self.topic_name in Producer.existing_topics:
            logger.info(f"topic exits {self.topic_name}")

           

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        if self.producer is not None:
            self.producer.flush(timeout=10)
            self.producer.close()
        return

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
