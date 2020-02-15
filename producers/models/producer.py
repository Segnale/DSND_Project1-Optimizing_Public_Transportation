"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"

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
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
            "URL1": "PLAINTEXT://localhost:9092",
            "URL2": "PLAINTEXT://localhost:9093",
            "URL3": "PLAINTEXT://localhost:9094"
        }
        
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        
        # Schema Registry for Avro
        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        # DONE: Configure the AvroProducer
        self.producer = AvroProducer(
            {"bootstrap.servers": BROKER_URL},
            schema_registry = schema_registry
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # DONE: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        exists = self.topic_exists(client)
        if exists is False:
            features = client.create_topics(
                [
                    NewTopic(
                        topic = self.topic_name,
                        num_partitions = self.num_partitions,
                        replication_factor = self.num_replicas,
                        config = {
                            "cleanup.policy": "compact",
                            "delete.retention.ms": 3000,
                            "compression.type": "lz4"
                        },
                    )
                ]
            )
            #
            # Logging the result of the topic creation
            for topic, feature in features.items():
                try:
                    # Succesful topic creation
                    feature.result()
                    logger.info(f"topic {self.topic_name} created")
                except Exception as e:
                    # Failing topic creation
                    logger.info(f"failed to create topic {self.topic_name}: {e}")
        else:
            logger.info(f"topic {self.topic_name} has not been created because already existing")



    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
    
    def topic_exists(self, client):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        return self.topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))
