"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    logging.info("attempt to create jdbc connector")
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
    #            # DONE
                "connection.url": "jdbc:postgresql://localhost:5432/cta",
    #            # DONE
                "connection.user": "cta_admin",
    #            # DONE
                "connection.password": "chicago",
    #            # DONE
                "table.whitelist": "stations",
    #            # DONE
                "mode": "incrementing",
    #            # DONE
                "incrementing.column.name": "stop_id",
    #            # DONE
                "topic.prefix": "il.cta.station.",
    #            # DONE
                "poll.interval.ms": "30000",
            }
        }),
    )

    ## Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        logger.info("Failed to create the jdbc connector")
        
    logging.debug("connector created successfully")
    logging.info("connector jdbc created")

if __name__ == "__main__":
    configure_connector()
