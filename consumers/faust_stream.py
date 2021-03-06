"""Defines trends calculations for stations"""
import logging
from dataclasses import asdict, dataclass
import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# DONE: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("il.cta.station.stations", value_type=Station)
# DONE: Define the output Kafka Topic
out_topic = app.topic("il.cta.station.transformedstations", value_type=TransformedStation, partitions=1)
# TODO: Define a Faust Table
station_table = app.Table(
    "stations_table",
    default=str,
    partitions=1,
    changelog_topic=out_topic,
)
#
#
# DONE: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
@app.agent(topic)
async def Station_Transformation(stations):
    async for station in stations:
        
        if station.red:
            line = "red"
        elif station.blue:
            line = "blue"
        elif station.green:
            line = "green"
        else:
            line = "none"
        
        station_table[station.station_id] = TransformedStation(
            station_id = station.station_id,
            station_name = station.station_name,
            order = station.order,
            line = line)
            
        await out_topic.send(value=station_table[station.station_id])


if __name__ == "__main__":
    app.main()
