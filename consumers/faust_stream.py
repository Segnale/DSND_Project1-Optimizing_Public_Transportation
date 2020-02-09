"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass 
import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record, validation=True):
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


# Processor function to transform color states to line color state
def line_color(station,colored_station):
    if station.red = True:
        colored_station.line = "red"
    else if station.blue = True:
        colored_station.line = "blue"
    else if station.green = True:
        colored_station.line = "green"
    else:
        colored_station.line = "None"
    return colored_station


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
# topic = app.topic("TODO", value_type=Station)
# TODO: Define the output Kafka Topic
# out_topic = app.topic("TODO", partitions=1, value_type=TransformedStation)
# TODO: Define a Faust Table
#table = app.Table(
#    # "TODO",
#    # default=TODO,
#    partitions=1,
#    changelog_topic=out_topic,
#)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def StationProcessing(stations):
    async for station in stations:
        
        await out_topic.send(value=)


if __name__ == "__main__":
    app.main()
