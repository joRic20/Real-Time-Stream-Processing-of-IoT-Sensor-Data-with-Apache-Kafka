import logging
from quixstreams import Application
from quixstreams.models.windows import TumblingWindow

def main():
    # Enable logging
    logging.basicConfig(level=logging.INFO)

    # Initialize QuixStreams application for consuming alert messages
    app = Application(
        broker_address="localhost:9092",         # Kafka broker address
        consumer_group="alert-counter",          # Unique consumer group name
        auto_offset_reset="latest"               # Start from latest if no prior offset
    )

    # Define the input and output Kafka topics
    alert_topic = app.topic("alert", value_deserializer="json")
    output_topic = app.topic("alert-count")  # New topic to write alert counts

    # Create a dataframe from the alert topic stream
    sdf = app.dataframe(alert_topic)

    # Apply 5-second tumbling window to count number of alerts
    count_df = sdf.window(TumblingWindow(duration="5s")).count()

    # Write the count to the output topic as JSON
    count_df = count_df.to_topic(output_topic, value_serializer="json")

    # Start the application loop
    app.run(count_df)

if __name__ == "__main__":
    main()
