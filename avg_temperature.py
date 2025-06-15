import logging
from quixstreams import Application
from quixstreams.models.windows import TumblingWindow

def main():
    # Enable logging
    logging.basicConfig(level=logging.INFO)

    # Set up the QuixStreams application
    app = Application(
        broker_address="localhost:9092",         # Kafka broker address
        consumer_group="avg-temp",               # Consumer group for this task
        auto_offset_reset="latest"               # Start from latest messages
    )

    # Define the input sensor data topic and the output topic for averages
    sensor_topic = app.topic("sensor", value_deserializer="json")
    output_topic = app.topic("avg-temperature")

    # Create dataframe from sensor topic
    sdf = app.dataframe(sensor_topic)

    # Compute average of the temperature over a 10-second tumbling window
    avg_df = sdf.window(TumblingWindow(duration="10s")).avg("temperature")

    # Write results to the new topic as JSON
    avg_df = avg_df.to_topic(output_topic, value_serializer="json")

    # Start streaming
    app.run(avg_df)

if __name__ == "__main__":
    main()
