import logging                            # Used to log messages for debugging or monitoring
from quixstreams import Application       # Import the main QuixStreams class for Kafka integration






def temp_transform(msg):
    # Extract the temperature in Celsius from the incoming message
    celsius = msg["temperature"]

    # Convert Celsius to Fahrenheit
    fahrenheit = (celsius * 9 / 5) + 32

    # Convert Celsius to Kelvin
    kelvin = celsius + 273.15

    # Construct a new message dictionary with all three temperature scales
    new_msg = {
        "celsius": celsius,                        # Original temperature
        "fahrenheit": round(fahrenheit, 2),        # Fahrenheit, rounded to 2 decimal places
        "kelvin": round(kelvin, 2),                # Kelvin, rounded to 2 decimal places
        "device_id": msg["device_id"],             # Include device ID from original message
        "timestamp": msg["timestamp"],             # Include timestamp from original message
    }

    # Return the transformed message
    return new_msg







def alert(msg):
    # Extract the temperature in Kelvin from the incoming message
    kelvin = msg["kelvin"]

    # Check if the temperature exceeds the threshold (303 K ≈ 30°C)
    if kelvin > 303:
        # Log an error message if the temperature is too high
        logging.error("🚨 Temperature too high!")

        # Return True to indicate an alert condition
        return True
    else:
        # Return False if temperature is within normal range
        return False








def main():
    logging.info("START...")              # Log an informational message indicating the app is starting

    # Initialize the QuixStreams Kafka application with:
    # - broker_address: Kafka broker location
    # - consumer_group: name used to track offsets for consuming messages
    # - auto_offset_reset: start reading from the latest messages if no offset is found
    app = Application(
        broker_address="localhost:9092",
        consumer_group="alert",
        auto_offset_reset="latest",
    )

    # Define the input Kafka topic "sensor" and expect JSON-serialized values
    input_topic = app.topic("sensor", value_deserializer="json")



    # Define the output Kafka topic "alert" for sending alert messages, also expecting JSON serialization
    output_topic = app.topic("alert", value_serializer="json")


    # Create a streaming DataFrame from the topic for structured processing
    sdf = app.dataframe(input_topic)

    # Apply the temperature transformation function to each message in the stream
    sdf = sdf.apply(temp_transform)


    # Filter the stream to only include messages that trigger an alert
    sdf = sdf.filter(alert)



    # Write the filtered messages to the output topic "alert"
    sdf.to_topic(output_topic)

    # Print each record in the stream to the console/log for monitoring
    #sdf = sdf.print()

    # Run the application and begin processing the stream
    app.run(sdf)

# Entry point for the script
if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")    # Configure logging to show DEBUG-level messages
    main()                                # Start the main function

