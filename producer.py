import time                     # Used for sleep delays in the main loop
import logging                  # Used to log debug and info messages
import math                     # Provides math functions, including sine
import random                   # Provides random number generation
from datetime import datetime   # Used to generate timestamps
from quixstreams import Application  # QuixStreams Application for Kafka integration

def get_sensor_measurement(t, device_id="machine-01", frequency=0.05, noise_std=2, outlier_prob=0.05):
    """Simulates a temperature measurement for an IoT sensor at time t"""
    
    # Generate a base temperature using a sine wave pattern
    base_temp = 22 + 15 * math.sin(2 * math.pi * frequency * t)
    
    # Add Gaussian (normal) noise to simulate measurement error
    noise = random.gauss(0, noise_std)
    
    # Final temperature is the sum of base temperature and noise
    temp = base_temp + noise

    # Occasionally add a large outlier to simulate faulty readings
    if random.random() < outlier_prob:
        temp += random.choice([20, 50])

    # Create a measurement record as a dictionary (to later serialize to JSON)
    message = {
        "timestamp": datetime.now().isoformat(),  # Current timestamp in ISO format
        "device_id": device_id,                   # ID of the device
        "temperature": round(temp, 2)             # Temperature rounded to 2 decimal places
    }

    return message

def main():
    # Initialize a Quix Kafka application pointing to a local Kafka broker
    app = Application(broker_address="localhost:9092")
    
    # Define or access the Kafka topic named "sensor"
    topic = app.topic(name="sensor")
    
    t = 0  # Time counter

    # Open a producer context for sending messages
    with app.get_producer() as producer:
        while True:
            # Simulate a sensor measurement
            measurement = get_sensor_measurement(t)
            
            # Log the generated measurement at DEBUG level
            logging.debug(f"Got measurement: {measurement}")
            
            # Serialize the message for Kafka (keyed by device ID)
            kafka_msg = topic.serialize(key=measurement["device_id"], value=measurement)
            
            # Produce the message to the Kafka topic
            producer.produce(
                topic=topic.name,
                key=kafka_msg.key,
                value=kafka_msg.value,
            )

            # Log production info and sleep for 1 second before next reading
            logging.info("Produced. Sleeping...")
            time.sleep(1)
            t = t + 1  # Increment time

# Run the main function if the script is executed directly
if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")  # Set logging level to DEBUG
    main()                              # Start the main function





# Open a database connection using a context manager (ensures proper setup and teardown)
with open_db_connection() as connection:
    
    # Pass the active connection to a function that runs some analysis on the data
    run_analysis(connection)
