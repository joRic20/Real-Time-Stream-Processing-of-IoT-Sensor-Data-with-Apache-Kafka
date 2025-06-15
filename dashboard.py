from datetime import datetime, timedelta
from collections import deque
import streamlit as st
from quixstreams import Application
import logging

# Set up chart buffers
temperature_buffer = deque(maxlen=100)
timestamp_buffer = deque(maxlen=100)

# Buffers for 5s and 10s windowed KPIs
alert_window = deque()
temp_window = deque()

# Streamlit page setup
st.set_page_config(page_title="IoT Dashboard", layout="wide")
st.title("📡 Real-Time IoT Dashboard")

@st.cache_resource
def kafka_connection():
    # Setup Kafka application using Quix Streams
    return Application(
        broker_address="localhost:9092",
        consumer_group="dashboard",
        auto_offset_reset="latest",
    )

# Connect to Kafka
app = kafka_connection()

# Topics
sensor_topic = app.topic("sensor", value_deserializer="json")
alert_topic = app.topic("alert", value_deserializer="json")  # Raw alerts, not pre-aggregated

# Streamlit placeholders
st_chart = st.empty()
col1, col2, col3 = st.columns(3)
st_metric_temp = col1.empty()
st_metric_alerts = col2.empty()
st_metric_avg_temp = col3.empty()

# Start Kafka consumer
with app.get_consumer() as consumer:
    consumer.subscribe([sensor_topic.name, alert_topic.name])

    previous_temp = 0

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        now = datetime.now()

        # Process Sensor Data
        if msg.topic() == sensor_topic.name:
            sensor_msg = sensor_topic.deserialize(msg)
            temperature = sensor_msg.value.get("temperature")
            device_id = sensor_msg.value.get("device_id")
            timestamp = datetime.fromisoformat(sensor_msg.value.get("timestamp"))

            # Append to 10s temperature window
            temp_window.append((timestamp, temperature))

            # Remove outdated entries older than 10s
            temp_window = deque([(t, temp) for t, temp in temp_window if now - t <= timedelta(seconds=10)])

            # Compute avg temp over 10s
            avg_temp = round(sum(t for _, t in temp_window) / len(temp_window), 2) if temp_window else 0

            # Format chart time string
            time_str = timestamp.strftime("%H:%M:%S")
            temperature_buffer.append(temperature)
            timestamp_buffer.append(time_str)

            # Compute delta from previous reading
            delta = temperature - previous_temp
            previous_temp = temperature

            # Update temperature metric and chart
            st_metric_temp.metric(label=f"Current Temp ({device_id})", value=f"{temperature:.2f} °C", delta=f"{delta:.2f} °C")
            st_metric_avg_temp.metric(label="🌡 Avg Temp (10s)", value=f"{avg_temp} °C")

            # Update real-time temperature chart
            st_chart.line_chart(
                data={
                    "time": list(timestamp_buffer),
                    "temperature": list(temperature_buffer)
                },
                x="time",
                y="temperature",
                use_container_width=True,
            )

        # Process Alert Messages
        elif msg.topic() == alert_topic.name:
            alert_msg = alert_topic.deserialize(msg)
            alert_time = datetime.now()

            # Append alert timestamp
            alert_window.append(alert_time)

            # Retain only alerts in the last 5 seconds
            alert_window = deque([t for t in alert_window if now - t <= timedelta(seconds=5)])

            # Update alert count metric
            st_metric_alerts.metric(label="⚠️ Alerts (last 5s)", value=str(len(alert_window)))
