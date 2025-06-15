from datetime import datetime
from quixstreams import Application
import streamlit as st


st.title("Real-Time IoT Dashboard")

@st.cache_resource
def kafka_connection():
    return Application(
        broker_address="localhost:9092",
        consumer_group="dashboard",
        auto_offset_reset="latest",
    )

app = kafka_connection()
sensor_topic = app.topic("sensor")
alert_topic = app.topic("alert")

st_metric_temp = st.empty() # Placeholder for temperature metric

with app.get_consumer() as consumer:
    consumer.subscribe([sensor_topic.name, alert_topic.name])
    previous_temp = 0
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None and msg.topic() == sensor_topic.name:
            sensor_msg = sensor_topic.deserialize(msg)
            temperature = sensor_msg.value.get('temperature')
            device_id = sensor_msg.value.get('device_id')
            timestamp = datetime.fromisoformat(sensor_msg.value.get('timestamp'))
            diff = temperature - previous_temp
            previous_temp = temperature
            timestamp_str = timestamp.strftime("%H:%M:%S")
            st_metric_temp.metric(label=device_id, value=f"{temperature:.2f} °C", delta=f"{diff:.2f} °C")