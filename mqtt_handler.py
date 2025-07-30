import paho.mqtt.client as mqtt
from backend.database import SessionLocal
from backend.models.sensor_data import SensorData
from sqlalchemy.ext.asyncio import AsyncSession

MQTT_BROKER = "mqtt.eclipse.org"
TOPICS = ["air_quality", "traffic", "water_levels", "energy_usage"]

async def save_sensor_data(sensor_type, value):
    async with SessionLocal() as db:
        sensor_data = SensorData(sensor_type=sensor_type, value=value)
        db.add(sensor_data)
        await db.commit()

def on_message(client, userdata, msg):
    sensor_type = msg.topic
    value = float(msg.payload.decode())
    save_sensor_data(sensor_type, value)

client = mqtt.Client()
client.on_message = on_message
client.connect(MQTT_BROKER, 1883, 60)

for topic in TOPICS:
    client.subscribe(topic)

client.loop_start()