from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
import json
from config import loop, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP

app = FastAPI()

# estados posibles
ESTADOS = ["recibido", "preparando", "entregando", "finalizado"]

async def process_message(message_data):
    estado_actual = message_data["estado"]
    if estado_actual == "finalizado":
        print(f"Procesamiento completo para el mensaje: {message_data}")
        return

    # cambiar al siguiente estado
    siguiente_estado = ESTADOS[ESTADOS.index(estado_actual) + 1]
    message_data["estado"] = siguiente_estado

    # mandar a la cola nuevamente con el nuevo estado
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        value_json = json.dumps(message_data).encode('utf-8')
        await producer.send_and_wait(topic=KAFKA_TOPIC, value=value_json)
        print(f"Enviado mensaje con nuevo estado: {message_data}")
    finally:
        await producer.stop()

    # tiempo antes de cambiar al siguiente estado
    await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        loop=loop,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP
    )
    await consumer.start()
    try:
        async for msg in consumer:
            message_data = json.loads(msg.value.decode('utf-8'))
            print(f"Consumiendo mensaje: {message_data}")
            await process_message(message_data)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
