from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
import asyncio
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import loop, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP

app = FastAPI()

SENDER_EMAIL = 'cuenta.de.pruebatarea2@gmail.com'
SENDER_PASSWORD = 'oukk notb yqtg kkmt'

async def send_email(message_data):
    receiver_email = message_data["correo"]
    estado = message_data["estado"]
    subject = f'Actualización de estado: {estado}'
    body = f"""\
    Hola,

    Tu solicitud ha sido actualizada al siguiente estado:

    ID: {message_data["id"]}
    Nombre: {message_data["nombre"]}
    Precio: {message_data["precio"]}
    Estado: {estado}

    Gracias por tu paciencia.

    Atentamente,
    Tu Aplicación
    """

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, receiver_email, msg.as_string())
        server.quit()
        print(f'Correo enviado a {receiver_email} sobre el estado {estado}')
    except Exception as e:
        print(f'Error enviando correo electrónico: {e}')

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
            print(f'Consumiendo mensaje: {message_data}')
            await send_email(message_data)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)