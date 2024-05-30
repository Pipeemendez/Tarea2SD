from fastapi import APIRouter, UploadFile, File
from schema import Message
from config import loop, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import asyncio
import csv

route = APIRouter()

# Variable global ID autoincremental
current_id = 1

@route.post('/enviar_pedido')
async def send(message: Message):
    global current_id

    # se asigna un ID único al pedido
    message.id = current_id
    message.estado = "recibido"
    current_id += 1

    # envía el mensaje al servidor Kafka
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        message_data = message.dict()
        print(f'Enviando mensaje con la siguiente información: {message_data}')
        value_json = json.dumps(message_data).encode('utf-8')
        await producer.send_and_wait(topic=KAFKA_TOPIC, value=value_json)
    finally:
        await producer.stop()

    # envía un correo electrónico inicial con el estado "recibido"
    await enviar_correo(message_data)

    return {"id": message.id, "status": "Pedido recibido y en proceso"}

@route.post("/cargar_pedidos")
async def cargar_pedidos(file: UploadFile = File(...)):
    # se procesa el archivo CSV pedidos.csv
    pedidos = []
    content = await file.read()
    decoded_content = content.decode("utf-8").splitlines()
    csv_reader = csv.DictReader(decoded_content)
    global current_id
    for row in csv_reader:
        pedido = Message(
            id=current_id,  # se asigna un ID único al pedido
            nombre=row["nombre"],
            precio=float(row["precio"]),
            correo=row["correo"],
            estado="recibido"
        )
        current_id += 1
        pedidos.append(pedido)

    # se envian los pedidos a Kafka
    await enviar_a_kafka(pedidos)

    return {"message": "Pedidos cargados correctamente"}

async def enviar_a_kafka(pedidos):
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        for pedido in pedidos:
            message_data = pedido.dict()
            print(f'Enviando mensaje con la siguiente información: {message_data}')
            value_json = json.dumps(message_data).encode('utf-8')
            await producer.send_and_wait(topic=KAFKA_TOPIC, value=value_json)
            await enviar_correo(message_data)
    finally:
        await producer.stop()

async def consume():
    consumer = AIOKafkaConsumer(KAFKA_TOPIC, loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=KAFKA_CONSUMER_GROUP)
    await consumer.start()
    try:
        async for msg in consumer:
            message_data = json.loads(msg.value)
            print(f'Mensaje del consumidor: {message_data}')

            # actualizar el estado del mensaje
            await procesar_mensaje(message_data)
    finally:
        await consumer.stop()

async def procesar_mensaje(message_data):
    estados = ["recibido", "preparando", "entregando", "finalizado"]
    if message_data["estado"] != "finalizado":
        next_estado = estados[estados.index(message_data["estado"]) + 1]
        message_data["estado"] = next_estado
        await enviar_correo(message_data)
        await reenviar_mensaje(message_data)
        await asyncio.sleep(5)  # espera antes de pasar al siguiente estado

async def reenviar_mensaje(message_data):
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        print(f'Reenviando mensaje con la siguiente información: {message_data}')
        value_json = json.dumps(message_data).encode('utf-8')
        await producer.send_and_wait(topic=KAFKA_TOPIC, value=value_json)
    finally:
        await producer.stop()

async def enviar_correo(message_data):
    try:
        sender_email = 'cuenta.de.pruebatarea2@gmail.com'  # correo desde donde se mandan los mensajes
        receiver_email = message_data["correo"]
        password = 'oukk notb yqtg kkmt'  # contraseña de correo desde donde se mandan los mensajes

        message_body = f"""\
Buenas tardes, esperando que se encuentre bien.

Su pedido con ID {message_data["id"]} ha cambiado su estado a {message_data["estado"]}.
Nombre: {message_data["nombre"]}
Precio: {message_data["precio"]}

Atentamente,
Universidad Deigo Morales
"""

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = 'Actualización de Estado de Pedido'
        msg.attach(MIMEText(message_body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
    except Exception as e:
        print(f'Error enviando correo electrónico: {e}')
