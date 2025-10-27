from fastapi import FastAPI
from pydantic import BaseModel
import pika
import json
import time

app = FastAPI()

class Mensagem(BaseModel):
    nome: str
    texto: str

# Função para conectar ao RabbitMQ (com tentativas)
def conectar_rabbit():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq", credentials=pika.PlainCredentials("user", "password"))
            )
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("Aguardando RabbitMQ iniciar...")
            time.sleep(5)

@app.post("/enviar")
def enviar_mensagem(mensagem: Mensagem):
    connection = conectar_rabbit()
    channel = connection.channel()
    channel.queue_declare(queue="fila_mensagens")

    body = json.dumps(mensagem.dict())
    channel.basic_publish(exchange="", routing_key="fila_mensagens", body=body)
    connection.close()

    return {"status": "Mensagem enviada com sucesso", "mensagem": mensagem.dict()}
