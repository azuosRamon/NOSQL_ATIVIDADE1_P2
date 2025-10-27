import pika
import json
import time

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

def callback(ch, method, properties, body):
    mensagem = json.loads(body)
    print(f"Mensagem recebida: {mensagem}")

def consumir():
    connection = conectar_rabbit()
    channel = connection.channel()
    channel.queue_declare(queue="fila_mensagens")

    channel.basic_consume(queue="fila_mensagens", on_message_callback=callback, auto_ack=True)
    print("Aguardando mensagens. Pressione CTRL+C para sair.")
    channel.start_consuming()

if __name__ == "__main__":
    consumir()
