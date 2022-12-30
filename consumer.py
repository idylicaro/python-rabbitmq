""" The consumer of RabbitMQ module."""
import logging
from typing import Dict
import pika

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(funcName) -35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

HOST = "localhost"
PORT = 5672
USERNAME = "guest"
PASSWORD = "guest"
QUEUE = "test"
QUEUE_ARGUMENTS = {}
# QUEUE_ARGUMENTS = {
#     "x-overflow": "reject-publish",
# }

class RabbitmqConsumer:
    """Rabbitmq Consumer class
    """
    def __init__(self, callback, queue_arguments: Dict = {}) -> None:
        self.__host = HOST
        self.__port = PORT
        self.__username= USERNAME
        self.__password = PASSWORD
        self.__queue = QUEUE
        self.__queue_arguments = queue_arguments
        self.__callback = callback
        self.__channel = self.__create_channel()

    def __create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )
        )
        channel = pika.BlockingConnection(connection_parameters).channel()
        channel.queue_declare(
            queue=self.__queue,
            durable=True,
            arguments=self.__queue_arguments
        )

        channel.basic_consume(
            queue=self.__queue,
            auto_ack=True,
            on_message_callback=self.__callback
        )
        LOGGER.info("Channel Created!")
        return channel

    def start(self):
        LOGGER.info(f'Listen rabbitMQ on {self.__host}:{self.__port}')
        self.__channel.start_consuming()


def message_handler_callback(ch, method, properties, body):
    print(body)


logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
rabbitmqConsumer=RabbitmqConsumer(message_handler_callback, queue_arguments=QUEUE_ARGUMENTS)
rabbitmqConsumer.start() 
