import logging
from typing import Dict
import json
import pika

# LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) %(funcName) '
#               '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

HOST = "localhost"
PORT = 5672
USERNAME = "guest"
PASSWORD = "guest"
EXCHANGE = "test"
ROUTING_KEY=""
RABBITMQ_DELIVERY_MODE = 2


class RabbitmqPublisher:
    def __init__(self) -> None:
        self.__host = HOST
        self.__port = PORT
        self.__username= USERNAME
        self.__password = PASSWORD
        self.__exchange = EXCHANGE
        self.__routing_key = ROUTING_KEY
        self.__delivery_mode = RABBITMQ_DELIVERY_MODE
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
        LOGGER.info(f'Channel Created')
        return channel

    def send_message(self, body: Dict):
        self.__channel.basic_publish(
            exchange=self.__exchange,
            routing_key=self.__routing_key,
            body=json.dumps(body) ,
            properties=pika.BasicProperties(
                delivery_mode=self.__delivery_mode
            )

        )
        LOGGER.info(f'Message Sended')

logging.basicConfig(level=logging.INFO)
rabbitmq_publisher = RabbitmqPublisher()
rabbitmq_publisher.send_message({"message": "Hello World!"})
