import json
import os

class Config:
    def __init__(self) -> None:
        self.amqp_broker = os.environ.get('AMQP_BROKER',' http://localhost:5672/')
        self.capability_period = os.environ.get('CAPABILITY_PERIOD', 5)