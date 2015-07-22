from __future__ import absolute_import

import json

from kafka import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

from lymph.events.base import BaseEventSystem
from lymph.core.events import Event


class KafkaEventSystem(BaseEventSystem):
    def __init__(self):
        self.client = KafkaClient("192.168.33.10:9092")
        self._producer = None

    def _consume(self, handler, consumer, event_type, **kwargs):
        for event in consumer:
            event = Event.deserialize(json.loads(event.message.value))
            handler(event)

    def subscribe(self, handler, **kwargs):
        group = handler.interface.name
        for event_type in handler.event_types:
            print group
            consumer = SimpleConsumer(self.client, group, event_type)
            self.container.spawn(self._consume, handler, consumer, event_type)

    def unsubscribe(self, container, event_type, **kwargs):
        pass

    @property
    def producer(self):
        if not self._producer:
            self._producer = SimpleProducer(self.client)
        return self._producer

    def emit(self, event, **kwargs):
        print event.evt_type
        self.producer.send_messages(event.evt_type, json.dumps(event.serialize()))

    def on_stop(self, *args, **kwargs):
        super(KafkaEventSystem, self).on_stop(*args, **kwargs)
