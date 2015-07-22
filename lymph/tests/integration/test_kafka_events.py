from kafka.producer import SimpleProducer

import lymph

from lymph.events.kafka import KafkaEventSystem
from lymph.discovery.static import StaticServiceRegistryHub
from lymph.testing import LymphIntegrationTestCase, AsyncTestsMixin


class TestInterface(lymph.Interface):
    def __init__(self, *args, **kwargs):
        super(TestInterface, self).__init__(*args, **kwargs)
        self.collected_events = []

    @lymph.event('foo')
    def on_foo(self, event):
        print '$$$', event
        self.collected_events.append(event)

    @lymph.event('foo_broadcast', broadcast=True)
    def on_foo_broadcast(self, event):
        print '***', event
        self.collected_events.append(event)


class TestEventBroadcastInterface(lymph.Interface):
    def __init__(self, *args, **kwargs):
        super(TestEventBroadcastInterface, self).__init__(*args, **kwargs)
        self.collected_events = []

    @lymph.event('foo_broadcast', broadcast=True)
    def on_foo_broadcast(self, event):
        print '>>>', event
        self.collected_events.append(event)

    @lymph.event('foo')
    def on_foo(self, event):
        print '%%%', event
        self.collected_events.append(event)


class KafkaIntegrationTest(LymphIntegrationTestCase, AsyncTestsMixin):
    use_zookeeper = False

    def setUp(self):
        super(KafkaIntegrationTest, self).setUp()
        self.discovery_hub = StaticServiceRegistryHub()

        self.the_container, self.the_interface = self.create_container(TestInterface, 'test')
        self.the_container_broadcast, self.the_interface_broadcast = self.create_container(TestEventBroadcastInterface, 'test')
        self.lymph_client = self.create_client()

    def tearDown(self):
        super(KafkaIntegrationTest, self).tearDown()
        self.the_interface.collected_events = []
        self.the_interface_broadcast.collected_events = []

    def create_event_system(self, **kwargs):
        return KafkaEventSystem()

    def create_registry(self, **kwargs):
        return self.discovery_hub.create_registry(**kwargs)

    def received_check(self, n):
        def check():
            return len(self.the_interface.collected_events) == n
        return check

    def received_broadcast_check(self, n):
        def check():
            return (len(self.the_interface.collected_events) + len(self.the_interface_broadcast.collected_events)) == n
        return check

    def test_emit(self):
        self.lymph_client.emit('foo', {})
        self.assert_eventually_true(self.received_broadcast_check(1), timeout=10)
        self.assertEqual(self.the_interface.collected_events[0].evt_type, 'foo')


    def test_delayed_emit(self):
        self.lymph_client.emit('foo', {}, delay=.5)
        self.assert_temporarily_true(self.received_check(0), timeout=.2)
        self.assert_eventually_true(self.received_check(1), timeout=10)
        self.assertEqual(self.the_interface.collected_events[0].evt_type, 'foo')

    def test_broadcast_event(self):
        self.lymph_client.emit('foo_broadcast', {})
        self.assert_eventually_true(self.received_broadcast_check(2), timeout=10)
        self.assertEqual(self.the_interface.collected_events[0].evt_type, 'foo_broadcast')
        self.assertEqual(self.the_interface_broadcast.collected_events[0].evt_type, 'foo_broadcast')
