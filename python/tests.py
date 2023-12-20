import concurrent.futures
import unittest
from utils import Event, EventStore

class EventTest(unittest.TestCase):
    def test_warning(self):
        event = Event("some_type", 123)
        self.assertEqual(123, event.timestamp)
        self.assertEqual("some_type", event.event_type)

    def test_insert(self):
        event_store = EventStore()
        for i in range(20):
            event_store.insert(Event("some_type", i))
        self.assertEqual(event_store.size(), 20)

    def test_remove_all(self):
        event_store = EventStore()
        for i in range(20):
            event_store.insert(Event("some_type", i))
        event_store.insert(Event("some_other_type", 123))
        event_store.remove_all("some_type")
        self.assertEqual(event_store.size(), 1)

    def test_query(self):
        event_store = EventStore()
        for i in range(20):
            event_store.insert(Event("some_type", i))
        event_iterator = event_store.query("some_type", 5, 18)
        for i in range(5, 20):
            if i <= 17:
                self.assertTrue(event_iterator.move_next())
                self.assertEqual(event_iterator.current().timestamp, i)
            else:
                self.assertFalse(event_iterator.move_next())

    def test_remove(self):
        event_store = EventStore()
        for i in range(20):
            event_store.insert(Event("some_type", i))
        event_iterator = event_store.query("some_type", 5, 20)
        event_iterator.move_next()
        self.assertEqual(event_iterator.current().timestamp, 5)
        event_iterator.remove()
        yet_another_iterator = event_store.query("some_type", 5, 20)
        yet_another_iterator.move_next()
        self.assertEqual(yet_another_iterator.current().timestamp, 6)

    def test_current_should_throw_error(self):
        event_store = EventStore()
        for i in range(20):
            event_store.insert(Event("some_type", i))
        event_iterator = event_store.query("some_type", 5, 20)
        with self.assertRaises(IndexError):
            this_should_not_exist = event_iterator.current()

    def test_end_time_must_be_greater_than_start_time(self):
        event_store = EventStore()
        for i in range(20):
            event_store.insert(Event("some_type", i))
        with self.assertRaises(ValueError):
            event_iterator = event_store.query("some_type", 20, 5)

    def test_query_length_and_remove_all(self):
        event_store = EventStore()
        event1 = Event("some_type", 1)
        event2 = Event("some_type", 2)
        event3 = Event("some_type", 4)
        event_store.insert(event1)
        event_store.insert(event2)
        event_store.insert(event3)
        iterator = event_store.query("some_type", 0, 4)
        self.assertTrue(iterator.move_next())
        self.assertEqual(iterator.current(), event1)
        self.assertTrue(iterator.move_next())
        self.assertEqual(iterator.current(), event2)
        self.assertFalse(iterator.move_next())
        event_store.remove_all("some_type")
        with self.assertRaises(ValueError):
            iterator = event_store.query("some_type", 0, 3)

    def test_concurrent_insert_and_remove(self):
        event_store = EventStore()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future1 = executor.submit(self._insert_events, event_store, 100)
            future2 = executor.submit(self._remove_even_events, event_store)
            concurrent.futures.wait([future1, future2])

        self.assertEqual(event_store.size(), 90)

    def test_concurrent_insert_and_remove_and_insert_thread(self):
        event_store = EventStore()
        for i in range(21):
            event_store.insert(Event("some_type", i))

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future1 = executor.submit(self._insert_events, event_store, 100, "hundred_events_of_this_type")
            future2 = executor.submit(self._remove_even_events, event_store)
            future3 = executor.submit(self._insert_events, event_store, 20, "some_type")
            concurrent.futures.wait([future1, future2, future3])

        self.assertEqual(event_store.size(), 131)

    def _insert_events(self, event_store, num_events, event_type="some_type"):
        for i in range(num_events):
            event_store.insert(Event(event_type, i))

    def _remove_even_events(self, event_store):
        event_iterator = event_store.query("some_type", 0, 20)
        while event_iterator.move_next():
            if event_iterator.current().timestamp % 2 == 0:
                event_iterator.remove()

if __name__ == '__main__':
    unittest.main()
