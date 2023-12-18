package net.intelie.challenges;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class EventTest {
    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        // THIS IS A WARNING:
        // Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }

    // Testing the insert method
    @Test
    public void insertTest() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        for (int i = 0; i < 20; i++) {
            eventStore.insert(new Event("some_type", i)); 
        }

        // Asserts all 20 events of type "some_type" were inserted
        assertEquals(eventStore.size(), 20);
    }

    // Testing the removeAll method
    @Test
    public void removeAllTest() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        
        for (int i = 0; i < 20; i++) {
            eventStore.insert(new Event("some_type", i)); 
        }
        // Inserts another type
        eventStore.insert(new Event("some_other_type", 123L));
        // Delete all 20 events with type "some_type"
        eventStore.removeAll("some_type");

        // Asserts only event of type "some_other_type" is present
        assertEquals(eventStore.size(), 1);
    }

    // Testing the query method
    @Test
    public void queryTest() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        
        for (int i = 0; i < 20; i++) {
            eventStore.insert(new Event("some_type", i)); 
        }

        InMemoryEventIterator eventIterator = eventStore.query("some_type", 5, 18);

        for (int i = 5; i < 20; i++) {
            if (i <= 17) {
                boolean hasNext = eventIterator.moveNext();
                assertEquals(eventIterator.current().timestamp(), i);
            } else {
                assertFalse(eventIterator.moveNext());
            }
        }
    }
        
    // Testing the remove method
    @Test
    public void removeTest() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        
        for (int i = 0; i < 20; i++) {
            eventStore.insert(new Event("some_type", i)); 
        }

        InMemoryEventIterator eventIterator = eventStore.query("some_type", 5, 20);

        // Moves to the next element (Event with timestamp 5, in this case)
        eventIterator.moveNext();
        assertEquals(eventIterator.current().timestamp(), 5);
        // Removes the event with timestamp 5
        eventIterator.remove();
        
        InMemoryEventIterator yetAnotherIterator = eventStore.query("some_type", 5, 20);
        yetAnotherIterator.moveNext();

        // Asserts the first event from new query has timestamp 6
        assertEquals(yetAnotherIterator.current().timestamp(), 6);
    }

    // Testing the condition that the current event, when we start an iterator,
    // should throw an error before the moveNext method is called
    @Test 
    public void currentShouldThrowErrorTest() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        
        for (int i = 0; i < 20; i++) {
            eventStore.insert(new Event("some_type", i)); 
        }

        InMemoryEventIterator eventIterator = eventStore.query("some_type", 5, 20);
        try {
            Event thisShouldNotExist = eventIterator.current();
            fail("Expected error, but query did not throw an exception.");
        } catch (IllegalStateException e) {
            // Expected behaviour
        }

    }
    
    // Testing the condition that the endTime must be greater than startTime
    @Test 
    public void endTimeMustBeGreaterThanStartTimeTest() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        
        for (int i = 0; i < 20; i++) {
            eventStore.insert(new Event("some_type", i)); 
        }

        try {
            InMemoryEventIterator eventIterator = eventStore.query("some_type", 20, 5);
            fail("Expected error, but query did not throw an exception.");
        } catch (IllegalArgumentException e) {
            // Expected behaviour
        }

    }

    @Test
    public void queryLengthAndRemoveAllTest() {
        InMemoryEventStore eventStore = new InMemoryEventStore();

        // Insert events
        Event event1 = new Event("some_type", 1L);
        Event event2 = new Event("some_type", 2L);
        Event event3 = new Event("some_type", 4L); // This one should not be filtered in query
        eventStore.insert(event1);
        eventStore.insert(event2);

        EventIterator iterator = eventStore.query("some_type", 0L, 3L);
        assertTrue(iterator.moveNext());
        assertEquals(event1, iterator.current());
        assertTrue(iterator.moveNext());
        assertEquals(event2, iterator.current());
        assertFalse(iterator.moveNext());

        eventStore.removeAll("some_type");

        try {
            iterator = eventStore.query("some_type", 0L, 3L);
            fail("Expected IllegalArgumentException, but query did not throw an exception.");
        } catch (IllegalArgumentException e) {
            // Expected behaviour
        }
    }

    @Test
    public void concurrentInsertAndRemoveTest() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore();

        Thread t1 = new Thread(() -> {
            // Adds 100 events of type some_type 
            for (int i = 0; i < 100; i++) {
                eventStore.insert(new Event("some_type", i));
            }
        });

        Thread t2 = new Thread(() -> {
            // Deletes 10 events of type some_type
            try { 
                InMemoryEventIterator eventIterator = eventStore.query("some_type", 0, 20);
                while (eventIterator.moveNext()) {
                    if (eventIterator.current().timestamp() % 2 == 0) {
                        eventIterator.remove();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        try {
            t1.join();
            t2.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(eventStore.size(), 90);

    }

    @Test 
    public void concurrentInsertAndRemoveAndInsertThreadTest() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        // Adds 21 events of type some_type
        // Adding the event with timestamp 20 so that it can be filtered in the query
        for (int i = 0; i <= 20; i++) {
            eventStore.insert(new Event("some_type", i));
        }

        Thread t1 = new Thread(() -> {
            // Adds 100 events of type hundred_events_of_this_type
            for (int i = 0; i < 100; i++) {
                eventStore.insert(new Event("hundred_events_of_this_type", i));
            }
        });

        Thread t2 = new Thread(() -> {
            // Deletes 10 events of type some_type
            try (InMemoryEventIterator it = eventStore.query("some_type", 0, 20)) {
                while (it.moveNext()) {
                    if (it.current().timestamp() % 2 == 0) {
                        it.remove();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread t3 = new Thread(() -> {
            // Adds 20 events of type some_type
            for (int i = 200; i < 220; i++) {
                eventStore.insert(new Event("some_type", i));
            }
        });

        t1.start();
        t2.start();
        t3.start();
        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(131, eventStore.size());
    }
}
