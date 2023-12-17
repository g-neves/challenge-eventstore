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

    @Test
    public void insertTest() {
        InMemoryEventStore myTest = new InMemoryEventStore();
        for (int i = 0; i < 20; i++) {
            myTest.insert(new Event("some_type", i)); 
        }

        // Asserts all 20 events of type "some_type" were inserted
        assertEquals(myTest.size(), 20);
    }

    @Test
    public void removeAllTest() {
        InMemoryEventStore myTest = new InMemoryEventStore();
        
        for (int i = 0; i < 20; i++) {
            myTest.insert(new Event("some_type", i)); 
        }
        // Inserts another type
        myTest.insert(new Event("some_other_type", 123L));
        // Delete all 20 events with type "some_type"
        myTest.removeAll("some_type");

        // Asserts only event of type "some_other_type" is present
        assertEquals(myTest.size(), 1);
    }

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
        
    @Test
    public void removeTest() {
        InMemoryEventStore myTest = new InMemoryEventStore();
        
        for (int i = 0; i < 20; i++) {
            myTest.insert(new Event("some_type", i)); 
        }

        InMemoryEventIterator myIterator = myTest.query("some_type", 5, 20);

        // Moves to the next element (Event with timestamp 5, in this case)
        myIterator.moveNext();
        assertEquals(myIterator.current().timestamp(), 5);
        // Removes the event with timestamp 5
        myIterator.remove();
        
        InMemoryEventIterator yetAnotherIterator = myTest.query("some_type", 5, 20);
        yetAnotherIterator.moveNext();

        // Asserts the first event from new query has timestamp 6
        assertEquals(yetAnotherIterator.current().timestamp(), 6);
    }

    @Test 
    public void currentShouldThrowError() {
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
    
    @Test 
    public void endTimeMustBeGreaterThanStartTime() {
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
    public void singleThreadInsertQueryRemove() {
        InMemoryEventStore eventStore = new InMemoryEventStore();

        // Insert events
        Event event1 = new Event("type1", 1L);
        Event event2 = new Event("type1", 2L);
        eventStore.insert(event1);
        eventStore.insert(event2);

        // Query events
        EventIterator iterator = eventStore.query("type1", 0L, 3L);
        assertTrue(iterator.moveNext());
        assertEquals(event1, iterator.current());
        assertTrue(iterator.moveNext());
        assertEquals(event2, iterator.current());
        assertFalse(iterator.moveNext());

        // Remove events
        eventStore.removeAll("type1");

        // Verify removal
        try {
            iterator = eventStore.query("type1", 0L, 3L);
            fail("Expected IllegalArgumentException, but query did not throw an exception.");
        } catch (IllegalArgumentException e) {
            // Expected exception
        }
    }

    @Test
    public void concurrentInsertQueryRemove() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        int numThreads = 10;
        int numEventsPerThread = 1000;

        // Concurrent insert
        for (int i = 0; i < numThreads; i++) {
            final int threadNum = i;
            executorService.submit(() -> {
                for (int j = 0; j < numEventsPerThread; j++) {
                    Event event = new Event("type" + threadNum, j);
                    eventStore.insert(event);
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        // Concurrent query and remove
        executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < numThreads; i++) {
            final int threadNum = i;
            executorService.submit(() -> {
                EventIterator iterator = eventStore.query("type" + threadNum, 0L, numEventsPerThread);
                int count = 0;
                while (iterator.moveNext()) {
                    count++;
                }
                assertEquals(numEventsPerThread, count);

                eventStore.removeAll("type" + threadNum);

                iterator = eventStore.query("type" + threadNum, 0L, numEventsPerThread);
                assertFalse(iterator.moveNext());
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test 
    public void threadTest() {
        InMemoryEventStore myTest = new InMemoryEventStore();
        for (int i = 0; i < 21; i++) {
            myTest.insert(new Event("B", i));
        }
        myTest.insert(new Event("C", 0));
        myTest.insert(new Event("D", 0));
        myTest.insert(new Event("E", 0));
        myTest.insert(new Event("F", 0));

        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                myTest.insert(new Event("A", i));
            }
        });

        Thread t2 = new Thread(() -> {
            try (InMemoryEventIterator it = myTest.query("B", 0, 20)) {
                while (it.moveNext()) {
                    if (it.current().timestamp() % 2 == 0) {
                        it.remove();
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

        assertEquals(115, myTest.size());
    }
}
