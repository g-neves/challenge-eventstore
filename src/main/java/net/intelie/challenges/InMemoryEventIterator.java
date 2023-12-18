package net.intelie.challenges;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryEventIterator implements EventIterator {

    // For this class, I will have 4 variables
    // The iterator itself; the type used in the query;
    // the original hash map (for the remove method); 
    // and finally the currentEvent, so that the methods behave exactly as asked in the challenge
    private final Iterator<ConcurrentNavigableMap.Entry<Long, Event>> iterator;
    private final String type;
    private final ConcurrentHashMap<String, ConcurrentNavigableMap<Long, Event>> eventMap;
    private Event currentEvent;

    /**
     * Constructs an iterator for the specified type and events.
     *
     * @param type   The type of events being iterated.
     * @param events The underlying map of events to iterate over.
     * @param eventMap The original InMemoryEventStore object with all events
     */
    public InMemoryEventIterator(String type, ConcurrentNavigableMap<Long, Event> events, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, Event>> eventMap) {
        this.iterator = events.entrySet().iterator();
        this.type = type;
        this.eventMap = eventMap;
        this.currentEvent = null; // Since the moveNext method was not called yet, we have no current event
    }

    /**
     * Moves the iterator to the next event.
     *
     * @return true if the iterator has more events, false otherwise.
     */
    @Override
    public boolean moveNext() {
        if (iterator.hasNext()) {
            currentEvent = iterator.next().getValue();
            return true;
        } else {
            currentEvent = null; // To normalize the treatment in the current method
            return false;
        }
    }

    /**
     * Retrieves the current event from the iterator.
     *
     * @return The current event.
     * @throws IllegalStateException If there is no current event.
     */
    @Override
    public Event current() {
        if (currentEvent != null) {
            return currentEvent;
        } else {
            throw new IllegalStateException("No current event"); // Very beginning of very end of the iterator
        }
    }

    /**
     * Removes the current event from original InMemoryEventStore object
     *
     * @throws IllegalArgumentException If there not such type in the store
     */
    @Override
    public void remove() {
        // This I had some trouble performing. I used the compute
        // so that I read it was an atomic operation and I would have no 
        // problem with concurrency.
        eventMap.compute(type, (key, value) -> {
            if (value != null) { // If we have the inner map with according key
                value.remove(current().timestamp()); // remove the event from inner map
                return value.isEmpty() ? null : value; // This is just a return for the lambda functin of the compute method; return the inner map if it is not null
            } else {
                throw new IllegalArgumentException("Type not found: " + type); // If there is no inner map in the according key, throws an error
            }
        });
    }

    @Override
    public void close() throws Exception { // This I had to create so that the code could compile
    }

}

