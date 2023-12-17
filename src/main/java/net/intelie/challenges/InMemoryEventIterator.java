package net.intelie.challenges;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryEventIterator implements EventIterator {

    private final Iterator<ConcurrentNavigableMap.Entry<Long, Event>> iterator;
    private final String type;
    private final ConcurrentHashMap<String, ConcurrentNavigableMap<Long, Event>> eventMap;
    private Event currentEvent;

    /**
     * Constructs an iterator for the specified type and events.
     *
     * @param type   The type of events being iterated.
     * @param events The underlying map of events to iterate over.
     */
    public InMemoryEventIterator(String type, ConcurrentNavigableMap<Long, Event> events, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, Event>> eventMap) {
        this.iterator = events.entrySet().iterator();
        this.type = type;
        this.eventMap = eventMap;
        this.currentEvent = null;
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
            currentEvent = null;
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
            throw new IllegalStateException("No current event");
        }
    }

    @Override
    public void remove() {
        eventMap.compute(type, (key, value) -> {
            if (value != null) {
                value.remove(current().timestamp());
                return value.isEmpty() ? null : value;
            } else {
                throw new IllegalArgumentException("Type not found: " + type);
            }
        });
    }

    @Override
    public void close() throws Exception {
    }

}

