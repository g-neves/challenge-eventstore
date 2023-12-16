package net.intelie.challenges;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryEventStore implements EventStore {

    // I'm using the ConcurrentNavigableMap to avoid the concurrence problems.
    // At first, I was implementing the solution with the Semaphore class for everything, but
    // I thought it would be easier and simpler to use a class that already deals
    // with concurrence and is available in the java.util package. 
    private final ConcurrentHashMap<String, ConcurrentNavigableMap<Long, Event>> eventMap = new ConcurrentHashMap<>();
    // TODO: Check if there exists ConcurrentMap -> Better than ConcurrentNavigableMap

    /**
     * Stores an event.
     *
     * @param event The event to be stored.
     */
    @Override
    public void insert(Event event) {

        ConcurrentNavigableMap<Long, Event> events = eventMap.computeIfAbsent(event.type(), k -> new ConcurrentSkipListMap<Long, Event>());
        // ConcurrentNavigableMap<Long, Event> events = eventMap.get(event.type());

        // if (events == null) {
        //     events = new ConcurrentSkipListMap<>();
        // }

        events.put(event.timestamp(), event);
        eventMap.put(event.type(), events);
    }

    /**
     * Removes all events of a specific type.
     *
     * @param type The type of events to be removed.
     * @throws IllegalArgumentException If the specified type is not found in the event store.
     */
    @Override
    public void removeAll(String type) {
        if (eventMap.remove(type) == null) {
            throw new IllegalArgumentException("Type not found: " + type);
        }
    }

    public int size() {
        return eventMap.values().stream().mapToInt(ConcurrentNavigableMap::size).sum();
    }

    /**
     * Retrieves an iterator for events based on their type and timestamp.
     *
     * @param type      The type we are querying for.
     * @param startTime Start timestamp (inclusive).
     * @param endTime   End timestamp (exclusive).
     * @return An iterator where all its events have the same type as
     * {@param type} and timestamp between {@param startTime}
     * (inclusive) and {@param endTime} (exclusive).
     * @throws IllegalArgumentException If the endTime is not greater than the startTime.
     */
    @Override
    public InMemoryEventIterator query(String type, long startTime, long endTime) {

        if (startTime >= endTime) {
            throw new IllegalArgumentException("endTime must be greater than startTime");
        }

        ConcurrentNavigableMap<Long, Event> eventsType = eventMap.get(type);

        if (eventsType != null) {
            ConcurrentNavigableMap<Long, Event> filteredEvents = eventsType.subMap(startTime, true, endTime, false);
            return new InMemoryEventIterator(type, filteredEvents, eventMap);
        } else {
            // TODO: Throw error in this situation
            throw new IllegalArgumentException("Type not found: " + type);
            // return new InMemoryEventIterator(type, new ConcurrentSkipListMap<>(), eventMap);
        }
    }

