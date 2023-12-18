package net.intelie.challenges;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryEventStore implements EventStore {

    // I'm using two maps to store the events. 
    // First, I was trying an approach with binary search trees and the Semaphore class.
    // However, after I discovered the already implemented concurrent maps, I thought it 
    // would make no sense to recreate the wheel and used them.
    //
    // For the outer map, I used the ConcurrentHashMap, because,since it is a HashMap,
    // it is O(1).
    //
    // For the inner map, I needed to filter value, so the keys should be ordered. Thus, 
    // I used the ConcurrentNavigableMap that does the job.
    private final ConcurrentHashMap<String, ConcurrentNavigableMap<Long, Event>> eventMap = new ConcurrentHashMap<>();

    /**
     * Stores an event.
     *
     * @param event The event to be stored.
     */
    @Override
    public void insert(Event event) {
        // Get the inner map. If the key does not exists, create a new empty inner map with that key
        ConcurrentNavigableMap<Long, Event> events = eventMap.computeIfAbsent(event.type(), k -> new ConcurrentSkipListMap<Long, Event>());

        events.put(event.timestamp(), event); // Inserts the event into the inner map
        eventMap.put(event.type(), events); // Insert the inner map into the outer map
    }

    /**
     * Removes all events of a specific type.
     *
     * @param type The type of events to be removed.
     * @throws IllegalArgumentException If the specified type is not found in the event store.
     */
    @Override
    public void removeAll(String type) {
        if (eventMap.remove(type) == null) { // When we check for the key, we already remove it.
            throw new IllegalArgumentException("Type not found: " + type); // If the return for the check is null, the type is not present
        }
    }

    public int size() {
        return eventMap.values().stream().mapToInt(ConcurrentNavigableMap::size).sum(); // Very useful method for the tests
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
            throw new IllegalArgumentException("endTime must be greater than startTime"); // Useful check to avoid unnecessary calculations
        }

        ConcurrentNavigableMap<Long, Event> eventsType = eventMap.get(type); // Gets the inner map according to type

        if (eventsType != null) {
            ConcurrentNavigableMap<Long, Event> filteredEvents = eventsType.subMap(startTime, true, endTime, false); // If it exists, filter according to start and end times
            return new InMemoryEventIterator(type, filteredEvents, eventMap); // Returns the according iterator
        } else {
            throw new IllegalArgumentException("Type not found: " + type);
        }
    }

}
