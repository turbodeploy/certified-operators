package com.vmturbo.cost.component.savings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.TreeMultimap;

/**
 * In memory (default) implementation of events store.
 */
class InMemoryEntityEventsJournal implements EntityEventsJournal {
    /**
     * Sorted (by event timestamp) map of events.
     */
    private final TreeMultimap<Long, SavingsEvent> events;

    /**
     * Lock to protect journal during read/write.
     */
    private final ReadWriteLock journalLock = new ReentrantReadWriteLock();

    /**
     * Creates a new instance. There should be just one instance of this in the system.
     */
    InMemoryEntityEventsJournal() {
        // Keys are sorted by timestamp. In the event that events have the same timestamp,
        // the values (SavingsEvents) are sorted according to a sorting priority. The sorting
        // priority is:
        //   - ActionEvent RECOMMENDATION_ADDED
        //   - ActionEvent EXECUTION_SUCCESS
        //   - ActionEvent RECOMMENDATION_REMOVED
        //   - Any TopologyEvent
        //   - All other events, including ActionEvents not listed above.
        events = TreeMultimap.create(Long::compareTo, SavingsEvent::compare);
    }

    @Override
    public void addEvents(@Nonnull final Collection<SavingsEvent> newEvents) {
        journalLock.writeLock().lock();
        try {
            newEvents.forEach(evt -> events.put(evt.getTimestamp(), evt));
        } finally {
            journalLock.writeLock().unlock();
        }
    }

    @Override
    public void addEvent(@Nonnull final SavingsEvent newEvent) {
        journalLock.writeLock().lock();
        try {
            events.put(newEvent.getTimestamp(), newEvent);
        } finally {
            journalLock.writeLock().unlock();
        }
    }

    @Override
    @Nonnull
    public List<SavingsEvent> removeEventsSince(long startTime) {
        final List<SavingsEvent> returnEvents = new ArrayList<>();
        journalLock.writeLock().lock();
        try {
            final Set<Long> keysSince = events.keySet().tailSet(startTime);
            keysSince.forEach(keyTimestamp -> returnEvents.addAll(events.get(keyTimestamp)));
            // Cannot iterate over the multimap's live key set.
            (new HashSet<>(keysSince)).forEach(events::removeAll);
        } finally {
            journalLock.writeLock().unlock();
        }
        return Collections.unmodifiableList(returnEvents);
    }

    @Nonnull
    public List<SavingsEvent> removeEventsBetween(long startTime, long endTime) {
        final List<SavingsEvent> returnEvents = new ArrayList<>();
        journalLock.writeLock().lock();
        try {
            final Set<Long> keysBetween = new HashSet<>(events.keySet().subSet(startTime, endTime));
            keysBetween.forEach(keyTimestamp -> returnEvents.addAll(events.get(keyTimestamp)));
            // Cannot iterate over the multimap's live key set.
            (new HashSet<>(keysBetween)).forEach(events::removeAll);
        } finally {
            journalLock.writeLock().unlock();
        }
        return Collections.unmodifiableList(returnEvents);
    }

    @Override
    @Nonnull
    public List<SavingsEvent> removeAllEvents() {
        return removeEventsSince(0L);
    }

    @Nullable
    public Long getOldestEventTime() {
        Long oldestEventTime = null;
        journalLock.readLock().lock();
        try {
            if (!events.isEmpty()) {
                // Get the least time that is greater than 0.
                oldestEventTime = events.keySet().higher(0L);
            }
        } finally {
            journalLock.readLock().unlock();
        }
        return oldestEventTime;
    }

    @Override
    public int size() {
        int size = 0;
        journalLock.readLock().lock();
        try {
            size = events.size();
        } finally {
            journalLock.readLock().unlock();
        }
        return size;
    }
}
