package com.vmturbo.cost.component.savings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
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
     * For writing to audit log as soon as events are added to the journal, rather than waiting
     * for next time (hour+15 min).
     */
    private final AuditLogWriter auditLogWriter;

    /**
     * Creates a new instance. There should be just one instance of this in the system.
     *
     * @param auditLogWriter For writing audit entries to DB as soon as events are added.
     */
    InMemoryEntityEventsJournal(@Nonnull final AuditLogWriter auditLogWriter) {
        // Keys are sorted by timestamp. In the event that events have the same timestamp,
        // the values (SavingsEvents) are sorted according to a sorting priority. The sorting
        // priority is:
        //   - ActionEvent ACTION_EXPIRED
        //   - ActionEvent RECOMMENDATION_ADDED
        //   - ActionEvent EXECUTION_SUCCESS
        //   - ActionEvent RECOMMENDATION_REMOVED
        //   - Any TopologyEvent
        //   - All other events, including ActionEvents not listed above.
        events = TreeMultimap.create(Long::compareTo, SavingsEvent::compare);
        this.auditLogWriter = auditLogWriter;
    }

    @Override
    public void addEvents(@Nonnull final Collection<SavingsEvent> newEvents) {
        journalLock.writeLock().lock();
        try {
            newEvents.forEach(evt -> events.put(evt.getTimestamp(), evt));
        } finally {
            journalLock.writeLock().unlock();
        }
        auditLogWriter.write(new ArrayList<>(newEvents));
    }

    @Override
    public void addEvent(@Nonnull final SavingsEvent newEvent) {
        journalLock.writeLock().lock();
        try {
            events.put(newEvent.getTimestamp(), newEvent);
        } finally {
            journalLock.writeLock().unlock();
        }
        auditLogWriter.write(ImmutableList.of(newEvent));
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
        return returnEvents;
    }

    @Nonnull
    public List<SavingsEvent> removeEventsBetween(long startTime, long endTime) {
        final List<SavingsEvent> returnEvents = new ArrayList<>();
        journalLock.writeLock().lock();
        try {
            final Set<Long> keysBetween = events.keySet().subSet(startTime, endTime);
            keysBetween.forEach(keyTimestamp -> returnEvents.addAll(events.get(keyTimestamp)));
            // Cannot iterate over the multimap's live key set.
            (new HashSet<>(keysBetween)).forEach(events::removeAll);
        } finally {
            journalLock.writeLock().unlock();
        }
        return returnEvents;
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
                oldestEventTime = events.keySet().first();
            }
        } finally {
            journalLock.readLock().unlock();
        }
        return oldestEventTime;
    }

    @Nullable
    public Long getNewestEventTime() {
        Long newestEventTime = null;
        journalLock.readLock().lock();
        try {
            if (!events.isEmpty()) {
                // Get the least time that is greater than 0.
                newestEventTime = events.keySet().last();
            }
        } finally {
            journalLock.readLock().unlock();
        }
        return newestEventTime;
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
