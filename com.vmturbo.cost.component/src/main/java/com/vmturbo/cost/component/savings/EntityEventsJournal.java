package com.vmturbo.cost.component.savings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

/**
 * Keeps track of events that can affect savings calculations. Events (like power state change)
 * get added to this store. They get periodically processed by the SavingsProcessor, which removes
 * a chunk of events, uses them to calculate savings.
 * Events are sorted by timestamp - the time event occurred, not when they were added.
 */
interface EntityEventsJournal {
    /**
     * Adds a set of events of different types, to the store.
     *
     * @param newEvents Events to add.
     */
    void addEvents(@Nonnull Collection<SavingsEvent> newEvents);

    /**
     * Adds 1 event to the store.
     *
     * @param newEvent Event to add to store.
     */
    void addEvent(@Nonnull SavingsEvent newEvent);

    /**
     * Gets events between start and end times.
     *
     * @param startTime Start time inclusive.
     * @param endTime End time exclusive.
     * @return Stream of SavingsEvent instances.
     */
    Stream<SavingsEvent> getEventsBetween(long startTime, long endTime);

    /**
     * Gets events between start and end time, but if entityOids are specified, only gets the matching ones.
     *
     * @param startTime Start time inclusive.
     * @param endTime End time exclusive.
     * @param entityOids If non-empty, only events with entity oids that match this set will be returned.
     * @return Stream of SavingsEvent instances.
     */
    @Nonnull
    default Stream<SavingsEvent> getEventsBetween(long startTime, long endTime, @Nonnull final Set<Long> entityOids) {
        return getEventsBetween(startTime, endTime)
                .filter(event -> entityOids.isEmpty() || entityOids.contains(event.getEntityId()));
    }

    /**
     * Purge all events older than the given timestamp.
     *
     * @param eventTime Timestamp (exclusive) before which events need to be purged from the store.
     * @return Number of events that were purged.
     */
    int purgeEventsOlderThan(long eventTime);

    /**
     * Whether events are persisted in DB or not.
     *
     * @return true if events are persisted.
     */
    default boolean persistEvents() {
        return false;
    }

    /**
     * Returns and removes the events that occurred since (and including) the specified start time,
     * events are returned in ascending order of timestamp.
     * Note: Events are guaranteed to be in order only if their timestamps are different. For
     * 2 events with exact same timestamp, their order is not guaranteed, only that no such events
     * will be lost.
     *
     * @param startTime Start time (inclusive) since which events need to be fetched.
     * @return Events since the start time, sorted by time, also get removed from store.
     * @deprecated Not being used for persistent events store going forward, replaced with 'get'.
     */
    @Nonnull
    @Deprecated
    List<SavingsEvent> removeEventsSince(long startTime);

    /**
     * Gets events between the specified start (inclusive) and end time (exclusive), and removes
     * them from the journal as well.
     * E.g requesting event removal between 10:00:00 and 11:00:00 will remove any events with
     * timestamps like 10:00:00, 10:00:01, ... 10:59:50, 10:59:59 but not anything with timestamp
     * 11:00:00.
     * Deprecated in favor of getEventsBetween(startTime, endTime).
     *
     * @param startTime Start time (inclusive).
     * @param endTime End time (exclusive).
     * @return Events in order between the time range.
     * @deprecated Not being used for persistent events store going forward, replaced with 'get'.
     */
    @Nonnull
    @Deprecated
    default List<SavingsEvent> removeEventsBetween(long startTime, long endTime) {
        return removeEventsBetween(startTime, endTime, Collections.emptySet());
    }

    /**
     * Gets events between the specified start (inclusive) and end time (exclusive), and removes
     * them from the journal as well.  If the UUIDs list is non-empty, only the events related to
     * the UUIDs in the list will be returned.
     * E.g requesting event removal between 10:00:00 and 11:00:00 will remove any events with
     * timestamps like 10:00:00, 10:00:01, ... 10:59:50, 10:59:59 but not anything with timestamp
     * 11:00:00.
     * Deprecated in favor of getEventsBetween(startTime, endTime, entityOids).
     *
     * @param startTime Start time (inclusive).
     * @param endTime End time (exclusive).
     * @param entityOids set of UUIDs to get events for.  If the set is empty, all UUIDs will be returned.
     * @return Events in order between the time range for the selected UUIDs.
     * @deprecated Not being used for persistent events store going forward, replaced with 'get'.
     */
    @Nonnull
    @Deprecated
    List<SavingsEvent> removeEventsBetween(long startTime, long endTime, @Nonnull Set<Long> entityOids);

    /**
     * Removes all events in the store and returns them (in timestamp ascending order).
     *
     * @return All outstanding events.
     * @deprecated Not being used for persistent events store going forward, replaced with 'get'.
     */
    @Nonnull
    @Deprecated
    List<SavingsEvent> removeAllEvents();

    /**
     * Returns the event current in the journal with the oldest available time. Can return null
     * if journal is empty.
     *
     * @return Timestamp of oldest event, or null if no events present currently.
     * @deprecated Not being used for persistent events store going forward, replaced with 'get'.
     */
    @Nullable
    @Deprecated
    Long getOldestEventTime();

    /**
     * Gets current count of events in the store.
     * NOTE: This is only there for unit test verification and not to be used in production runtime
     * code - too inefficient, especially for persistent events.
     *
     * @return Number of outstanding events.
     * @deprecated Not being used for persistent events store going forward, heavyweight call.
     */
    @Deprecated
    @VisibleForTesting
    int size();
}
