package com.vmturbo.cost.component.savings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.immutables.value.Value;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;
import org.jooq.tools.StringUtils;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
     * Returns and removes the events that occurred since (and including) the specified start time,
     * events are returned in ascending order of timestamp.
     * Note: Events are guaranteed to be in order only if their timestamps are different. For
     * 2 events with exact same timestamp, their order is not guaranteed, only that no such events
     * will be lost.
     *
     * @param startTime Start time (inclusive) since which events need to be fetched.
     * @return Events since the start time, sorted by time, also get removed from store.
     */
    @Nonnull
    List<SavingsEvent> removeEventsSince(long startTime);

    /**
     * Gets events between the specified start (inclusive) and end time (exclusive), and removes
     * them from the journal as well.
     * E.g requesting event removal between 10:00:00 and 11:00:00 will remove any events with
     * timestamps like 10:00:00, 10:00:01, ... 10:59:50, 10:59:59 but not anything with timestamp
     * 11:00:00.
     *
     * @param startTime Start time (inclusive).
     * @param endTime End time (exclusive).
     * @return Events in order between the time range.
     */
    @Nonnull
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
     *
     * @param startTime Start time (inclusive).
     * @param endTime End time (exclusive).
     * @param uuids set of UUIDs to get events for.  If the set is empty, all UUIDs will be returned.
     * @return Events in order between the time range for the selected UUIDs.
     */
    @Nonnull
    List<SavingsEvent> removeEventsBetween(long startTime, long endTime, @Nonnull Set<Long> uuids);

    /**
     * Removes all events in the store and returns them (in timestamp ascending order).
     *
     * @return All outstanding events.
     */
    @Nonnull
    List<SavingsEvent> removeAllEvents();

    /**
     * Returns the event current in the journal with the oldest available time. Can return null
     * if journal is empty.
     *
     * @return Timestamp of oldest event, or null if no events present currently.
     */
    @Nullable
    Long getOldestEventTime();

    /**
     * Returns the event current in the journal with the newest available time. Can return null
     * if journal is empty.
     *
     * @return Timestamp of newest event, or null if no events present currently.
     */
    @Nullable
    Long getNewestEventTime();

    /**
     * Gets current count of events in the store.
     *
     * @return Number of outstanding events.
     */
    int size();

    /**
     * Represents events of various types.
     */
    @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
    @Immutable(lazyhash = true)
    interface SavingsEvent {
        /**
         * OID of VM/DB/Volume etc that we are interested in.
         *
         * @return OID of VM/DB/Volume etc that we are interested in.
         */
        long getEntityId();

        /**
         * Time event received/occurred.
         *
         * @return Time event received/occurred.
         */
        long getTimestamp();

        /**
         * Time that the price change will become inactive.
         *
         * @return Time in milliseconds after execution when the action will expire.
         */
        Optional<Long> getExpirationTime();

        /**
         * Checks whether topology event is set.
         *
         * @return True if topology event is set.
         */
        @Derived
        default boolean hasTopologyEvent() {
            return getTopologyEvent().isPresent();
        }

        /**
         * Gets TopologyEvent info if present, check first with hasTopologyEvent().
         *
         * @return TopologyEvent Optional, only present if set previously.
         */
        Optional<TopologyEvent> getTopologyEvent();

        /**
         * Checks whether action event is set.
         *
         * @return True if action event is set.
         */
        @Derived
        default boolean hasActionEvent() {
            return getActionEvent().isPresent();
        }

        /**
         * Gets action event, if hasActionEvent() is true.
         *
         * @return Action event Optional, only present if set previously.
         */
        Optional<ActionEvent> getActionEvent();

        /**
         * Makes sure that one of action or topology events is set, but not both.
         */
        @Check
        default void validate() {
            Preconditions.checkArgument(hasTopologyEvent() ^ hasActionEvent());
        }

        /**
         * Checks whether price change info is available.
         *
         * @return True if price change info is there.
         */
        @Derived
        default boolean hasEntityPriceChange() {
            return getEntityPriceChange().isPresent();
        }

        /**
         * Gets info about price change - before and after action prices. This is set for most
         * ActionEvents, except for recommendation_removed event where it doesn't make sense.
         *
         * @return If applicable for the event, contains price change info.
         */
        Optional<EntityPriceChange> getEntityPriceChange();

        /**
         * Return a sorting code for a SavingsEvent.  The events sort from high to low:
         * - ActionEvent RECOMMENDATION_ADDED
         * - ActionEvent EXECUTION_SUCCESS
         * - ActionEvent RECOMMENDATION_REMOVED
         * - Any TopologyEvent
         *
         * @param event event used to generate code
         * @return a sorting code
         */
        default int getSortingPriority(SavingsEvent event) {
            if (event.hasActionEvent()) {
                return event.getActionEvent().get().getEventType().getSortingPriority();
            }
            if (event.hasTopologyEvent()) {
                return 1;
            }
            return 0;
        }

        /**
         * Compares this SavingsEvent to another.  Descending sort.
         * @param other the SavingsEvent to compare to
         * @return positive if other sorts higher, lower if other sorts lower, zero if equal.
         */
        default int compare(SavingsEvent other) {
            int thisCode = getSortingPriority(this);
            int otherCode = getSortingPriority(other);
            int result = otherCode - thisCode;
            if (result != 0) {
                return result;
            }
            // If the events that we are about are equal, fall back to a hash compare.
            return other.hashCode() - hashCode();
        }

        /**
         * Compares this SavingsEvent to another using the sorting priority and timestamp.
         * Descending sort.
         * @param other the SavingsEvent to compare to
         * @return positive if other sorts higher, lower if other sorts lower, zero if equal.
         */
        default int compareConsideringTimestamp(SavingsEvent other) {
            // Compare timestamps.  If equal, use the normal sorting priority.
            int result = Long.compare(getTimestamp(), other.getTimestamp());
            if (result != 0) {
                return result;
            }
            return compare(other);
       }

       /**
        * Creates a new builder.
        */
       class Builder extends ImmutableSavingsEvent.Builder {}
    }

    /**
     * Event sub-types that are related to actions.
     * Example actions are recommendation added/removed, and execution success.
     */
    @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
    @Immutable(lazyhash = true)
    interface ActionEvent {
        /**
         * Returns the OID of the associated action.
         *
         * @return Action OID.
         */
        long getActionId();

        /**
         * Gets the type of action event.
         *
         * @return Action event type.
         */
        ActionEventType getEventType();

        /**
         * Action description.
         *
         * @return E.g "vol-0bff4fc40fa6b045e from GP2 to GP3".
         */
        @Value.Default
        default String getDescription() {
            return StringUtils.EMPTY;
        }

        /**
         * Type of target entity for this action. E.g 60 for Volumes.
         *
         * @return Entity type.
         */
        @Value.Default
        default int getEntityType() {
            return EntityType.VIRTUAL_MACHINE_VALUE;
        }

        /**
         * Type of action, e.g SCALE or DELETE.
         *
         * @return Action type code.
         */
        @Value.Default
        default int getActionType() {
            return ActionType.SCALE_VALUE;
        }

        /**
         * Category for action, e.g Performance or Efficiency.
         *
         * @return ActionCategory from ActionSpec.
         */
        @Value.Default
        default int getActionCategory() {
            return ActionCategory.EFFICIENCY_IMPROVEMENT_VALUE;
        }

        /**
         * Creates a new builder.
         */
        class Builder extends ImmutableActionEvent.Builder {}

        /**
         * Types of actions.
         */
        enum ActionEventType {
            /**
             * A new action recommendation was detected, trigger for missed savings.
             */
            RECOMMENDATION_ADDED(4, 10),

            /**
             * A existing action recommendation is no longer detected, stops missed savings.
             */
            RECOMMENDATION_REMOVED(2, 11),

            /**
             * Action executed successfully, trigger for realized savings.
             */
            SCALE_EXECUTION_SUCCESS(3, 12),

            /**
             * One or more actions expired.
             */
            ACTION_EXPIRED(100, 13),

            /**
             * Delete action recommendation added.  There is no corresponding delete recommendation
             * removed action.
             */
            DELETE_EXECUTION_SUCCESS(3, 14);

            private final int sortingPriority;

            /**
             * Unique type code across all event types.
             */
            private final int typeCode;

            /**
             * Get the sorting priority for the event. Numerically higher priority events sort
             * before lower priority events. This is used when two events have the same timestamp.
             *
             * @param sortingPriority ordering for the event.
             * @param typeCode Code to make the event unique across all events (including TEP).
             */
            ActionEventType(int sortingPriority, int typeCode) {
                this.sortingPriority = sortingPriority;
                this.typeCode = typeCode;
            }

            public int getSortingPriority() {
                return this.sortingPriority;
            }

            public int getTypeCode() {
                return typeCode;
            }
        }
    }
}
