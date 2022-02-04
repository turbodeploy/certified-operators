package com.vmturbo.cost.component.savings;

import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.Maps;

import org.immutables.value.Value;

/**
 * Action related event.
 * Example actions are recommendation added/removed, and execution success.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
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
     * Action description. Not being persisted.
     *
     * @return E.g "vol-0bff4fc40fa6b045e from GP2 to GP3".
     * @deprecated Not really used for any calculations. Used to be in audit Db, won't be in
     *      events table due to storage considerations.
     */
    @Deprecated
    String getDescription();

    /**
     * Type of target entity for this action. E.g 60 for Volumes.
     *
     * @return Entity type.
     */
    Integer getEntityType();

    /**
     * Type of action, e.g SCALE or DELETE.
     *
     * @return Action type code.
     */
    int getActionType();

    /**
     * Category for action, e.g Performance or Efficiency.
     *
     * @return ActionCategory from ActionSpec.
     */
    int getActionCategory();

    /**
     * Checks if this event is valid.
     *
     * @return Confirms that certain required fields are present, returns true if so.
     */
    @Value.Derived
    default boolean isValid() {
        return getActionId() != 0L && getEventType() != null;
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
        DELETE_EXECUTION_SUCCESS(3, 14),

        /**
         * Unknown event placeholder in case some bad value is read.
         */
        UNKNOWN(101, 0);

        private final int sortingPriority;

        /**
         * Unique type code across all event types.
         */
        private final int typeCode;

        /**
         * Reverse lookup map.
         */
        private static final Map<Integer, ActionEventType> lookup = Maps.uniqueIndex(
                Arrays.asList(ActionEventType.values()), ActionEventType::getTypeCode
        );

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

        /**
         * Gets ActionEventType instance given the type code (e.g 10 for RECOMMENDATION_ADDED).
         *
         * @param actionTypeCode Event type code.
         * @return ActionEventType instance, or UNKNOWN if a valid one not found.
         */
        public static ActionEventType fromType(int actionTypeCode) {
            ActionEventType eventType = lookup.get(actionTypeCode);
            return eventType == null ? ActionEventType.UNKNOWN : eventType;
        }
    }
}
