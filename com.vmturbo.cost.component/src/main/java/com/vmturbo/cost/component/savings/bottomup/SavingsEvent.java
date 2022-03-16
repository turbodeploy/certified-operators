package com.vmturbo.cost.component.savings.bottomup;

import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;

import com.vmturbo.cost.component.savings.bottomup.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.bottomup.SerializableSavingsEvent.SerializableActionEvent;
import com.vmturbo.cost.component.savings.bottomup.SerializableSavingsEvent.SerializableEntityPriceChange;
import com.vmturbo.cost.component.savings.bottomup.SerializableSavingsEvent.SerializableTopologyEvent;
import com.vmturbo.cost.component.savings.tem.ProviderInfo;
import com.vmturbo.cost.component.savings.tem.ProviderInfoSerializer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Wrapper over different types of savings related events (e.g action, topology etc.) that need to processed.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
public interface SavingsEvent {
    /**
     * Used for json serialization and deserialization.
     */
    Gson gson = new GsonBuilder()
            .registerTypeAdapterFactory(new GsonAdaptersSerializableSavingsEvent())
            .registerTypeAdapter(ProviderInfo.class, new ProviderInfoSerializer())
            .create();

    /**
     * Event ID, set only if event has been saved, otherwise null.
     *
     * @return Optional numeric event id.
     */
    @Nullable
    Long getEventId();

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
    @Value.Derived
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
    @Value.Derived
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
    @Value.Check
    default void validate() {
        Preconditions.checkArgument(hasTopologyEvent() ^ hasActionEvent());
    }

    /**
     * Checks whether price change info is available.
     *
     * @return True if price change info is there.
     */
    @Value.Derived
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
    @Value.Derived
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
     * Whether this is a valid event with fields set.
     *
     * @return True if required fields are set correctly.
     */
    @Value.Derived
    default boolean isValid() {
        if (!hasTopologyEvent() && !hasActionEvent()) {
            return false;
        }
        if (hasTopologyEvent() && !getTopologyEvent().get().isValid()) {
            return false;
        }
        if (hasActionEvent() && !getActionEvent().get().isValid()) {
            return false;
        }
        return getEntityId() != 0L
                && getEntityType().getNumber() != 0
                && getTimestamp() != 0L
                && getEventType() != 0;
    }

    /**
     * Gets the entity type from the underlying action or topology event.
     *
     * @return Entity type for this event, or UNKNOWN.
     */
    @Value.Derived
    default EntityType getEntityType() {
        if (hasActionEvent()) {
            return EntityType.forNumber(getActionEvent().get().getEntityType());
        }
        if (hasTopologyEvent()) {
            return EntityType.forNumber(getTopologyEvent().get().getEntityType());
        }
        return EntityType.UNKNOWN;
    }

    /**
     * Gets the event type code, e.g 10 for recommendation_added event.
     *
     * @return Event type code.
     */
    @Value.Derived
    default int getEventType() {
        if (hasActionEvent()) {
            return getActionEvent().get().getEventType().getTypeCode();
        }
        if (hasTopologyEvent()) {
            return getTopologyEvent().get().getEventType();
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
     * Converts this SavingsEvent into a map that is later serialized to DB.
     *
     * @return Map info.
     */
    default String serialize() {
        final SerializableSavingsEvent.Builder builder = new SerializableSavingsEvent.Builder();
        if (getExpirationTime().isPresent()) {
            builder.expirationTime(getExpirationTime().get());
        }
        if (getActionEvent().isPresent()) {
            final ActionEvent actionEvent = getActionEvent().get();
            builder.actionEvent(new SerializableActionEvent.Builder()
                            .actionId(actionEvent.getActionId())
                            .actionType(actionEvent.getActionType())
                            .actionCategory(actionEvent.getActionCategory())
                    .build());
        } else if (getTopologyEvent().isPresent()) {
            final TopologyEvent topologyEvent = getTopologyEvent().get();
            final SerializableTopologyEvent.Builder topologyBuilder =
                    new SerializableTopologyEvent.Builder();
            if (topologyEvent.getProviderInfo().isPresent()) {
                topologyBuilder.providerInfo(topologyEvent.getProviderInfo().get());
            }
            if (topologyEvent.getEntityRemoved().isPresent()) {
                topologyBuilder.entityRemoved(topologyEvent.getEntityRemoved().get());
            }
            builder.topologyEvent(topologyBuilder.build());
        }
        if (getEntityPriceChange().isPresent()) {
            final EntityPriceChange priceChange = getEntityPriceChange().get();
            builder.entityPriceChange(new SerializableEntityPriceChange.Builder()
                            .sourceCost(priceChange.getSourceCost())
                            .destinationCost(priceChange.getDestinationCost())
                            .sourceOid(priceChange.getSourceOid())
                            .destinationOid(priceChange.getDestinationOid())
                            .active(priceChange.active() ? 1 : 0)
                    .build());
        }
        return gson.toJson(builder.build());
    }

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableSavingsEvent.Builder {
        /**
         * Creates a SavingsEvent.Builder with data read from DB.
         *
         * @param eventId Savings event id with DB auto generated id.
         * @param eventTimestamp Timestamp of event when it was last created or updated.
         * @param entityOid Entity ID.
         * @param eventType Event type.
         * @param entityType Entity type.
         * @param serializedJson Additional info that needs to be deserialized.
         * @return SavingsEvent.Builder with details.
         */
        public final SavingsEvent.Builder deserialize(long eventId, long eventTimestamp,
                long entityOid, int eventType, int entityType, final String serializedJson) {
            eventId(eventId);
            timestamp(eventTimestamp);
            entityId(entityOid);

            final SerializableSavingsEvent savingsEvent = gson.fromJson(serializedJson,
                    SerializableSavingsEvent.class);
            if (savingsEvent.getExpirationTime().isPresent()) {
                expirationTime(savingsEvent.getExpirationTime().get());
            }
            if (savingsEvent.getActionEvent().isPresent()) {
                final SerializableActionEvent actionEvent = savingsEvent.getActionEvent().get();
                actionEvent(new ActionEvent.Builder()
                        .actionId(actionEvent.getActionId())
                        .eventType(ActionEventType.fromType(eventType))
                        .entityType(entityType)
                        .actionType(actionEvent.getActionType())
                        .actionCategory(actionEvent.getActionCategory())
                        .description(StringUtils.EMPTY)
                        .build());
            } else if (savingsEvent.getTopologyEvent().isPresent()) {
                final SerializableTopologyEvent topologyEvent = savingsEvent.getTopologyEvent().get();
                final TopologyEvent.Builder topologyBuilder = new TopologyEvent.Builder();
                topologyBuilder
                        .timestamp(eventTimestamp)
                        .entityOid(entityOid)
                        .eventType(eventType)
                        .entityType(entityType);
                topologyBuilder.providerInfo(topologyEvent.getProviderInfo());
                if (topologyEvent.getEntityRemoved().isPresent()) {
                    topologyBuilder.entityRemoved(topologyEvent.getEntityRemoved().get());
                }
                topologyEvent(topologyBuilder.build());
            }
            if (savingsEvent.getEntityPriceChange().isPresent()) {
                final SerializableEntityPriceChange priceChange = savingsEvent.getEntityPriceChange().get();
                entityPriceChange(new EntityPriceChange.Builder()
                        .sourceCost(priceChange.getSourceCost())
                        .destinationCost(priceChange.getDestinationCost())
                        .sourceOid(priceChange.getSourceOid())
                        .destinationOid(priceChange.getDestinationOid())
                        .active(priceChange.active() == 1)
                        .build());
            }
            return this;
        }
    }
}
