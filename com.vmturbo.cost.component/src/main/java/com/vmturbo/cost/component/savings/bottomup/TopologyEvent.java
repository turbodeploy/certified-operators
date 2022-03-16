package com.vmturbo.cost.component.savings.bottomup;

import java.util.Optional;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cost.component.savings.tem.ProviderInfo;

/**
 * Describes TEM topology events.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable(lazyhash = true)
public interface TopologyEvent {
    /**
     * Timestamp of the topology event.
     *
     * @return timestamp of the event in milliseconds. This is copied from the timestamp of the
     * discovery that triggered the event.
     */
    Long getTimestamp();

    /**
     * Entity OID.
     *
     * @return the entity OID.  This can be absent in events where the OID is not necessary.
     */
    Long getEntityOid();

    /**
     * Topology event type.
     *
     * @return the topology event type.
     */
    Integer getEventType();

    /**
     * Type of target entity for this action. E.g 60 for Volumes.
     *
     * @return Entity type.
     */
    Integer getEntityType();

    /**
     * Get provider OID, if changed.
     *
     * @return if the provider OID changed, the new provider OID, else empty.
     * @deprecated This method is only used for TEP events. Provider OID is set in ProviderInfo for
     * TEM events.
     */
    @Deprecated
    Optional<Long> getProviderOid();

    /**
     * Provider Info.
     *
     * @return Provider info
     */
    Optional<ProviderInfo> getProviderInfo();

    /**
     * Entity was removed.
     *
     * @return true if present and the entity was removed.
     */
    Optional<Boolean> getEntityRemoved();

    /**
     * Checks if the event is valid.
     *
     * @return Confirms some required fields are present.
     */
    @Value.Derived
    default boolean isValid() {
        return getEntityOid() != 0L && getEventType() != 0;
    }

    /**
     * Get power state.
     *
     * @return Current power state.  True if the power state is on, else false.
     * @deprecated This only exists for backward compatibility until the savings calculator
     * supports costs based on usage reports.
     */
    @Deprecated
    Optional<Boolean> getPoweredOn();

    /**
     * Topology event types.
     */
    enum EventType {
        /**
         * Power state changed.
         */
        STATE_CHANGE(100),
        /**
         * Provider or commodity usage changed.
         */
        PROVIDER_CHANGE(200),
        /**
         * Commodity usage changed. This is only used for the audit log.  Commodity changes are
         * handled as a provider change.
         */
        COMMODITY_USAGE(400),
        /**
         * Entity removed.
         */
        ENTITY_REMOVED(800);

        private final int value;
        EventType(int value) {
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }
    }

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableTopologyEvent.Builder {}
}
