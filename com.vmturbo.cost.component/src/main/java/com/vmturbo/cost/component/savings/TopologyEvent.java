package com.vmturbo.cost.component.savings;

import java.util.Map;
import java.util.Optional;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

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
    @Value.Default
    default Long getEntityOid() {
        return 0L;
    }

    /**
     * Topology event type.
     *
     * @return the topology event type.
     */
    Integer getEventType();

    /**
     * Get provider OID, if changed.
     *
     * @return if the provider OID changed, the new provider OID, else empty.
     */
    Optional<Long> getProviderOid();

    /**
     * Get commodity usage changes.
     *
     * @return map from commodity type to amount.
     */
    Map<Integer, Double> getCommodityUsage();

    /**
     * Entity was removed.
     *
     * @return true if present and the entity was removed.
     */
    Optional<Boolean> getEntityRemoved();

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
