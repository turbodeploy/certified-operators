package com.vmturbo.cost.component.savings.calculator;

import org.immutables.value.Value;

/**
 * Immutable object definition for holding values of a data point in the savings graph.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
public interface ActionDataPoint {
    /**
     * Get the timestamp.
     *
     * @return timestamp
     */
    long getTimestamp();

    /**
     * The destination provider OID. (i.e. the provider of the entity after the timestamp of this
     * data point.) Default value is 0.
     *
     * @return destination provider OID
     */
    @Value.Default
    default long getDestinationProviderOid() {
        return 0;
    }
}
