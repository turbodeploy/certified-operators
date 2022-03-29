package com.vmturbo.cost.component.savings.calculator;

import org.immutables.value.Value;

/**
 * Immutable object definition for holding values of a data point in the watermark graph.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
public interface Watermark {
    /**
     * Get the timestamp.
     *
     * @return timestamp
     */
    long getTimestamp();

    /**
     * High watermark: the highest before-action cost of the entity between the beginning of the
     * period considered for savings calculation and the timestamp of this data point.
     *
     * @return high watermark
     */
    double getHighWatermark();

    /**
     * Low watermark: the lowest before-action cost of the entity between the beginning of the
     * period considered for savings calculation and the timestamp of this data point.
     *
     * @return low watermark
     */
    double getLowWatermark();

    /**
     * The destination provider OID. (i.e. the provider of the entity after the timestamp of this
     * data point.)
     *
     * @return destination provider OID
     */
    long getDestinationProviderOid();

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableWatermark.Builder {}
}
