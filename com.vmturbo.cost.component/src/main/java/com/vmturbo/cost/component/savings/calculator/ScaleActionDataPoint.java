package com.vmturbo.cost.component.savings.calculator;

import org.immutables.value.Value;

/**
 * Immutable object definition for holding values to be used for savings calculations in a data
 * point in the savings graph for scale actions.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
public interface ScaleActionDataPoint extends ActionDataPoint {

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
     * Creates a new builder.
     */
    class Builder extends ImmutableScaleActionDataPoint.Builder {}
}
