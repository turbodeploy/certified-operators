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
     * Boolean value to show if the entity was expected to be covered by RI at the time when
     * the action was executed.
     *
     * @return true if RI coverage was expected when the action was executed; false otherwise.
     */
    boolean isCloudCommitmentExpectedAfterAction();

    /**
     * Boolean value to show if the action was expected to generate savings or investment.
     *
     * @return true if savings was expected (efficiency action);
     *         false if investments was expected (performance action)
     */
    boolean isSavingsExpectedAfterAction();

    /**
     * The cost of the entity before taking the action.
     *
     * @return The on-demand price of the destination tier.
     */
    double getBeforeActionCost();

    /**
     * The on-demand price of the destination tier.
     *
     * @return The on-demand price of the destination tier.
     */
    double getDestinationOnDemandCost();

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableScaleActionDataPoint.Builder {}
}
