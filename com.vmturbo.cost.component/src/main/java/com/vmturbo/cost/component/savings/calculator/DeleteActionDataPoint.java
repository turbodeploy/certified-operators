package com.vmturbo.cost.component.savings.calculator;

import org.immutables.value.Value;

/**
 * Immutable object definition for holding values to be used for savings calculations in a data
 * point in the savings graph for delete actions.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
public interface DeleteActionDataPoint extends ActionDataPoint {

    /**
     * Expected savings per hour resulted from a delete action.
     *
     * @return savings per hour
     */
    double savingsPerHour();

    /**
     * Delete actions will be assigned a dummy provider ID of 0 as the destination provider ID.
     *
     * @return provider ID (always 0 for delete actions)
     */
    @Value.Derived
    default long getDestinationProviderOid() {
        return 0;
    }

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableDeleteActionDataPoint.Builder {}
}
