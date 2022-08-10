package com.vmturbo.cost.component.savings.calculator;

import org.immutables.value.Value;

/**
 * Immutable object definition that hold information about a commodity resize.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
public interface CommodityResize {

    /**
     * Commodity Type.
     *
     * @return commodity type
     */
    int getCommodityType();

    /**
     * Commodity capacity before the resize.
     *
     * @return old capacity
     */
    double getOldCapacity();

    /**
     * Commodity capacity after the resize.
     *
     * @return new capacity
     */
    double getNewCapacity();

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableCommodityResize.Builder {}
}
