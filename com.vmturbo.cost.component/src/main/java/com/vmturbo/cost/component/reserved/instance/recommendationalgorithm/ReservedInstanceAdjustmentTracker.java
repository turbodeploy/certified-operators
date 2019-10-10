package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Objects;

import javax.annotation.Nonnull;

public class ReservedInstanceAdjustmentTracker {
    private final float[] smallAdjustmentArray;
    private final float[] largeAdjustmentArray;
    private final float smallAdjustment;
    private final float largeAdjustment;

    /**
     * Creates an instance of ReservedInstanceAdjustmentTracker with the provided adjustment values.
     *
     * @param smallAdjustmentArray containing smallAdjustment values for the RI.
     * @param largeAdjustmentArray containing largeAdjustment values for the RI.
     * @param smallAdjustment value for the RI.
     * @param largeAdjustment value for the RI.
     */
    ReservedInstanceAdjustmentTracker(@Nonnull final float[] smallAdjustmentArray,
                                      @Nonnull final float[] largeAdjustmentArray,
                                      float smallAdjustment, float largeAdjustment) {
        this.smallAdjustmentArray = Objects.requireNonNull(smallAdjustmentArray);
        this.largeAdjustmentArray = Objects.requireNonNull(largeAdjustmentArray);
        this.smallAdjustment = smallAdjustment;
        this.largeAdjustment = largeAdjustment;
    }

    @Nonnull
    float[] getSmallAdjustmentArray() {
        return smallAdjustmentArray;
    }

    @Nonnull
    float[] getLargeAdjustmentArray() {
        return largeAdjustmentArray;
    }

    float getSmallAdjustment() {
        return smallAdjustment;
    }

    float getLargeAdjustment() {
        return largeAdjustment;
    }
}
