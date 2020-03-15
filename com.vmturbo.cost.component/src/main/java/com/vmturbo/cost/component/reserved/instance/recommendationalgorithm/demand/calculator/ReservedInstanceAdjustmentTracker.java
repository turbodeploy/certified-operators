package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Data type to store Adjustment values for an inventory RI in terms of number of coupons  so that
 * they can be applied across separate Regional Contexts. In Azure, the same RI can be applied to
 * more than one regional context. The reason being Azure RIs are platform flexible - i.e. can be
 * applied to Windows/Linux/Suse/RHEL VMs.
 * For eg. So zonalContext1, zonalContext2 -> regionalContext1 and
 * zonalContext3,zonalContext4 -> regionalContext2. The only difference between them is OS.
 * So an RI which is applicable to regionalContext1 is also applicable to regionalContext2 in Azure.
 */
public class ReservedInstanceAdjustmentTracker {
    private final float[] smallAdjustmentArray;
    private final float[] largeAdjustmentArray;
    private final float smallAdjustment;
    private final float largeAdjustment;

    /**
     * Creates an instance of ReservedInstanceAdjustmentTracker with the provided adjustment values.
     * In consumption mode, it's unless the current hours is the same hour
     * of the week as the hour when the RI was purchased, some hours will
     * have been updated one more time than others, and thus need to
     * be adjusted by a smaller amount
     * Thus there are two amounts that may apply, a larger and a smaller one.
     * In allocation mode, both are just the full amount of the RI's
     * normalized coupons.
     * For more explanation on these values refer ReservedInstanceDataProcessor::subtractCouponsFromDemand
     *
     * @param smallAdjustmentArray Arrays of adjustment for every hour of the week derived either
     *                             from smallAdjustment or which may be obtained from a tracker
     *                             representing the remainder of an adjustment that may have been
     *                             "used up" in whole or part for a different platform.
     * @param largeAdjustmentArray Arrays of adjustment for every hour of the week derived either
     *                             from largeAdjustment or which may be obtained from a tracker
     *                             representing the remainder of an adjustment that may have been
     *                             "used up" in whole or part for a different platform.
     * @param smallAdjustment value for the RI
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
