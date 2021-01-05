package com.vmturbo.market.cloudscaling.sma.entities;

/**
 * Class to represent cost, both compute and license costs.
 */
public class SMACost {
    /**
     * Compute cost.
     */
    private final float compute;
    /**
     * license cost.
     */
    private final float license;

    /**
     * Constructor for the SMA Cost.
     *
     * @param compute the compute cost
     * @param license the license cost
     */
    public SMACost(final float compute, final float license) {
        this.compute = compute;
        this.license = license;
    }

    public float getCompute() {
        return compute;
    }

    public float getLicense() {
        return license;
    }

    public float getTotal() {
        return compute + license;
    }

    @Override
    public String toString() {
        return "SMACost{" +
                "compute=" + compute +
                ", license=" + license +
                '}';
    }
}