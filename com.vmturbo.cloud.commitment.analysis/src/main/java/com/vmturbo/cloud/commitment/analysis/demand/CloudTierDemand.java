package com.vmturbo.cloud.commitment.analysis.demand;

/**
 * The root data structure representing cloud tier demand.
 */
public interface CloudTierDemand {

    /**
     * The cloud tier OID of the demand.
     *
     * @return The cloud tier OID of the demand.
     */
    long cloudTierOid();
}
