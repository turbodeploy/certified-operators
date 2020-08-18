package com.vmturbo.cloud.commitment.analysis.spec;

/**
 * The minimum coverage scope of a cloud commitment. A coverage scope is the set of attributes required
 * to match to an entity, for that entity to be considered within scope of the cloud commitment.
 */
public interface CloudCommitmentCoverageScope {

    /**
     * The cloud tier OID of this coverage scope. A cloud commitment may be able to cover multiple
     * tiers and therefore would have multiple coverage scopes.
     * @return The cloud tier OID of this coverage scope.
     */
    long cloudTierOid();
}
