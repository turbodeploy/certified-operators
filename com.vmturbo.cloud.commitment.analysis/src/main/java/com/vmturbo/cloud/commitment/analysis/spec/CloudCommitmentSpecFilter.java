package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Map;

/**
 * Interface describing the cloud commitment spec filter.
 *
 * @param <SPEC_TYPE> The spec type.
 */
public interface CloudCommitmentSpecFilter<SPEC_TYPE> {

    /**
     * Given a region oid, returns a map of reserved instance spec key by reserved instance spec data.
     *
     * @param regionOid The region oid.
     *
     * @return Map of reserved instance spec key by reserved instance spec data.
     */
    Map<ReservedInstanceSpecKey, ReservedInstanceSpecData> filterRISpecs(long regionOid);
}
