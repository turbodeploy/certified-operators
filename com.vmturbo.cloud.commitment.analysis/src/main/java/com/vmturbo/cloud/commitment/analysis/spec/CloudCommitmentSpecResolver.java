package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.List;

/**
 * An interface for a resolver of cloud commitment specs.
 *
 * @param <SPEC_TYPE> The spec type.
 */
public interface CloudCommitmentSpecResolver<SPEC_TYPE> {

    /**
     * Given a set of region oids, returns the list of specs which are applicable to the given region.
     *
     * @param regionOid The region oid.
     *
     * @return The list of cloud commitment specs
     */
    List<SPEC_TYPE> getSpecsForRegion(long regionOid);
}
