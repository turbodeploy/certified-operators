package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.List;

/**
 * An interface describing the CloudCommitmentSpecResolver.
 *
 * @param <SPEC_TYPE> The spec type.
 */
public interface CloudCommitmentSpecResolver<SPEC_TYPE> {

    /**
     * Given a set of region oids, returns the list of ReservedInstanceSpecs which match the given
     * region oid.
     *
     * @param regionOid The region oid.
     *
     * @return The list of reserved instance specs.
     */
    List<SPEC_TYPE> getRISpecsForRegion(Long regionOid);
}
