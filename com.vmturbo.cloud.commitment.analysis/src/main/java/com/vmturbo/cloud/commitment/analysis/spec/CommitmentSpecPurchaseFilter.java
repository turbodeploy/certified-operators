package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Map;

/**
 * Interface describing the cloud commitment spec purchase filter, which is responsible for filtering
 * the spec inventory based on the purchase profile constraints.
 *
 * @param <SPEC_TYPE> The spec type.
 * @param <COVERAGE_SCOPE_TYPE> The type of coverage scope that can be covered by {@code SPEC_TYPE}.
 */
public interface CommitmentSpecPurchaseFilter<COVERAGE_SCOPE_TYPE, SPEC_TYPE> {

    /**
     * Given a region oid, returns a map of {@link CloudCommitmentCoverageScope} to the spec to recommend.
     *
     * @param regionOid The region oid.
     * @param <T> The cloud commitment data implementation for the underlying purchase filter.
     * @return An instance of the spec matcher.
     */
    <T extends CloudCommitmentSpecData<SPEC_TYPE>> Map<COVERAGE_SCOPE_TYPE, T> getSpecsByCoverageScope(long regionOid);

}
