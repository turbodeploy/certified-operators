package com.vmturbo.market.reservations;

import java.util.List;
import java.util.Optional;

import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;

/**
 * The object to hold the placement decision.
 */
public class InitialPlacementDecision {

    /**
     *  he oid of  the commoditiesBoughtByProvider in a given {@link com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer}.
     */
    public long slOid;

    /**
     * The supplier of shopping list, if it exists.
     */
    public Optional<Long> supplier;

    /**
     * The unplaced explanation of shopping list, if there is no supplier.
     */
    public List<FailureInfo> failureInfos;

    /**
     * Constructor.
     *
     * @param slOid the shopping list oid.
     * @param supplier the supplier oid of a shopping list, it could be empty.
     * @param failureInfos a list of {@link FailureInfo}s
     */
    public InitialPlacementDecision(final long slOid, final Optional<Long> supplier,
            final List<FailureInfo> failureInfos) {
        this.slOid = slOid;
        this.supplier = supplier;
        this.failureInfos = failureInfos;
    }
}
