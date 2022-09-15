package com.vmturbo.market.reservations;

import java.util.List;
import java.util.Optional;

import com.vmturbo.common.protobuf.market.InitialPlacement.InvalidInfo.InvalidConstraints;
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
     * Whether the failure is in real time or historical cache.
     */
    public boolean isFailedInRealtimeCache;

    /**
     * The invalid constraints info of the buyer.
     */
    public final Optional<InvalidConstraints> invalidConstraints;

    /**
     * The providerType of the supplier, if it exists.
     */
    public final Optional<Integer> providerType;

    /**
     * Constructor.
     *
     * @param slOid the shopping list oid.
     * @param supplier the supplier oid of a shopping list, it could be empty.
     * @param failureInfos a list of {@link FailureInfo}s
     * @param invalidConstraints the list of invalid constraints
     * @param isFailedInRealtimeCache whether the failure is in real time or historical cache.
     */
    public InitialPlacementDecision(final long slOid, final Optional<Long> supplier,
                                    final List<FailureInfo> failureInfos,
                                    final Optional<InvalidConstraints> invalidConstraints,
                                    final boolean isFailedInRealtimeCache,
                                    final Optional<Integer> providerType) {
        this.slOid = slOid;
        this.supplier = supplier;
        this.failureInfos = failureInfos;
        this.invalidConstraints = invalidConstraints;
        this.isFailedInRealtimeCache = isFailedInRealtimeCache;
        this.providerType = providerType;
    }
}
