package com.vmturbo.cost.component.cca;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecResolver;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;

/**
 * A Local Reserved instance spec resolver class. implementing the CloudCommitmentSpecResolver interface.
 */
public class LocalReservedInstanceSpecResolver implements CloudCommitmentSpecResolver<ReservedInstanceSpec> {

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    /**
     * Constructor for the local reserved instance spec resolver.
     *
     * @param reservedInstanceSpecStore The reserved instance spec store.
     */
    public LocalReservedInstanceSpecResolver(@Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore) {
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
    }

    @Override
    public List<ReservedInstanceSpec> getSpecsForRegion(final long regionOid) {
        return reservedInstanceSpecStore.getAllRISpecsForRegion(regionOid);
    }
}
