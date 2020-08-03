package com.vmturbo.cost.component.reserved.instance;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;

public class ReservedInstanceCoverageValidatorFactory {

    @Nonnull
    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    @Nonnull
    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    public ReservedInstanceCoverageValidatorFactory(
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore) {

        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
    }

    /**
     * Creates a new instance of {@link ReservedInstanceCoverageValidator}. On creation, the validator
     * will eagerly load data from the {@link CloudTopology}, as well as the currently stored instances
     * of {@link com.vmturbo.cost.component.db.tables.ReservedInstanceBought} and
     * {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec}
     *
     * @param cloudTopology An instance of {@link CloudTopology}, used to verify entity existence and
     *                      extract relevant relationships (e.g. the compute tier of covered entities)
     * @return A newly created instance of {@link ReservedInstanceCoverageValidator}
     */
    public ReservedInstanceCoverageValidator newValidator(CloudTopology<TopologyEntityDTO> cloudTopology) {
        return new ReservedInstanceCoverageValidator(reservedInstanceBoughtStore,
                reservedInstanceSpecStore,
                cloudTopology);
    }
}
