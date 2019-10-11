package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

public class AbstractReservedInstanceCoverageAllocatorTest {

    protected final TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            new DefaultTopologyEntityCloudTopologyFactory();

    protected CoverageTopology generateCoverageTopology(
            @Nonnull Set<ReservedInstanceBought> reservedInstances,
            @Nonnull Set<ReservedInstanceSpec> riSpecs,
            TopologyEntityDTO... entityDtos) {
        final CloudTopology<TopologyEntityDTO> cloudTopology =
                cloudTopologyFactory.newCloudTopology(Arrays.stream(entityDtos));

        return CoverageTopologyFactory.createCoverageTopology(cloudTopology,
                riSpecs,
                reservedInstances);
    }

    protected ReservedInstanceCoverageProvider createCoverageProvider(@Nonnull Table<Long, Long, Double> coverages,
                                                                    @Nonnull Map<Long, Double> entityCapcities,
                                                                    @Nonnull Map<Long, Double> riCapacities) {
        return new ReservedInstanceCoverageProvider() {
            @Nonnull
            @Override
            public Table<Long, Long, Double> getAllCoverages() {
                return Tables.unmodifiableTable(coverages);
            }

            @Nonnull
            @Override
            public Map<Long, Double> getCapacityForEntities() {
                return Collections.unmodifiableMap(entityCapcities);
            }

            @Nonnull
            @Override
            public Map<Long, Double> getCapacityForReservedInstances() {
                return Collections.unmodifiableMap(riCapacities);
            }
        };
    }

    protected ReservedInstanceCoverageProvider createCoverageProvider(@Nonnull Map<Long, Double> entityCapcities,
                                                                    @Nonnull Map<Long, Double> riCapacities) {
        return createCoverageProvider(ImmutableTable.of(), entityCapcities, riCapacities);
    }
}
