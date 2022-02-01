package com.vmturbo.market.topology.conversions.cloud;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;

/**
 * A factory class for constructing {@link JournalActionSavingsCalculator} instances.
 */
public class JournalActionSavingsCalculatorFactory {

    /**
     * Constructs a new {@link JournalActionSavingsCalculator} instance.
     * @param sourceTopologyMap The source topology map.
     * @param sourceCloudTopology The source cloud topology.
     * @param sourceCostCalculator The source cloud cost calculator.
     * @param projectedTopologyMap The projected topology map.
     * @param projectedJournalsMap The projected entity cost journals.
     * @param projectedRICoverage The projected RI coverage map.
     * @return The newly constructed {@link JournalActionSavingsCalculator} instance.
     */
    @Nonnull
    public JournalActionSavingsCalculator newCalculator(@Nonnull Map<Long, TopologyEntityDTO> sourceTopologyMap,
                                                        @Nonnull CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                                                        @Nonnull TopologyCostCalculator sourceCostCalculator,
                                                        @Nonnull Map<Long, ProjectedTopologyEntity> projectedTopologyMap,
                                                        @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedJournalsMap,
                                                        @Nonnull Map<Long, EntityReservedInstanceCoverage> projectedRICoverage) {

        return new JournalActionSavingsCalculator(
                sourceTopologyMap,
                sourceCloudTopology,
                sourceCostCalculator,
                projectedTopologyMap,
                projectedJournalsMap,
                projectedRICoverage);
    }
}
