package com.vmturbo.cost.component.topology.cloud.listener;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.topology.CostJournalRecorder;
import com.vmturbo.sql.utils.DbException;

/**
 * The entity cost writer updates the RI coverage for entities and is responsible for calculating costs
 * and writing them to the db.
 */
public class EntityCostWriter implements LiveCloudTopologyListener {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;

    private final TopologyCostCalculatorFactory topologyCostCalculatorFactory;

    private final CostJournalRecorder journalRecorder;

    private final EntityCostStore entityCostStore;

    /**
     * The constructor for the entity cost writer.
     *
     * @param reservedInstanceCoverageUpdate An instance of the reserved instance coverage update to
     * update RI coverages.
     * @param topologyCostCalculatorFactory The topology cost calculation factory for calculating costs.
     * @param costJournalRecorder The cost journal recorder.
     * @param entityCostStore The entity cost store.
     */
    public EntityCostWriter(@Nonnull final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate,
            @Nonnull final TopologyCostCalculatorFactory topologyCostCalculatorFactory,
            @Nonnull final CostJournalRecorder costJournalRecorder,
            @Nonnull final EntityCostStore entityCostStore) {
        this.reservedInstanceCoverageUpdate = reservedInstanceCoverageUpdate;
        this.topologyCostCalculatorFactory = topologyCostCalculatorFactory;
        this.journalRecorder = costJournalRecorder;
        this.entityCostStore = entityCostStore;
    }

    @Override
    public void process(CloudTopology cloudTopology, TopologyInfo topologyInfo) {
        // update reserved instance coverage data. RI coverage must be updated
        // before cost calculation to accurately reflect costs based on up-to-date
        // RI coverage
        reservedInstanceCoverageUpdate.updateAllEntityRICoverageIntoDB(topologyInfo, cloudTopology);
        final TopologyCostCalculator topologyCostCalculator = topologyCostCalculatorFactory.newCalculator(topologyInfo, cloudTopology);
        final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                topologyCostCalculator.calculateCosts(cloudTopology);

        journalRecorder.recordCostJournals(costs);
        try {
            entityCostStore.persistEntityCost(costs, cloudTopology, topologyInfo.getCreationTime(), false);
        } catch (DbException e) {
            logger.error("Failed to persist entity costs.", e);
        }
    }
}

