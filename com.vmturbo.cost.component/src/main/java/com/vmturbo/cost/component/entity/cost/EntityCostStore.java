package com.vmturbo.cost.component.entity.cost;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.sql.utils.DbException;

/**
 * This class is used to manage the entity cost table. It store calculated entity cost (after discount).
 */
public interface EntityCostStore {
    /**
     * Persist bulk entity costs based on EntityCost DTOs.
     *
     * @param entityCosts                  the list of entity costs.
     * @param cloudTopology                the cloud topology used for getting info related to
     *                                     entities.
     * @throws DbException                 if anything goes wrong in the database
     * @throws InvalidEntityCostsException if the provided entity cost DTO is not valid
     */
    void persistEntityCosts(@Nonnull List<EntityCost> entityCosts,
                            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology)
        throws DbException, InvalidEntityCostsException;

    /**
     * Persist bulk entity costs based on {@link CostJournal}s.
     *
     * @param costJournals  The journals, arranged by entity ID.
     * @param cloudTopology The cloud topology used for getting info related to
*                           entities.
     * @throws DbException If there is an error saving to the database.
     */
    void persistEntityCost(@Nonnull Map<Long, CostJournal<TopologyEntityDTO>> costJournals,
                           @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) throws DbException;

    /**
     * Get entity costs based on the the entity cost filter .
     * It returns Map with entry (timestamp -> (entityId -> entity costs)).
     * In a timestamp/snapshot, the entity costs with same ids will be combined to one entity cost.
     *
     * @param filter entityCostFilter which has resolved start and end dates and appropriated tables
     * @return Map with entry (timestamp -> (entityId -> entity costs))
     * @throws DbException if anything goes wrong in the database
     */
    Map<Long, Map<Long, EntityCost>> getEntityCosts(@Nonnull final CostFilter filter) throws DbException;
}
