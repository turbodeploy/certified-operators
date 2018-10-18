package com.vmturbo.cost.component.entity.cost;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.sql.utils.DbException;

/**
 * This class is used to manage the entity cost table. It store calculated entity cost (after discount).
 */
public interface EntityCostStore {
    /**
     * Persist bulk entity costs based on EntityCost DTOs.
     *
     * @throws DbException                 if anything goes wrong in the database
     * @throws InvalidEntityCostsException if the provided entity cost DTO is not valid
     */
    void persistEntityCosts(@Nonnull final List<EntityCost> entityCosts) throws DbException, InvalidEntityCostsException;

    /**
     * Persist bulk entity costs based on {@link CostJournal}s.
     *
     * @param costJournals The journals, arranged by entity ID.
     * @throws DbException If there is an error saving to the database.
     */
    void persistEntityCost(@Nonnull final Map<Long, CostJournal<TopologyEntityDTO>> costJournals) throws DbException;

    /**
     * Get entity costs between the start (minValue) and end (maxValue) dates.
     * It returns Map with entry (timestamp -> (entityId -> entity costs)).
     * In a timestamp/snapshot, the entity costs with same ids will be combined to one entity cost.
     *
     * @param startDate start date
     * @param endDate   end date
     * @return Map with entry (timestamp -> (entityId -> entity costs))
     * @throws DbException if anything goes wrong in the database
     */
    Map<Long, Map<Long, EntityCost>> getEntityCosts(@Nonnull final LocalDateTime startDate,
                                                    @Nonnull final LocalDateTime endDate) throws DbException;

    /**
     * Get entity costs based on the the entity cost filter .
     * It returns Map with entry (timestamp -> (entityId -> entity costs)).
     * In a timestamp/snapshot, the entity costs with same ids will be combined to one entity cost.
     *
     * @param filter entityCostFilter which has resolved start and end dates and appropriated tables
     * @return Map with entry (timestamp -> (entityId -> entity costs))
     * @throws DbException if anything goes wrong in the database
     */
    Map<Long, Map<Long, EntityCost>> getEntityCosts(@Nonnull final EntityCostFilter filter) throws DbException;

    /**
     * Get entity costs for the provided entity ids and between the start (minValue) and end (maxValue) dates.
     * It returns Map with entry (timestamp -> (entityId -> entity costs)).
     * In a timestamp/snapshot, the entity costs with same ids will be combined to one entity cost.
     *
     * @param entityIds entity ids
     * @param startDate start date
     * @param endDate   end date
     * @return Map with entry (timestamp -> (entityId -> entity costs))
     * @throws DbException if anything goes wrong in the database
     */
    Map<Long, Map<Long, EntityCost>> getEntityCosts(@Nonnull final Set<Long> entityIds,
                                                    @Nonnull final LocalDateTime startDate,
                                                    @Nonnull final LocalDateTime endDate) throws DbException;

    /**
     * Get the latest entity cost.
     * It returns Map with entry (timestamp -> (associatedEntityId -> EntityCost)).
     * In a timestamp/snapshot, the entity cost with same ids will be combined to one entity cost.
     *
     * @return Map with entry (timestamp -> (associatedEntityId -> EntityCost))
     * @throws DbException if anything goes wrong in the database
     */
    Map<Long,Map<Long,EntityCost>> getLatestEntityCost() throws DbException;
}