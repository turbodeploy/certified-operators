package com.vmturbo.cost.component.entity.cost;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.sql.utils.DbException;

/**
 * This class is used to manage the entity cost table. It store calculated entity cost (after discount).
 */
public interface EntityCostStore extends MultiStoreDiagnosable {

    /**
     * Persist bulk entity costs based on {@link CostJournal}s.
     *
     * @param costJournals the journals, arranged by entity ID
     * @param cloudTopology the cloud topology used for getting info related to entities
     * @param topologyCreationTimeOrContextId the time of the topology created in milliseconds or
     *                                        the topology context Identifier.
     * @param isPlan true if saving a plan snapshot.  If true, topologyCreationTimeOrContextId
     *                indicates the plan ID.
     * @throws DbException if there is an error saving to the database
     */
    void persistEntityCost(@Nonnull Map<Long, CostJournal<TopologyEntityDTO>> costJournals,
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                           long topologyCreationTimeOrContextId, boolean isPlan)
            throws DbException;

    /**
     * Get List of StatRecords based on the the entity cost filter .
     * It returns Map with entry (timestamp -> List of {@link StatRecord}).
     * In a timestamp/snapshot, the entity costs with same ids will be combined to one entity cost.
     *
     * @param filter entityCostFilter which has resolved start and end dates and appropriated tables
     * @return Map with entry (timestamp -> List of {@link StatRecord}).
     * @throws DbException if anything goes wrong in the database
     */
    Map<Long, Collection<StatRecord>> getEntityCostStats(@Nonnull CostFilter filter) throws DbException;

    /**
     * Get entity costs based on the the entity cost filter .
     * It returns Map with entry (timestamp -> (entityId -> entity costs)).
     * In a timestamp/snapshot, the entity costs with same ids will be combined to one entity cost.
     *
     * @param filter entityCostFilter which has resolved start and end dates and appropriated tables
     * @return Map with entry (timestamp -> (entityId -> entity costs))
     * @throws DbException if anything goes wrong in the database
     */
    Map<Long, Map<Long, EntityCost>> getEntityCosts(@Nonnull CostFilter filter) throws DbException;

    /**
     * Remove plan entity cost snapshot that was created by a plan.
     * @param planId the ID of the plan for which to remove cost data.
     * @throws DbException if there was an error removing the plan cost data.
     */
    void deleteEntityCosts(long planId) throws DbException;
}
