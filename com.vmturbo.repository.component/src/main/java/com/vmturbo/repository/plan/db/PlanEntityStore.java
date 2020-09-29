package com.vmturbo.repository.plan.db;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;

/**
 * Provides access to entities and supply chains in the source and projected topologies.
 */
public interface PlanEntityStore {

    /**
     * Retrieve entities from a plan.
     *
     * @param topologySelection The topology selection, obtained via a call to
     * {@link PlanEntityStore#getTopologySelection(long)} or
     * {@link PlanEntityStore#getTopologySelection(long, TopologyType)}
     * @param planEntityFilter The {@link PlanEntityFilter} that specifies the plan and any filter
     *                         properties to restrict the number of returned entities.
     * @param partialEntityType The type of partial entity to return.
     * @return A stream of {@link PartialEntity} objects for entities that match the filter.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    Stream<PartialEntity> getPlanEntities(@Nonnull TopologySelection topologySelection,
            @Nonnull PlanEntityFilter planEntityFilter,
            @Nonnull PartialEntity.Type partialEntityType) throws DataAccessException;

    /**
     * Retrieve entities for plan stats calculation.
     *
     * <p/>This is "hacky" because this implementation was best for compatibility with the current
     * {@link com.vmturbo.repository.service.PlanStatsService}, to share that stats calculation
     * code across the Arango and SQL implementations.
     *
     * @param topologySelection The topology selection, obtained via a call to
     * {@link PlanEntityStore#getTopologySelection(long)} or
     * {@link PlanEntityStore#getTopologySelection(long, TopologyType)}
     * @param planEntityFilter The {@link PlanEntityFilter} that specifies the plan and any filter
     *                         properties to restrict the number of returned entities.
     * @return A stream of iterators over {@link ProjectedTopologyEntity} objects. Each object is
     *         wrapped in a list for compatibility with the
     *         {@link com.vmturbo.repository.service.PlanStatsService} implementation.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    Iterator<List<ProjectedTopologyEntity>> getHackyStatsEntities(
            @Nonnull TopologySelection topologySelection,
            @Nonnull PlanEntityFilter planEntityFilter) throws DataAccessException;

    /**
     * Retrieve supply chains for entities in plans. The supply chains come from the projected
     * topology.
     *
     * <p/>Note - The supply chain nodes will not have connected consumer/provider types set.
     * We don't need this information for plans, where we just care about the final member list.
     *
     * @param topologySelection The topology selection, obtained via a call to
     * {@link PlanEntityStore#getTopologySelection(long)} or
     * {@link PlanEntityStore#getTopologySelection(long, TopologyType)}
     * @param supplyChainScope The {@link SupplyChainScope} which specifies which entities to target.
     * @return The {@link SupplyChain}. Note - this supply chain will just be a list of nodes with
     *         members. It will not have the connected entity types set on the individual nodes.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    SupplyChain getSupplyChain(@Nonnull TopologySelection topologySelection,
            @Nonnull SupplyChainScope supplyChainScope) throws DataAccessException;

    /**
     * Given a plan and a topology type, return the {@link TopologySelection} this refers to.
     *
     * @param planId The ID of the plan.
     * @param topologyType The topology type.
     * @return The topology ID of this topology type in this plan.
     * @throws TopologyNotFoundException If there is no data associated with that topology.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    TopologySelection getTopologySelection(long planId, @Nonnull TopologyType topologyType)
            throws TopologyNotFoundException, DataAccessException;

    /**
     * Given a topology id, return the {@link TopologySelection} it refers to.
     *
     * @param topologyId The ID of the topology.
     * @return The topology ID of this topology type in this plan.
     * @throws TopologyNotFoundException If there is no data associated with that topology.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    TopologySelection getTopologySelection(long topologyId)
            throws TopologyNotFoundException, DataAccessException;

    /**
     * Delete all data for a particular plan. No-op if there is no data for that plan.
     *
     * @param planId The id of the plan to delete data for.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    void deletePlanData(long planId) throws DataAccessException;

    /**
     * List all plan IDs that have any data in the {@link PlanEntityStore}.
     *
     * <p/>Note - this will include plans that only have partial data (e.g. where the source topology
     * ingestion succeeded but the projected topology ingestion failed).
     *
     * @return The set of plan IDs with data.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    Set<Long> listRegisteredPlans() throws DataAccessException;
}
