package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * An interface similar to a cloud topology, providing topology information required for coverage
 * assignments.
 */
@Immutable
public interface CoverageTopology {

    /**
     * Gets all entity OIDs of the provided {@code entityType}.
     * @param entityType The target entity type.
     * @return The set of entity OIDs matching the specified type.
     */
    @Nonnull
    Set<Long> getEntitiesOfType(@Nonnull int entityType);

    /**
     * Finds and returns the cloud commitment aggregate corresponding to the provided ID.
     * @param commitmentAggregateOid The aggregate ID.
     * @return The commitment aggregate, if found.
     */
    @Nonnull
    Optional<CloudCommitmentAggregate> getCloudCommitment(long commitmentAggregateOid);

    /**
     * Returns the set of RI aggregates.
     * @return The set of RI aggregates.
     */
    @Nonnull
    Set<ReservedInstanceAggregate> getAllRIAggregates();

    /**
     * Resolves the capacity for each instance of {@link CloudCommitmentAggregate} contained within
     * this topology.
     * @return An immutable map of {@literal <Commitment Aggregate ID, Coverage Capacity>}.
     */
    @Nonnull
    Map<Long, Double> getCommitmentCapacityByOid();

    /**
     * Resolves the probe types for an entity. This will be resolved through the discovered
     * targets of the entity, querying the topology-processor for the target into (including the probe
     * type)
     * @param entityOid The OID fo the entity
     * @return A set of {@link SDKProbeType} from the discovery origin target list of the entity
     */
    @Nonnull
    Set<SDKProbeType> getProbeTypesForEntity(long entityOid);

    /**
     * Get the coverage capacity for the specified {@code entityOid}. The capacity will be specified
     * in coupons.
     * @param entityOid The target entity OID.
     * @return The coverage capacity of the entity. Zero will be returned if the entity is not found.
     */
    double getCoverageCapacityForEntity(long entityOid);

    /**
     * Resolves and returns the aggregation info for the specified {@code entityOid}.
     * @param entityOid The target entity OID.
     * @return The {@link CloudAggregationInfo} for the entity, if the entity is found in the topology
     * and all required aggregation info can be resolved.
     */
    @Nonnull
    Optional<CloudAggregationInfo> getAggregationInfo(long entityOid);

    /**
     * Resolves and returns the entity information for {@code entityOid}. The implementation of
     * {@link CoverageEntityInfo} will be specific to the entity type of {@code entityOid}.
     * @param entityOid The target entity OID.
     * @return The {@link CoverageEntityInfo}.
     */
    @Nonnull
    Optional<CoverageEntityInfo> getEntityInfo(long entityOid);

    /**
     * Resolves and returns the compute tier info consumed by {@code entityOid}.
     * @param entityOid The target entity OID.
     * @return The {@link ComputeTierInfo} for {@code entityOid}. Will be empty if the entity cannot
     * be found or it is not consuming for a compute tier provider.
     */
    @Nonnull
    Optional<ComputeTierInfo> getComputeTierInfoForEntity(long entityOid);
}
