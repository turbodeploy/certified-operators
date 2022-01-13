package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;

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
    Set<Long> getEntitiesOfType(int entityType);

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
     * The immutable set of all {@link CloudCommitmentAggregate} instances in the topology.
     * @return The immutable set of all {@link CloudCommitmentAggregate} instances in the topology.
     */
    @Nonnull
    Set<CloudCommitmentAggregate> getAllCloudCommitmentAggregates();

    /**
     * Queries the capacity of {@code commitmentOid} for the specified coverage type. If the commitment
     * does not exist or does not have capacity of that coverage type, an empty {@link CloudCommitmentAmount}
     * will be returned.
     * @param commitmentOid The commitment aggregate OID.
     * @param coverageTypeInfo The coverage type info.
     * @return The commitment capacity of the commitment aggregate.
     */
    @Nonnull
    double getCommitmentCapacity(long commitmentOid, CloudCommitmentCoverageTypeInfo coverageTypeInfo);

    /**
     * Get the coverage capacity for the specified {@code entityOid}. The capacity will be specified
     * in coupons.
     * @param entityOid The target entity OID.
     * @param coverageTypeInfo The coverage type info.
     * @return The coverage capacity of the entity. Zero will be returned if the entity is not found.
     */
    double getCoverageCapacityForEntity(long entityOid,
                                        @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo);

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

    /**
     * Gets the {@link ServiceProviderInfo} for the specified service provider OID.
     * @param serviceProviderOid The service provider OID.
     * @return The {@link ServiceProviderInfo}, if the service provider can be found.
     */
    @Nonnull
    Optional<ServiceProviderInfo> getServiceProviderInfo(long serviceProviderOid);
}
