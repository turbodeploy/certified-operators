package com.vmturbo.cloud.common.commitment;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;

/**
 * An interface for extracting information from the topology, specific to cloud commitments.
 */
public interface CloudCommitmentTopology {

    /**
     * Resolves and returns the list of covered cloud services for the specified {@code commitmentId}.
     * @param commitmentId the commitment ID.
     * @throws IllegalArgumentException Thrown if the commitment cannot be found.
     * @return The set of covered cloud services.
     */
    @Nonnull
    Set<Long> getCoveredCloudServices(long commitmentId) throws IllegalArgumentException;

    /**
     * Resolves and returns the list of covered accounts for the specified {@code commitmentId}, if the
     * commitment is account scoped. If the instance is not account scoped, the returned set will be
     * empty.
     * @param commitmentId The commitment ID.
     * @return The set of covered accounts.
     * @throws IllegalArgumentException Thrown if the commitment cannot be found.
     */
    @Nonnull
    Set<Long> getCoveredAccounts(long commitmentId) throws IllegalArgumentException;

    /**
     * Resolves the capacity of {@code entityOid} for the coverage vector type of {@code coverageTypeInfo}. If the entity
     * cannot be resolved, a capacity of zero will be returned.
     * @param entityOid The entity OID.
     * @param coverageTypeInfo The coverage vector info.
     * @return The coverage capacity of the entity for the specified coverage vector.
     */
    double getCoverageCapacityForEntity(long entityOid,
                                        @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo);

    /**
     * A factory for creating {@link CloudCommitmentTopology} instances.
     * @param <EntityClassT> The entity type of the underlying cloud topology.
     */
    interface CloudCommitmentTopologyFactory<EntityClassT> {

        /**
         * Creates a new {@link CloudCommitmentTopology} instance.
         * @param cloudTopology The cloud topology.
         * @return The newly created {@link CloudCommitmentTopology} instance.
         */
        @Nonnull
        CloudCommitmentTopology createTopology(@Nonnull CloudTopology<EntityClassT> cloudTopology);
    }
}
