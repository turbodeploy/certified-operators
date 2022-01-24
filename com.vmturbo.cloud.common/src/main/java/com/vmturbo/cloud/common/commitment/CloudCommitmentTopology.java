package com.vmturbo.cloud.common.commitment;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cost.calculation.integration.CloudTopology;

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
