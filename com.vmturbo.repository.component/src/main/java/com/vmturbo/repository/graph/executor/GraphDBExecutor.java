package com.vmturbo.repository.graph.executor;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.arangodb.ArangoDBException;

import javaslang.control.Try;

import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph;

/**
 * The executor abstracts away the actual graph database backend.
 */
public interface GraphDBExecutor {
    /**
     * Compute a supply chain.
     *
     * The {@link GraphCmd.GetSupplyChain} command contains the starting service entity and other data
     * for computing a supply chain.
     *
     * @param supplyChain A command that contains information needed to compute a supply chain.
     * @return {@link SupplyChainSubgraph} containing the results of executing the command.
     *         If the start ID is not found, return a {@link Throwable} with a {@link java.util.NoSuchElementException}.
     */
    Try<SupplyChainSubgraph> executeSupplyChainCmd(final GraphCmd.GetSupplyChain supplyChain);

    /**
     * Search a service entity by its <code>displayName</code>.
     *
     * @param searchServiceEntity The {@link GraphCmd.SearchServiceEntity} command contains the query.
     * @return A collection of {@link ServiceEntityRepoDTO}.
     */
    Try<Collection<ServiceEntityRepoDTO>> executeSearchServiceEntityCmd(final GraphCmd.SearchServiceEntity searchServiceEntity);

    /**
     * Execute a {@link GraphCmd.ServiceEntityMultiGet} command.
     *
     * @param serviceEntityMultiGet The command to execute.
     * @return The collection of found {@link ServiceEntityRepoDTO}s.
     */
    Try<Collection<ServiceEntityRepoDTO>> executeServiceEntityMultiGetCmd(final GraphCmd.ServiceEntityMultiGet serviceEntityMultiGet);

    @Nonnull
    Map<String, TagValuesDTO> executeTagCommand(
            @Nonnull String databaseName, @Nonnull SearchTagsRequest request) throws ArangoDBException;
}
