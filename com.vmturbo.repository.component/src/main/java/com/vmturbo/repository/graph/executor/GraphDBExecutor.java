package com.vmturbo.repository.graph.executor;

import java.util.Collection;
import java.util.List;

import javaslang.control.Try;

import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.SupplyChainExecutorResult;
import com.vmturbo.repository.graph.result.SupplyChainInstancesType;

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
     * @return {@link SupplyChainExecutorResult}.
     */
    Try<SupplyChainExecutorResult> executeSupplyChainCmd(final GraphCmd.GetSupplyChain supplyChain);

    /**
     * Compute the global supply chain.
     *
     * @param globalSupplyChain A command the contains information needed to compute the global supply chain.
     * @return A list of {@link SupplyChainInstancesType}
     */
    Try<List<SupplyChainInstancesType>> executeGlobalSupplyChainCmd(final GraphCmd.GetGlobalSupplyChain globalSupplyChain);

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
}
