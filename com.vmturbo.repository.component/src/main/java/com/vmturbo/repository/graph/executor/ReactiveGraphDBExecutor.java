package com.vmturbo.repository.graph.executor;

import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.GlobalSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.ScopedEntity;
import com.vmturbo.repository.graph.result.SupplyChainFluxResult;
import com.vmturbo.repository.topology.TopologyDatabase;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

public interface ReactiveGraphDBExecutor {
    /**
     * Compute a supply chain starting from a particular entity.
     *
     * The {@link GraphCmd.GetSupplyChain} command contains the starting service entity and other data
     * for computing a supply chain.
     *
     * @param supplyChain A command that contains information needed to compute a supply chain.
     * @return {@link SupplyChainFluxResult}.
     */
    SupplyChainFluxResult executeSupplyChainCmd(final GraphCmd.GetSupplyChain supplyChain);

    /**
     * Compute the global supplychain
     *
     * @param globalSupplyChainCmd A command that contains information needed to compute the global supply chain.
     * @return {@link GlobalSupplyChainFluxResult}.
     */
    GlobalSupplyChainFluxResult executeGlobalSupplyChainCmd(final GraphCmd.GetGlobalSupplyChain globalSupplyChainCmd);

    /**
     * Get the list of entities from the database.
     *
     * @param database The {@link TopologyDatabase} instance
     * @param collection The collection the the entities reside.
     * @param oids The list of OIDs to retrieve.
     * @return {@link ScopedEntity}.
     */
    Flux<ScopedEntity> fetchScopedEntities(final TopologyDatabase database,
                                           final String collection,
                                           final List<Long> oids);
}
