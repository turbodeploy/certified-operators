package com.vmturbo.repository.graph.executor;

import javaslang.collection.List;
import reactor.core.publisher.Flux;

import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.GlobalSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.ScopedEntity;
import com.vmturbo.repository.topology.TopologyDatabase;

public interface ReactiveGraphDBExecutor {
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
