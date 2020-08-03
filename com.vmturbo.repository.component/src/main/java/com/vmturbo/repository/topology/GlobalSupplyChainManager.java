package com.vmturbo.repository.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.repository.graph.executor.GraphDBExecutor;

/**
 * This class manages the global supply chains.
 *
 */
public class GlobalSupplyChainManager {

    private static final Logger logger = LogManager.getLogger();

    private final GraphDBExecutor executor;

    /**
     * Mapping from TopologyID -> GlobalSupplyChain
     */
    private final Map<TopologyID, GlobalSupplyChain> topologyIdToGlobalSupplyChainMap;

    public GlobalSupplyChainManager(@Nonnull final GraphDBExecutor executor) {

        this.executor = Objects.requireNonNull(executor);
        this.topologyIdToGlobalSupplyChainMap = new HashMap<>();
    }


    public void addNewGlobalSupplyChain(@Nonnull TopologyID topologyId,
                                        @Nonnull GlobalSupplyChain globalSupplyChain) {
        this.topologyIdToGlobalSupplyChainMap.put(topologyId, globalSupplyChain);
    }


    public void removeGlobalSupplyChain(@Nonnull TopologyID topologyId) {
        logger.info("Removing global supply chain for: {}", topologyId);
        this.topologyIdToGlobalSupplyChainMap.remove(topologyId);
    }

    /**
     *  Return the current global supply chain for the given Topology ID
     *
     * @param topologyId The topology info.
     * @return The global supply chain for the given topology.
     */
    public Optional<GlobalSupplyChain> getGlobalSupplyChain(@Nonnull TopologyID topologyId) {
         return Optional.ofNullable(this.topologyIdToGlobalSupplyChainMap.get(topologyId));
    }

    public void loadGlobalSupplyChainFromDb(@Nonnull TopologyID topologyId) {
        GlobalSupplyChain globalSupplyChain = new GlobalSupplyChain(topologyId, executor);
        if (globalSupplyChain.loadFromDatabase()) {
            addNewGlobalSupplyChain(topologyId, globalSupplyChain);
        }
    }

}
