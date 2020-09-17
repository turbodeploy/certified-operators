package com.vmturbo.repository.diagnostics;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.topology.GlobalSupplyChain;
import com.vmturbo.repository.topology.GlobalSupplyChainManager;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

/**
 * Class to collect and restore supply chain diagnostics.
 */
public class RealtimeSupplyChainDiagnostics implements DiagsRestorable<Void> {
    private final Logger logger = LogManager.getLogger(getClass());
    private final TopologyLifecycleManager topologyLifecycleManager;
    private final GlobalSupplyChainManager globalSupplyChainManager;
    private final GraphDBExecutor graphDBExecutor;

    /**
     * Constructs supply chain diagnostics.
     *
     * @param topologyLifecycleManager topology manager
     * @param globalSupplyChainManager global supply chain manager
     * @param graphDBExecutor graph DB executor
     */
    public RealtimeSupplyChainDiagnostics(
            @Nonnull TopologyLifecycleManager topologyLifecycleManager,
            @Nonnull GlobalSupplyChainManager globalSupplyChainManager,
            @Nonnull GraphDBExecutor graphDBExecutor) {
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.globalSupplyChainManager = Objects.requireNonNull(globalSupplyChainManager);
        this.graphDBExecutor = Objects.requireNonNull(graphDBExecutor);
    }

    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
        Optional<TopologyID> realtimeTopologyId = topologyLifecycleManager.getRealtimeTopologyId();
        if (realtimeTopologyId.isPresent()) {
            GlobalSupplyChain globalSupplyChain =
                    new GlobalSupplyChain(realtimeTopologyId.get(), graphDBExecutor);
            globalSupplyChain.restoreDiags(collectedDiags, null);
            globalSupplyChainManager.addNewGlobalSupplyChain(realtimeTopologyId.get(),
                    globalSupplyChain);
            logger.info("Restored {} ", GlobalSupplyChain.GLOBAL_SUPPLY_CHAIN_DIAGS_FILE);
        } else {
            logger.info("Cannot restore global supply chain as no realtimeTopologyId is available");
        }
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final Optional<TopologyID> sourceTopologyId =
                topologyLifecycleManager.getRealtimeTopologyId(TopologyType.SOURCE);
        if (sourceTopologyId.isPresent()) {
            Optional<GlobalSupplyChain> globalSupplyChain =
                    globalSupplyChainManager.getGlobalSupplyChain(sourceTopologyId.get());
            // Dumps the SE provider relationship
            if (globalSupplyChain.isPresent()) {
                logger.info("Dumping global supply chain");
                globalSupplyChain.get().collectDiags(appender);
            }
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return GlobalSupplyChain.GLOBAL_SUPPLY_CHAIN_DIAGS_FILE;
    }
}
