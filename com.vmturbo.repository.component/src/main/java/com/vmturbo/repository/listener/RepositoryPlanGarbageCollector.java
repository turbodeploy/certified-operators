package com.vmturbo.repository.listener;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyDeletionException;

/**
 * Responsible for cleaning data of deleted plans from the repository component.
 */
public class RepositoryPlanGarbageCollector implements PlanGarbageCollector {
    private final Logger logger = LogManager.getLogger();

    private final TopologyLifecycleManager topologyLifecycleManager;

    /**
     * Create new instance.
     *
     * @param topologyLifecycleManager Topology lifecycle manager.
     */
    public RepositoryPlanGarbageCollector(final TopologyLifecycleManager topologyLifecycleManager) {
        this.topologyLifecycleManager = topologyLifecycleManager;
    }

    @Nonnull
    @Override
    public List<ListExistingPlanIds> listPlansWithData() {
        return Collections.singletonList(topologyLifecycleManager::listRegisteredContexts);
    }

    @Override
    public void deletePlanData(final long planId) {
        try {
            final Optional<TopologyID> srcTopologyId = topologyLifecycleManager.getTopologyId(planId, TopologyType.SOURCE);
            if (srcTopologyId.isPresent()) {
                logger.info("Deleting topology {}", srcTopologyId.get());
                topologyLifecycleManager.deleteTopology(srcTopologyId.get());
            } else {
                logger.info("No source topology to delete for plan {}", planId);
            }
        } catch (TopologyDeletionException | RuntimeException e) {
            logger.error("Failed to delete source topology of plan " + planId, e);
        }

        try {
            final Optional<TopologyID> projTopoId = topologyLifecycleManager.getTopologyId(planId, TopologyType.PROJECTED);
            if (projTopoId.isPresent()) {
                logger.info("Deleting topology {}", projTopoId.get());
                topologyLifecycleManager.deleteTopology(projTopoId.get());
            } else {
                logger.info("No projected topology to delete for plan {}", planId);
            }
        } catch (TopologyDeletionException | RuntimeException e) {
            logger.error("Failed to delete projected topology of plan " + planId, e);
        }
    }
}
