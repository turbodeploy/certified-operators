package com.vmturbo.plan.orchestrator.project.headroom;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessor;

/**
 * A post-processor to store cluster headroom for a particular cluster.
 */
@ThreadSafe
public class ClusterHeadroomPlanPostProcessor implements ProjectPlanPostProcessor {

    private static final Logger logger = LogManager.getLogger();

    private final long planId;

    /**
     * The cluster for which we're trying to calculate headroom.
     */
    private final long clusterId;

    /**
     * The number of clones added to the cluster in the plan.
     */
    private final long addedClones;

    private final RepositoryServiceBlockingStub repositoryService;

    private Consumer<ProjectPlanPostProcessor> onCompleteHandler;

    /**
     * Whether the actual calculation of headroom (kicked off by the projected topology being
     * available) has started.
     */
    private final AtomicBoolean calculationStarted = new AtomicBoolean(false);

    public ClusterHeadroomPlanPostProcessor(final long planId,
                                            final long clusterId,
                                            @Nonnull final Channel repositoryChannel,
                                            final long addedClones) {
        this.planId = planId;
        this.clusterId = clusterId;
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(repositoryChannel);
        this.addedClones = addedClones;
    }

    @Override
    public long getPlanId() {
        return planId;
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan) {
        if (plan.hasProjectedTopologyId()) {
            // We may have multiple updates to the plan status after the initial one that set
            // the projected topology. However, we only want to calculate headroom once.
            if (calculationStarted.compareAndSet(false, true)) {
                // This is all we need for post-processing, don't need to wait for plan
                // to succeed.
                final Iterable<RetrieveTopologyResponse> response = () ->
                        repositoryService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                                .setTopologyId(plan.getProjectedTopologyId())
                                .setEntityFilter(TopologyEntityFilter.newBuilder()
                                        .setUnplacedOnly(true))
                                .build());
                // In a headroom plan only the clones are unplaced, and nothing else changes. Therefore
                // the number of unplaced entities = the number of unplaced clones.
                final long unplacedClones = StreamSupport.stream(response.spliterator(), false)
                        .map(RetrieveTopologyResponse::getEntitiesList)
                        .mapToLong(List::size)
                        .sum();
                final long headroom = addedClones - unplacedClones;
                storeHeadroom(headroom);
                if (onCompleteHandler != null) {
                    onCompleteHandler.accept(this);
                }
            }
        } else if (plan.getStatus() == PlanStatus.FAILED) {
            logger.error("Cluster headroom plan for cluster ID {} failed! Error: {}",
                    clusterId, plan.getStatusMessage());
        } else {
            // Do nothing.
            logger.info("Cluster headroom plan for cluster ID {} has new status: {}",
                    clusterId, plan.getStatus());
        }
    }

    /**
     * This method only exists as a placeholder.
     *
     * @param headroom The calculated headroom.
     */
    @VisibleForTesting
    void storeHeadroom(final long headroom) {
        logger.info("Cluster headroom for cluster {} is {}", clusterId, headroom);
        // TODO (roman, Nov 28 2017): Store the headroom wherever it needs to be stored.
    }

    @Override
    public void registerOnCompleteHandler(final Consumer<ProjectPlanPostProcessor> handler) {
        this.onCompleteHandler = handler;
    }
}
