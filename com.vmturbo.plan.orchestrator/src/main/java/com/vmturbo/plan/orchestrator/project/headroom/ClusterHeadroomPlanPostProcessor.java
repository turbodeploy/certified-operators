package com.vmturbo.plan.orchestrator.project.headroom;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
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
    private final Group cluster;

    /**
     * The number of clones added to the cluster in the plan.
     */
    private final long addedClones;

    private final RepositoryServiceBlockingStub repositoryService;

    private final StatsHistoryServiceBlockingStub statsHistoryService;

    private final SupplyChainServiceBlockingStub supplyChainRpcService;

    private final GroupServiceGrpc.GroupServiceBlockingStub groupRpcService;

    private PlanDao planDao;

    private Consumer<ProjectPlanPostProcessor> onCompleteHandler;

    /**
     * Whether the actual calculation of headroom (kicked off by the projected topology being
     * available) has started.
     */
    private final AtomicBoolean calculationStarted = new AtomicBoolean(false);

    public ClusterHeadroomPlanPostProcessor(final long planId,
                                            @Nonnull final Group cluster,
                                            @Nonnull final Channel repositoryChannel,
                                            @Nonnull final Channel historyChannel,
                                            final long addedClones,
                                            @Nonnull final PlanDao planDao,
                                            @Nonnull final Channel groupChannel) {
        this.planId = planId;
        this.cluster = Objects.requireNonNull(cluster);
        this.repositoryService =
                RepositoryServiceGrpc.newBlockingStub(Objects.requireNonNull(repositoryChannel));
        this.statsHistoryService =
                StatsHistoryServiceGrpc.newBlockingStub(Objects.requireNonNull(historyChannel));
        this.supplyChainRpcService =
                SupplyChainServiceGrpc.newBlockingStub(Objects.requireNonNull(repositoryChannel));
        this.groupRpcService =
                GroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));

        this.addedClones = addedClones;
        this.planDao = Objects.requireNonNull(planDao);
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
                final long numVMs = getNumberOfVMs();

                createStatsRecords(headroom, numVMs);
            }
        }

        // Wait until the plan completes - that is, until all pieces are finished processing -
        // to delete it, so that everything gets deleted properly.
        // In the future, we should be able to issue a delete to an in-progress plan and
        // have no orphaned data.
        if (plan.getStatus() == PlanStatus.SUCCEEDED || plan.getStatus() == PlanStatus.FAILED) {
            if (plan.getStatus() == PlanStatus.FAILED) {
                logger.error("Cluster headroom plan for cluster ID {} failed! Error: {}",
                        cluster.getCluster().getName(), plan.getStatusMessage());
            } else {
                logger.info("Cluster headroom plan for cluster ID {} completed!",
                        cluster.getCluster().getName());
            }

            try {
                planDao.deletePlan(plan.getPlanId());
            } catch (NoSuchObjectException e) {
                // This shouldn't happen because the plan must have existed in order to
                // have succeeded.
            } finally {
                if (onCompleteHandler != null) {
                    onCompleteHandler.accept(this);
                }
            }
        } else {
            // Do nothing.
            logger.info("Cluster headroom plan for cluster ID {} has new status: {}",
                    cluster.getCluster().getName(), plan.getStatus());
        }
    }

    /**
     * Save the headroom value.
     *
     * @param headroom The calculated headroom.
     */
    @VisibleForTesting
    void createStatsRecords(final long headroom, final long numVMs) {
        if (headroom == addedClones) {
            logger.info("Cluster headroom for cluster {} is over {}",
                    cluster.getCluster().getName(), headroom);
        } else {
            logger.info("Cluster headroom for cluster {} is {}",
                    cluster.getCluster().getName(), headroom);
        }

        // Save the headroom in the history component.
        try {
            statsHistoryService.saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
                    .setClusterId(cluster.getId())
                    .setNumVMs(numVMs)
                    .setHeadroom(headroom)
                    .build());
        } catch (StatusRuntimeException e) {
            logger.error("Failed to save cluster headroom: {}", e.getMessage());
        }
    }

    @Override
    public void registerOnCompleteHandler(final Consumer<ProjectPlanPostProcessor> handler) {
        this.onCompleteHandler = handler;
    }

    /**
     * Get the number of VMs running in the cluster.
     *
     * @return number of VMs
     */
    private long getNumberOfVMs() {
        // Use the group service to get a list of IDs of all members (physical machines)
        GetMembersResponse response = groupRpcService.getMembers(GetMembersRequest.newBuilder()
                .setId(cluster.getId())
                .build());
        List<Long> memberIds = response.getMembers().getIdsList();

        // Use the supply chain service to get all VM nodes that belong to the physical machines
        Iterator<SupplyChainNode> supplyChainNodeIterator = supplyChainRpcService.getSupplyChain(
                SupplyChainRequest.newBuilder()
                        .addAllStartingEntityOid(memberIds)
                        .addEntityTypesToInclude("VirtualMachine")
                        .build());

        Iterable<SupplyChainNode> iterable = () -> supplyChainNodeIterator;
        Stream<SupplyChainNode> nodeStream = StreamSupport.stream(iterable.spliterator(), false);
        return nodeStream.map(RepositoryDTOUtil::getMemberCount).reduce(0, Integer::sum);
    }
}
