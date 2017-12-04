package com.vmturbo.plan.orchestrator.project;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo.PlanProjectScenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.project.headroom.ClusterHeadroomPlanPostProcessor;

/**
 * This class executes a plan project
 */
public class PlanProjectExecutor {
    private final Logger logger = LogManager.getLogger();
    private final PlanDao planDao;
    private final GroupServiceGrpc.GroupServiceBlockingStub groupRpcService;
    private final PlanRpcService planService;
    private final ProjectPlanPostProcessorRegistry projectPlanPostProcessorRegistry;
    private final Channel repositoryChannel;

    // TODO this value will be made configurable or determined dynamically.
    // It is put here as a constant for now.
    // This value is the number of clones of a template to be added to a cluster to calculate
    // cluster headroom
    public static final long ADDED_CLONES = 50;

    /**
     * Constructor for {@link PlanProjectExecutor}
     *
     * @param planDao Plan DAO
     * @param groupRpcService Group PRC Service
     * @param planRpcService Plan PRC Service
     * @param projectPlanPostProcessorRegistry Registry for post processors of plans
     * @param repositoryChannel Repository channel
     */
    public PlanProjectExecutor(@Nonnull final PlanDao planDao,
                               @Nonnull final GroupServiceGrpc.GroupServiceBlockingStub groupRpcService,
                               @Nonnull final PlanRpcService planRpcService,
                               @Nonnull final ProjectPlanPostProcessorRegistry projectPlanPostProcessorRegistry,
                               @Nonnull final Channel repositoryChannel) {
        this.groupRpcService = groupRpcService;
        this.planService = planRpcService;
        this.projectPlanPostProcessorRegistry = projectPlanPostProcessorRegistry;
        this.repositoryChannel = repositoryChannel;
        this.planDao = planDao;
    }

    /**
     * Executes a plan project according to the instructions provided in the plan project.
     * See PlanDTO.proto for detailed documentation of fields in the plan project object and how
     * they will be processed during plan execution.
     *
     * @param planProject a plan project
     */
    public void executePlan(final PlanProject planProject) {
        // get scope of the topology where the scenarios will be applied
        boolean perCluster = planProject.getPlanProjectInfo().getPerClusterScope();

        if (perCluster) {
            runPlanInstancePerCluster(planProject);
        } else {
            // TODO: handle per-cluster=false case

        }
    }

    /**
     * If the per_cluster_scope value of the plan is set to true, we will apply the plan project
     * on each cluster. For each cluster, we will create one plan project instance for each
     * scenario.
     *
     * @param planProject
     */
    private void runPlanInstancePerCluster(PlanProject planProject) {

        final Iterator<Group> clusterIterator;
        // get all cluster group IDs from the topology
        clusterIterator = groupRpcService.getGroups(
                GroupDTO.GetGroupsRequest.newBuilder()
                        .setTypeFilter(GroupDTO.Group.Type.CLUSTER)
                        .build());

        // Create one plan project instance per cluster per Scenario.
        // Total number of plan project instance to be created equals number of clusters times number of
        // scenarios in the plan project.
        while (clusterIterator.hasNext()) {
            GroupDTO.Group cluster = clusterIterator.next();
            List<PlanProjectScenario> scenarioList =
                    planProject.getPlanProjectInfo().getScenariosList();
            for (PlanProjectInfo.PlanProjectScenario scenario : scenarioList) {
                // Create plan instance
                PlanInstance planInstance = null;
                try {
                    planInstance = createPlanInstance(cluster.getId(), scenario);
                } catch (IntegrityException e) {
                    logger.error("Failed to create a plan instance for cluster {}: {}",
                            cluster.getId(), e.getMessage());
                    continue;
                }

                // Register post process handler
                ProjectPlanPostProcessor planProjectPostProcessor = null;
                if (planProject.getPlanProjectInfo().getType().equals(PlanProjectType.CLUSTER_HEADROOM)) {
                    planProjectPostProcessor = new ClusterHeadroomPlanPostProcessor(planInstance.getPlanId(),
                            cluster.getId(), repositoryChannel, ADDED_CLONES);
                }
                if (planProjectPostProcessor != null) {
                    projectPlanPostProcessorRegistry.registerPlanPostProcessor(planProjectPostProcessor);
                } else {
                    continue;
                }

                // Run plan instance
                runPlanInstance(planInstance);
            }
        }
    }

    /**
     * Creates a plan instance from a plan project scenario, and sets the cluster ID in plan scope
     *
     * @param clusterId the ID of the cluster where this plan is applied
     * @param planProjectScenario the plan project scenario
     * @return a plan instance
     * @throws IntegrityException
     */
    private PlanInstance createPlanInstance(final long clusterId,
                                    final PlanProjectInfo.PlanProjectScenario planProjectScenario)
            throws IntegrityException {
        PlanScopeEntry planScopeEntry = PlanScopeEntry.newBuilder()
                .setScopeObjectOid(clusterId)
                .build();
        PlanScope planScope = PlanScope.newBuilder()
                .addScopeEntries(planScopeEntry)
                .build();
        ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .addAllChanges(planProjectScenario.getChangesList())
                .setScope(planScope)
                .build();
        Scenario scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfo)
                .setId(IdentityGenerator.next()).build();
        PlanInstance planInstance = planDao.createPlanInstance(scenario);
        return planInstance;
    }

    /**
     * Calls the plan service to run the plan instance.
     *
     * @param planInstance
     */
    private void runPlanInstance(PlanInstance planInstance) {
        planService.runPlan(PlanId.newBuilder()
                .setPlanId(planInstance.getPlanId())
                .build(), new StreamObserver<PlanInstance>() {
                    @Override
                    public void onNext(PlanInstance value) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error("Error occurred while executing plan {}.",
                                planInstance.getPlanId());
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
    }
}
