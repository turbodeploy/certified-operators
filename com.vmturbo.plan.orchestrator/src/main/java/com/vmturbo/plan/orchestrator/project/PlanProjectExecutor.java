package com.vmturbo.plan.orchestrator.project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo.PlanProjectScenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanInstanceQueue;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.project.headroom.ClusterHeadroomPlanPostProcessor;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;


/**
 * This class executes a plan project
 */
public class PlanProjectExecutor {
    private final Logger logger = LogManager.getLogger();

    private final PlanDao planDao;

    private final PlanProjectDao planProjectDao;

    private final PlanRpcService planService;

    private final ProjectPlanPostProcessorRegistry projectPlanPostProcessorRegistry;

    private final Channel repositoryChannel;

    private final Channel historyChannel;

    private final TemplatesDao templatesDao;

    private final Channel groupChannel;

    private final GroupServiceGrpc.GroupServiceBlockingStub groupRpcService;

    private final SettingServiceBlockingStub settingService;

    private final PlanInstanceQueue planInstanceQueue;

    /**
     * The number of clones to add to a cluster headroom plan for every host that's
     * in the cluster.
     *
     * TODO this value will be made configurable or determined dynamically.
     * It is put here as a constant for now.
     */
    private static final int ADDED_CLONES_PER_HOST_IN_CLUSTER = 20;

    /**
     * Constructor for {@link PlanProjectExecutor}
     *
     * @param planDao Plan DAO
     * @param planProjectDao PlanProject DAO
     * @param groupChannel  Group service channel
     * @param planRpcService Plan RPC Service
     * @param projectPlanPostProcessorRegistry Registry for post processors of plans
     * @param repositoryChannel Repository channel
     * @param templatesDao templates DAO
     * @param historyChannel history channel
     */
    public PlanProjectExecutor(@Nonnull final PlanDao planDao,
                               @Nonnull final PlanProjectDao planProjectDao,
                               @Nonnull final Channel groupChannel,
                               @Nonnull final PlanRpcService planRpcService,
                               @Nonnull final ProjectPlanPostProcessorRegistry projectPlanPostProcessorRegistry,
                               @Nonnull final Channel repositoryChannel,
                               @Nonnull final TemplatesDao templatesDao,
                               @Nonnull final Channel historyChannel,
                               @Nonnull final PlanInstanceQueue planInstanceQueue) {
        this.groupChannel = Objects.requireNonNull(groupChannel);
        this.planService = Objects.requireNonNull(planRpcService);
        this.projectPlanPostProcessorRegistry = Objects.requireNonNull(projectPlanPostProcessorRegistry);
        this.repositoryChannel = Objects.requireNonNull(repositoryChannel);
        this.planDao = Objects.requireNonNull(planDao);
        this.planProjectDao = Objects.requireNonNull(planProjectDao);
        this.templatesDao = Objects.requireNonNull(templatesDao);
        this.historyChannel = Objects.requireNonNull(historyChannel);
        this.planInstanceQueue = Objects.requireNonNull(planInstanceQueue);

        groupRpcService = GroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        settingService = SettingServiceGrpc.newBlockingStub(groupChannel);
    }

    /**
     * Executes a plan project according to the instructions provided in the plan project.
     * See PlanDTO.proto for detailed documentation of fields in the plan project object and how
     * they will be processed during plan execution.
     *
     * @param planProject a plan project
     */
    public void executePlan(final PlanProject planProject) {
        logger.info("Executing plan project: {} (name: {})",
                planProject.getPlanProjectId(), planProject.getPlanProjectInfo().getName());
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
        Set<Group> clusters = new HashSet<>();

        // get all cluster group IDs from the topology
        groupRpcService.getGroups(
                GroupDTO.GetGroupsRequest.newBuilder()
                        .setTypeFilter(GroupDTO.Group.Type.CLUSTER)
                        .setClusterFilter(ClusterFilter.newBuilder()
                                .setTypeFilter(ClusterInfo.Type.COMPUTE)
                                .build())
                        .build()).forEachRemaining(clusters::add);

        // Limit the number of clusters for each run.
        clusters = restrictNumberOfClusters(clusters);

        logger.info("Running plan project on {} clusters.", clusters.size());
        // Create one plan project instance per cluster per Scenario.
        // Total number of plan project instance to be created equals number of clusters times
        // number of scenarios in the plan project.
        clusters.forEach(cluster -> {
            for (PlanProjectScenario scenario : planProject.getPlanProjectInfo().getScenariosList()) {
                // Create plan instance
                PlanInstance planInstance = null;
                try {
                    planInstance = createClusterPlanInstance(cluster,
                            scenario,
                            planProject.getPlanProjectInfo().getType());
                } catch (IntegrityException e) {
                    logger.error("Failed to create a plan instance for cluster {}: {}",
                            cluster.getId(), e.getMessage());
                    continue;
                }

                // Register post process handler
                ProjectPlanPostProcessor planProjectPostProcessor = null;
                if (planProject.getPlanProjectInfo().getType().equals(PlanProjectType.CLUSTER_HEADROOM)) {
                    planProjectPostProcessor = new ClusterHeadroomPlanPostProcessor(planInstance.getPlanId(),
                            cluster, repositoryChannel, historyChannel, getNumClonesToAddForCluster(cluster),
                            planDao, groupChannel);
                }
                if (planProjectPostProcessor != null) {
                    projectPlanPostProcessorRegistry.registerPlanPostProcessor(planProjectPostProcessor);
                } else {
                    continue;
                }

                // Run plan instance
                logger.info("Starting plan for cluster {} and plan project {}",
                        cluster.getCluster().getName(),
                        planProject.getPlanProjectInfo().getName());

                // This will make a synchronous call to the Topology Processor's RPC service,
                // and return after the plan topology is broadcast out of the Topology Processor.
                // At that point it's safe to start running the next plan instance, without
                // fear of overrunning the system with plans.
                //
                // In the future, we still need to address maximum concurrent plans (between
                // all plan projects), and the interaction between user and system plans.
                runPlanInstance(planInstance);
            }
        });
    }

    /**
     * The global setting "MaxPlanInstancesPerPlan" specifies the maximum number of cluster
     * a project plan can analyse on each execution.  For example, if there are 100 clusters
     * and this settings is set at 20, only 20 project instances will be created.
     * If the max number is exceeded, the list of clusters is first randomized, and a subset
     * of the list will be returned.
     * It is a simple algorithm that can achieve a level of "fairness" to this function.
     * i.e. we won't always analyse the same clusters on every run.
     * TODO Provide algorithms that ensure all clusters are cycled through in subsequent executions
     * of the plan project. Additional information may be required to achieve this goal.
     *
     * @param clusters
     * @return
     */
    @VisibleForTesting
    Set<Group> restrictNumberOfClusters(Set<Group> clusters) {
        final GetGlobalSettingResponse response = settingService.getGlobalSetting(
                GetSingleGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(GlobalSettingSpecs.MaxPlanInstancesPerPlan
                                .getSettingName())
                        .build());

        final Float maxPlanInstancesPerPlan;
        if (response.hasSetting()) {
            maxPlanInstancesPerPlan = response.getSetting().getNumericSettingValue().getValue();
        } else {
            maxPlanInstancesPerPlan = GlobalSettingSpecs.MaxPlanInstancesPerPlan.createSettingSpec()
                .getNumericSettingValueType().getDefault();
        }

        if (clusters.size() <= maxPlanInstancesPerPlan) {
            return clusters;
        }

        List<Group> clustersAsList = new ArrayList(clusters);
        Collections.shuffle(clustersAsList);
        clustersAsList = clustersAsList.subList(0, maxPlanInstancesPerPlan.intValue());
        return new HashSet<>(clustersAsList);
    }

    /**
     * Creates a plan instance from a plan project scenario, and sets the cluster ID in plan scope
     *
     * @param cluster the cluster where this plan is applied
     * @param planProjectScenario the plan project scenario
     * @param type
     * @return a plan instance
     * @throws IntegrityException
     */
    @VisibleForTesting
    PlanInstance createClusterPlanInstance(final Group cluster,
                                                   @Nonnull final PlanProjectScenario planProjectScenario,
                                                   @Nonnull final PlanProjectType type)
            throws IntegrityException {
        final PlanScopeEntry planScopeEntry = PlanScopeEntry.newBuilder()
                .setScopeObjectOid(cluster.getId())
                .setClassName("Cluster")
                .build();
        final PlanScope planScope = PlanScope.newBuilder()
                .addScopeEntries(planScopeEntry)
                .build();
        final ScenarioInfo.Builder scenarioInfoBuilder = ScenarioInfo.newBuilder()
                .addAllChanges(planProjectScenario.getChangesList())
                .setScope(planScope);

        if (type.equals(PlanProjectType.CLUSTER_HEADROOM)) {

            // Start of code by mkalath
            SystemLoadInfoRequest.Builder builder = SystemLoadInfoRequest.newBuilder();
            SystemLoadInfoResponse response = planProjectDao.getSystemLoadInfo(builder.setClusterId(cluster.getId()).build());

            // End of code by mkalath, the following has to be reconsidered and changed appropriately


            // TODO (roman, Dec 5 2017): Project-type-specific logic should not be in the main
            // executor class. We should refactor this to separate the general and type-specific
            // processing steps.
            Template headroomTemplate = null;
            boolean changeTemplateToDefaultTemplate = false;
            if (cluster.hasCluster() && cluster.getCluster().hasClusterHeadroomTemplateId()) {
                Optional<Template> clusterTemplate = templatesDao.getTemplate(
                        cluster.getCluster().getClusterHeadroomTemplateId());
                if (clusterTemplate.isPresent()) {
                    headroomTemplate = clusterTemplate.get();
                } else {
                    // A headroom template ID is set in cluster object, but a template with that ID
                    // is not found.  In this case, use the system default template instead.
                    changeTemplateToDefaultTemplate = true;
                }
            }
            if (headroomTemplate == null) {
                headroomTemplate = templatesDao.getTemplatesByName("headroomVM").stream()
                        .filter(template -> template.getType().equals(Type.SYSTEM))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("No system headroom VM found!"));
                if (changeTemplateToDefaultTemplate) {
                    groupRpcService.updateClusterHeadroomTemplate(
                            UpdateClusterHeadroomTemplateRequest.newBuilder()
                                    .setClusterHeadroomTemplateId(headroomTemplate.getId())
                                    .build());
                }
            }
            scenarioInfoBuilder.addChanges(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                    .setAdditionCount(getNumClonesToAddForCluster(cluster))
                    .setTemplateId(headroomTemplate.getId())));
        }

        final Scenario scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfoBuilder)
                .setId(IdentityGenerator.next())
                .build();

        return planDao.createPlanInstance(scenario, type);
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

    private int getNumClonesToAddForCluster(@Nonnull final Group cluster) {
        Preconditions.checkArgument(cluster.getType().equals(Group.Type.CLUSTER));
        return ADDED_CLONES_PER_HOST_IN_CLUSTER *
                cluster.getCluster().getMembers().getStaticMemberOidsCount();
    }
}
