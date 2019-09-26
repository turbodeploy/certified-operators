package com.vmturbo.plan.orchestrator.project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo.PlanProjectScenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanInstanceQueue;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.project.headroom.ClusterHeadroomPlanPostProcessor;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile.Operation;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadProfileCreator;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.plan.orchestrator.templates.exceptions.IllegalTemplateOperationException;

/**
 * This class executes a plan project
 */
public class PlanProjectExecutor {
    private final Logger logger = LogManager.getLogger();

    private final PlanDao planDao;

    private final PlanRpcService planService;

    private final ProjectPlanPostProcessorRegistry projectPlanPostProcessorRegistry;

    private final Channel repositoryChannel;

    private final Channel historyChannel;

    private final TemplatesDao templatesDao;

    private final Channel groupChannel;

    private final GroupServiceBlockingStub groupRpcService;

    private final SettingServiceBlockingStub settingService;

    private final StatsHistoryServiceBlockingStub statsHistoryService;

    // If true, calculate headroom for all clusters in one plan instance.
    // If false, calculate headroom for restricted number of clusters in multiple plan instances.
    private final boolean headroomCalculationForAllClusters;

    // Number of days for which system load was considered in history.
    private static final int LOOPBACK_DAYS = 10;

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
     * @param groupChannel  Group service channel
     * @param planRpcService Plan RPC Service
     * @param projectPlanPostProcessorRegistry Registry for post processors of plans
     * @param repositoryChannel Repository channel
     * @param templatesDao templates DAO
     * @param historyChannel history channel
     * @param headroomCalculationForAllClusters specifies how to run cluster headroom plan
     */
    PlanProjectExecutor(@Nonnull final PlanDao planDao,
                        @Nonnull final Channel groupChannel,
                        @Nonnull final PlanRpcService planRpcService,
                        @Nonnull final ProjectPlanPostProcessorRegistry projectPlanPostProcessorRegistry,
                        @Nonnull final Channel repositoryChannel,
                        @Nonnull final TemplatesDao templatesDao,
                        @Nonnull final Channel historyChannel,
                        final boolean headroomCalculationForAllClusters) {
        this.groupChannel = Objects.requireNonNull(groupChannel);
        this.planService = Objects.requireNonNull(planRpcService);
        this.projectPlanPostProcessorRegistry = Objects.requireNonNull(projectPlanPostProcessorRegistry);
        this.repositoryChannel = Objects.requireNonNull(repositoryChannel);
        this.planDao = Objects.requireNonNull(planDao);
        this.templatesDao = Objects.requireNonNull(templatesDao);
        this.historyChannel = Objects.requireNonNull(historyChannel);
        this.headroomCalculationForAllClusters = headroomCalculationForAllClusters;

        this.groupRpcService = GroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.settingService = SettingServiceGrpc.newBlockingStub(groupChannel);
        this.statsHistoryService =
            StatsHistoryServiceGrpc.newBlockingStub(Objects.requireNonNull(historyChannel));
    }

    /**
     * Executes a plan project according to the instructions provided in the plan project.
     * See PlanDTO.proto for detailed documentation of fields in the plan project object and how
     * they will be processed during plan execution.
     *
     * @param planProject a plan project
     */
    public void executePlan(@Nonnull final PlanProject planProject) {
        logger.info("Executing plan project: {} (name: {})",
                planProject.getPlanProjectId(), planProject.getPlanProjectInfo().getName());

        PlanProjectType type = planProject.getPlanProjectInfo().getType();
        if (type == PlanProjectType.CLUSTER_HEADROOM) {
            if (headroomCalculationForAllClusters) {
                runPlanInstanceAllCluster(planProject);
            } else {
                runPlanInstancePerClusters(planProject);
            }
        } else {
            logger.error("No such plan project type: {}", type);
        }
    }

    /**
     * Create one plan instance per cluster per scenario.
     *
     * @param planProject a plan project
     */
    private void runPlanInstancePerClusters(@Nonnull final PlanProject planProject) {
        Set<Group> clusters = getAllComputeClusters();
        // Limit the number of clusters for each run.
        clusters = restrictNumberOfClusters(clusters);
        logger.info("Running plan project {} on {} clusters. " +
                "(one plan instance per cluster per scenario)",
            planProject.getPlanProjectInfo().getName(), clusters.size());

        // Total number of plan instances to be created equals the number of clusters times
        // number of scenarios in the plan project.
        clusters.forEach(cluster ->
            createClusterPlanInstanceAndRun(planProject, Collections.singleton(cluster)));
    }

    /**
     * Create one plan instance for all clusters per scenario.
     *
     * @param planProject a plan project
     */
    private void runPlanInstanceAllCluster(@Nonnull final PlanProject planProject) {
        Set<Group> clusters = getAllComputeClusters();
        logger.info("Running plan project {} on {} clusters. " +
                "(one plan instance for all clusters per scenario)",
            planProject.getPlanProjectInfo().getName(), clusters.size());

        createClusterPlanInstanceAndRun(planProject, clusters);
    }

    /**
     * Get all compute cluster groups from the topology.
     *
     * @return a set of compute clusters
     */
    private Set<Group> getAllComputeClusters() {
        Set<Group> clusters = new HashSet<>();

        groupRpcService.getGroups(
            GroupDTO.GetGroupsRequest.newBuilder()
                .addTypeFilter(GroupDTO.Group.Type.CLUSTER)
                .setClusterFilter(ClusterFilter.newBuilder()
                    .setTypeFilter(ClusterInfo.Type.COMPUTE)
                    .build())
                .build()).forEachRemaining(clusters::add);

        return clusters;
    }

    /**
     * Create one plan instance for the given clusters per scenario and run it.
     *
     * @param planProject a plan project
     * @param clusters the clusters where this plan is applied
     */
    private void createClusterPlanInstanceAndRun(@Nonnull final PlanProject planProject,
                                                 @Nonnull final Set<Group> clusters) {
        for (PlanProjectScenario scenario : planProject.getPlanProjectInfo().getScenariosList()) {
            // Create plan instance
            PlanInstance planInstance;
            try {
                planInstance = createClusterPlanInstance(clusters, scenario,
                    planProject.getPlanProjectInfo().getType());
            } catch (IntegrityException e) {
                logger.error("Failed to create a plan instance for plan project {}: {}",
                    planProject.getPlanProjectInfo().getName(), e.getMessage());
                continue;
            }

            // Register post process handler
            ProjectPlanPostProcessor planProjectPostProcessor = null;
            if (planProject.getPlanProjectInfo().getType().equals(PlanProjectType.CLUSTER_HEADROOM)) {
                planProjectPostProcessor = new ClusterHeadroomPlanPostProcessor(planInstance.getPlanId(),
                    clusters.stream().map(Group::getId).collect(Collectors.toSet()),
                    repositoryChannel, historyChannel, planDao, groupChannel, templatesDao);
            }
            if (planProjectPostProcessor != null) {
                projectPlanPostProcessorRegistry.registerPlanPostProcessor(planProjectPostProcessor);
            } else {
                continue;
            }

            logger.info("Starting plan instance for plan project {}",
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
     * @param clusters the original clusters where this plan is applied
     * @return the restricted clusters where this plan is applied
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

        List<Group> clustersAsList = new ArrayList<>(clusters);
        Collections.shuffle(clustersAsList);
        clustersAsList = clustersAsList.subList(0, maxPlanInstancesPerPlan.intValue());
        return new HashSet<>(clustersAsList);
    }

    /**
     * Creates a plan instance from a plan project scenario, and sets the cluster ID in plan scope.
     *
     * @param clusters the clusters where this plan is applied
     * @param planProjectScenario the plan project scenario
     * @param type of plan project
     * @return a plan instance
     * @throws IntegrityException if some integrity constraints violated
     */
    @VisibleForTesting
    PlanInstance createClusterPlanInstance(@Nonnull final Set<Group> clusters,
                                           @Nonnull final PlanProjectScenario planProjectScenario,
                                           @Nonnull final PlanProjectType type)
                                           throws IntegrityException {
        final PlanScope.Builder planScopeBuilder = PlanScope.newBuilder();

        for (Group cluster : clusters) {
            boolean addScopeEntry = true;

            // The failure for a single cluster should not fail the entire plan.
            if (type.equals(PlanProjectType.CLUSTER_HEADROOM)) {
                try {
                    updateClusterHeadroomTemplate(cluster);
                } catch (NoSuchObjectException | IllegalTemplateOperationException |
                         DuplicateTemplateException e) {
                    addScopeEntry = false;
                    if (logger.isTraceEnabled()) {
                        logger.trace("Failed to update headroom template for cluster name {}, id {}: {}",
                            cluster.getCluster().getDisplayName(), cluster.getId(), e.getMessage());
                    }
                } catch (StatusRuntimeException e) {
                    logger.error("Failed to retrieve system load of cluster name {}, id {}: {}",
                        cluster.getCluster().getDisplayName(), cluster.getId(), e.getMessage());
                }
            }

            // If exception was encountered during pre-processing, don't add to the scope entry list.
            if (addScopeEntry) {
                planScopeBuilder.addScopeEntries(PlanScopeEntry.newBuilder()
                    .setScopeObjectOid(cluster.getId())
                    .setClassName("Cluster")
                    .build());
            }
        }

        // No scope entry found means no cluster was included in the plan instance.
        // So there's no need to create and run the plan instance.
        if (planScopeBuilder.getScopeEntriesCount() == 0) {
            throw new IntegrityException("No scope entry found");
        }

        final ScenarioInfo.Builder scenarioInfoBuilder = ScenarioInfo.newBuilder()
                .addAllChanges(planProjectScenario.getChangesList())
                .setScope(planScopeBuilder);
        final Scenario scenario = Scenario.newBuilder()
                .setScenarioInfo(scenarioInfoBuilder)
                .setId(IdentityGenerator.next())
                .build();

        return planDao.createPlanInstance(scenario, type);
    }

    /**
     * Create or update the headroom template of a cluster.
     * if headroomTemplateInfo exists:
     *     if cluster has clusterHeadroomTemplateId:
     *         update the associated headroom template using headroomTemplateInfo
     *     else:
     *         create a new headroom template using headroomTemplateInfo
     * else:
     *     use the default headroom template
     * update the clusterHeadroomTemplateId of the cluster
     *
     * @param cluster the cluster whose headroom template will be updated
     * @throws NoSuchObjectException if can not find system load records for this cluster or an existing template
     * @throws IllegalTemplateOperationException if the operation is not allowed created template
     * @throws DuplicateTemplateException if there are errors when a user tries to create templates
     */
    private void updateClusterHeadroomTemplate(@Nonnull Group cluster)
                throws NoSuchObjectException, IllegalTemplateOperationException,
                       DuplicateTemplateException {
        List<SystemLoadRecord> systemLoadRecordList = new ArrayList<>();
        statsHistoryService.getSystemLoadInfo(
                SystemLoadInfoRequest.newBuilder().addClusterId(cluster.getId()).build())
            .forEachRemaining(response -> systemLoadRecordList.addAll(response.getRecordList()));

        if (systemLoadRecordList.isEmpty()) {
            throw new NoSuchObjectException("No system load records found for cluster : "
                + cluster.getCluster().getDisplayName());
        }

        SystemLoadProfileCreator profiles = new SystemLoadProfileCreator(
            cluster, systemLoadRecordList, LOOPBACK_DAYS);
        Map<Operation, SystemLoadCalculatedProfile> profilesMap = profiles.createAllProfiles();
        SystemLoadCalculatedProfile avgProfile = profilesMap.get(Operation.AVG);
        Optional<TemplateInfo> headroomTemplateInfo = avgProfile.getHeadroomTemplateInfo();

        // TODO (roman, Dec 5 2017): Project-type-specific logic should not be in the main
        // executor class. We should refactor this to separate the general and type-specific
        // processing steps.
        Template usedTemplate = null;
        long clusterHeadroomTemplateId = cluster.getCluster().getClusterHeadroomTemplateId();
        Optional<Template> optionalTemplate = templatesDao.getTemplate(clusterHeadroomTemplateId);
        // Change the template only if it is an average or a headroomVM template
        if (isHeadroomVmOrAverageTemplate(clusterHeadroomTemplateId, avgProfile.getProfileName(), optionalTemplate)) {
            if (headroomTemplateInfo.isPresent()) {
                if (cluster.hasCluster() && cluster.getCluster().hasClusterHeadroomTemplateId()) {
                    logger.debug("Updating template for : " + cluster.getCluster().getDisplayName() +
                            " with template id : " + clusterHeadroomTemplateId);
                    // if template Id already exists, update template with new values
                    templatesDao.editTemplate(clusterHeadroomTemplateId, headroomTemplateInfo.get());
                } else {
                    // Create template using template info and set it as headroomTemplate.
                    usedTemplate = templatesDao.createTemplate(headroomTemplateInfo.get());
                }
            } else {
                // Set headroomTemplate  to default template
                usedTemplate = templatesDao.getFilteredTemplates(TemplatesFilter.newBuilder()
                        .addTemplateName(StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME)
                        .build()).stream()
                        .filter(template -> template.getType().equals(Type.SYSTEM))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("No system headroom VM found!"));
            }
        }
        if (usedTemplate != null) {
            // Update template with current headroomTemplate
            groupRpcService.updateClusterHeadroomTemplate(
                UpdateClusterHeadroomTemplateRequest.newBuilder()
                    .setGroupId(cluster.getId())
                    .setClusterHeadroomTemplateId(usedTemplate.getId())
                    .build());
        }
    }

    private boolean isHeadroomVmOrAverageTemplate(long templateId, String profileName, Optional<Template> optionalTemplate) {
        String headroomVMName = StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME;
        if (optionalTemplate != null) {
            if (optionalTemplate.isPresent()) {
                Template template = optionalTemplate.get();
                if (template.hasTemplateInfo()) {
                    TemplateInfo templateInfo = template.getTemplateInfo();
                    if (templateInfo.hasName()) {
                        String name = templateInfo.getName();
                        if (name.equals(profileName) || (name.equals(headroomVMName))) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Calls the plan service to run the plan instance.
     *
     * @param planInstance a plan instance
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
