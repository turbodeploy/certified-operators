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

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo.PlanProjectScenario;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
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
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.project.headroom.ClusterHeadroomPlanPostProcessor;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile.Operation;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadProfileCreator;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.plan.orchestrator.templates.exceptions.IllegalTemplateOperationException;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

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
        Set<Grouping> clusters = getAllComputeClusters();
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
        Set<Grouping> clusters = getAllComputeClusters();
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
    private Set<Grouping> getAllComputeClusters() {
        Set<Grouping> clusters = new HashSet<>();

        groupRpcService.getGroups(
                        GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                        .setGroupType(GroupType.COMPUTE_HOST_CLUSTER))
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
                                                 @Nonnull final Set<Grouping> clusters) {
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
                    clusters.stream().map(Grouping::getId).collect(Collectors.toSet()),
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
    Set<Grouping> restrictNumberOfClusters(Set<Grouping> clusters) {
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

        List<Grouping> clustersAsList = new ArrayList<>(clusters);
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
    PlanInstance createClusterPlanInstance(@Nonnull final Set<Grouping> clusters,
                                           @Nonnull final PlanProjectScenario planProjectScenario,
                                           @Nonnull final PlanProjectType type)
                                           throws IntegrityException {
        final PlanScope.Builder planScopeBuilder = PlanScope.newBuilder();

        // TODO (roman, Dec 5 2017): Project-type-specific logic should not be in the main
        // executor class. We should refactor this to separate the general and type-specific
        // processing steps.
        if (type.equals(PlanProjectType.CLUSTER_HEADROOM)) {
            // Get the default cluster headroom template.
            Optional<Template> defaultHeadroomTemplate = templatesDao
                .getFilteredTemplates(TemplatesFilter.newBuilder()
                    .addTemplateName(StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME).build())
                .stream().filter(template -> template.getType().equals(Type.SYSTEM))
                .findFirst();

            for (Grouping cluster : clusters) {
                // The failure for a single cluster should not fail the entire plan.
                boolean addScopeEntry = true;

                try {
                    updateClusterHeadroomTemplate(cluster, defaultHeadroomTemplate);
                } catch (NoSuchObjectException | IllegalTemplateOperationException |
                    DuplicateTemplateException e) {
                    addScopeEntry = false;
                    if (logger.isTraceEnabled()) {
                        logger.trace("Failed to update headroom template for cluster name {}, id {}: {}",
                            cluster.getDefinition().getDisplayName(), cluster.getId(), e.getMessage());
                    }
                } catch (StatusRuntimeException e) {
                    logger.error("Failed to retrieve system load of cluster name {}, id {}: {}",
                        cluster.getDefinition().getDisplayName(), cluster.getId(), e.getMessage());
                }

                // If exception was encountered during pre-processing, don't add to the scope entry list.
                if (addScopeEntry) {
                    planScopeBuilder.addScopeEntries(PlanScopeEntry.newBuilder()
                        .setScopeObjectOid(cluster.getId())
                        .setClassName("Cluster")
                        .build());
                }
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
     * Create or update the headroom template for a cluster.
     *
     * <p>Here's the logic:
     * if the cluster has sufficient system load records:
     *     if cluster template exists in the db:
     *         if the associated template is avg template:
     *             update this existing avg template
     *         else if the associated template is default headroom template:
     *             create a new avg template
     *             newClusterHeadroomTemplateId = id of the new avg template
     *     else:
     *         create a new avg template
     *         newClusterHeadroomTemplateId = id of the new avg template
     * else:
     *     if cluster template doesn't exist in the db:
     *         newClusterHeadroomTemplateId = default headroom template id
     * Send a request to group component to update the clusterHeadroomTemplateId of the cluster.
     *
     * <p>Note that sometimes a cluster has an associated template id, but the corresponding template
     * doesn't exist in the db, e.g. when loading group diags.
     * This has never happened at a customer.
     *
     * @param cluster the cluster whose headroom template will be updated
     * @param defaultHeadroomTemplate an Optional of default cluster headroom template
     * @throws NoSuchObjectException if default cluster headroom template not found
     * @throws IllegalTemplateOperationException if the operation is not allowed created template
     * @throws DuplicateTemplateException if there are errors when a user tries to create templates
     */
    @VisibleForTesting
    void updateClusterHeadroomTemplate(@Nonnull final Grouping cluster,
                                       @Nonnull final Optional<Template> defaultHeadroomTemplate)
                throws NoSuchObjectException, IllegalTemplateOperationException,
                       DuplicateTemplateException {
        final List<SystemLoadRecord> systemLoadRecordList = new ArrayList<>();
        statsHistoryService.getSystemLoadInfo(
                SystemLoadInfoRequest.newBuilder().addClusterId(cluster.getId()).build())
            .forEachRemaining(response -> systemLoadRecordList.addAll(response.getRecordList()));

        Optional<Long> newClusterHeadroomTemplateId = Optional.empty();
        Optional<Template> headroomTemplate =
            templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId());

        if (!systemLoadRecordList.isEmpty()) {
            // Cluster has sufficient system load data.
            // Create avg template info.
            final SystemLoadProfileCreator profileCreator = new SystemLoadProfileCreator(
                cluster, systemLoadRecordList, LOOPBACK_DAYS);
            final Map<Operation, SystemLoadCalculatedProfile> profiles = profileCreator.createAllProfiles();
            final SystemLoadCalculatedProfile avgProfile = profiles.get(Operation.AVG);
            // In this case, avgTemplateInfo exists for sure.
            final TemplateInfo avgTemplateInfo = avgProfile.getHeadroomTemplateInfo().get();
            // The target that discovered this group.
            final Optional<Long> targetId;
            if (cluster.hasOrigin() && cluster.getOrigin().hasDiscovered() &&
                cluster.getOrigin().getDiscovered().getDiscoveringTargetIdCount() != 0) {
                targetId = Optional.of(cluster.getOrigin().getDiscovered().getDiscoveringTargetId(0));
            } else {
                targetId = Optional.empty();
            }

            if (headroomTemplate.isPresent()) {
                // Cluster has sufficient system load data and has associated headroom template.
                if (headroomTemplate.get().hasTemplateInfo() &&
                    headroomTemplate.get().getTemplateInfo().hasName()) {
                    if (headroomTemplate.get().getTemplateInfo().getName()
                        .equals(avgProfile.getProfileName())) {
                        // Associated headroom template is avg template.
                        logger.info("Updating existing avg template for cluster {}, id {}",
                            cluster.getDefinition().getDisplayName(), cluster.getId());
                        // Provide targetId for editTemplate so that existing templates without a targetId
                        // will be assigned a targetId and then can be deleted while removing target.
                        templatesDao.editTemplate(headroomTemplate.get().getId(), avgTemplateInfo, targetId);
                    } else if (headroomTemplate.get().getTemplateInfo().getName()
                        .equals(StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME)) {
                        // Associated headroom template is default template.
                        // Switch from default template to avg template.
                        logger.info("Creating avg template for cluster {}, id {}. Switch from default " +
                            "template to avg template.", cluster.getDefinition().getDisplayName(), cluster.getId());
                        newClusterHeadroomTemplateId =
                            Optional.of(templatesDao.createTemplate(avgTemplateInfo, targetId).getId());
                    } else {
                        // Create or update AVG template even though current template is not AVG template.
                        templatesDao.createTemplate(avgTemplateInfo, targetId);
                    }
                }
            } else {
                // Cluster has sufficient system load data but doesn't have associated headroom template.
                logger.info("Creating avg template for cluster {}, id {}",
                    cluster.getDefinition().getDisplayName(), cluster.getId());
                newClusterHeadroomTemplateId =
                    Optional.of(templatesDao.createTemplate(avgTemplateInfo, targetId).getId());
            }
        } else {
            if (!headroomTemplate.isPresent()) {
                // Cluster doesn't have sufficient system load data and associated headroom template.
                if (defaultHeadroomTemplate.isPresent()) {
                    newClusterHeadroomTemplateId = Optional.of(defaultHeadroomTemplate.get().getId());
                    logger.info("No system load records found for cluster {}, id {}. " +
                        "Use default headroom template.", cluster.getDefinition().getDisplayName(), cluster.getId());
                } else {
                    throw new NoSuchObjectException("Default cluster headroom template not found for cluster " +
                        cluster.getDefinition().getDisplayName() + ", id " + cluster.getId());
                }
            }
        }

        // Update cluster with new clusterHeadroomTemplateId.
        newClusterHeadroomTemplateId.ifPresent(headroomTemplateId ->
            templatesDao.setOrUpdateHeadroomTemplateForCluster(cluster.getId(), headroomTemplateId));
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
