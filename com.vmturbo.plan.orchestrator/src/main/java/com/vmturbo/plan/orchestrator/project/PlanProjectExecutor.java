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
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.project.headroom.ClusterHeadroomPlanPostProcessor;
import com.vmturbo.plan.orchestrator.project.headroom.ClusterHeadroomPlanProjectExecutor;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadCalculatedProfile.Operation;
import com.vmturbo.plan.orchestrator.project.headroom.SystemLoadProfileCreator;
import com.vmturbo.plan.orchestrator.project.migration.CloudMigrationPlanProjectExecutor;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.plan.orchestrator.templates.exceptions.IllegalTemplateOperationException;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * This class executes a plan project.
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

    private final ClusterHeadroomPlanProjectExecutor headroomExecutor;

    private final CloudMigrationPlanProjectExecutor cloudMigrationExecutor;

    // If true, calculate headroom for all clusters in one plan instance.
    // If false, calculate headroom for restricted number of clusters in multiple plan instances.
    private final boolean headroomCalculationForAllClusters;

    private final TopologyProcessor topologyProcessor;

    // Number of days for which system load was considered in history.
    private static final int LOOPBACK_DAYS = 10;

    /**
     * Constructor for {@link PlanProjectExecutor}.
     *
     * @param planDao Plan DAO
     * @param planProjectDao DAO for plan project status update.
     * @param groupChannel  Group service channel
     * @param planRpcService Plan RPC Service
     * @param projectPlanPostProcessorRegistry Registry for post processors of plans
     * @param repositoryChannel Repository channel
     * @param templatesDao templates DAO
     * @param historyChannel history channel
     * @param projectNotifier Used to send plan project related notification updates.
     * @param headroomCalculationForAllClusters specifies how to run cluster headroom plan
     * @param topologyProcessor a REST call to get target info
     */
    PlanProjectExecutor(@Nonnull final PlanDao planDao,
                        @Nonnull final PlanProjectDao planProjectDao,
                        @Nonnull final Channel groupChannel,
                        @Nonnull final PlanRpcService planRpcService,
                        @Nonnull final ProjectPlanPostProcessorRegistry projectPlanPostProcessorRegistry,
                        @Nonnull final Channel repositoryChannel,
                        @Nonnull final TemplatesDao templatesDao,
                        @Nonnull final Channel historyChannel,
                        @Nonnull final PlanProjectNotificationSender projectNotifier,
                        final boolean headroomCalculationForAllClusters,
                        @Nonnull final TopologyProcessor topologyProcessor) {
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
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        headroomExecutor = new ClusterHeadroomPlanProjectExecutor(planDao, groupChannel,
                planRpcService, projectPlanPostProcessorRegistry, repositoryChannel, templatesDao, historyChannel,
                headroomCalculationForAllClusters, topologyProcessor);

        cloudMigrationExecutor = new CloudMigrationPlanProjectExecutor(planDao, planProjectDao,
                planRpcService, projectPlanPostProcessorRegistry, projectNotifier);
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
        switch (type) {
            case CLUSTER_HEADROOM:
                headroomExecutor.executePlanProject(planProject);
                break;
            case CLOUD_MIGRATION:
                cloudMigrationExecutor.executePlanProject(planProject);
                break;
            default:
                logger.error("Unsupported project {} type {}. Cannot execute plan.",
                        planProject.getPlanProjectId(), type);
        }
    }

    /**
     * Calls the plan service to run the plan instance.
     *
     * @param planService Plan service to call.
     * @param planInstance Instance of plan to start running.
     * @param logger Used for logging errors.
     */
    public static void runPlanInstance(@Nonnull final PlanRpcService planService,
                                       @Nonnull final PlanInstance planInstance,
                                       @Nonnull final Logger logger) {
        planService.runPlan(PlanId.newBuilder()
                .setPlanId(planInstance.getPlanId())
                .build(), new StreamObserver<PlanInstance>() {
            @Override
            public void onNext(PlanInstance value) {
            }

            @Override
            public void onError(Throwable t) {
                logger.error("Error executing plan {} of project type {}.",
                        planInstance.getPlanId(), planInstance.getProjectType(), t);
            }

            @Override
            public void onCompleted() {
            }
        });
    }
}
