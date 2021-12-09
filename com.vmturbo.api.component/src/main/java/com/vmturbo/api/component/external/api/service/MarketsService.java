package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.PaginatedEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedPlanInfo;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.action.ActionInputUtil;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.component.external.api.util.cost.CostStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
import com.vmturbo.api.component.external.api.util.stats.PlanEntityStatsFetcher;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.cost.CostInputApiDTO;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.ActionResourceImpactStatApiInputDTO;
import com.vmturbo.api.dto.action.NoDetailsApiDTO;
import com.vmturbo.api.dto.entity.BaseCommodityApiDTO;
import com.vmturbo.api.dto.entity.FailedResourceApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.UnplacementDetailsApiDTO;
import com.vmturbo.api.dto.entity.UnplacementReasonApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.PlacementProblem;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityOrderBy;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IMarketsService;
import com.vmturbo.api.utils.ParamStrings.MarketOperations;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest.ActionQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyDeleteResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyEditResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.DeletePlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetAllPlanProjectsRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetPlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetPlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject.PlanProjectStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.RunPlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectServiceGrpc.PlanProjectServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.GlobalIgnoreEntityType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioId;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioRequest;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.FailedResources;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.plan.orchestrator.api.PlanUtils;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Service implementation of Markets.
 **/
public class MarketsService implements IMarketsService {

    private static final String USER_DOES_NOT_HAVE_ACCESS_TO_PLAN =
            "User does not have access to plan.";
    private final Logger logger = LogManager.getLogger();

    private final PlanServiceBlockingStub planRpcService;

    private final PlanProjectServiceBlockingStub planProjectRpcService;

    private final ActionSpecMapper actionSpecMapper;

    private final UuidMapper uuidMapper;

    private final PoliciesService policiesService;

    private final PolicyServiceBlockingStub policyRpcService;

    private final ScenarioServiceBlockingStub scenarioServiceClient;

    private final GroupServiceBlockingStub groupRpcService;

    private final RepositoryServiceBlockingStub repositoryRpcService;

    private final PolicyMapper policyMapper;

    private final MarketMapper marketMapper;

    private final StatsService statsService;

    private final PaginationMapper paginationMapper;

    private final UINotificationChannel uiNotificationChannel;

    private final long realtimeTopologyContextId;

    private final MergePolicyHandler mergePolicyHandler;

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final RepositoryApi repositoryApi;

    private final ServiceEntityMapper serviceEntityMapper;

    private final ThinTargetCache thinTargetCache;

    private final PriceIndexPopulator priceIndexPopulator;

    private final SeverityPopulator severityPopulator;

    private final SearchServiceBlockingStub searchServiceBlockingStub;

    private final ActionsServiceBlockingStub actionOrchestratorRpcService;

    private final ActionSearchUtil actionSearchUtil;

    private final PlanProjectBuilder planProjectBuilder;

    /**
     * For fetching plan entities and their related stats.
     */
    private final PlanEntityStatsFetcher planEntityStatsFetcher;

    private final EntitySettingQueryExecutor entitySettingQueryExecutor;

    private final LicenseCheckClient licenseCheckClient;

    private final EntityAspectMapper entityAspectMapper;

    private final CostStatsQueryExecutor costStatsQueryExecutor;

    /**
     * Entity types which support being shown as unplaced in plan result.
     */
    private static final Set<Integer> ENTITY_TYPES_SUPPORTING_UNPLACED =
        ImmutableSet.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.CONTAINER_POD_VALUE);

    /**
     * Entity types which are not valid for plan scopes.
     */
    private static final Set<ApiEntityType> INVALID_SCOPE_ENTITY_TYPES =
        ImmutableSet.of(ApiEntityType.COMPUTE_TIER, ApiEntityType.DATABASE_TIER, ApiEntityType.STORAGE_TIER);

    public MarketsService(@Nonnull final ActionSpecMapper actionSpecMapper,
                          @Nonnull final UuidMapper uuidMapper,
                          @Nonnull final PoliciesService policiesService,
                          @Nonnull final PolicyServiceBlockingStub policyRpcService,
                          @Nonnull final PlanServiceBlockingStub planRpcService,
                          @Nonnull final PlanProjectServiceBlockingStub planProjectRpcService,
                          @Nonnull final ScenarioServiceBlockingStub scenariosService,
                          @Nonnull final PolicyMapper policyMapper,
                          @Nonnull final MarketMapper marketMapper,
                          @Nonnull final PaginationMapper paginationMapper,
                          @Nonnull final GroupServiceBlockingStub groupRpcService,
                          @Nonnull final RepositoryServiceBlockingStub repositoryRpcService,
                          @Nonnull final UINotificationChannel uiNotificationChannel,
                          @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
                          @Nonnull final ThinTargetCache thinTargetCache,
                          @Nonnull final StatsService statsService,
                          @Nonnull final RepositoryApi repositoryApi,
                          @Nonnull final ServiceEntityMapper serviceEntityMapper,
                          @Nonnull final SeverityPopulator severityPopulator,
                          @Nonnull final PriceIndexPopulator priceIndexPopulator,
                          @Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                          @Nonnull final PlanEntityStatsFetcher planEntityStatsFetcher,
                          @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                          @Nonnull final ActionSearchUtil actionSearchUtil,
                          @Nonnull final EntitySettingQueryExecutor entitySettingQueryExecutor,
                          @Nonnull final LicenseCheckClient licenseCheckClient,
                          @Nonnull final EntityAspectMapper entityAspectMapper,
                          final long realtimeTopologyContextId,
                          @Nonnull final CostStatsQueryExecutor costStatsQueryExecutor) {
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.policiesService = Objects.requireNonNull(policiesService);
        this.policyRpcService = Objects.requireNonNull(policyRpcService);
        this.planRpcService = Objects.requireNonNull(planRpcService);
        this.planProjectRpcService = Objects.requireNonNull(planProjectRpcService);
        this.scenarioServiceClient = Objects.requireNonNull(scenariosService);
        this.policyMapper = Objects.requireNonNull(policyMapper);
        this.marketMapper = Objects.requireNonNull(marketMapper);
        this.uiNotificationChannel = Objects.requireNonNull(uiNotificationChannel);
        this.groupRpcService = Objects.requireNonNull(groupRpcService);
        this.repositoryRpcService = Objects.requireNonNull(repositoryRpcService);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);
        this.statsService = Objects.requireNonNull(statsService);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.priceIndexPopulator = Objects.requireNonNull(priceIndexPopulator);
        this.actionOrchestratorRpcService = Objects.requireNonNull(actionOrchestratorRpcService);
        this.planEntityStatsFetcher = Objects.requireNonNull(planEntityStatsFetcher);
        this.searchServiceBlockingStub = Objects.requireNonNull(searchServiceBlockingStub);
        this.mergePolicyHandler =
                new MergePolicyHandler(groupRpcService, policyRpcService, repositoryApi);
        this.actionSearchUtil = actionSearchUtil;
        this.planProjectBuilder = new PlanProjectBuilder(this.planProjectRpcService,
                this.scenarioServiceClient,
                this.planRpcService);
        this.entitySettingQueryExecutor = Objects.requireNonNull(entitySettingQueryExecutor);
        this.licenseCheckClient = Objects.requireNonNull(licenseCheckClient);
        this.entityAspectMapper = Objects.requireNonNull(entityAspectMapper);
        this.costStatsQueryExecutor = Objects.requireNonNull(costStatsQueryExecutor);
    }

    /**
     * Retrieves the main {@link PlanInstance} from a given {@link PlanProject}.
     *
     * @param planProject from which to extract the main plan instance
     * @return the main {@link PlanInstance} from the {@link PlanProject} specified
     * @throws UnknownObjectException when the corresponding {@link PlanProjectInfo}
     * has no main plan ID
     */
    private PlanInstance getMainPlanInstance(PlanProject planProject)
            throws UnknownObjectException {
        final PlanProjectInfo projectInfo = planProject.getPlanProjectInfo();
        // All projects being returned via API have a main plan.
        if (!projectInfo.hasMainPlanId()) {
            throw new UnknownObjectException("Missing main plan id for plan project "
                    + planProject.getPlanProjectId());
        }
        return getPlanInstance(projectInfo.getMainPlanId());
    }

    /**
     * Get all markets/plans from the plan orchestrator. Any cloud migration plans created
     * by user are also returned.
     *
     * @param scopeUuids this argument is currently ignored.
     * @return list of all markets
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws UnknownObjectException if a {@link PlanProject} has an unknown main plan id
     */
    @Override
    @Nonnull
    public List<MarketApiDTO> getMarkets(@Nullable List<String> scopeUuids)
            throws ConversionException, InterruptedException, UnknownObjectException {
        licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);
        logger.debug("Get all markets in scopes: {}", scopeUuids);
        final Iterator<PlanInstance> plans =
                planRpcService.getAllPlans(GetPlansOptions.getDefaultInstance());
        final List<MarketApiDTO> result = new ArrayList<>();
        // Add the realtime market api dto to the list of markets
        result.add(createRealtimeMarketApiDTO());
        // Get the plan markets and create api dtos for these
        while (plans.hasNext()) {
            final PlanInstance plan = plans.next();
            if (plan.hasPlanProjectId()) {
                // Skip any plans that are part of projects (like migration), they are shown
                // separately below.
                continue;
            }
            final MarketApiDTO dto = marketMapper.dtoFromPlanInstance(plan);
            result.add(dto);
        }
        // Get all user visible plan projects as well.
        final List<PlanProject> planProjects = planProjectRpcService
                .getAllPlanProjects(GetAllPlanProjectsRequest.getDefaultInstance())
                .getProjectsList()
                .stream()
                .filter(planProject -> PlanDTOUtil.isDisplayablePlan(planProject
                        .getPlanProjectInfo().getType()))
                .collect(Collectors.toList());
        for (PlanProject planProject : planProjects) {
            try {
                final PlanInstance mainPlanInstance = getMainPlanInstance(planProject);
                // If user cannot access the plan instance, skip it instead of failing completely
                if (!PlanUtils.canCurrentUserAccessPlan(mainPlanInstance)) {
                    continue;
                }
                result.add(getMarketApiDto(planProject, mainPlanInstance, null));
            } catch (ConversionException ce) {
                // When listing all projects, if there is a conversion issue for a few projects
                // (e.g because of missing plans), then try and return rest of the good projects,
                // otherwise user doesn't see any plans in the UI.
                logger.warn("Unable to convert plan project {}. Message: {}",
                        planProject.getPlanProjectId(), ce.getMessage());
            }
        }
        return result;
    }

    @Override
    public MarketApiDTO getMarketByUuid(String uuid) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        if (apiId.isRealtimeMarket()) {
           return createRealtimeMarketApiDTO();
        }
        if (!apiId.isPlan()) {
            throw new UnknownObjectException("The ID " + uuid
                    + " doesn't belong to a plan or a real-time market.");
        }
        licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);        // Look up the plan instance.
        final PlanInstance planInstance = getPlanInstance(apiId.oid());
        planAccessAuthorizationEnforcement(planInstance);
        return getMarketApiDto(planInstance);

    }

    /**
     * Gets API DTO to be returned to UI when plan is updated.
     *
     * @param planInstance Instance of plan that is updated, e.g plan name change.
     * @return Instance of MarketApiDTO
     * @throws Exception Thrown on translation error.
     */
    @Nonnull
    private MarketApiDTO getMarketApiDto(@Nonnull final PlanInstance planInstance) throws Exception {
        MarketApiDTO marketApiDTO;
        if (planInstance.hasPlanProjectId()) {
            // This could be the main plan (like Consumption plan), that is part of a MCP project.
            final PlanProject planProject = getPlanProject(planInstance.getPlanProjectId());
            final String planName = planInstance.hasName() ? planInstance.getName() : "";
            logger.info("{} Getting MarketApiDTO for plan {}.",
                    TopologyDTOUtil.formatPlanLogPrefix(planInstance.getPlanId(),
                            planProject.getPlanProjectId()), planName);
            marketApiDTO = getMarketApiDto(planProject, getMainPlanInstance(planProject), null);
        } else {
            // A regular plan (like OCP) without an associated project.
            marketApiDTO = marketMapper.dtoFromPlanInstance(planInstance);
        }
        marketApiDTO.setEnvironmentType(getEnvironmentType());
        // set unplaced entities
        try {
            marketApiDTO.setUnplacedEntities(!getUnplacedEntitiesByMarketUuid(String.valueOf(
                    planInstance.getPlanId())).isEmpty());
        } catch (Exception e) {
            logger.error("getMarketByUuid(): Error while checking if there are unplaced entities: {}",
                    e.getMessage());
        }
        return marketApiDTO;
    }

    @Override
    public MarketApiDTO executeOperation(String uuid, MarketOperations op) throws Exception {
        logger.debug("Trying to execute operation {} on {}.", op, uuid);
        if (op.equals(MarketOperations.save)) {
            // Not sure what to do with this in XL, since everything is saved automatically.
            return getMarketByUuid(uuid);
        } else if (op.equals(MarketOperations.stop)) {
            // note that, for XL, the "marketUuid" in the request is interpreted as the Plan Instance ID
            final ApiId planInstanceId;
            try {
                planInstanceId = uuidMapper.fromUuid(uuid);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid market id: " + uuid);
            }

            final PlanInstance updatedInstance = planRpcService.cancelPlan(PlanId.newBuilder()
                    .setPlanId(planInstanceId.oid())
                    .build());
            return marketMapper.dtoFromPlanInstance(updatedInstance);
        }
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<LogEntryApiDTO> getNotificationsByMarketUuid(String uuid, String starttime, String endtime, String filters) throws Exception {
        logger.debug("Get notifications from market {} with start time {} and end time {}",
                uuid, starttime, endtime);
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LogEntryApiDTO getNotificationByMarketUuid(String uuid, String notificationUuid) throws Exception {
        logger.debug("Get notification {} from market {}", notificationUuid, uuid);
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ActionPaginationResponse getCurrentActionsByMarketUuid(@Nonnull final String uuid,
                                                                  EnvironmentType environmentType,
                                                                  @Nullable final ActionDetailLevel detailLevel,
                                                                  ActionPaginationRequest paginationRequest)
            throws Exception {
        ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setEnvironmentType(environmentType);
        inputDto.setDetailLevel(detailLevel);
        return getActionsByMarketUuid(uuid, inputDto, paginationRequest);
    }

    @Override
    public ActionPaginationResponse getActionsByMarketUuid(String uuid,
                                                           ActionApiInputDTO inputDto,
                                                           ActionPaginationRequest paginationRequest) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        planAccessEnforcement(apiId);
        final ActionQueryFilter filter = actionSpecMapper.createActionFilter(
                                                inputDto, Optional.empty(), apiId);
        return actionSearchUtil.callActionService(filter, paginationRequest, inputDto.getDetailLevel(),
                apiId.getTopologyContextId());
    }

    /**
     * Get an action by the market.
     *
     * @param marketUuid    the market uuid.
     * @param actionUuid    the action uuid.
     * @param detailLevel   the level of Action details to be returned
     *
     * @return an action.
     * @throws Exception exception.
     */
    @Override
    public ActionApiDTO getActionByMarketUuid(@Nonnull final String marketUuid,
                                              @Nonnull final String actionUuid,
                                              @Nullable final ActionDetailLevel detailLevel) throws Exception {
        logger.debug("Request to get action by market UUID: {}, action ID: {}", marketUuid, actionUuid);
        // require "planner" feature access to request plan market actions
        if (Long.valueOf(marketUuid) != realtimeTopologyContextId) {
            licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);
        }

        ActionOrchestratorAction action = actionOrchestratorRpcService
                .getAction(actionRequest(marketUuid, actionUuid));
        if (!action.hasActionSpec()) {
            throw new UnknownObjectException("Action with given action uuid: " + actionUuid + " not found");
        }
        logger.debug("Mapping actions for: {}", marketUuid);
        final ActionApiDTO answer = actionSpecMapper.mapActionSpecToActionApiDTO(action.getActionSpec(),
                realtimeTopologyContextId, detailLevel);
        logger.trace("Result: {}", () -> answer.toString());
        return answer;
    }


    @Override
    public EntityPaginationResponse getEntitiesByMarketUuid(String uuid,
                                                            @Nullable List<String> entityUuids,
                                                            @Nullable List<String> types,
                                                            @Nullable List<String> aspectNames,
                                                            EntityPaginationRequest paginationRequest) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);

        // We don't currently support any sort order other than "name".
        if (paginationRequest.getOrderBy() != EntityOrderBy.NAME) {
            logger.warn("Unimplemented sort order {} for market entities. Defaulting to NAME",
                paginationRequest.getOrderBy());
        }

        if (apiId.isRealtimeMarket()) {
            final SearchQuery.Builder searchQueryBuilder = SearchQuery.newBuilder();
            if (!CollectionUtils.isEmpty(entityUuids)) {
                searchQueryBuilder.addAllSearchParameters(
                    Collections.singleton(SearchProtoUtil.makeSearchParameters(
                        SearchProtoUtil.stringIdFilter(entityUuids)).build()));
            }
            if (!CollectionUtils.isEmpty(types)) {
                searchQueryBuilder.addAllSearchParameters(
                    Collections.singleton(SearchProtoUtil.makeSearchParameters(
                        SearchProtoUtil.entityTypeFilter(types)).build()));
            }

            final PaginatedEntitiesRequest paginatedEntitiesRequest =
                repositoryApi.newPaginatedEntities(
                    searchQueryBuilder.build(),
                    Collections.emptySet(),
                    paginationRequest);
            if (!CollectionUtils.isEmpty(aspectNames)) {
                paginatedEntitiesRequest.requestAspects(entityAspectMapper, aspectNames);
            }

            return paginatedEntitiesRequest.getResponse();
        } else if (apiId.isPlan()) {
            // require the "planner" license feature to get plan actions
            licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);

            // In the plan case we do the pagination in the API component. The main reason is
            // that we need to use the repository service (instead of the search service), and that
            // service doesn't currently support pagination parameters.
            //
            // TODO (roman, Jan 22 2019) OM-54686: Add pagination parameters to the
            // retrieveTopologyEntities method, and convert this logic to use the repository's pagination.
            final PlanInstance planInstance = getPlanInstance(apiId.oid());
            planAccessAuthorizationEnforcement(planInstance);
            final Optional<Long> projectedTopologyId =
                planInstance.hasProjectedTopologyId()
                        ? Optional.of(planInstance.getProjectedTopologyId())
                        : Optional.empty();

            final RetrieveTopologyEntitiesRequest.Builder requestBuilder = RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(apiId.oid())
                .setReturnType(Type.FULL)
                .setTopologyType(TopologyType.PROJECTED);

            // Request specific entities by OID if specified.
            if (!CollectionUtils.isEmpty(entityUuids)) {
                requestBuilder.addAllEntityOids(entityUuids.stream()
                    .map(stringOid -> {
                        try {
                            return Long.parseLong(stringOid);
                        } catch (NumberFormatException e) {
                            logger.error("Unparsable OID string", e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));
            }

            // Request specific entity types if specified.
            if (!CollectionUtils.isEmpty(types)) {
                requestBuilder.addAllEntityType(types.stream()
                    .map(type -> ApiEntityType.fromString(type).typeNumber())
                    .collect(Collectors.toList()));
            }

            final List<TopologyEntityDTO> allResults = RepositoryDTOUtil.topologyEntityStream(
                repositoryRpcService.retrieveTopologyEntities(requestBuilder.build()))
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());

            final List<TopologyAndApiEntity> allConvertedResults = convertServiceEntitiesForPlan(
                planInstance, allResults);

            // We do pagination before fetching price index and severity.
            final int skipCount;
            if (paginationRequest.getCursor().isPresent()) {
                try {
                    // Avoid negative skips.
                    skipCount = Math.max(0, Integer.parseInt(paginationRequest.getCursor().get()));
                } catch (NumberFormatException e) {
                    throw new InvalidOperationException("Cursor "
                            + paginationRequest.getCursor().get()
                            + " is invalid. Must be positive integer.");
                }
            } else {
                skipCount = 0;
            }

            // Avoid non-positive limits.
            final int limit = Math.max(paginationRequest.getLimit(), 1);

            final List<TopologyAndApiEntity> nextEntityPage = allConvertedResults.stream()
                // assumes that ServiceEntityApiDTO::getDisplayName will never return null
                .sorted(Comparator.<TopologyAndApiEntity, String>comparing(e -> e.apiEntity.getDisplayName())
                        .thenComparing(e -> e.apiEntity.getUuid()))
                .skip(skipCount)
                .limit(limit + 1)
                .collect(Collectors.toList());

            final Optional<String> nextCursor;
            if (nextEntityPage.size() > limit) {
                nextCursor = Optional.of(Integer.toString(skipCount + limit));
                // Remove the last element from the results. It was only there to check if there
                // are more results.
                nextEntityPage.remove(limit);
            } else {
                nextCursor = Optional.empty();
            }

            if (!CollectionUtils.isEmpty(aspectNames) && !nextEntityPage.isEmpty()) {
                try {
                    final Map<Long, Map<AspectName, EntityAspect>> aspectsByEntities = entityAspectMapper
                        .getAspectsByEntities(nextEntityPage.stream()
                            .map(e -> e.topologyEntity)
                            .collect(Collectors.toList()), aspectNames);
                    nextEntityPage.forEach(entity -> serviceEntityMapper.populateAspects(
                        entity.apiEntity, aspectsByEntities, entity.topologyEntity.getOid()));
                } catch (StatusRuntimeException e) {
                    logger.error("Unable to populate entity aspects", e);
                }
            }

            // populate priceIndex, which is always based on the projected topology
            final List<ServiceEntityApiDTO> nextPage = nextEntityPage.stream()
                .map(e -> e.apiEntity)
                .collect(Collectors.toList());
            projectedTopologyId.ifPresent(id -> priceIndexPopulator.populatePlanEntities(id, nextPage));
            severityPopulator.populate(apiId.oid(), nextPage);

            return nextCursor.map(cursor -> paginationRequest.nextPageResponse(nextPage, cursor, allResults.size()))
                .orElseGet(() -> paginationRequest.finalPageResponse(nextPage, allResults.size()));
        } else {
            throw new UnknownObjectException(uuid + " is not the realtime market or a plan.");
        }
    }

    // Market UUID is ignored for now, since there is only one Market in XL.
    @Override
    public List<PolicyApiDTO> getPoliciesByMarketUuid(String uuid) throws Exception {
        return policiesService.getPolicies();
    }

    @Override
    public PolicyApiDTO getPolicy(final String marketUuid, final String policyUuid) throws Exception {
        return policiesService.getPolicyByUuid(policyUuid);
    }

    @Override
    public ResponseEntity<PolicyApiDTO> addPolicy(final String uuid,
            PolicyApiInputDTO policyApiInputDTO) throws UnknownObjectException, ConversionException, InterruptedException {
        try {
            if (mergePolicyHandler.isPolicyNeedsHiddenGroup(policyApiInputDTO)) {
                // Create a hidden group to support the merge policy for entities that are not groups.
                mergePolicyHandler.createHiddenGroup(policyApiInputDTO);
            }
            final PolicyDTO.PolicyInfo policyInfo = policyMapper.policyApiInputDtoToProto(policyApiInputDTO);
            final PolicyDTO.PolicyCreateRequest createRequest = PolicyDTO.PolicyCreateRequest.newBuilder()
                    .setPolicyInfo(policyInfo)
                    .build();
            final PolicyDTO.PolicyCreateResponse policyCreateResp = policyRpcService.createPolicy(createRequest);
            final long createdPolicyID = policyCreateResp.getPolicy().getId();

            String createdPolicyUuid = Long.toString(createdPolicyID);
            final PolicyApiDTO response = policiesService.getPolicyByUuid(createdPolicyUuid);

            final String details = String.format("Created policy %s", policyApiInputDTO.getPolicyName());
            AuditLog.newEntry(AuditAction.CREATE_POLICY,
                details, true)
                .targetName(policyApiInputDTO.getPolicyName())
                .audit();

            return new ResponseEntity<>(response, HttpStatus.CREATED);
        } catch (RuntimeException e) {
            final String details = String.format("Failed to create policy %s",
                policyApiInputDTO.getPolicyName());
            AuditLog.newEntry(AuditAction.CREATE_POLICY,
                details, false)
                .targetName(policyApiInputDTO.getPolicyName())
                .audit();
            logger.error("Error while adding a policy with name "
                    + policyApiInputDTO.getPolicyName(), e.getMessage());
            throw e;
        }
    }

    @Override
    public PolicyApiDTO editPolicy(final String uuid, final String policyUuid,
            PolicyApiInputDTO policyApiInputDTO) throws ConversionException, InterruptedException,
            OperationFailedException {
        try {
            if (mergePolicyHandler.isPolicyNeedsHiddenGroup(policyApiInputDTO)) {
                // Update a hidden group to support the merge policy for entities that are not groups.
                mergePolicyHandler.updateHiddenGroup(policyUuid, policyApiInputDTO);
            }
            final PolicyDTO.PolicyInfo policyInfo = policyMapper.policyApiInputDtoToProto(policyApiInputDTO);
            final PolicyDTO.PolicyEditRequest policyEditRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                    .setPolicyId(Long.parseLong(policyUuid))
                    .setNewPolicyInfo(policyInfo)
                    .build();
            PolicyEditResponse response = policyRpcService.editPolicy(policyEditRequest);

            final String details = String.format("Changed policy %s", policyInfo.getName());
            AuditLog.newEntry(AuditAction.CHANGE_POLICY,
                details, true)
                .targetName(policyInfo.getName())
                .audit();
            if (!response.hasPolicy()) {
                throw new OperationFailedException("Failed to fetch updated policy");
            }
            return policiesService.toPolicyApiDTO(response.getPolicy());
        } catch (RuntimeException e) {
            final String details = String.format("Changed policy with uuid: %s", policyUuid);
            AuditLog.newEntry(AuditAction.CHANGE_POLICY,
                details, false)
                .targetName(policyApiInputDTO.getPolicyName())
                .audit();
            logger.error("Fail to edit policy " + policyUuid + " with name "
                    + policyApiInputDTO.getPolicyName(), e);
            throw e;
        }
    }

    @Override
    // uuid is not used because there is only one Market in XL.
    public Boolean deletePolicy(String uuid, String policyUuid) {
        final PolicyDTO.PolicyDeleteRequest policyDeleteRequest = PolicyDTO.PolicyDeleteRequest.newBuilder()
                .setPolicyId(Long.parseLong(policyUuid))
                .build();

        try {
            final PolicyDeleteResponse response = policyRpcService.deletePolicy(policyDeleteRequest);
            final PolicyInfo policy = response.getPolicy().getPolicyInfo();
            if (mergePolicyHandler.isPolicyNeedsHiddenGroup(policy)) {
                // Remove a hidden group to support the merge policy for entities that are not groups.
                mergePolicyHandler.deleteHiddenGroup(policyUuid, policy);
            }

            final String details = String.format("Deleted policy %s", policy.getName());
            AuditLog.newEntry(AuditAction.DELETE_POLICY,
                details, true)
                .targetName(policy.getName())
                .audit();
            return true;
        } catch (RuntimeException e) {
            final String details = String.format("Failed to deleted policy with uuid: %s", policyUuid);
            AuditLog.newEntry(AuditAction.DELETE_POLICY,
                details, false)
                .targetName(policyUuid)
                .audit();
            logger.error("Failed to delete policy " + policyUuid, e);
            return false;
        }
    }

    @Override
    public List<DemandReservationApiDTO> getReservationsByMarketUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Get real-time Actions by Market.
     *
     * @param uuid              uuid of the Market
     * @param deleteScenario    delete scenario if market plan is being deleted
     * @return {@link ActionPaginationResponse}
     * @throws Exception
     */
    @Override
    public MarketApiDTO deleteMarketByUuid(String uuid, boolean deleteScenario) throws Exception {
        logger.debug("Deleting market with UUID: {}", uuid);
        // TODO (roman, June 16 2017) OM-20328: Plan deletion is currently synchronous in XL.
        // The delete plan call won't return until the plan actually got deleted. The API
        // treats it asynchronously, which is why it expects the DELETING -> DELETED status updates
        // over websocket. We should switch to doing it asynchronously as well, to give the
        // Plan Orchestrator more room to deal with the various parts of deleting a plan and
        // error-handling (e.g. if the Action Orchestrator is down and we can't erase the plan's
        // action plan).
        try {
            // We don't broadcast a DELETING notification, because that significantly complicates
            // error handling from the RPC call. If the deletePlan method returned an exception,
            // what's the state of the plan? It may be deleting (i.e. partially deleted) or
            // fully intact.
            final long marketId = Long.parseLong(uuid);
            final PlanInstance oldPlanInstance = getPlanInstance(marketId);
            if (oldPlanInstance.hasPlanProjectId()) {
                // If plan is part of a project, get the project first, we need to delete all
                // the plans under the project, and then the project.
                final long projectId = oldPlanInstance.getPlanProjectId();
                final PlanProject planProject = getPlanProject(projectId);
                final PlanProjectInfo projectInfo = planProject.getPlanProjectInfo();
                if (projectInfo.hasMainPlanId()) {
                    // Main plan scenario is deleted from UI.
                    deletePlanInstance(projectInfo.getMainPlanId(), projectId);
                    for (Long relatedPlanId : projectInfo.getRelatedPlanIdsList()) {
                        final PlanInstance relatedPlan = getPlanInstance(relatedPlanId);
                        // Any related plan scenarios need to be deleted here.
                        deletePlanInstance(relatedPlanId, projectId);
                        deleteScenario(relatedPlan.getScenario().getId(), relatedPlanId);
                    }
                }
                deletePlanProject(projectId);
            } else {
                // Delete just the plan, not in a project, like OCP.
                deletePlanInstance(oldPlanInstance.getPlanId(), null);
            }

            if (deleteScenario && oldPlanInstance.getScenario().hasId()) {
                //Deleting plan scenario
                Long scenarioId = oldPlanInstance.getScenario().getId();
                final Iterator<PlanInstance> plans = planRpcService.getAllPlans(
                        GetPlansOptions.newBuilder()
                                .addScenarioId(scenarioId)
                                .build());
                if (!plans.hasNext()) {
                    //If the scenario is not connected to any other plans, safe to delete
                    deleteScenario(scenarioId, oldPlanInstance.getPlanId());
                }
            }

            uiNotificationChannel.broadcastMarketNotification(MarketNotification.newBuilder()
                    .setMarketId(uuid)
                    .setStatusNotification(StatusNotification.newBuilder()
                            .setStatus(Status.DELETED))
                    .build());
            return marketMapper.dtoFromPlanInstance(oldPlanInstance);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
    }

    @Override
    public MarketApiDTO applyAndRunScenario(@Nonnull String marketUuid,
                                            @Nonnull Long scenarioId,
                                            @Nullable Boolean ignoreConstraints,
                                            @Nullable final String planMarketName,
                                            final boolean savePlan) throws Exception {
        // TODO (roman, May 8 2019): Right now we ignore "savePlan" because in XL we always save
        // plan results. Consider whether or not we want to make saving plans optional in the
        // future.
        return applyAndRunScenario(marketUuid, scenarioId, ignoreConstraints, planMarketName);
    }

    @Override
    public MarketApiDTO applyAndRunScenario(@Nonnull String marketUuid,
                                            @Nonnull Long scenarioId,
                                            @Nullable Boolean ignoreConstraints,
                                            @Nullable String planMarketName) throws Exception {
        logger.info("Running scenario. Market UUID: {}, Scenario ID: {}, "
                        + "Ignore Constraints: {}, Plan Market Name: {}",
                marketUuid, scenarioId, ignoreConstraints, planMarketName);
        // this feature requires access to the "planner" feature in the license.
        licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);

        // note that, for XL, the "marketUuid" in the request is interpreted as the Plan Instance ID
        final ApiId sourceMarketId;
        try {
            sourceMarketId = uuidMapper.fromUuid(marketUuid);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid market id: " + marketUuid);
        }

        Scenario existingScenario = getScenario(scenarioId);
        // Scenario scope validation.
        if (!existingScenario.hasScenarioInfo() || !existingScenario.getScenarioInfo().hasScope()
            || existingScenario.getScenarioInfo().getScope().getScopeEntriesCount() == 0) {
            throw new IllegalArgumentException("Scenario with UUID: " + scenarioId
                    + " does not have a defined scope.");
        }
        for (PlanScopeEntry scope : existingScenario.getScenarioInfo().getScope().getScopeEntriesList()) {
            ApiId scopeId = uuidMapper.fromOid(scope.getScopeObjectOid());
            if (scopeId.isGroup()) {
                if (scopeId.getCachedGroupInfo().get().getEntityIds().isEmpty()) {
                    throw new IllegalArgumentException("Scenario with UUID: " + scenarioId
                        + " has invalid empty group scope oid: " + scope.getScopeObjectOid());
                } else if (INVALID_SCOPE_ENTITY_TYPES.stream()
                    .anyMatch(scopeId.getCachedGroupInfo().get().getEntityTypes()::contains)) {
                    throw new IllegalArgumentException("Scenario with UUID: " + scenarioId
                        + " has invalid group scope entity types oid: " + scope.getScopeObjectOid());
                }
            } else if (scopeId.isEntity()) {
                if (INVALID_SCOPE_ENTITY_TYPES.contains(scopeId.getCachedEntityInfo().get().getEntityType())) {
                    throw new IllegalArgumentException("Scenario with UUID: " + scenarioId
                        + " has invalid entity type scope oid: " + scope.getScopeObjectOid()
                        + " - " + scopeId.getCachedEntityInfo().get().getEntityType());
                }
            } else if (scopeId.isRealtimeMarket()) {
                throw new IllegalArgumentException("Cannot use the whole market as a scope. "
                    + "Please specify a group or an entity as a scope. "
                    + " for Scenario with UUID: " + scenarioId);
            } else {
                throw new IllegalArgumentException("Scenario with UUID: " + scenarioId
                    + " has invalid scope oid: " + scope.getScopeObjectOid());
            }
        }

        if (Boolean.TRUE.equals(ignoreConstraints)) {
            existingScenario = updateIgnoreConstraints(existingScenario);
        }

        if (planProjectBuilder.isPlanProjectRequired(sourceMarketId, existingScenario)) {
            final PlanProject planProject;
            try {
                planProject = planProjectBuilder.createPlanProject(existingScenario);
                // plan project validation
                if (planProject == null || planProject.getStatus() == PlanProjectStatus.FAILED) {
                    throw new OperationFailedException(String.format(
                    "Plan creation was not successful for market %s with scenario %s. %s ",
                        marketUuid, scenarioId, planProject == null
                            ? "PlanProject is null" : "Status: " + planProject.getStatus()));
                }
            } catch (StatusRuntimeException e) {
                throw new OperationFailedException(String.format(
                    "Plan creation was not successful for market %s with scenario %s. %s ",
                    marketUuid, scenarioId, e.getMessage()));
            }
            final PlanInstance mainPlan = getMainPlanInstance(planProject);
            logger.info("{} Initiated plan project main plan '{}' run by user {}.",
                    TopologyDTOUtil.formatPlanLogPrefix(mainPlan.getPlanId(),
                            planProject.getPlanProjectId()),
                    (mainPlan.hasName() ? mainPlan.getName() : ""),
                    (mainPlan.hasCreatedByUser() ? mainPlan.getCreatedByUser() : ""));

            planProjectRpcService.runPlanProject(RunPlanProjectRequest.newBuilder()
                    .setId(planProject.getPlanProjectId()).build());

            return getMarketApiDto(planProject, getMainPlanInstance(planProject), existingScenario);
        }
        // For regular plans (like OCP) that are not part of a project.
        final PlanInstance planInstance;
        try {
            if (sourceMarketId.isRealtimeMarket()) {
                // for realtime market create a new plan; topologyId is not set, defaulting to "0"
                planInstance = planRpcService.createPlan(CreatePlanRequest.newBuilder()
                        .setScenarioId(scenarioId)
                        .build());
            } else {
                // plan market: create new plan where source topology is the prev projected topology
                PlanDTO.PlanScenario planScenario = PlanDTO.PlanScenario.newBuilder()
                        .setPlanId(sourceMarketId.oid())
                        .setScenarioId(scenarioId)
                        .build();
                planInstance = planRpcService.createPlanOverPlan(planScenario);
            }
        } catch (StatusRuntimeException e) {
            throw new OperationFailedException(String.format(
                "Plan creation was not successful for market %s with scenario %s. %s ",
                marketUuid, scenarioId, e.getMessage()));
        }
        // plan instance validation
        if (planInstance == null || planInstance.getStatus() == PlanStatus.FAILED) {
            throw new OperationFailedException(String.format(
            "Plan creation was not successful for market %s with scenario %s. %s ",
                marketUuid, scenarioId, planInstance == null
                    ? "PlanInstance is null" : "Status: " + planInstance.getStatusMessage()));
        }
        final PlanInstance updatedInstance = planRpcService.runPlan(PlanId.newBuilder()
                .setPlanId(planInstance.getPlanId())
                .build());
        logger.info("{} Initiated plan '{}' run by user {}.",
                TopologyDTOUtil.formatPlanLogPrefix(updatedInstance.getPlanId()),
                (updatedInstance.hasName() ? updatedInstance.getName() : ""),
                (updatedInstance.hasCreatedByUser() ? updatedInstance.getCreatedByUser() : ""));

        return marketMapper.dtoFromPlanInstance(updatedInstance);
    }

    /**
     * Checks if the ignoreConstrains change is already there in the scenario. If not, adds it
     * and updates the Scenario in DB.
     *
     * @param scenario User scenario to check for ignoreConstraints.
     * @return Updated scenario if ignoreConstraints was added, same as input otherwise.
     */
    @Nonnull
    private Scenario updateIgnoreConstraints(@Nonnull final Scenario scenario) {
        ScenarioInfo.Builder scenarioInfo = scenario.getScenarioInfo().toBuilder();
        final List<IgnoreConstraint> ignoredConstraints =
                scenarioInfo.getChangesList().stream()
                        .flatMap(change -> change.getPlanChanges()
                                .getIgnoreConstraintsList().stream())
                        .collect(Collectors.toList());
        if (!ignoredConstraints.isEmpty()) {
            // IgnoreConstrains has already been set as a ScenarioChange in the Scenario, so
            // no need to update. Not sure if this should be a warning.
            logger.warn("Ignoring \"ignore constraints\" option because the scenario {} already"
                    + " has ignored constraints: {}", scenario.getId(), ignoredConstraints);
            return scenario;
        }
        // NOTE: Currently we only remove constraints for VM entities.
        scenarioInfo.addChanges(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder()
                        .addIgnoreConstraints(IgnoreConstraint.newBuilder()
                                .setGlobalIgnoreEntityType(
                                        GlobalIgnoreEntityType.newBuilder()
                                                .setEntityType(EntityType.VIRTUAL_MACHINE))
                                .build())));
        UpdateScenarioResponse updateScenarioResponse =
                scenarioServiceClient.updateScenario(UpdateScenarioRequest.newBuilder()
                        .setScenarioId(scenario.getId())
                        .setNewInfo(scenarioInfo.build())
                        .build());
        Scenario updateScenario = updateScenarioResponse.getScenario();
        logger.debug("Updated scenario: {}", updateScenario);
        return updateScenario;
    }

    /**
     * Renames a plan.
     *
     * @param marketUuid    interpreted as planUuid
     * @param displayName   new name for plan
     * @return
     * @throws Exception    if no plan matches marketUuid or user does not have plan access
     */
    @Override
    public MarketApiDTO renameMarket(String marketUuid, String displayName) throws Exception {

        //marketUuid is interpreted as planUuid
        final ApiId planInstanceId = uuidMapper.fromUuid(marketUuid);
        final Optional<CachedPlanInfo> planInfoOptional = planInstanceId.getCachedPlanInfo();

        if (!planInfoOptional.isPresent()) {
            throw new InvalidOperationException("Invalid market id: " + marketUuid);
        }

        final PlanInstance planInstance = planInfoOptional.get().getPlanInstance();

        // verify the user can access the plan
        if (!PlanUtils.canCurrentUserAccessPlan(planInstance)) {
            throw new UserAccessException("User does not have access to modify this plan.");
        }

        final UpdatePlanRequest updatePlanRequest = UpdatePlanRequest.newBuilder()
                .setPlanId(planInstance.getPlanId())
                .setName(displayName)
                .build();
        return getMarketApiDto(planRpcService.updatePlan(updatePlanRequest));
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByMarketUuid(final String uuid, final String encodedQuery) throws Exception {
        return statsService.getStatsByEntityUuid(uuid, encodedQuery);
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByMarketQuery(final String uuid,
                                                          final StatPeriodApiInputDTO inputDto)
            throws Exception {
        return statsService.getStatsByEntityQuery(uuid, inputDto);
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(String uuid, ActionApiInputDTO actionApiInputDTO) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        planAccessEnforcement(apiId);
        try {
            final Map<ApiId, List<StatSnapshotApiDTO>> retStats =
                    actionStatsQueryExecutor.retrieveActionStats(
                            ImmutableActionStatsQuery.builder()
                                    .scopes(Collections.singleton(apiId))
                                    .actionInput(actionApiInputDTO)
                                    .build());
            return retStats.getOrDefault(apiId, Collections.emptyList());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                return Collections.emptyList();
            } else {
                throw e;
            }
        }
    }

    /**
     * Plan access enforcement on: </p>
     * 1. The instance has planner feature. </p>
     * 2. Current user is allowed to access this plan. </p>
     *
     * @param apiId API id
     * @throws UnknownObjectException if cannot find the plan from the plan id.
     */
    private void planAccessEnforcement(@Nonnull final ApiId apiId) throws UnknownObjectException {
        if (apiId != null && apiId.isPlan()) {
            licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);
            final PlanInstance planInstance = getPlanInstance(apiId.oid());
            planAccessAuthorizationEnforcement(planInstance);
        }
    }

    /**
     * Enforce current user is allowed to access this plan.
     *
     * @param planInstance plan instance.
     * @throws UserAccessException if the user is not allowed to access this plan instance.
     */
    private void planAccessAuthorizationEnforcement(@Nonnull final PlanInstance planInstance) {
        if (!PlanUtils.canCurrentUserAccessPlan(planInstance)) {
            throw new UserAccessException(USER_DOES_NOT_HAVE_ACCESS_TO_PLAN);
        }
    }

    @Override
    public List<StatSnapshotApiDTO> getActionStatsResourceImpactByUuid(String uuid, ActionResourceImpactStatApiInputDTO inputDTO) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        if (apiId.isPlan()) {
            licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);
        }
        try {
            if (CollectionUtils.isEmpty(inputDTO.getActionResourceImpactStatList())) {
                throw new InvalidOperationException("Missing list of ActionResourceImpactStat");
            }

            final Map<ApiId, List<StatSnapshotApiDTO>> retStats =
                    actionStatsQueryExecutor.retrieveActionStats(
                            ImmutableActionStatsQuery.builder()
                                    .scopes(Collections.singleton(apiId))
                                    .actionInput(ActionInputUtil.toActionApiInputDTO(inputDTO))
                                    .actionResourceImpactIdentifierSet(ActionInputUtil.toActionResourceImpactIdentifierSet(inputDTO))
                                    .build());
            return retStats.getOrDefault(apiId, Collections.emptyList());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                return Collections.emptyList();
            } else {
                throw e;
            }
        }
    }

    @Override
    public List<StatSnapshotApiDTO> getNotificationCountStatsByUuid(final String s,
                                                                    final ActionApiInputDTO actionApiInputDTO)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public EntityStatsPaginationResponse getStatsByEntitiesInMarketQuery(final String marketUuid,
                                                                         final StatScopesApiInputDTO statScopesApiInputDTO,
                                                                         final EntityStatsPaginationRequest paginationRequest)
            throws Exception {
        // TODO (roman, Mar 7 2018) OM-32684: The UI will migrate to using this endpoint for per-entity
        // stats in both realtime and plans, so we will need to expand this method to
        // support both.
        if (UuidMapper.isRealtimeMarket(marketUuid)) {
            return statsService.getStatsByUuidsQuery(statScopesApiInputDTO, paginationRequest);
        }

        // make sure the planner feature is available
        licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);

        final long planId = Long.parseLong(marketUuid);
        final PlanInstance planInstance = getPlanInstance(planId);
        // verify the user can access the plan
        planAccessAuthorizationEnforcement(planInstance);

        return planEntityStatsFetcher
            .getPlanEntityStats(planInstance, statScopesApiInputDTO, paginationRequest);
    }

    /**
     * {@inheritDoc}
     * This api has both a group id as input and also a StatScopesApiInputDTO.  The StatScopesApiInputDTO may
     * include a scope.  If the scope is specified, the logic will use the intersection of the group members and scope ids.
     * It is typically expected that the users specify only a group id.
     */
    @Override
    public EntityStatsPaginationResponse getStatsByEntitiesInGroupInMarketQuery(final String marketUuid,
                                                                                final String groupUuid,
                                                                                final StatScopesApiInputDTO statScopesApiInputDTO,
                                                                                final EntityStatsPaginationRequest paginationRequest)
            throws Exception {
        // Look up the group members and then check how the members overlap with an input scope, if specified
        // If the plan was scoped to a different group
        // we won't return anything (since the requested entities won't be found in the repository).
        ApiId apiId = uuidMapper.fromUuid(marketUuid);

        // if this is a plan, make sure the planner feature is available
        if (apiId.isPlan()) {
            licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);
        }
        // if this is for a plan market, we will not check user scope.
        boolean enforceUserScope = !apiId.isPlan();

        final GetMembersRequest getGroupMembersReq = GetMembersRequest.newBuilder()
                .addId(Long.parseLong(groupUuid))
                .setEnforceUserScope(enforceUserScope)
                .setExpectPresent(false)
                .build();

        final Iterator<GetMembersResponse> groupMembersResp =
                groupRpcService.getMembers(getGroupMembersReq);
        final List<String> membersUuids;
        if (groupMembersResp.hasNext()) {
            membersUuids = groupMembersResp.next().getMemberIdList().stream()
                    .map(id -> Long.toString(id))
                    .collect(Collectors.toList());
        } else {
            membersUuids = Collections.emptyList();
        }
        // If an input scope was also specified, use the intersection.
        if (!CollectionUtils.isEmpty(statScopesApiInputDTO.getScopes())) {
            membersUuids.retainAll(statScopesApiInputDTO.getScopes());
            if (membersUuids.isEmpty()) {
                throw new IllegalArgumentException("Group scope: " + groupUuid + " and input scope conflict: " + statScopesApiInputDTO.getScopes());
            }
        }
        statScopesApiInputDTO.setScopes(membersUuids);
        return getStatsByEntitiesInMarketQuery(marketUuid, statScopesApiInputDTO, paginationRequest);
    }

    @Override
    public List<ServiceEntityApiDTO> getUnplacedEntitiesByMarketUuid(String uuid) throws Exception {
        // requires "planner" feature to access
        licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);

        // get the topology id for the plan
        OptionalPlanInstance planResponse = planRpcService.getPlan(PlanId.newBuilder()
                .setPlanId(Long.valueOf(uuid))
                .build());

        if (!planResponse.hasPlanInstance()) {
            throw new UnknownObjectException(uuid);
        }

        // Get the list of unplaced entities from the repository for this plan
        final PlanInstance plan = getPlanInstance(Long.parseLong(uuid));
        long projectedTopologyId = plan.getProjectedTopologyId();
        if (projectedTopologyId == 0) {
            return Collections.emptyList();
        }
        planAccessAuthorizationEnforcement(plan);

        // If this is a migration plan, get the list of unplaced entities from the action
        // list instead.
        Set<Long> placedOids = new HashSet<>();
        boolean isMigrationPlan = plan.getProjectType() == PlanProjectType.CLOUD_MIGRATION;
        if (isMigrationPlan) {
            logger.info("{} Finding unplaced entities for cloud migration plan.",
                    TopologyDTOUtil.formatPlanLogPrefix(plan.getPlanId()));
            AtomicReference<String> cursor = new AtomicReference<>("0");
            do {
                FilteredActionRequest filteredActionRequest =
                        FilteredActionRequest.newBuilder()
                                .setTopologyContextId(plan.getPlanId())
                                .setPaginationParams(PaginationParameters.newBuilder()
                                        .setLimit(TopologyDTOUtil.CLOUD_MIGRATION_ACTION_QUERY_CURSOR_LIMIT)
                                        .setCursor(cursor.getAndSet("")))
                                .addActionQuery(ActionQuery.newBuilder().setQueryFilter(
                                    ActionQueryFilter.newBuilder()
                                        .setVisible(true)
                                        .addTypes(ActionType.MOVE)))
                                .build();
                actionOrchestratorRpcService.getAllActions(filteredActionRequest)
                        .forEachRemaining(rsp -> {
                            // These are the actions related to the queried entities.  Only include entities
                            // with an associated action.
                            if (rsp.hasActionChunk()) {
                                for (ActionOrchestratorAction action : rsp.getActionChunk().getActionsList()) {
                                    placedOids.add(action.getActionSpec()
                                            .getRecommendation()
                                            .getInfo()
                                            .getMove()
                                            .getTarget()
                                            .getId());
                                }
                            } else if (rsp.hasPaginationResponse()) {
                                cursor.set(rsp.getPaginationResponse().getNextCursor());
                            }
                        });
            } while (!StringUtils.isEmpty(cursor.get()));
            logger.info("{} Found {} placed action oids.",
                    TopologyDTOUtil.formatPlanLogPrefix(plan.getPlanId()), placedOids.size());
        }

        final Iterable<RetrieveTopologyResponse> response = () ->
                repositoryRpcService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                        .setTopologyId(projectedTopologyId)
                        .setEntityFilter(TopologyEntityFilter.newBuilder()
                                // For migration plans, we want all entities, else just the unplaced.
                                // We filter out the placed entities below.
                                .setUnplacedOnly(!isMigrationPlan))
                        .build());
        Map<Long, TopologyEntityDTO> entities = StreamSupport.stream(response.spliterator(), false)
                .flatMap(rtResponse -> rtResponse.getEntitiesList().stream())
                .map(PartialEntity::getFullEntity)
                // include the entities in ENTITY_TYPES_SUPPORTING_UNPLACED set
                .filter(entity -> ENTITY_TYPES_SUPPORTING_UNPLACED.contains(entity.getEntityType()))
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        // We have a map of all requested entities in the plan. If this is a migration plan,
        // remove the entities that we've seen in its action list.  The remaining entities are
        // unplaced.
        int originalEntityCount = entities.size();
        for (Long oid : placedOids) {
            entities.remove(oid);
        }
        // convert to ServiceEntityApiDTOs, filling in data about unplaced entities
        List<TopologyAndApiEntity> unplacedApiDTOs =
                convertServiceEntitiesForPlan(plan, entities.values());

        logger.debug("{} Found {} unplaced entities. Total {} original entities.",
                TopologyDTOUtil.formatPlanLogPrefix(plan.getPlanId()),
                unplacedApiDTOs.size(), originalEntityCount);
        return unplacedApiDTOs.stream()
            .map(e -> e.apiEntity)
            .collect(Collectors.toList());
    }

    /**
     * Get details for an action by the market.
     *
     * @param marketUuid uuid of the market.
     * @param actionuuid uuid of the action.
     * @return details about the action in a market.
     * @throws Exception exception.
     */
    @Override
    public ActionDetailsApiDTO getActionsDetailsByUuid(String marketUuid, String actionuuid) throws Exception {
        // if the request is for a plan-related action, make sure the feature is available.
        ApiId apiId = uuidMapper.fromUuid(marketUuid);
        if (apiId.isPlan()) {
            licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);
        }
        ActionOrchestratorAction action = actionOrchestratorRpcService
                .getAction(actionRequest(marketUuid, actionuuid));
        if (action.hasActionSpec()) {
            // create action details dto based on action api dto which contains "explanation" with coverage information.
            ActionDetailsApiDTO actionDetailsApiDTO = actionSpecMapper.createActionDetailsApiDTO(
                    action,
                    Strings.isBlank(marketUuid) ? null : Long.valueOf(marketUuid));
            if (actionDetailsApiDTO == null) {
                return new NoDetailsApiDTO();
            }
            return actionDetailsApiDTO;
        }
        return new NoDetailsApiDTO();
    }

    /**
     * Create a list of ServiceEntityApiDTO objects from a collection of TopologyEntityDTOs.
     *
     * @param plan       plan instance object
     * @param entityDTOs The collection of TopologyEntityDTOs to be converted into ServiceEntityApiDTO
     * @return A list of ServiceEntityApiDTO objects representing with information on where it is placed/unplaced.
     *         The ServiceEntityApiDTO objects are paired with their original TopologyEntityDTO objects.
     */
    List<TopologyAndApiEntity> convertServiceEntitiesForPlan(PlanInstance plan,
                                                             Collection<TopologyEntityDTO> entityDTOs) {
        // if no unplaced entities, return an empty collection
        if (entityDTOs.size() == 0) {
            return Collections.emptyList();
        }

        // get the set of unique supplier ids. It's not guaranteed that there are any suppliers for
        // any of these commodities, so we need to be prepared for the possibility of an empty set.
        Set<Long> providerOids = entityDTOs.stream()
                .flatMap(entity -> entity.getCommoditiesBoughtFromProvidersList().stream())
                .filter(CommoditiesBoughtFromProvider::hasProviderId) // a provider is not guaranteed
                .filter(comm -> comm.getProviderId() > 0)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());

        // Add in providers that aren't used but were the closest match in a failed placement
        providerOids.addAll(entityDTOs.stream()
            .flatMap(entity -> entity.getUnplacedReasonList().stream())
            .filter(UnplacementReason::hasClosestSeller)
            .map(UnplacementReason::getClosestSeller)
            .collect(Collectors.toSet()));

        // An entity can be unplaced because it depends on another entity, for example a VM
        // due to one of its volumes being unplaced in a migration. In this case the
        // FailedResources in the UnplacementReason are those of the "resource owner" (eg volume),
        // not the VM. Add in these resource owners.
        providerOids.addAll(entityDTOs.stream()
            .flatMap(entity -> entity.getUnplacedReasonList().stream())
            .filter(UnplacementReason::hasResourceOwnerOid)
            .map(UnplacementReason::getResourceOwnerOid)
            .collect(Collectors.toSet()));

        // Eg, if a VM was unplaced because of one of its volumes count be placed, add the volume.

        final Map<Long, TopologyEntityDTO> providers = new HashMap<>(providerOids.size());
        entityDTOs.forEach(entity -> {
            if (providerOids.contains(entity.getOid())) {
                providers.put(entity.getOid(), entity);
            }
        });

        // If not all providers are present in the entityDTOs list
        if (providers.size() < providerOids.size()) {
            final Set<Long> missingProviders = Sets.difference(providerOids, providers.keySet());
            // Retrieve missing providers and put them in the providers map
            repositoryApi.entitiesRequest(missingProviders)
                .contextId(plan.getPlanId())
                .projectedTopology()
                .getFullEntities()
                .forEach(dto -> providers.put(dto.getOid(), dto));
        }

        // If there are still providers missing, look for them in the source topology
        if (providers.size() < providerOids.size()) {
            final Set<Long> missingProviders = Sets.difference(providerOids, providers.keySet());
            repositoryApi.entitiesRequest(missingProviders)
                .contextId(plan.getPlanId())
                .getFullEntities()
                .forEach(dto -> providers.put(dto.getOid(), dto));
        }

        // convert to ServiceEntityApiDTOs, filling in the blanks of the placed on / not placed on fields
        return createServiceEntityApiDTOs(entityDTOs, providers);
    }

    /**
     * Create a list of ServiceEntityApiDTO objects from a collection of unplaced TopologyEntityDTOs.
     *
     * @param unplacedDTOs The collection of unplaced TopologyEntityDTOs to use as the source
     * @param providers    A map of providers (provider id -> provider) that is used for looking up
     *                     provider names.
     * @return A list of ServiceEntityApiDTO objects representing the unplaced entities.
     */
    private List<TopologyAndApiEntity> createServiceEntityApiDTOs(Collection<TopologyEntityDTO> unplacedDTOs,
                                                                  Map<Long, TopologyEntityDTO> providers) {
        return unplacedDTOs.stream()
                // TODO: (OM-31842) After Market side fix the bug of unplaced cloned
                // entity, we can remove this filter logic.
                .filter(entity -> entity.getCommoditiesBoughtFromProvidersList().stream()
                        .allMatch(CommoditiesBoughtFromProvider::hasProviderEntityType))
                .map(entity -> new TopologyAndApiEntity(entity, createServiceEntityApiDTO(entity, providers)))
                .collect(Collectors.toList());
    }

    /**
     * Create an unplaced ServiceEntityApiDTO from an unplaced TopologyEntityDTO.
     *
     * @param entity    the (unplaced) TopologyEntityDTO to use as the basis for the new
     *                  ServiceEntityApiDTO object.
     * @param providers the set of provider entities, used for looking up provider names.
     * @return the ServiceEntityApiDTO that represents the unplaced TopologyEntityDTO
     */
    private ServiceEntityApiDTO createServiceEntityApiDTO(TopologyEntityDTO entity,
                                                          Map<Long, TopologyEntityDTO> providers) {
        ServiceEntityApiDTO seEntity = serviceEntityMapper.toServiceEntityApiDTO(entity);
        List<BaseApiDTO> placedProviders = new ArrayList<>();
        StringJoiner placedOnJoiner = new StringJoiner(",");
        StringJoiner notPlacedOnJoiner = new StringJoiner(",");
        // for all of the commodities bought, build a string description of which are placed
        // and which are not for the UI to display.
        for (CommoditiesBoughtFromProvider comm : entity.getCommoditiesBoughtFromProvidersList()) {
            if (comm.hasProviderId() && comm.getProviderId() > 0) {
                // add to "placed on" list
                // if no commodity provider DTO was retrieved for this provider id, fall back to
                // displaying the id instead of the name
                TopologyEntityDTO provider = providers.get(comm.getProviderId());
                if (provider != null) {
                    placedProviders.add(serviceEntityMapper.toBaseServiceEntityApiDTO(provider));
                } else {
                    logger.warn("Entity with oid {} found in neither projected nor source topology",
                        comm.getProviderId());
                    BaseApiDTO providerDto = new BaseApiDTO();
                    providerDto.setUuid(String.valueOf(comm.getProviderId()));
                    placedProviders.add(providerDto);
                }
                placedOnJoiner.add((provider != null)
                        ? provider.getDisplayName()
                        : String.valueOf(comm.getProviderId()));
            } else {
                // 'not placed on' list contains the list of provider entity types that were not found
                // during analysis. These entity types could have provided the commodities needed by
                // this unplaced entity
                notPlacedOnJoiner.add(EntityType.forNumber(comm.getProviderEntityType()).name());
            }
        }

        UnplacementDetailsApiDTO unplacedData = new UnplacementDetailsApiDTO();
        seEntity.setUnplacementDetails(unplacedData);

        unplacedData.setPlacedOn(placedProviders);
        unplacedData.setReasons(entity.getUnplacedReasonList().stream()
            .map(reason -> convertUnplacedReason(reason, providers, entity))
            .collect(Collectors.toList()));

        // Deprecated fields, for backward compatibility.
        seEntity.setUnplacedExplanation(entity.getUnplacedReasonList().stream()
            .map(reason -> explainUnplacementReason(reason, providers))
            .collect(Collectors.joining(" ")));

        seEntity.setPlacedOn(placedOnJoiner.toString());
        seEntity.setNotPlacedOn(notPlacedOnJoiner.toString());

        return seEntity;
    }

    @Nonnull
    private String explainUnplacementReason(@Nonnull UnplacementReason reason,
                                            @Nonnull Map<Long, TopologyEntityDTO> providers) {
        StringBuilder sb = new StringBuilder();

        if (reason.hasProviderType()) {
            sb.append("When looking for supplier of type ")
                .append(ApiEntityType.fromType(reason.getProviderType()).apiStr());

            if (reason.hasResourceOwnerOid()) {
                TopologyEntityDTO resourceOwner = providers.get(reason.getResourceOwnerOid());
                if (resourceOwner != null) {
                    sb.append(" for ")
                        .append(ApiEntityType.fromType(resourceOwner.getEntityType()).apiStr())
                        .append(" ")
                        .append(resourceOwner.getDisplayName());
                }
            }

            sb.append(": ");
        }

        List<String> parts = new ArrayList<>();

        for (FailedResources resource : reason.getFailedResourcesList()) {
            StringBuilder resourceStringBuilder = new StringBuilder();
            resourceStringBuilder.append("needed resource ")
                .append(UICommodityType.fromType(resource.getCommType()).apiStr());

            if (resource.getCommType().hasKey()) {
                resourceStringBuilder.append(":").append(resource.getCommType().getKey());
            }

            resourceStringBuilder.append(" with a requested amount of ")
                .append(resource.getRequestedAmount());

            if (resource.hasMaxAvailable()) {
                resourceStringBuilder.append(" but the max available was ")
                    .append(resource.getMaxAvailable());
            }

            parts.add(resourceStringBuilder.toString());
        }

        if (reason.hasPlacementProblem()) {
            switch (reason.getPlacementProblem()) {
                case COSTS_NOT_FOUND:
                    parts.add("costs were not found");
                    break;

                case NOT_CONTROLLABLE:
                    parts.add("entity is not controllable");
                    break;
            }
        }

        sb.append(parts.stream().collect(Collectors.joining(", ")));

        if (reason.hasClosestSeller()) {
            TopologyEntityDTO closestSeller = providers.get(reason.getClosestSeller());
            if (closestSeller != null) {
                sb.append("; the closest match was ").append(closestSeller.getDisplayName());
            }
        }

        sb.append(".");

        return sb.toString();
    }

    @Nonnull
    private UnplacementReasonApiDTO convertUnplacedReason(@Nonnull UnplacementReason reasonDTO,
                                                          @Nonnull Map<Long, TopologyEntityDTO> providers,
                                                          @Nonnull TopologyEntityDTO entity) {
        UnplacementReasonApiDTO reasonApiDTO = new UnplacementReasonApiDTO();

        reasonApiDTO.setFailedResources(reasonDTO.getFailedResourcesList().stream()
            .map(f -> convertFailedResource(f, entity))
            .collect(Collectors.toList()));

        if (reasonDTO.hasProviderType()) {
            reasonApiDTO.setProviderType(com.vmturbo.api.enums.EntityType.fromString(
                ApiEntityType.fromType(reasonDTO.getProviderType()).apiStr()));
        }

        if (reasonDTO.hasClosestSeller()) {
            TopologyEntityDTO closestSeller = providers.get(reasonDTO.getClosestSeller());
            if (closestSeller != null) {
                reasonApiDTO.setClosestSeller(
                    ServiceEntityMapper.toBaseServiceEntityApiDTO(closestSeller));
            }
        }

        if (reasonDTO.hasResourceOwnerOid()) {
            TopologyEntityDTO resourceOwner = providers.get(reasonDTO.getResourceOwnerOid());
            if (resourceOwner != null) {
                reasonApiDTO.setResourceOwner(
                    ServiceEntityMapper.toBaseServiceEntityApiDTO(resourceOwner));
            }
        }

        if (reasonDTO.hasPlacementProblem()) {
            reasonApiDTO.setPlacementProblem(PlacementProblem.valueOf(
                reasonDTO.getPlacementProblem().name()));
        } else {
            reasonApiDTO.setPlacementProblem(PlacementProblem.UNSATISFIED_COMMODITIES);
        }

        return reasonApiDTO;
    }

    @Nonnull
    private FailedResourceApiDTO convertFailedResource(@Nonnull FailedResources failedResource,
                                                       @Nonnull TopologyEntityDTO entity) {
        FailedResourceApiDTO resourceApiDTO = new FailedResourceApiDTO();

        BaseCommodityApiDTO commodity = new BaseCommodityApiDTO();
        resourceApiDTO.setCommodity(commodity);

        CommonDTO.CommodityDTO.CommodityType sdkType =
            UICommodityType.fromType(failedResource.getCommType()).sdkType();

        commodity.setType(com.vmturbo.api.enums.CommodityType.valueOf(sdkType.name()));

        String units = null;
        try {
            units = CommodityTypeMapping.getUnitForEntityCommodityType(entity.getEntityType(), sdkType.getNumber());
        } catch (IllegalArgumentException ex) {
            logger.warn("Unable to find units for commodity type {}", sdkType.name());
        }
        if (StringUtils.isNotBlank(units)) {
            commodity.setUnits(units);
        }

        if (failedResource.getCommType().hasKey()) {
            commodity.setKey(failedResource.getCommType().getKey());
        }

        // We pick the used amount from commodity sold for vcpu related commodities
        // of pods (and only for pods) to ensure that the values are in millicores.
        resourceApiDTO.setRequestedAmount(requestedAmountFromCommoditySold(failedResource, entity));

        if (failedResource.hasMaxAvailable()) {
            // To keep the values consistent in millicores, we update the maxAvailable on pods
            // VCPU entities too.
            resourceApiDTO.setMaxAvailable(maxAvailableToRawUnits(failedResource, entity));
        }

        return resourceApiDTO;
    }

    /**
     * Checks if we should update the failed resource. Failed resource commodity values are
     * updated only for vcpu related commodities (vcpu and vcpu_request) for pods.
     *
     * @param failedResource {@link FailedResources} the resource with failed commodity
     *                                              and its values.
     * @param entity  {@link TopologyEntityDTO} the actual entity.
     * @return boolean indicating if resource needs update.
     */
    private boolean shouldUpdateFailedResource(@Nonnull FailedResources failedResource,
                                                @Nonnull TopologyEntityDTO entity) {
        return entity.getEntityType() == EntityType.CONTAINER_POD_VALUE
                && (failedResource.getCommType().getType() == CommodityDTO.CommodityType.VCPU_VALUE
                || failedResource.getCommType().getType() == CommodityDTO.CommodityType.VCPU_REQUEST_VALUE);
    }

    /**
     * Set the requestedAmount to value read from the actual entity's sold commodity used value.
     * We do this only for vcpu related commodities (vcpu and vcpu_request) for pods.
     *
     * @param failedResource {@link FailedResources} the resource with failed commodity
     *                                              and its values.
     * @param entity  {@link TopologyEntityDTO} the actual entity.
     * @return new value if found.
     */
    private double requestedAmountFromCommoditySold(@Nonnull FailedResources failedResource,
                                               @Nonnull TopologyEntityDTO entity) {
        if (shouldUpdateFailedResource(failedResource, entity)) {
            for (CommoditySoldDTO commoditySoldDTO : entity.getCommoditySoldListList()) {
                if (commoditySoldDTO.getCommodityType().getType() == failedResource.getCommType().getType()) {
                    return commoditySoldDTO.getUsed();
                }
            }
        }
        return failedResource.getRequestedAmount();
    }

    /**
     * Convert the maxAvailable amount to raw unit values (millicore).
     * We convert only if the entity is pod and the commodities are vcpu related
     * commodities (vcpu and vcpu_request).
     *
     * @param failedResource {@link FailedResources} the resource with failed commodity
     *                                              and its values.
     * @param entity  {@link TopologyEntityDTO} the actual entity.
     * @return converted value.
     */
    private double maxAvailableToRawUnits(@Nonnull FailedResources failedResource,
                                          @Nonnull TopologyEntityDTO entity) {
        if (shouldUpdateFailedResource(failedResource, entity)) {
            return convertToMilliCores(failedResource.getMaxAvailable(), failedResource.getCommType(), entity);
        }

        return failedResource.getMaxAvailable();
    }

    /**
     * Convert the market set normalized Mhz value to millicores (for a Pod).
     *
     * @param amount the amount to convert.
     * @param commodityType the type of the commodity.
     * @param entity the actual entity.
     * @return converted value.
     */
    private double convertToMilliCores(@Nonnull final double amount,
                                         final TopologyDTO.CommodityType commodityType,
                                         @Nonnull final TopologyEntityDTO entity) {
        double cpuFreqMhz = 1000.0;
        double scalingFactor = getScalingFactor(commodityType, entity);
        if (entity.hasTypeSpecificInfo() && entity.getTypeSpecificInfo().hasContainerPod() &&
                entity.getTypeSpecificInfo().getContainerPod().hasHostingNodeCpuFrequency()) {
            cpuFreqMhz = entity.getTypeSpecificInfo().getContainerPod().getHostingNodeCpuFrequency();
            logger.trace("Converted failed resource entity type {} of unplaced entity {} to millicores.",
                    commodityType, entity.getDisplayName());
        }

        return (amount / scalingFactor / (cpuFreqMhz / 1000));
    }

    /**
     * Get scaling factor from the entity for a specific commodity.
     *
     * @param commodityType the type of the commodity.
     * @param entity the actual entity.
     * @return scaling factor.
     */
    private double getScalingFactor(final TopologyDTO.CommodityType commodityType,
                                    @Nonnull final TopologyEntityDTO entity) {
        for (CommoditySoldDTO commoditySoldDTO : entity.getCommoditySoldListList()) {
            if (commoditySoldDTO.getCommodityType().getType() == commodityType.getType()) {
                return commoditySoldDTO.getScalingFactor();
            }
        }
        return 1.0;
    }

    /**
     * Creates an action request using a market uuid and an action uuid.
     *
     * @param marketUuid uuid of the market.
     * @param actionUuid uuid of the action.
     * @return the action request.
     */
    private SingleActionRequest actionRequest(String marketUuid, String actionUuid) throws UnknownObjectException {
        long actionId = actionSearchUtil.getActionInstanceId(actionUuid, marketUuid).orElseThrow(() -> {
            logger.error("Cannot lookup action as one with ID {} cannot be found.", actionUuid);
            return new UnknownObjectException("Cannot find action with ID " + actionUuid);
        });
        return SingleActionRequest.newBuilder()
                .setTopologyContextId(Long.valueOf(marketUuid))
                .setActionId(actionId)
                .build();
    }

    /**
     * Create a dto for the realtime market.
     * @return a dto with data including the oid and name of the realtime market
     */
    private MarketApiDTO createRealtimeMarketApiDTO() {
        // TODO (roman, Dec 21 2016): Not sure what the API
        // expects from the real-time market as far as state,
        // unplaced entities, state progress, and all that jazz.
        MarketApiDTO realtimeDto = new MarketApiDTO();
        realtimeDto.setUuid(new Long(realtimeTopologyContextId).toString());
        realtimeDto.setDisplayName("Market");
        realtimeDto.setClassName("Market");
        realtimeDto.setState("RUNNING");
        realtimeDto.setUnplacedEntities(false);
        realtimeDto.setStateProgress(100);
        realtimeDto.setEnvironmentType(EnvironmentType.HYBRID);
        return realtimeDto;
    }

    /**
     * Class to handle all the operation related to merge policy.
     */
    private static class MergePolicyHandler {

        private static final Logger logger = LogManager.getLogger();

        /**
         * Max char length of group name.
         */
        private static final int MAX_GROUP_NAME_LENGTH = 255;

        private static final Set<MergeType> MERGE_TYPE_NEEDS_HIDDEN_GROUP =
                ImmutableSet.of(MergeType.DATACENTER, MergeType.DESKTOP_POOL);

        private final GroupServiceBlockingStub groupService;
        private final PolicyServiceBlockingStub policyService;
        private final RepositoryApi repositoryApi;

        /**
         * Constructor.
         *
         * @param groupService the {@link GroupServiceBlockingStub}
         * @param policyService the {@link PolicyServiceBlockingStub}
         * @param repositoryApi the {@link RepositoryApi}
         */
        MergePolicyHandler(@Nonnull GroupServiceBlockingStub groupService,
                @Nonnull PolicyServiceBlockingStub policyService,
                @Nonnull RepositoryApi repositoryApi) {
            this.groupService = groupService;
            this.policyService = policyService;
            this.repositoryApi = repositoryApi;
        }

        /**
         * Returns {@code true} if the policy needs a hidden group to support merge. Since some
         * entity types are not a group, we need a hidden group to apply the policy, and set the
         * merge group members to the hidden group id.
         *
         * @param policyApiInputDTO the {@link PolicyApiInputDTO}
         * @return {@code true} if the policy needs a hidden group
         */
        public boolean isPolicyNeedsHiddenGroup(final PolicyApiInputDTO policyApiInputDTO) {
            return policyApiInputDTO.getType() == PolicyType.MERGE
                    && MERGE_TYPE_NEEDS_HIDDEN_GROUP.contains(PolicyMapper.MERGE_TYPE_API_TO_PROTO
                            .get(policyApiInputDTO.getMergeType()));
        }

        /**
         * Returns {@code true} if the policy needs a hidden group to support merge. Since some
         * entity types are not a group, we need a hidden group to apply the policy, and set the
         * merge group members to the hidden group id.
         *
         * @param policyInfo the {@link PolicyInfo}
         * @return {@code true} if the policy needs a hidden group
         */
        public boolean isPolicyNeedsHiddenGroup(final PolicyInfo policyInfo) {
            return policyInfo.hasMerge() && policyInfo.getMerge().hasMergeType()
                    && MERGE_TYPE_NEEDS_HIDDEN_GROUP.contains(policyInfo.getMerge().getMergeType());
        }

        /**
         * Create hidden group to support merge policy. Since some entity types are not a group, we
         * need to create a hidden group to apply the policy, and set the merge group members to the
         * hidden group id.
         *
         * @param policyApiInputDTO policy data from user input, we need to change the merge group
         * member uuids to the hidden group uuid
         */
        public void createHiddenGroup(final PolicyApiInputDTO policyApiInputDTO) {
            final CreateGroupResponse createGroupResponse = groupService.createGroup(
                    CreateGroupRequest.newBuilder()
                            .setGroupDefinition(
                                    createGroupDefinition(policyApiInputDTO.getMergeUuids()))
                            .setOrigin(Origin.newBuilder()
                                    .setSystem(Origin.System.newBuilder()
                                            .setDescription(String.format(
                                                    "Hidden group to support merge with type %s.",
                                                    policyApiInputDTO.getMergeType()))))
                            .build());

            logger.debug(
                    "Created new hidden group {} to support merge with type {} with members {}",
                    () -> createGroupResponse.getGroup().getId(), policyApiInputDTO::getMergeType,
                    policyApiInputDTO::getMergeUuids);
            // Set the hidden group uuid to the group member list
            policyApiInputDTO.setMergeUuids(Collections.singletonList(
                    Long.toString(createGroupResponse.getGroup().getId())));
        }

        /**
         * Update hidden group when user select new merge policy. The merge group member will still
         * the hidden group.
         *
         * @param policyUuid the policy uuid to fetch the current contained group.
         * @param policyApiInputDTO policy data from user input, we need to change the merge group
         * member uuids to the hidden group uuid.
         */
        public void updateHiddenGroup(final String policyUuid,
                final PolicyApiInputDTO policyApiInputDTO) {
            // Fetch the current group members in the merge policy.
            // It should only has 1 hidden group that contains all the selected entities.
            final PolicyDTO.Policy policy = policyService.getPolicy(
                    PolicyDTO.SinglePolicyRequest.newBuilder()
                            .setPolicyId(Long.parseLong(policyUuid))
                            .build()).getPolicy();
            final Set<Long> oldPolicyGroupIds = GroupProtoUtil.getPolicyGroupIds(policy);
            // There should be only 1 hidden group in the policy member list, so this shouldn't happen.
            if (oldPolicyGroupIds.size() != 1) {
                logger.error(
                        "More than 1 group member in merge {} policy {}, update will be skipped.",
                        policyApiInputDTO.getMergeType(), policyUuid);
                return;
            }

            // If group members don't change in the policy, then we skip the update.
            final long groupId = oldPolicyGroupIds.iterator().next();
            final List<String> mergeUuids = Collections.singletonList(String.valueOf(groupId));
            if (mergeUuids.equals(policyApiInputDTO.getMergeUuids())) {
                return;
            }

            groupService.updateGroup(UpdateGroupRequest.newBuilder()
                    .setId(groupId)
                    .setNewDefinition(createGroupDefinition(policyApiInputDTO.getMergeUuids()))
                    .build());

            logger.debug("Updated hidden group {} to support merge with type {} with members {}",
                    () -> groupId, policyApiInputDTO::getMergeType,
                    policyApiInputDTO::getMergeUuids);
            // Replace the hidden group uuid to the group member list
            policyApiInputDTO.setMergeUuids(mergeUuids);
        }

        /**
         * Delete the hidden group when user deletes the merge policy.
         *
         * @param policyUuid the deleted policy uuid.
         * @param policyInfo the info of the removed policy.
         */
        public void deleteHiddenGroup(final String policyUuid, final PolicyInfo policyInfo) {
            policyInfo.getMerge().getMergeGroupIdsList().forEach(groupId -> {
                groupService.deleteGroup(GroupID.newBuilder().setId(groupId).build());
                logger.debug("Deleted hidden group {} for policy {}.", groupId, policyUuid);
            });
        }

        private GroupDefinition.Builder createGroupDefinition(final List<String> mergeUuids) {
            final Map<Integer, List<MinimalEntity>> entitiesByType = repositoryApi.entitiesRequest(
                    mergeUuids.stream().map(Long::parseLong).collect(Collectors.toSet()))
                    .getMinimalEntities()
                    .collect(Collectors.groupingBy(MinimalEntity::getEntityType));
            final String displayName = StringUtils.abbreviate(entitiesByType.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .map(MinimalEntity::getDisplayName)
                    .collect(Collectors.joining(",", "Merge: ", "")), MAX_GROUP_NAME_LENGTH);
            final StaticMembers.Builder staticGroupMembers = StaticMembers.newBuilder();
            entitiesByType.entrySet()
                    .stream()
                    .map(e -> StaticMembersByType.newBuilder()
                            .setType(MemberType.newBuilder().setEntity(e.getKey()))
                            .addAllMembers(e.getValue()
                                    .stream()
                                    .map(MinimalEntity::getOid)
                                    .collect(Collectors.toSet())))
                    .forEach(staticGroupMembers::addMembersByType);
            return GroupDefinition.newBuilder()
                    .setDisplayName(displayName)
                    .setIsHidden(true)
                    .setStaticGroupMembers(staticGroupMembers);
        }
    }

    /**
     * Gets a plan project with the given id.
     *
     * @param planProjectId Id of the plan project.
     * @return PlanProject from DB matching the input id.
     * @throws UnknownObjectException Thrown if no project found with the given id.
     */
    private PlanProject getPlanProject(long planProjectId) throws UnknownObjectException {
        GetPlanProjectResponse response = planProjectRpcService.getPlanProject(
                GetPlanProjectRequest.newBuilder().setProjectId(planProjectId).build());
        if (!response.hasProject()) {
            throw new UnknownObjectException("Could not look up a plan project "
                    + planProjectId);
        }
        return response.getProject();
    }

    /**
     * Convenience method to get a plan instance given a plan id, from the plan rpc service.
     *
     * @param planId Id of plan to fetch.
     * @return PlanInstance object.
     * @throws UnknownObjectException Thrown if no plan could be found with the id.
     * @throws StatusRuntimeException Thrown on problem getting  plan.
     */
    @Nonnull
    private PlanInstance getPlanInstance(long planId)
            throws UnknownObjectException, StatusRuntimeException {
        OptionalPlanInstance planResponse = planRpcService.getPlan(PlanId.newBuilder()
                .setPlanId(planId)
                .build());

        if (!planResponse.hasPlanInstance()) {
            throw new UnknownObjectException("Could not locate a plan instance with id: "
                    + planId);
        }
        return planResponse.getPlanInstance();
    }

    /**
     * Deletes an existing plan instance from DB.
     *
     * @param planId Id of the plan to be deleted.
     * @param planProjectId Optional, plan project id if applicable, for logging only.
     * @throws UnknownObjectException Thrown on unexpected delete api response.
     * @throws StatusRuntimeException Thrown on problem deleting plan.
     */
    private void deletePlanInstance(long planId, @Nullable final Long planProjectId)
            throws UnknownObjectException, StatusRuntimeException {
        if (planProjectId == null) {
            logger.debug("Deleting standalone plan {}.", planId);
        } else {
            logger.debug("Delete plan {} under project {}.", planId, planProjectId);
        }
        if (planRpcService.deletePlan(PlanId.newBuilder()
                .setPlanId(planId)
                .build()) == null) {
            throw new UnknownObjectException("Could not delete plan instance "
                    + planId + (planProjectId == null ? "" : ". Project: " + planProjectId));
        }
    }

    /**
     * Deletes the plan scenario from DB.
     *
     * @param scenarioId Id of scenario to delete.
     * @param planId Id of plan that scenario belongs to.
     * @throws UnknownObjectException Thrown on unexpected delete api response.
     * @throws StatusRuntimeException Thrown on problem deleting scenario.
     */
    private void deleteScenario(final long scenarioId, long planId)
            throws UnknownObjectException, StatusRuntimeException {
        logger.debug("Deleting scenario {} for plan {}.", scenarioId, planId);
        if (scenarioServiceClient.deleteScenario(ScenarioId.newBuilder()
                .setScenarioId(scenarioId).build()) == null) {
            throw new UnknownObjectException("Could not delete scenario " + scenarioId
                    + " under plan " + planId);
        }
    }

    /**
     * Deletes the plan project from DB.
     *
     * @param planProjectId Id of plan project.
     * @throws UnknownObjectException Thrown on unexpected delete api response.
     * @throws StatusRuntimeException Thrown on problem deleting plan project.
     */
    private void deletePlanProject(long planProjectId)
            throws UnknownObjectException, StatusRuntimeException {
        logger.debug("Deleting plan project {}.", planProjectId);
        if (!planProjectRpcService.deletePlanProject(DeletePlanProjectRequest.newBuilder()
                .setProjectId(planProjectId)
                .build()).hasProjectId()) {
            throw new UnknownObjectException("Could not delete plan project " + planProjectId);
        }
    }

    /**
     * Gets the Scenario instance given a scenario id, from the scenario rpc service.
     *
     * @param scenarioId Id of Scenario to fetch.
     * @return Scenario object.
     * @throws UnknownObjectException Thrown if no scenario could be found with the id.
     */
    @Nonnull
    private Scenario getScenario(long scenarioId) throws UnknownObjectException {
        final Scenario scenario = scenarioServiceClient.getScenario(ScenarioId.newBuilder()
                        .setScenarioId(scenarioId)
                        .build());
        if (scenario == null) {
            throw new UnknownObjectException("Could not locate a Scenario with id: " + scenarioId);
        }
        return scenario;
    }

    /**
     * Returns the MarketApiDTO for the plan project and the given project scenario.
     *
     * @param planProject Plan project like migration plan project.
     * @param mainPlanInstance The main {@link PlanInstance} corresponding to the {@link PlanProject}
     * @param projectScenario Project scenario. If null, then the plan's scenario is used.
     * @return MarketApiDTO with values filled.
     * @throws ConversionException Thrown when issue converting.
     * @throws InterruptedException Thrown on interruption.
     */
    @Nonnull
    private MarketApiDTO getMarketApiDto(@Nonnull final PlanProject planProject,
                                         @Nonnull final PlanInstance mainPlanInstance,
                                         @Nullable final Scenario projectScenario)
            throws ConversionException, InterruptedException {
        long projectId = planProject.getPlanProjectId();
        try {
            // Get scenario for this project's main plan, if not already specified in input.
            final Scenario existingScenario = projectScenario != null ? projectScenario
                    : mainPlanInstance.getScenario();
            List<PlanInstance> relatedPlans = new ArrayList<>();
            for (Long planId : planProject.getPlanProjectInfo().getRelatedPlanIdsList()) {
                relatedPlans.add(getPlanInstance(planId));
            }
            return marketMapper.dtoFromPlanProject(planProject, mainPlanInstance, relatedPlans,
                    existingScenario);
        } catch (UnknownObjectException uoe) {
            throw new ConversionException("Could not convert plan project " + projectId, uoe);
        }
    }

    /**
     * Convenience method to get env type based on what targets are available. Code refactored
     * here for clarify from previous method, without any changes.
     *
     * @return Type of environment.
     */
    @Nonnull
    private EnvironmentType getEnvironmentType() {
        // TODO: The implementation for the environment type might be possibly altered for the case
        // of a scoped plan (should we consider only the scoped targets?)
        EnvironmentType envType = null;
        boolean isOnPrem = false;
        boolean isCloud = false;
        boolean isHybrid = false;
        for (ThinTargetInfo thinTargetInfo : thinTargetCache.getAllTargets()) {
            switch (ProbeCategory.create(thinTargetInfo.probeInfo().category())) {
                case CLOUD_MANAGEMENT:
                case CLOUD_NATIVE:
                case PAAS:
                case FAAS:
                    isCloud = true;
                    break;
                default:
                    isOnPrem = true;
                    break;
            }
            if (isOnPrem && isCloud) {
                isHybrid = true;
                break;
            }
        }
        if (isHybrid) {
            return EnvironmentType.HYBRID;
        }
        if (isCloud) {
            return EnvironmentType.CLOUD;
        }
        return EnvironmentType.ONPREM;
    }

    @Override
    public List<SettingsManagerApiDTO> getSettingsByEntityAndMarketId(String marketUuid, String entityUuid) throws Exception {
        final ApiId id = uuidMapper.fromUuid(entityUuid);
        final ApiId marketId = uuidMapper.fromUuid(marketUuid);
        if (!marketId.isRealtimeMarket() && !marketId.isPlan()) {
            throw new UnknownObjectException("The ID " + marketUuid
                    + " doesn't belong to a plan or a real-time market.");
        }
        final List<SettingsManagerApiDTO> retMgrs =
                entitySettingQueryExecutor.getEntitySettings(id, false, null, marketId.getTopologyContextId());
        return retMgrs;
    }

    @VisibleForTesting
    static class TopologyAndApiEntity {
        final TopologyEntityDTO topologyEntity;
        final ServiceEntityApiDTO apiEntity;

        private TopologyAndApiEntity(@Nonnull final TopologyEntityDTO topologyEntity,
                                    @Nonnull final ServiceEntityApiDTO apiEntity) {
            this.topologyEntity = Objects.requireNonNull(topologyEntity);
            this.apiEntity = Objects.requireNonNull(apiEntity);
        }
    }

    /**
     * Get list of cloud cost statistics for given market. Market's cloud entities will be considered.
     * POST /markets/{market_Uuid}/cost
     *
     * @param marketUuid uuid of a market who's cloud entities will be considered
     * @param costInputApiDTO Filters and groupings applied to cost statistic
     * @return List of {@link StatSnapshotApiDTO} containing cloud cost data
     * @throws OperationFailedException In case when cannot resolve Market UUID.
     * @throws UnsupportedOperationException In case when market UUID doesn't refer to realtime Market.
     */
    @Override
    public List<StatSnapshotApiDTO> getMarketCloudCostStats(
            @Nonnull String marketUuid,
            @Nullable CostInputApiDTO costInputApiDTO)
            throws OperationFailedException, UnsupportedOperationException {
        final ApiId marketId = uuidMapper.fromUuid(marketUuid);
        if (!marketId.isRealtimeMarket()) {
            throw new UnsupportedOperationException(String.format("Not supported - %s is not a real-time market.", marketUuid));
        }
        return costStatsQueryExecutor.getGlobalCostStats(costInputApiDTO);
    }
}
