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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

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
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
import com.vmturbo.api.component.external.api.util.stats.PlanEntityStatsFetcher;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.NoDetailsApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
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
import com.vmturbo.api.enums.EnvironmentType;
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
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyDeleteResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyEditResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.DeletePlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetAllPlanProjectsRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetPlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetPlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.RunPlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectServiceGrpc.PlanProjectServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
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
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.plan.orchestrator.api.PlanUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Service implementation of Markets.
 **/
public class MarketsService implements IMarketsService {

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
                          final long realtimeTopologyContextId) {
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
        long planId = apiId.oid();
        final PlanInstance planInstance = getPlanInstance(planId);

        MarketApiDTO marketApiDTO;
        if (planInstance.hasPlanProjectId()) {
            // This could be the main plan (like Consumption plan), that is part of a MCP project.
            final PlanProject planProject = getPlanProject(planInstance.getPlanProjectId());
            marketApiDTO = getMarketApiDto(planProject, getMainPlanInstance(planProject), null);
        } else {
            // A regular plan (like OCP) without an associated project.
            marketApiDTO = marketMapper.dtoFromPlanInstance(planInstance);
        }
        marketApiDTO.setEnvironmentType(getEnvironmentType());

        // set unplaced entities
        try {
            marketApiDTO.setUnplacedEntities(!getUnplacedEntitiesByMarketUuid(String.valueOf(planId))
                    .isEmpty());
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
        final ActionQueryFilter filter = actionSpecMapper.createActionFilter(
                                                inputDto, Optional.empty(), apiId);
        if (apiId.isPlan()) {
            // require the "planner" license feature to get plan actions
            licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);
        }
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
    public EntityPaginationResponse getEntitiesByMarketUuid(String uuid, EntityPaginationRequest paginationRequest) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);

        // We don't currently support any sort order other than "name".
        if (paginationRequest.getOrderBy() != EntityOrderBy.NAME) {
            logger.warn("Unimplemented sort order {} for market entities. Defaulting to NAME",
                paginationRequest.getOrderBy());
        }

        if (apiId.isRealtimeMarket()) {
            // In the realtime case we let the repository handle the pagination parameters.
            final SearchEntitiesResponse response =
                searchServiceBlockingStub.searchEntities(SearchEntitiesRequest.newBuilder()
                    .setReturnType(Type.API)
                    .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                    .build());
            final List<ServiceEntityApiDTO> nextPage = serviceEntityMapper.toServiceEntityApiDTO(
                response.getEntitiesList().stream()
                    .map(PartialEntity::getApi)
                    .collect(Collectors.toList()));
            priceIndexPopulator.populateRealTimeEntities(nextPage);
            severityPopulator.populate(realtimeTopologyContextId, nextPage);
            return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                .map(nextCursor -> paginationRequest.nextPageResponse(nextPage, nextCursor, response.getPaginationResponse().getTotalRecordCount()))
                .orElseGet(() -> paginationRequest.finalPageResponse(nextPage, response.getPaginationResponse().getTotalRecordCount()));
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
            final Optional<Long> projectedTopologyId =
                planInstance.hasProjectedTopologyId()
                        ? Optional.of(planInstance.getProjectedTopologyId())
                        : Optional.empty();

            final List<TopologyEntityDTO> allResults = RepositoryDTOUtil.topologyEntityStream(
                repositoryRpcService.retrieveTopologyEntities(
                    RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyContextId(apiId.oid())
                        .setReturnType(Type.FULL)
                        .setTopologyType(TopologyType.PROJECTED)
                        .build()))
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());

            final List<ServiceEntityApiDTO> allConvertedResults = populatePlacedOnUnplacedOnForEntitiesForPlan(
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

            final List<ServiceEntityApiDTO> nextPage = allConvertedResults.stream()
                // assumes that ServiceEntityApiDTO::getDisplayName will never return null
                .sorted(Comparator.comparing(ServiceEntityApiDTO::getDisplayName)
                        .thenComparing(ServiceEntityApiDTO::getUuid))
                .skip(skipCount)
                .limit(limit + 1)
                .collect(Collectors.toList());

            final Optional<String> nextCursor;
            if (nextPage.size() > limit) {
                nextCursor = Optional.of(Integer.toString(skipCount + limit));
                // Remove the last element from the results. It was only there to check if there
                // are more results.
                nextPage.remove(limit);
            } else {
                nextCursor = Optional.empty();
            }

            // populate priceIndex, which is always based on the projected topology
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
        try {
            final Iterator<PolicyDTO.PolicyResponse> allPolicyResps = policyRpcService
                    .getPolicies(PolicyDTO.PolicyRequest.getDefaultInstance());
            final List<Policy> policies = Streams.stream(allPolicyResps)
                    .filter(PolicyResponse::hasPolicy)
                    .map(PolicyResponse::getPolicy)
                    .collect(Collectors.toList());

            final Set<Long> groupingIDS = policies.stream()
                    .map(GroupProtoUtil::getPolicyGroupIds)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            final Collection<Grouping> groupings;
            if (groupingIDS.isEmpty()) {
                groupings = Collections.emptySet();
            } else {
                final Iterator<Grouping> iterator = groupRpcService.getGroups(
                        GetGroupsRequest.newBuilder()
                                .setGroupFilter(GroupFilter.newBuilder()
                                        .addAllId(groupingIDS)
                                        .setIncludeHidden(true))
                                .build());
                groupings = Sets.newHashSet(iterator);
            }
            final List<PolicyApiDTO> result = policyMapper.policyToApiDto(policies, groupings);
            return result;
        } catch (RuntimeException e) {
            logger.error("Problem getting policies", e);
            throw e;
        }
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
     * @return {@link ActionPaginationResponse}
     * @throws Exception
     */
    @Override
    public MarketApiDTO deleteMarketByUuid(String uuid) throws Exception {
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
        if (Boolean.TRUE.equals(ignoreConstraints)) {
            existingScenario = updateIgnoreConstraints(existingScenario);
        }

        if (planProjectBuilder.isPlanProjectRequired(sourceMarketId, existingScenario)) {
            final PlanProject planProject = planProjectBuilder.createPlanProject(existingScenario);

            logger.info("Running a newly created {} plan project {}.",
                    existingScenario.getScenarioInfo().getType(), planProject.getPlanProjectId());
            planProjectRpcService.runPlanProject(RunPlanProjectRequest.newBuilder()
                    .setId(planProject.getPlanProjectId()).build());

            return getMarketApiDto(planProject, getMainPlanInstance(planProject), existingScenario);
        }
        // For regular plans (like OCP) that are not part of a project.
        final PlanInstance planInstance;
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
        final PlanInstance updatedInstance = planRpcService.runPlan(PlanId.newBuilder()
                .setPlanId(planInstance.getPlanId())
                .build());
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

        final PlanInstance updatePlanInstance = planRpcService.updatePlan(updatePlanRequest);

        return marketMapper.dtoFromPlanInstance(updatePlanInstance);
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
        if (apiId.isPlan()) {
            licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);
        }
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
        if (!PlanUtils.canCurrentUserAccessPlan(planInstance)) {
            throw new UserAccessException("User does not have access to plan.");
        }

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
        // If this is a migration plan, get the list of unplaced entities from the action
        // list instead.
        Set<Long> placedOids = new HashSet<>();
        boolean isMigrationPlan = plan.getProjectType() == PlanProjectType.CLOUD_MIGRATION;
        if (isMigrationPlan) {
            AtomicReference<String> cursor = new AtomicReference<>("0");
            do {
                FilteredActionRequest filteredActionRequest =
                        FilteredActionRequest.newBuilder()
                                .setTopologyContextId(plan.getPlanId())
                                .setPaginationParams(PaginationParameters.newBuilder()
                                        .setCursor(cursor.getAndSet("")))
                                .setFilter(ActionQueryFilter.newBuilder()
                                        .setVisible(true)
                                        .addTypes(ActionType.MOVE))
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
                // right now, legacy and UI only expect unplaced virtual machine.
                .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        // We have a map of all requested entities in the plan. If this is a migration plan,
        // remove the entities that we've seen in its action list.  The remaining entities are
        // unplaced.
        for (Long oid : placedOids) {
            entities.remove(oid);
        }
        // convert to ServiceEntityApiDTOs, filling in the blanks of the placed on / not placed on fields
        List<ServiceEntityApiDTO> unplacedApiDTOs =
                populatePlacedOnUnplacedOnForEntitiesForPlan(plan, entities.values());

        logger.debug("Found {} unplaced entities in plan {} results.", unplacedApiDTOs.size(), uuid);
        return unplacedApiDTOs;
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
     */
    List<ServiceEntityApiDTO> populatePlacedOnUnplacedOnForEntitiesForPlan(PlanInstance plan,
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
    private List<ServiceEntityApiDTO> createServiceEntityApiDTOs(Collection<TopologyEntityDTO> unplacedDTOs,
                                                                 Map<Long, TopologyEntityDTO> providers) {
        return unplacedDTOs.stream()
                // TODO: (OM-31842) After Market side fix the bug of unplaced cloned
                // entity, we can remove this filter logic.
                .filter(entity -> entity.getCommoditiesBoughtFromProvidersList().stream()
                        .allMatch(CommoditiesBoughtFromProvider::hasProviderEntityType))
                .map(entity -> createServiceEntityApiDTO(entity, providers))
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
        seEntity.setPlacedOn(placedOnJoiner.toString());
        seEntity.setNotPlacedOn(notPlacedOnJoiner.toString());
        return seEntity;
    }

    /**
     * Creates an action request using a market uuid and an action uuid.
     *
     * @param marketUuid uuid of the market.
     * @param actionUuid uuid of the action.
     * @return the action request.
     */
    private static SingleActionRequest actionRequest(String marketUuid, String actionUuid) {
        return SingleActionRequest.newBuilder()
                .setTopologyContextId(Long.valueOf(marketUuid))
                .setActionId(Long.valueOf(actionUuid))
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
}
