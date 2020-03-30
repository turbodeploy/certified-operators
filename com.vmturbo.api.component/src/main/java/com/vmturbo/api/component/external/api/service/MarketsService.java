package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
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
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
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
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
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
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
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
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Service implementation of Markets.
 **/
public class MarketsService implements IMarketsService {

    private final Logger logger = LogManager.getLogger();

    private final PlanServiceBlockingStub planRpcService;

    private final ActionSpecMapper actionSpecMapper;

    private final UuidMapper uuidMapper;

    private final ActionsServiceBlockingStub actionRpcService;

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

    /**
     * For fetching plan entities and their related stats.
     */
    private final PlanEntityStatsFetcher planEntityStatsFetcher;

    public MarketsService(@Nonnull final ActionSpecMapper actionSpecMapper,
                          @Nonnull final UuidMapper uuidMapper,
                          @Nonnull final ActionsServiceBlockingStub actionRpcService,
                          @Nonnull final PoliciesService policiesService,
                          @Nonnull final PolicyServiceBlockingStub policyRpcService,
                          @Nonnull final PlanServiceBlockingStub planRpcService,
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
                          final long realtimeTopologyContextId) {
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.actionRpcService = Objects.requireNonNull(actionRpcService);
        this.policiesService = Objects.requireNonNull(policiesService);
        this.policyRpcService = Objects.requireNonNull(policyRpcService);
        this.planRpcService = Objects.requireNonNull(planRpcService);
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
    }

    /**
     * Get all markets/plans from the plan orchestrator.
     *
     * @param scopeUuids this argument is currently ignored.
     * @return list of all markets
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Override
    @Nonnull
    public List<MarketApiDTO> getMarkets(@Nullable List<String> scopeUuids)
            throws ConversionException, InterruptedException {
        logger.debug("Get all markets in scopes: {}", scopeUuids);
        final Iterator<PlanInstance> plans =
                planRpcService.getAllPlans(GetPlansOptions.getDefaultInstance());
        final List<MarketApiDTO> result = new ArrayList<>();
        // Add the realtime market api dto to the list of markets
        result.add(createRealtimeMarketApiDTO());
        // Get the plan markets and create api dtos for these
        while (plans.hasNext()) {
            final PlanInstance plan = plans.next();
            final MarketApiDTO dto = marketMapper.dtoFromPlanInstance(plan);
            result.add(dto);
        }
        return result;
    }

    @Override
    public MarketApiDTO getMarketByUuid(String uuid) throws Exception {
        // TODO: The implementation for the environment type might be possibly altered for the case
        // of a scoped plan (should we consoder only the scoped tergets?)
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
            envType = EnvironmentType.HYBRID;
        } else if (isCloud) {
            envType = EnvironmentType.CLOUD;
        } else {
            envType = EnvironmentType.ONPREM;
        }

        final ApiId apiId = uuidMapper.fromUuid(uuid);
        if (apiId.isRealtimeMarket()) {
           return createRealtimeMarketApiDTO();
        } else if (apiId.isPlan()) {
            OptionalPlanInstance response = planRpcService.getPlan(PlanId.newBuilder()
                    .setPlanId(Long.valueOf(uuid))
                    .build());
            if (!response.hasPlanInstance()) {
                throw new UnknownObjectException(uuid);
            }
            MarketApiDTO planDto = marketMapper.dtoFromPlanInstance(response.getPlanInstance());
            planDto.setEnvironmentType(envType);

            // set unplaced entities
            try {
                planDto.setUnplacedEntities(!getUnplacedEntitiesByMarketUuid(uuid).isEmpty());
            } catch (Exception e) {
                logger.error("getMarketByUuid(): Error while checking if there are unplaced entities: {}",
                        e.getMessage());
            }

            return planDto;
        }

        // The UUID doesn't belong to a plan or real-time market.
        throw new UnknownObjectException("The ID " + uuid
                + " doesn't belong to a plan or a real-time market.");
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
        return actionSearchUtil.callActionService(filter, paginationRequest, inputDto.getDetailLevel());
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
            // In the plan case we do the pagination in the API component. The main reason is
            // that we need to use the repository service (instead of the search service), and that
            // service doesn't currently support pagination parameters.
            //
            // TODO (roman, Jan 22 2019) OM-54686: Add pagination parameters to the
            // retrieveTopologyEntities method, and convert this logic to use the repository's pagination.
            final OptionalPlanInstance optInstance = planRpcService.getPlan(PlanId.newBuilder()
                .setPlanId(apiId.oid())
                .build());
            final Optional<Long> projectedTopologyId =
                optInstance.getPlanInstance().hasProjectedTopologyId() ?
                    Optional.of(optInstance.getPlanInstance().getProjectedTopologyId()) :
                    Optional.empty();

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
                optInstance.getPlanInstance(), allResults);

            // We do pagination before fetching price index and severity.
            final int skipCount;
            if (paginationRequest.getCursor().isPresent()) {
                try {
                    // Avoid negative skips.
                    skipCount = Math.max(0, Integer.parseInt(paginationRequest.getCursor().get()));
                } catch (NumberFormatException e) {
                    throw new InvalidOperationException("Cursor " +
                        paginationRequest.getCursor().get() + " is invalid. Must be positive integer.");
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
                    .getAllPolicies(PolicyDTO.PolicyRequest.getDefaultInstance());
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
            PolicyApiInputDTO policyApiInputDTO) {
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
            policyRpcService.editPolicy(policyEditRequest);

            final String details = String.format("Changed policy %s", policyInfo.getName());
            AuditLog.newEntry(AuditAction.CHANGE_POLICY,
                details, true)
                .targetName(policyInfo.getName())
                .audit();
            return new PolicyApiDTO();
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
            final PlanInstance oldPlanInstance = planRpcService.deletePlan(PlanId.newBuilder()
                    .setPlanId(Long.parseLong(uuid))
                    .build());
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
                                            // TODO (roman, June 6 2017): The plan market name
                                            // should be the display name of the plan. However,
                                            // the UI doesn't currently use it, so postponing
                                            // this for later.
                                            @Nullable String planMarketName) throws Exception {
        logger.info("Running scenario. Market UUID: {}, Scenario ID: {}, " +
                        "Ignore Constraints: {}, Plan Market Name: {}",
                marketUuid, scenarioId, ignoreConstraints, planMarketName);

        // todo: The 'ignoreConstraints' parameter will move into the Scenario in OM-18012
        // Until OM-18012 is fixed, we will have to do a get and set of scenario to add the
        // ignoreConstraint changes to the existing scenario.
        if (ignoreConstraints) {
            Scenario existingScenario = scenarioServiceClient.getScenario(
                    ScenarioId.newBuilder()
                            .setScenarioId(scenarioId)
                            .build());
            ScenarioInfo.Builder updatedScenarioInfo = existingScenario.getScenarioInfo().toBuilder();
            final List<IgnoreConstraint> ignoredConstraints =
                    updatedScenarioInfo.getChangesList().stream()
                            .flatMap(change -> change.getPlanChanges().getIgnoreConstraintsList().stream())
                            .collect(Collectors.toList());
            if (ignoredConstraints.isEmpty()) {
                // NOTE: Currently we only remove constraints for VM entities.
                updatedScenarioInfo.addChanges(ScenarioChange.newBuilder()
                        .setPlanChanges(PlanChanges.newBuilder()
                                .addIgnoreConstraints(IgnoreConstraint.newBuilder()
                                        .setGlobalIgnoreEntityType(
                                                GlobalIgnoreEntityType.newBuilder()
                                                .setEntityType(EntityType.VIRTUAL_MACHINE))
                                        .build())));
            } else {
                logger.warn("Ignoring \"ignore constraints\" option because the scenario already has ignored "
                        + "constraints: {}", ignoredConstraints);
            }

            UpdateScenarioResponse updateScenarioResponse =
                    scenarioServiceClient.updateScenario(UpdateScenarioRequest.newBuilder()
                            .setScenarioId(scenarioId)
                            .setNewInfo(updatedScenarioInfo.build())
                            .build());
            logger.debug("Updated scenario: {}", updateScenarioResponse.getScenario());
        }

        // note that, for XL, the "marketUuid" in the request is interpreted as the Plan Instance ID
        final ApiId planInstanceId;
        try {
            planInstanceId = uuidMapper.fromUuid(marketUuid);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid market id: " + marketUuid);
        }
        final PlanInstance planInstance;
        if (planInstanceId.isRealtimeMarket()) {
            // for realtime market create a new plan; topologyId is not set, defaulting to "0"
            planInstance = planRpcService.createPlan(CreatePlanRequest.newBuilder()
                    .setScenarioId(scenarioId)
                    .build());
        } else {
            // plan market: create new plan where source topology is the prev projected topology
            PlanDTO.PlanScenario planScenario = PlanDTO.PlanScenario.newBuilder()
                    .setPlanId(planInstanceId.oid())
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
        final Optional<PlanInstance> planInstanceOptional = planInstanceId.getPlanInstance();

        if (!planInstanceOptional.isPresent()) {
            throw new InvalidOperationException("Invalid market id: " + marketUuid);
        }

        final PlanInstance planInstance = planInstanceOptional.get();

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

        final long planId = Long.parseLong(marketUuid);

        // fetch plan from plan orchestrator
        final PlanDTO.OptionalPlanInstance planInstanceOptional =
                planRpcService.getPlan(PlanDTO.PlanId.newBuilder()
                        .setPlanId(planId)
                        .build());
        if (!planInstanceOptional.hasPlanInstance()) {
            throw new InvalidOperationException("Invalid market id: " + marketUuid);
        }

        final PlanInstance planInstance = planInstanceOptional.getPlanInstance();
        // verify the user can access the plan
        if (!PlanUtils.canCurrentUserAccessPlan(planInstance)) {
            throw new UserAccessException("User does not have access to plan.");
        }

        return planEntityStatsFetcher
            .getPlanEntityStats(planInstance, statScopesApiInputDTO, paginationRequest);
    }

    @Override
    /**
     * {@inheritDoc}
     * This api has both a group id as input and also a StatScopesApiInputDTO.  The StatScopesApiInputDTO may
     * include a scope.  If the scope is specified, the logic will use the intersection of the group members and scope ids.
     * It is typically expected that the users specify only a group id.
     */
    public EntityStatsPaginationResponse getStatsByEntitiesInGroupInMarketQuery(final String marketUuid,
                                                                                final String groupUuid,
                                                                                final StatScopesApiInputDTO statScopesApiInputDTO,
                                                                                final EntityStatsPaginationRequest paginationRequest)
            throws Exception {
        // Look up the group members and then check how the members overlap with an input scope, if specified
        // If the plan was scoped to a different group
        // we won't return anything (since the requested entities won't be found in the repository).
            ApiId apiId = uuidMapper.fromUuid(marketUuid);
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
        // get the topology id for the plan
        OptionalPlanInstance planResponse = planRpcService.getPlan(PlanId.newBuilder()
                .setPlanId(Long.valueOf(uuid))
                .build());

        if (!planResponse.hasPlanInstance()) {
            throw new UnknownObjectException(uuid);
        }

        // Get the list of unplaced entities from the repository for this plan
        PlanInstance plan = planResponse.getPlanInstance();
        final Iterable<RetrieveTopologyResponse> response = () ->
                repositoryRpcService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                        .setTopologyId(plan.getProjectedTopologyId())
                        .setEntityFilter(TopologyEntityFilter.newBuilder()
                                .setUnplacedOnly(true))
                        .build());
        List<TopologyEntityDTO> unplacedDTOs = StreamSupport.stream(response.spliterator(), false)
                .flatMap(rtResponse -> rtResponse.getEntitiesList().stream())
                .map(PartialEntity::getFullEntity)
                // right now, legacy and UI only expect unplaced virtual machine.
                .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .collect(Collectors.toList());

        // if no unplaced entities, return an empty collection
        if (unplacedDTOs.size() == 0) {
            return Collections.emptyList();
        }

        // convert to ServiceEntityApiDTOs, filling in the blanks of the placed on / not placed on fields
        List<ServiceEntityApiDTO> unplacedApiDTOs = populatePlacedOnUnplacedOnForEntitiesForPlan(plan, unplacedDTOs);

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
        ActionOrchestratorAction action = actionOrchestratorRpcService
                .getAction(actionRequest(marketUuid, actionuuid));
        if (action.hasActionSpec()) {
            // create action details dto based on action api dto which contains "explanation" with coverage information.
            ActionDetailsApiDTO actionDetailsApiDTO = actionSpecMapper.createActionDetailsApiDTO(action);
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
    List<ServiceEntityApiDTO> populatePlacedOnUnplacedOnForEntitiesForPlan(PlanInstance plan, List<TopologyEntityDTO> entityDTOs) {
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
     * Create a dto for the realtime market
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
            return policyApiInputDTO.getType() == PolicyType.MERGE &&
                    MERGE_TYPE_NEEDS_HIDDEN_GROUP.contains(PolicyMapper.MERGE_TYPE_API_TO_PROTO.get(
                            policyApiInputDTO.getMergeType()));
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
            return policyInfo.hasMerge() && policyInfo.getMerge().hasMergeType() &&
                    MERGE_TYPE_NEEDS_HIDDEN_GROUP.contains(policyInfo.getMerge().getMergeType());
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
                    PolicyDTO.PolicyRequest.newBuilder()
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
}
