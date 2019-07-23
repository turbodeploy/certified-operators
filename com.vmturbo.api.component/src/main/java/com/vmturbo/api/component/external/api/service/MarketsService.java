package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.MergePolicyType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IMarketsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.ParamStrings.MarketOperations;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO;
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
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreEntityTypes;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioId;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdateScenarioRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdateScenarioResponse;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.plan.orchestrator.api.PlanUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

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

    private final StatsMapper statsMapper;

    private final PaginationMapper paginationMapper;

    private final UINotificationChannel uiNotificationChannel;

    private final long realtimeTopologyContextId;

    private final MergeDataCenterPolicyHandler mergeDataCenterPolicyHandler;

    private final UserSessionContext userSessionContext;

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final TopologyProcessor topologyProcessor;

    private final EntitySeverityServiceBlockingStub entitySeverity;

    private final StatsHistoryServiceBlockingStub statsHistory;

    private final RepositoryApi repositoryApi;

    private final ServiceEntityMapper serviceEntityMapper;

    // Exclude request for price index for commodities of real market not saved in DB
    private final Set<Integer> notSavedEntityTypes = ImmutableSet.of(EntityDTO.EntityType.NETWORK.getValue(),
            EntityDTO.EntityType.INTERNET.getValue(),
            EntityDTO.EntityType.VIRTUAL_VOLUME.getValue());

    public MarketsService(@Nonnull final ActionSpecMapper actionSpecMapper,
                          @Nonnull final UuidMapper uuidMapper,
                          @Nonnull final ActionsServiceBlockingStub actionRpcService,
                          @Nonnull final PoliciesService policiesService,
                          @Nonnull final PolicyServiceBlockingStub policyRpcService,
                          @Nonnull final PlanServiceBlockingStub planRpcService,
                          @Nonnull final ScenarioServiceBlockingStub scenariosService,
                          @Nonnull final PolicyMapper policyMapper,
                          @Nonnull final MarketMapper marketMapper,
                          @Nonnull final StatsMapper statsMapper,
                          @Nonnull final PaginationMapper paginationMapper,
                          @Nonnull final GroupServiceBlockingStub groupRpcService,
                          @Nonnull final RepositoryServiceBlockingStub repositoryRpcService,
                          @Nonnull final UserSessionContext userSessionContext,
                          @Nonnull final UINotificationChannel uiNotificationChannel,
                          @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
                          @Nonnull final TopologyProcessor topologyProcessor,
                          @Nonnull final EntitySeverityServiceBlockingStub entitySeverity,
                          @Nonnull final StatsHistoryServiceBlockingStub statsHistory,
                          @Nonnull final StatsService statsService,
                          @Nonnull final RepositoryApi repositoryApi,
                          @Nonnull final ServiceEntityMapper serviceEntityMapper,
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
        this.statsMapper = Objects.requireNonNull(statsMapper);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.entitySeverity = Objects.requireNonNull(entitySeverity);
        this.mergeDataCenterPolicyHandler = new MergeDataCenterPolicyHandler();
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.statsHistory = Objects.requireNonNull(statsHistory);
        this.statsService = Objects.requireNonNull(statsService);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
    }

    /**
     * Get all markets/plans from the plan orchestrator.
     *
     * @param scopeUuids this argument is currently ignored.
     * @return list of all markets
     */
    @Override
    @Nonnull
    public List<MarketApiDTO> getMarkets(@Nullable List<String> scopeUuids) {
        logger.debug("Get all markets in scopes: {}", scopeUuids);
        final Iterator<PlanInstance> plans =
                planRpcService.getAllPlans(GetPlansOptions.getDefaultInstance());
        final List<MarketApiDTO> result = new ArrayList<>();
        while (plans.hasNext()) {
            final PlanInstance plan = plans.next();
            final MarketApiDTO dto = marketMapper.dtoFromPlanInstance(plan);
            result.add(dto);
        }
        return result;
    }

    @Override
    public MarketApiDTO getMarketByUuid(String uuid) throws UnknownObjectException {
        // TODO: The implementation for the environment type might be possibly altered for the case
        // of a scoped plan (should we consoder only the scoped tergets?)
        EnvironmentType envType = null;
        try {
            Set<Long> probeIds = new HashSet<Long>();
            Set<TargetInfo> targetInfos = topologyProcessor.getAllTargets();
            for (TargetInfo targetInfo : targetInfos) {
                probeIds.add(targetInfo.getProbeId());
            }

            Set<ProbeInfo> probeInfos = topologyProcessor.getAllProbes();
            boolean isOnPrem = false;
            boolean isCloud = false;
            boolean isHybrid = false;
            for (ProbeInfo probeInfo : probeInfos) {
                if (probeIds.contains(probeInfo.getId())) {
                    switch (ProbeCategory.create(probeInfo.getCategory())) {
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
            }
            if (isHybrid) {
                envType = EnvironmentType.HYBRID;
            } else if (isOnPrem) {
                envType = EnvironmentType.ONPREM;
            } else {
                envType = EnvironmentType.CLOUD;
            }

        } catch (CommunicationException e) {
            logger.debug("getMarketByUuid(): Error while trying to calculate the environment type: {}",
                    e.getMessage());
        }
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        if (apiId.isRealtimeMarket()) {
            // TODO (roman, Dec 21 2016): Not sure what the API
            // expects from the real-time market as far as state,
            // unplaced entities, state progress, and all that jazz.
            MarketApiDTO realtimeDto = new MarketApiDTO();
            realtimeDto.setUuid(apiId.uuid());
            realtimeDto.setDisplayName("Market");
            realtimeDto.setClassName(apiId.uuid());
            realtimeDto.setState("RUNNING");
            realtimeDto.setUnplacedEntities(false);
            realtimeDto.setStateProgress(100);
            realtimeDto.setEnvironmentType(envType);
            return realtimeDto;
        } else {
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
    public ActionPaginationResponse getCurrentActionsByMarketUuid(String uuid,
                                                                  EnvironmentType environmentType,
                                                                  ActionPaginationRequest paginationRequest) throws Exception {
        ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setEnvironmentType(environmentType);
        return getActionsByMarketUuid(uuid, inputDto, paginationRequest);
    }

    @Override
    public ActionPaginationResponse getActionsByMarketUuid(String uuid,
                                                           ActionApiInputDTO inputDto,
                                                           ActionPaginationRequest paginationRequest) throws Exception {
        handleInvalidCases(inputDto.getStartTime(), inputDto.getEndTime());
        adjustEndTime(inputDto);
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        final ActionQueryFilter filter =
                actionSpecMapper.createActionFilter(inputDto, Optional.empty());
        final FilteredActionResponse response = actionRpcService.getAllActions(
                FilteredActionRequest.newBuilder()
                        .setTopologyContextId(apiId.oid())
                        .setFilter(filter)
                        .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                        .build());
        final List<ActionApiDTO> results = actionSpecMapper.mapActionSpecsToActionApiDTOs(
                response.getActionsList().stream()
                        .filter(ActionOrchestratorAction::hasActionSpec)
                        .map(ActionOrchestratorAction::getActionSpec)
                        .collect(Collectors.toList()), apiId.oid());
        return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                .map(nextCursor -> paginationRequest.nextPageResponse(results, nextCursor))
                .orElseGet(() -> paginationRequest.finalPageResponse(results));
    }

    @Override
    public ActionApiDTO getActionByMarketUuid(String uuid, String aUuid) throws Exception {
        logger.debug("Request to get action by market UUID: {}, action ID: {}", uuid, aUuid);
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ServiceEntityApiDTO> getEntitiesByMarketUuid(String uuid) throws Exception {
        List<ServiceEntityApiDTO> serviceEntityApiDTOs = null;
        HashMap<Long, Float> idsToPriceIndices = new HashMap<>();
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        if (apiId.isRealtimeMarket()) {
            serviceEntityApiDTOs = repositoryApi.entitiesRequest(Collections.emptySet())
                .allowGetAll()
                .getSEList();

            // Reading the price indices for the entities of real market
            final Multimap<Integer, Long> entityTypesToOids = ArrayListMultimap.create();
            for (ServiceEntityApiDTO entityDTO : serviceEntityApiDTOs) {
                entityTypesToOids.put(UIEntityType.fromString(entityDTO.getClassName()).typeNumber(),
                    Long.parseLong(entityDTO.getUuid()));
            }

            final List<EntityStats> entityStats = new ArrayList<>();
            for (Entry<Integer, Long> entry : entityTypesToOids.entries()) {
                if (notSavedEntityTypes.contains(entry.getKey())) {
                    continue;
                }
                String cursor = "0";
                do {
                    final GetEntityStatsResponse entityStatsResponse =
                        statsHistory.getEntityStats(
                            GetEntityStatsRequest.newBuilder()
                                .setScope(
                                    EntityStatsScope.newBuilder()
                                        .setEntityList(EntityList.newBuilder().addAllEntities(entityTypesToOids.get(entry.getKey()))).build())
                                .setFilter(
                                    StatsFilter.newBuilder()
                                        .addCommodityRequests(
                                            CommodityRequest.newBuilder()
                                                .setCommodityName(EntitiesService.PRICE_INDEX_COMMODITY).build())
                                        .build())
                                .setPaginationParams(PaginationParameters.newBuilder()
                                    .setCursor(cursor))
                                .build());
                    cursor = entityStatsResponse.getPaginationResponse().getNextCursor();
                    entityStats.addAll(entityStatsResponse.getEntityStatsList());
                } while (!StringUtils.isEmpty(cursor));

            }

            for (EntityStats stat : entityStats) {
                if (stat.getStatSnapshotsCount() > 0) {
                    long oid = stat.getOid();
                    float priceIndex = stat.getStatSnapshots(0).getStatRecords(0).getUsed().getMax();
                    idsToPriceIndices.put(oid, priceIndex);
                }
            }

        } else {
            OptionalPlanInstance planResponse = planRpcService.getPlan(PlanId.newBuilder()
                    .setPlanId(Long.valueOf(uuid))
                    .build());

            if (!planResponse.hasPlanInstance()) {
                throw new UnknownObjectException(uuid);
            }

            // Get the list of unplaced entities from the repository for this plan
            PlanInstance plan = planResponse.getPlanInstance();
            final long topologyId = plan.getProjectedTopologyId();

            Iterable<RetrieveTopologyResponse> response = () ->
                    repositoryRpcService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                            .setTopologyId(topologyId)
                            .build());

            List<TopologyEntityDTO> entities = new ArrayList<>();
            for (RetrieveTopologyResponse entitiesResponse : response) {
                entities.addAll(entitiesResponse.getEntitiesList());
            }
            serviceEntityApiDTOs = populatePlacedOnUnplacedOnForEntitiesForPlan(plan, entities);

            // Reading the price index for the entities of plans
            List<PlanEntityStats> entityStats = new ArrayList<>();
            String cursor = "0";
            do {
                PlanTopologyStatsResponse resp =
                        repositoryRpcService.getPlanTopologyStats(PlanTopologyStatsRequest.newBuilder()
                                .setTopologyId(topologyId)
                                .setFilter(StatsFilter.newBuilder()
                                        .setStartDate(System.currentTimeMillis() + 10000)
                                        .addCommodityRequests(CommodityRequest.newBuilder()
                                                .setCommodityName(StringConstants.PRICE_INDEX)))
                                .setPaginationParams(PaginationParameters.newBuilder()
                                        .setCursor(cursor))
                                .build());
                cursor = resp.getPaginationResponse().getNextCursor();
                entityStats.addAll(resp.getEntityStatsList());
            } while (!StringUtils.isEmpty(cursor));

            for (PlanEntityStats stat : entityStats) {
                if (stat.getPlanEntityStats().getStatSnapshotsCount() > 0) {
                    long oid = stat.getPlanEntity().getOid();
                    float priceIndex = stat.getPlanEntityStats().getStatSnapshots(0).getStatRecords(0).getUsed().getMax();
                    idsToPriceIndices.put(oid, priceIndex);
                }
            }
       }

        List<Long> entities = new ArrayList<>();
        for (ServiceEntityApiDTO serviceEntityApiDTO : serviceEntityApiDTOs) {
            entities.add(Long.valueOf(serviceEntityApiDTO.getUuid()));
        }

        EntitySeveritiesResponse entitySeveritiesResponse = entitySeverity.getEntitySeverities(MultiEntityRequest
                .newBuilder()
                .setTopologyContextId(apiId.isRealtimeMarket() ? realtimeTopologyContextId : Long.valueOf(uuid))
                .addAllEntityIds(entities)
                .build());

        HashMap<Long, String> idsToSeverities = new HashMap<>();
        List<EntitySeverity> entitySeverityList = entitySeveritiesResponse.getEntitySeverityList();
        for (EntitySeverity entitySeverity : entitySeverityList) {
            idsToSeverities.put(entitySeverity.getEntityId(), entitySeverity.getSeverity().toString());
        }

        return setPriceIndexAndSeverity(serviceEntityApiDTOs, idsToPriceIndices, idsToSeverities);
    }

    /**
     * Set the price index and severity for all the entities for which they exist.
     *
     * @param serviceEntityApiDTOs list of info for service entities
     * @param idsToPriceIndices map from service entity id to price index
     * @param idsToSeverities map from service entity id to severity
     * @return list of info for service entities
     */
    private List<ServiceEntityApiDTO> setPriceIndexAndSeverity(List<ServiceEntityApiDTO> serviceEntityApiDTOs,
                                                               HashMap<Long, Float> idsToPriceIndices,
                                                               HashMap<Long, String> idsToSeverities) {
        for (int i = 0; i < serviceEntityApiDTOs.size(); i++) {
            serviceEntityApiDTOs.get(i).setPriceIndex(idsToPriceIndices.get(Long.valueOf(serviceEntityApiDTOs.get(i).getUuid())));
            serviceEntityApiDTOs.get(i).setSeverity(idsToSeverities.get(Long.valueOf(serviceEntityApiDTOs.get(i).getUuid())));
        }

        return serviceEntityApiDTOs;

    }

    // Market UUID is ignored for now, since there is only one Market in XL.
    @Override
    public List<PolicyApiDTO> getPoliciesByMarketUuid(String uuid) throws Exception {
        try {
            final Iterator<PolicyDTO.PolicyResponse> allPolicyResps = policyRpcService
                    .getAllPolicies(PolicyDTO.PolicyRequest.getDefaultInstance());
            ImmutableList<PolicyResponse> policyRespList = ImmutableList.copyOf(allPolicyResps);

            Set<Long> groupingIDS = policyRespList.stream()
                    .filter(PolicyDTO.PolicyResponse::hasPolicy)
                    .flatMap(resp -> GroupProtoUtil.getPolicyGroupIds(resp.getPolicy()).stream())
                    .collect(Collectors.toSet());
            final Map<Long, Group> groupings = new HashMap<>();
            if (!groupingIDS.isEmpty()) {
                groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                        .addAllId(groupingIDS)
                        .build())
                        .forEachRemaining(group -> groupings.put(group.getId(), group));
            }
            return policyRespList.stream()
                    .filter(PolicyDTO.PolicyResponse::hasPolicy)
                    .map(resp -> policyMapper.policyToApiDto(resp.getPolicy(), groupings))
                    .collect(Collectors.toList());
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
                                                  PolicyApiInputDTO policyApiInputDTO)
            throws Exception {
        try {
            // Create hidden group and update policyApiInputDTO for data centers if the policy is
            // merge data center policy.
            if (isMergeDataCenterPolicy(policyApiInputDTO)) {
                mergeDataCenterPolicyHandler.createDataCenterHiddenGroup(policyApiInputDTO);
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
                    + policyApiInputDTO.getPolicyName(), e);
            throw e;
        }
    }

    @Override
    public PolicyApiDTO editPolicy(final String uuid,
                                   final String policyUuid,
                                   PolicyApiInputDTO policyApiInputDTO) throws Exception {
        try {
            // Update hidden group and update policyApiInputDTO for data centers if the policy is
            // merge data center policy.
            if (isMergeDataCenterPolicy(policyApiInputDTO)) {
                mergeDataCenterPolicyHandler.updateDataCenterHiddenGroup(policyUuid, policyApiInputDTO);
            }

            final PolicyDTO.PolicyInfo policyInfo = policyMapper.policyApiInputDtoToProto(policyApiInputDTO);
            final PolicyDTO.PolicyEditRequest policyEditRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                    .setPolicyId(Long.valueOf(policyUuid))
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
    public Boolean deletePolicy(String uuid, String policyUuid) throws Exception {
        final PolicyDTO.PolicyDeleteRequest policyDeleteRequest = PolicyDTO.PolicyDeleteRequest.newBuilder()
                .setPolicyId(Long.valueOf(policyUuid))
                .build();

        try {
            final PolicyDeleteResponse response = policyRpcService.deletePolicy(policyDeleteRequest);
            final PolicyInfo policy = response.getPolicy().getPolicyInfo();

            // we need to remove the hidden group as well if the policy is merge data center policy.
            if (isMergeDataCenterPolicy(policy)) {
                mergeDataCenterPolicyHandler.deleteDataCenterHiddenGroup(policyUuid, policy);
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
                                        .setIgnoreEntityTypes(IgnoreEntityTypes.newBuilder()
                                                .addEntityTypes(EntityType.VIRTUAL_MACHINE)
                                                .build())
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

    @Override
    public MarketApiDTO renameMarket(String marketUuid, String displayName) throws Exception {
        throw ApiUtils.notImplementedInXL();
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

        // Short-circuit if there are no input scopes.
        // At the time of this writing we always expect SOME kind of restriction on the entities.
        if (CollectionUtils.isEmpty(statScopesApiInputDTO.getScopes())) {
            return paginationRequest.allResultsResponse(Collections.emptyList());
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

        final PlanTopologyStatsRequest planStatsRequest = statsMapper.toPlanTopologyStatsRequest(
                planInstance, statScopesApiInputDTO, paginationRequest);

        final PlanTopologyStatsResponse response = repositoryRpcService.getPlanTopologyStats(planStatsRequest);

        final List<EntityStatsApiDTO> entityStatsList = response.getEntityStatsList().stream()
                .map(entityStats -> {
                    final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
                    final TopologyDTO.TopologyEntityDTO planEntity = entityStats.getPlanEntity();
                    final ServiceEntityApiDTO serviceEntityApiDTO =
                        serviceEntityMapper.toServiceEntityApiDTO(planEntity);
                    entityStatsApiDTO.setUuid(Long.toString(planEntity.getOid()));
                    entityStatsApiDTO.setDisplayName(planEntity.getDisplayName());
                    entityStatsApiDTO.setClassName(UIEntityType.fromType(planEntity.getEntityType()).apiStr());
                    entityStatsApiDTO.setRealtimeMarketReference(serviceEntityApiDTO);
                    final List<StatSnapshotApiDTO> statSnapshotsList = entityStats.getPlanEntityStats()
                            .getStatSnapshotsList()
                            .stream()
                            .map(statsMapper::toStatSnapshotApiDTO)
                            .collect(Collectors.toList());
                    entityStatsApiDTO.setStats(statSnapshotsList);
                    return entityStatsApiDTO;
                })
                .collect(Collectors.toList());

        if (response.hasPaginationResponse()) {
            return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                    .map(nextCursor -> paginationRequest.nextPageResponse(entityStatsList, nextCursor))
                    .orElseGet(() -> paginationRequest.finalPageResponse(entityStatsList));
        } else {
            return paginationRequest.allResultsResponse(entityStatsList);
        }
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
                    .setId(Long.parseLong(groupUuid))
                    .setEnforceUserScope(enforceUserScope)
                    .setExpectPresent(false)
                    .build();

            final GetMembersResponse groupMembersResp =
                    groupRpcService.getMembers(getGroupMembersReq);
            List<String> membersUuids = groupMembersResp.getMembers().getIdsList().stream()
                    .map(id -> Long.toString(id))
                    .collect(Collectors.toList());
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
     * Check if the policy type is merge data center from user input policy DTO.
     *
     * @param policyApiInputDTO
     * @return true if merge type is data center
     */
    private boolean isMergeDataCenterPolicy(final PolicyApiInputDTO policyApiInputDTO) {
        return policyApiInputDTO.getType() == PolicyType.MERGE
                && policyApiInputDTO.getMergeType() == MergePolicyType.DataCenter;
    }

    /**
     * Check if the policy type is merge data center from policy info DTO.
     *
     * @param policyInfo
     * @return true if merge type is data center
     */
    private boolean isMergeDataCenterPolicy(final PolicyInfo policyInfo) {
        return policyInfo.hasMerge()
                && policyInfo.getMerge().getMergeType() == MergeType.DATACENTER;
    }

    /**
     * Connect to the stats service.  We use a setter to avoid circular dependencies
     * in the Spring configuration in the API component.
     *
     * @param statsService the stats service.
     */
//    public void setStatsService(@Nonnull StatsService statsService) {
//        this.statsService = Objects.requireNonNull(statsService);
//    }

    /**
     * Handles invalid cases for the given startTime and endTime period and throws
     * IllegalArgumentException when those cases happen. The cases are:
     * <ul>
     * <li>If startTime is in the future.</li>
     * <li>If endTime precedes startTime.</li>
     * <li>If endTime only was passed.</li>
     * <ul/>
     *
     * @param startTime A DateTime string representing actions start Time query in the format
     *                  defined in {@link DateTimeUtil}
     * @param endTime   A DateTime string representing actions end Time query in the format
     *                  defined in {@link DateTimeUtil}
     */
    private void handleInvalidCases(@Nullable final String startTime,
                                    @Nullable final String endTime) {
        if (startTime != null && !startTime.isEmpty()) {
            Long startTimeLong = DateTimeUtil.parseTime(startTime);
            if (startTimeLong > DateTimeUtil.parseTime(DateTimeUtil.getNow())) {
                // startTime is in the future.
                throw new IllegalArgumentException("startTime " + startTime +
                        " can't be in the future");
            }
            if (endTime != null && !endTime.isEmpty()) {
                Long endTimeLong = DateTimeUtil.parseTime(endTime);
                if (endTimeLong < startTimeLong) {
                    // endTime is before startTime
                    throw new IllegalArgumentException("startTime " + startTime +
                            " must precede endTime " + endTime);
                }
            }
        } else if (endTime != null && !endTime.isEmpty()) {
            // endTime only was passed.
            throw new IllegalArgumentException("startTime is required along with endTime");
        }
    }

    /**
     * Adjusts endTime as part of the given {@link ActionApiInputDTO} if startTime was empty.
     *
     * @param inputDto The given {@link ActionApiInputDTO} that contains endTime to be adjusted
     */
    private static void adjustEndTime(ActionApiInputDTO inputDto) {
        if (inputDto.getStartTime() != null && !inputDto.getStartTime().isEmpty()) {
            if (inputDto.getEndTime() == null || inputDto.getEndTime().isEmpty()) {
                // Set endTime to now if it was null/empty and startTime was passed.
                inputDto.setEndTime(DateTimeUtil.getNow());
            }
        }
    }

    /**
     * Class to handle all the operation related to merge data center policy.
     */
    private class MergeDataCenterPolicyHandler {

        /**
         * Max char length of group name.
         */
        private final int MAX_GROUP_NAME_LENGTH = 255;

        /**
         * Create hidden group for data center list when user select merge data center policy. Since
         * data center is not kind of group, we need to create a hidden group to apply the policy, and
         * set the merge group members to the hidden group id.
         *
         * @param policyApiInputDTO policy data from user input, we need to change the merge group member
         *                          uuids to the hidden group uuid.
         */
        public void createDataCenterHiddenGroup(final PolicyApiInputDTO policyApiInputDTO) {
            final String groupName = StringUtils.abbreviate(String.format("Merge: %s",
                    Strings.join(fetchDataCenterNamesByOids(policyApiInputDTO.getMergeUuids()),
                            ',')), MAX_GROUP_NAME_LENGTH);
            // New hidden group creation request
            final GroupDTO.GroupInfo.Builder requestBuilder = GroupDTO.GroupInfo.newBuilder()
                    .setName(groupName)
                    .setEntityType(EntityType.DATACENTER.getNumber())
                    .setIsHidden(true);

            requestBuilder.setStaticGroupMembers(
                    GroupDTO.StaticGroupMembers.newBuilder().addAllStaticMemberOids(
                            policyApiInputDTO.getMergeUuids().stream()
                                    .map(Long::parseLong).collect(Collectors.toList())));

            final GroupDTO.CreateGroupResponse response = groupRpcService.createGroup(requestBuilder.build());

            logger.debug("Created new hidden group {} for data centers {}.",
                    response.getGroup().getId(),
                    policyApiInputDTO.getMergeUuids());
            // Set the hidden group uuid to the group member list
            policyApiInputDTO.setMergeUuids(Stream.of(response.getGroup().getId()).map(String::valueOf)
                    .collect(Collectors.toList()));

        }

        /**
         * Update hidden group for data center list when user select new merge data center policy. The
         * merge group member will still the hidden group.
         *
         * @param policyUuid        the policy uuid to fetch the current contained group.
         * @param policyApiInputDTO policy data from user input, we need to change the merge group member
         *                          uuids to the hidden group uuid.
         */
        public void updateDataCenterHiddenGroup(final String policyUuid,
                                                final PolicyApiInputDTO policyApiInputDTO) {
            final String groupName = StringUtils.abbreviate(String.format("Merge: %s",
                    Strings.join(fetchDataCenterNamesByOids(policyApiInputDTO.getMergeUuids()),
                            ',')), MAX_GROUP_NAME_LENGTH);
            try {
                // Fetch the current group members in the merge data center policy. It should only has
                // 1 hidden group that contains all the selected data centers.
                final PolicyApiDTO policyApiDTO = policiesService.getPolicyByUuid(policyUuid);
                final List<Long> oldPolicyGroupsList = policyApiDTO.getMergeGroups().stream()
                        .map(BaseApiDTO::getUuid)
                        .map(Long::parseLong)
                        .collect(Collectors.toList());

                // There should be only 1 hidden group in the policy member list, so this shouldn't happen.
                if (oldPolicyGroupsList.size() != 1) {
                    logger.error("More than 1 group member in merge data center policy {}, update" +
                                    " will be skipped.",
                            policyApiInputDTO.getPolicyName());
                    return;
                }
                final List<Long> currPolicyMemberList = policyApiInputDTO.getMergeUuids().stream()
                        .map(Long::parseLong)
                        .collect(Collectors.toList());

                final long groupId = oldPolicyGroupsList.get(0);
                // If group members don't change in the policy, then we skip the update.
                if (oldPolicyGroupsList.equals(currPolicyMemberList)) {
                    return;
                }

                // Update the hidden group if data centers in the policy have changed.
                final GroupDTO.GroupInfo.Builder newInfo = GroupDTO.GroupInfo.newBuilder()
                        .setName(groupName)
                        .setEntityType(EntityType.DATACENTER.getNumber())
                        .setIsHidden(true)
                        .setStaticGroupMembers(GroupDTO.StaticGroupMembers.newBuilder()
                                .addAllStaticMemberOids(currPolicyMemberList));

                final UpdateGroupResponse response = groupRpcService.updateGroup(UpdateGroupRequest.newBuilder()
                        .setId(groupId)
                        .setNewInfo(newInfo)
                        .build());

                logger.debug("Updated hidden group {} for data centers {}.",
                        response.getUpdatedGroup().getId(),
                        policyApiInputDTO.getMergeUuids());
                // Replace the data centers OIDs to the hidden group OID.
                policyApiInputDTO.setMergeUuids(Stream.of(String.valueOf(groupId))
                        .collect(Collectors.toList()));

            } catch (Exception e) {
                logger.error("Error when updating hidden group, {}", e);
            }
        }

        /**
         * Delete the hidden group when user deletes the merge data center policy.
         *
         * @param policyUuid the deleted policy uuid.
         * @param policyInfo the info of the removed policy.
         */
        public void deleteDataCenterHiddenGroup(final String policyUuid, final PolicyInfo policyInfo) {
            policyInfo.getMerge().getMergeGroupIdsList().stream()
                    .map(groupId -> GroupID.newBuilder().setId(groupId).build())
                    .forEach(groupRpcService::deleteGroup);
            logger.debug("Deleted hidden group for policy {}.", policyUuid);
        }

        /**
         * Fetch data center names according to their OIDs.
         *
         * @param dataCenterIds
         * @return list of data center names.
         */
        private List<String> fetchDataCenterNamesByOids(final List<String> dataCenterIds) {
            return repositoryApi.entitiesRequest(dataCenterIds.stream()
                    .map(Long::parseLong)
                    .collect(Collectors.toSet()))
                .getMinimalEntities()
                .map(MinimalEntity::getDisplayName)
                .collect(Collectors.toList());
        }
    }
}
