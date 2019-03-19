package com.vmturbo.api.component.external.api.service;

import java.nio.file.AccessDeniedException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.SearchMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.TagsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.constraints.ConstraintApiDTO;
import com.vmturbo.api.constraints.ConstraintApiInputDTO;
import com.vmturbo.api.controller.GroupsController;
import com.vmturbo.api.controller.MarketsController;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IEntitiesService;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterForEntityRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterForEntityResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.CommonDTOREST.GroupDTO.ConstraintType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

public class EntitiesService implements IEntitiesService {
    // these two constants are used to create hateos links for getEntitiesByUuid only.
    // TODO: discuss hateos with PM.  If we decide to support them, these constants
    // and the utility methods should go to their own class.  If not, these constants
    // and the utility methods should be removed.
    private final static String UUID = "{uuid}";
    private final static String REPLACEME = "#REPLACEME";

    public final static String PRICE_INDEX_COMMODITY = "priceIndex";

    Logger logger = LogManager.getLogger();

    private final ActionsServiceBlockingStub actionOrchestratorRpcService;

    private final ActionSpecMapper actionSpecMapper;

    private final long realtimeTopologyContextId;

    private final SupplyChainFetcherFactory supplyChainFetcher;

    private final PaginationMapper paginationMapper;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final GroupServiceBlockingStub groupServiceClient;

    private final EntityAspectMapper entityAspectMapper;

    private final TopologyProcessor topologyProcessor;

    private final EntitySeverityServiceBlockingStub entitySeverityService;

    private final StatsService statsService;

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final UuidMapper uuidMapper;

    private final StatsHistoryServiceBlockingStub statsHistoryService;

    // Entity types which are not part of Host or Storage Cluster.
    private static final ImmutableSet<String> NON_CLUSTER_ENTITY_TYPES =
            ImmutableSet.of(
                    UIEntityType.CHASSIS.getValue(),
                    UIEntityType.DATACENTER.getValue(),
                    UIEntityType.DISKARRAY.getValue(),
                    UIEntityType.LOGICALPOOL.getValue(),
                    UIEntityType.STORAGECONTROLLER.getValue());

    /**
     * When traversing the entities in a supply chain in the UI, the breadcrumb is
     * used to navigate back to a Cluster an entity belongs to.
     * The UI breadcrumb path is of the form:
     * DataCenterName|ChassisName/ClusterName/EntityName
     * For Cloud, we use Region in-place of Cluster and the path is of the form:
     * RegionName/EntityName
     * Entity types missing in the map are given the highest value(lowest precedence).
     * This map defines the precedence to be used while sorting the result to
     * create the correct breadcrumb path.
     */
    private static final Map<String, Integer> BREADCRUMB_ENTITY_PRECEDENCE_MAP =
            ImmutableMap.of(
                    UIEntityType.REGION.getValue(), 1,
                    UIEntityType.AVAILABILITY_ZONE.getValue(), 2,
                    UIEntityType.DATACENTER.getValue(), 2,
                    UIEntityType.CHASSIS.getValue(), 2,
                    ConstraintType.CLUSTER.toString(), 3);

    public EntitiesService(
            @Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
            @Nonnull final ActionSpecMapper actionSpecMapper,
            final long realtimeTopologyContextId,
            @Nonnull final SupplyChainFetcherFactory supplyChainFetcher,
            @Nonnull final PaginationMapper paginationMapper,
            @Nonnull final SearchServiceBlockingStub searchServiceRpc,
            @Nonnull final GroupServiceBlockingStub groupServiceClient,
            @Nonnull final EntityAspectMapper entityAspectMapper,
            @Nonnull final TopologyProcessor topologyProcessor,
            @Nonnull final EntitySeverityServiceBlockingStub entitySeverityService,
            @Nonnull final StatsService statsService,
            @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
            @Nonnull final UuidMapper uuidMapper,
            @Nonnull final StatsHistoryServiceBlockingStub statsHistoryService) {
        this.actionOrchestratorRpcService = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.supplyChainFetcher = Objects.requireNonNull(supplyChainFetcher);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.entityAspectMapper = Objects.requireNonNull(entityAspectMapper);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.entitySeverityService = Objects.requireNonNull(entitySeverityService);
        this.statsService = Objects.requireNonNull(statsService);
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.statsHistoryService = Objects.requireNonNull(statsHistoryService);
    }

    @Override
    public ServiceEntityApiDTO getEntities() throws Exception {
        final ServiceEntityApiDTO result = new ServiceEntityApiDTO();
        result.add(
            generateLinkTo(
                ControllerLinkBuilder.methodOn(MarketsController.class).getEntitiesByMarketUuid(REPLACEME),
                "Market Entities"));
        result.add(
            generateLinkTo(
                ControllerLinkBuilder.methodOn(GroupsController.class).getEntitiesByGroupUuid(REPLACEME),
                "Group entities"));
        return result;
    }

    @Override
    @Nonnull
    public ServiceEntityApiDTO getEntityByUuid(@Nonnull String uuid, boolean includeAspects)
            throws Exception {
        // get information about this entity from the repository
        final long oid = Long.valueOf(Objects.requireNonNull(uuid));
        final TopologyEntityDTO entityAsTopologyEntityDTO = getTopologyEntityDTO(oid);
        final ServiceEntityApiDTO result =
            ServiceEntityMapper.toServiceEntityApiDTO(
                entityAsTopologyEntityDTO,
                includeAspects ? entityAspectMapper : null);

        // fetch information about the discovering target
        if (entityAsTopologyEntityDTO.hasOrigin()
                && entityAsTopologyEntityDTO.getOrigin().hasDiscoveryOrigin()
                && entityAsTopologyEntityDTO
                        .getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsCount() != 0) {
            // get the target that appears first in the list of discovering targets
            final long targetId =
                entityAsTopologyEntityDTO.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIds(0);
            try {
                final TargetApiDTO target = new TargetApiDTO();
                target.setUuid(Long.toString(targetId));
                final TargetInfo targetInfo = topologyProcessor.getTarget(targetId);
                target.setDisplayName(
                    TargetData.getDisplayName(targetInfo).orElseGet(() -> {
                        logger.warn(
                            "Cannot find the display name of target with id {}" +
                                " as part of the request to get entity with id {} and name {}",
                            target::getUuid, () -> oid, result::getDisplayName);
                        return "";
                    }));
                final long probeId = targetInfo.getProbeId();
                try {
                    final ProbeInfo probeInfo = topologyProcessor.getProbe(probeId);
                    target.setType(probeInfo.getType());
                } catch (CommunicationException | TopologyProcessorException e) {
                    // fetching information about the discovering probe failed
                    // this information will not be added to the result
                    // the failure will otherwise be ignored
                    logger.warn(
                        "Failure to get information about probe with id {}" +
                                " as part of the request to get entity with id {} and name {}: {}",
                            () -> probeId, () -> oid, result::getDisplayName, e::toString);
                }
                result.setDiscoveredBy(target);
            } catch (CommunicationException | IllegalArgumentException | TopologyProcessorException e) {
                // fetching information about the target failed
                // this information will not be added to the result
                // the failure will otherwise be ignored
                logger.warn(
                    "Failure to get information about target with id {}" +
                            " as part of the request to get entity with id {} and name {}: {}",
                    () -> targetId, () -> oid, result::getDisplayName, e::toString);
            }
        }

        // fetch all consumers
        try {
            doForNeighbors(oid, TraversalDirection.PRODUCES, result::setConsumers);
        } catch (StatusRuntimeException e) {
            // fetching consumers failed
            // there will be no consumers in the result
            // the failure will otherwise be ignored
            logger.warn(
                "Cannot get the consumers of entity with id {} and name {}: {}",
                () -> oid, result::getDisplayName, e::toString);
        }

        // fetch all providers
        try {
            doForNeighbors(oid, TraversalDirection.CONSUMES, result::setProviders);
        } catch (StatusRuntimeException e) {
            // fetching consumers failed
            // there will be no consumers in the result
            // the failure will otherwise be ignored
            logger.warn(
                "Cannot get the producers of entity with id {} and name {}: {}",
                () -> oid, result::getDisplayName, e::toString);
        }

        // fetch entity severity
        SeverityPopulator.populate(
            entitySeverityService, realtimeTopologyContextId, Collections.singletonList(result));

        // fetch price index
        fetchAndSetPriceIndex(oid, result);

        return result;
    }

    private void fetchAndSetPriceIndex(final long oid, final ServiceEntityApiDTO result) {
        try {
            // fetch the first page of stats for this entity
            final GetEntityStatsResponse entityStatsResponse =
                statsHistoryService.getEntityStats(
                    GetEntityStatsRequest.newBuilder()
                        .setScope(
                            EntityStatsScope.newBuilder()
                                .setEntityList(EntityList.newBuilder().addEntities(oid)).build())
                        .setFilter(
                            StatsFilter.newBuilder()
                                .addCommodityRequests(
                                    CommodityRequest.newBuilder()
                                        .setCommodityName(PRICE_INDEX_COMMODITY).build())
                                .build())
                        .build());

            // read the first stats snapshot, if it exists
            if (entityStatsResponse.getEntityStatsCount() == 0 ||
                    entityStatsResponse.getEntityStats(0).getStatSnapshotsCount() == 0) {
                throw new OperationFailedException("No entity stats were returned");
            }
            if (entityStatsResponse.getEntityStats(0).getOid() != oid) {
                throw
                    new OperationFailedException(
                        "Erroneous stat record; refers to oid " +
                            entityStatsResponse.getEntityStats(0).getOid());
            }
            result.setPriceIndex(
                entityStatsResponse
                    .getEntityStats(0).getStatSnapshots(0).getStatRecordsList().stream()
                    .filter(x -> x.getName().equals(PRICE_INDEX_COMMODITY))
                    .findAny()
                    .map(StatRecord::getCurrentValue)
                    .orElseThrow(() -> new OperationFailedException("Cannot find price index")));
        } catch (StatusRuntimeException | OperationFailedException e) {
            // fetching price index failed
            // there will be no price index in the result
            // the failure will otherwise be ignored
            logger.warn(
                "Cannot get the price index of entity with id {} and name {}: {}",
                () -> oid, result::getDisplayName, e::toString);
        }
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityUuid(String uuid,
                                                         String encodedQuery) throws Exception {
        return statsService.getStatsByEntityUuid(uuid, encodedQuery);
    }

    @Override
    public List<LogEntryApiDTO> getNotificationsByEntityUuid(String uuid,
                                                             String starttime,
                                                             String endtime,
                                                             String category) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LogEntryApiDTO getNotificationByEntityUuid(String uuid,
                                                      String notificationUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Fetch a list of {@link ActionApiDTO} for the Actions generated for the given Service Entity uuid.
     *
     * The request for the {@link ActionSpec} objects from the Action Orchestrator for the given uuid. Map each
     * ActionSpec into the corresponding {@link ActionApiDTO}. This requires calls to the Repository component
     * to look up the displayName() for the corresponding entity, since the Action Orchestrator does not track that information.
     *
     * Note that the historical parameters are ignored. There is currently no history in the Action Orchestrator.
     *
     * Note that the filtering parameters are ignored. It would be relatively easy to implement these filters.
     *
     * @param uuid the unique id of Service Entity for which we are requesting actions.
     * @param inputDto A description of filter options on which actions to fetch.
     * @return a list of ActionApiDTOs for the Service Entity indicated by the given uuid.
     * @throws Exception if there is a communication exception
     */
    @Override
    public ActionPaginationResponse getActionsByEntityUuid(String uuid,
                                       ActionApiInputDTO inputDto,
                                       ActionPaginationRequest paginationRequest) throws Exception {
        // The search will be on a long value, not a String
        final long entityId = Long.valueOf(uuid);

        final ActionQueryFilter filter = actionSpecMapper.createActionFilter(
                                     inputDto, Optional.of(Collections.singleton(entityId)));

        final FilteredActionResponse response = actionOrchestratorRpcService.getAllActions(
                FilteredActionRequest.newBuilder()
                        .setTopologyContextId(realtimeTopologyContextId)
                        .setFilter(filter)
                        .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                        .build());

        final List<ActionApiDTO> results = actionSpecMapper.mapActionSpecsToActionApiDTOs(
            response.getActionsList().stream()
                .map(ActionOrchestratorAction::getActionSpec)
                .collect(Collectors.toList()), realtimeTopologyContextId);
        return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                .map(nextCursor -> paginationRequest.nextPageResponse(results, nextCursor))
                .orElseGet(() -> paginationRequest.finalPageResponse(results));
    }

    @Override
    @Nonnull
    public ActionApiDTO getActionByEntityUuid(@Nonnull String uuid, @Nonnull String aUuid)
            throws Exception {
        // remark: the first parameter is completely ignored.
        // an entity id is not needed to get an action by its id.
        try {
            return
                actionSpecMapper.mapActionSpecToActionApiDTO(
                    actionOrchestratorRpcService
                        .getAction(
                            SingleActionRequest.newBuilder()
                                .setActionId(Long.valueOf(Objects.requireNonNull(aUuid)))
                                .setTopologyContextId(realtimeTopologyContextId)
                                .build())
                        .getActionSpec(),
                    realtimeTopologyContextId);
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Override
    public List<PolicyApiDTO> getPoliciesByEntityUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Get the list of settings by entity.
     *
     * @param uuid The uuid of the entity.
     * @param includePolicies Include the group aspects in the response
     * @return the list of settings for the entity.
     * @throws Exception
     */
    @Override
    public List<SettingsManagerApiDTO> getSettingsByEntityUuid(String uuid, boolean includePolicies) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityQuery(String uuid,
                                              StatPeriodApiInputDTO inputDto) throws Exception {
        return statsService.getStatsByEntityQuery(uuid, inputDto);
    }

    @Override
    public List<BaseApiDTO> getGroupsByUuid(String uuid,
                                            Boolean path) throws Exception {
        if (!path) {
            // TODO: Get all groups which the service entity is member of
            throw ApiUtils.notImplementedInXL();
        }

        // Steps:
        // Get the supply chain for this entity.
        // If this entity is above the host in the supply-chain, then get the host it is connected to.
        // From the hostId, get the Cluster that the host belongs to.
        // If entity is Storage Device, check if it belongs to a Storage Cluster.
        // For entities below the host(e.g DC/Chassis/Storage etc..), there is no cluster
        // to fetch. So just return the entity.

        // We get the entire supply chain for the entity instead of only the entities which we are
        // interested in(BREADCRUMB_ENTITY_PRECEDENCE_MAP + host + storage + entityType of uuid). This
        // is because we don't know the entity type of the supplied uuid. If the entity is something
        // like DataCenter etc, the SupplyChain request will pull in lot of unnecessary entities which
        // may cause memory issue. It can be optimized by first querying the entityType for the uuid,
        // and then getting only the interested entities in the supply chain. So the trade-off is between
        // 2 rpc calls(latency) vs memory consumption. For now, we will have one single call as its simpler.
        // If the memory usage of pulling in the entire supply chain of the entity is deemed high, then
        // we can change it to the 2 rpc call approach.
        final SupplychainApiDTO supplyChain = supplyChainFetcher.newApiDtoFetcher()
                .topologyContextId(realtimeTopologyContextId)
                .addSeedUuids(Lists.newArrayList(uuid))
                .includeHealthSummary(false)
                .entityDetailType(EntityDetailType.entity)
                .fetch();
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap = supplyChain.getSeMap().entrySet().stream()
                .map(Entry::getValue)
                .flatMap(supplyChainEntryDTO -> supplyChainEntryDTO.getInstances().values().stream())
                .collect(Collectors.toMap(apiDTO -> Long.valueOf(apiDTO.getUuid()), Function.identity()));

        // extract the entities which we are interested in
        List<BaseApiDTO> result = serviceEntityMap.values()
                .stream()
                .filter(serviceEntityApiDTO ->
                        BREADCRUMB_ENTITY_PRECEDENCE_MAP.keySet().contains(serviceEntityApiDTO.getClassName())
                        || serviceEntityApiDTO.getUuid().equals(uuid))
                .collect(Collectors.toList());

        long entityOid = Long.valueOf(uuid);
        long oidToQuery = 0;
        // If the entity is of type STORAGE, get its oid. Else find the oid of the Host the entity is
        // connected to. For entities below the host such as DC, Chassis etc, the oidToQuery will be
        // set to 0.
        if (serviceEntityMap.get(entityOid).getClassName().equals(UIEntityType.STORAGE.getValue())) {
            oidToQuery = entityOid;
        }  else if (!NON_CLUSTER_ENTITY_TYPES.contains(serviceEntityMap.get(entityOid).getClassName())) {
            for (Entry<Long, ServiceEntityApiDTO> entry : serviceEntityMap.entrySet()) {
                ServiceEntityApiDTO serviceEntityApiDTO = entry.getValue();
                if (serviceEntityApiDTO.getClassName().equals(UIEntityType.PHYSICAL_MACHINE.getValue())) {
                    oidToQuery = Long.valueOf(serviceEntityApiDTO.getUuid());
                }
            }
        }

        // fetch the hostId/storageId -> ClusterName from Group component.
        if (oidToQuery != 0) {
            GetClusterForEntityResponse response =
                    groupServiceClient.getClusterForEntity(GetClusterForEntityRequest.newBuilder()
                            .setEntityId(oidToQuery)
                            .build());
            if (response.hasClusterInfo()) {
                ClusterInfo cluster = response.getClusterInfo();
                final ServiceEntityApiDTO serviceEntityApiDTO = new ServiceEntityApiDTO();
                serviceEntityApiDTO.setDisplayName(cluster.getDisplayName());
                serviceEntityApiDTO.setUuid(Long.toString(response.getClusterId()));
                serviceEntityApiDTO.setClassName(ConstraintType.CLUSTER.name());
                // Insert the clusterRecord before the Entity record.
                result.add(serviceEntityApiDTO);
            }
        }
        // the UI breadcrumb path is of the form:
        // DataCenterName|ChassisName/ClusterName/EntityName
        // For Cloud, we use Region in-place of Cluster and the path is of the form:
        // AvailabilityZone/RegionName/EntityName
        // Sort the result to create the correct path.
        result.sort((dto1, dto2) ->
                BREADCRUMB_ENTITY_PRECEDENCE_MAP.getOrDefault(dto1.getClassName(), Integer.MAX_VALUE)
                        .compareTo(BREADCRUMB_ENTITY_PRECEDENCE_MAP.getOrDefault(dto2.getClassName(),
                                Integer.MAX_VALUE)));
        return result;
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(
            @Nonnull String uuid, @Nonnull ActionApiInputDTO inputDto) throws Exception {
        try {
            final ApiId apiId = uuidMapper.fromUuid(uuid);
            return
                actionStatsQueryExecutor
                    .retrieveActionStats(
                        ImmutableActionStatsQuery.builder()
                            .addScopes(apiId)
                            .actionInput(inputDto)
                            .build())
                    .get(apiId);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                return Collections.emptyList();
            } else {
                throw ExceptionMapper.translateStatusException(e);
            }
        } catch (NumberFormatException e) {
            // TODO Remove it when default Cloud group is supported
            return Collections.emptyList();
        }
    }

    @Override
    public List<StatSnapshotApiDTO> getNotificationCountStatsByUuid(final String s,
                                    final ActionApiInputDTO actionApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<TagApiDTO> getTagsByEntityUuid(final String s) throws Exception {
        return
            TagsMapper
                .convertTagsToApi(
                    getTopologyEntityDTO(Long.valueOf(Objects.requireNonNull(s))).getTagsMap());
    }

    @Override
    public List<TagApiDTO> createTagByEntityUuid(final String s, final TagApiDTO tagApiDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteTagByEntityUuid(final String s, final String s1) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteTagsByEntityUuid(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public boolean loggingEntities(final List<String> arrayList) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public boolean loggingEntities() {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<SettingsPolicyApiDTO> getSettingPoliciesByEntityUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Map<String, EntityAspect> getAspectsByEntityUuid(String uuid) throws UnauthorizedObjectException, UnknownObjectException {
        return entityAspectMapper.getAspectsByEntity(getTopologyEntityDTO(uuid));
    }

    @Override
    public EntityAspect getAspectByEntityUuid(String uuid, String aspectTag) throws UnauthorizedObjectException, UnknownObjectException {
        return entityAspectMapper.getAspectByEntity(getTopologyEntityDTO(uuid), aspectTag);
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityUuidAspect(String uuid, String aspectTag, String encodedQuery) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityUuidAspectQuery(String uuid, String aspectTag, StatPeriodApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ActionApiDTO> getCurrentActionsByEntityUuidAspect(String uuid, String aspectTag) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ActionApiDTO> getActionsByEntityUuidAspect(String uuid, String aspectTag, ActionApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ConstraintApiDTO> getConstraintsByEntityUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ServiceEntityApiDTO> getPotentialEntitiesByEntity(String uuid, ConstraintApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Get TopologyEntityDTO based on provided oid.
     */
    private TopologyEntityDTO getTopologyEntityDTO(@Nonnull String uuid) throws UnknownObjectException {
        List<TopologyEntityDTO> entities = searchServiceRpc.searchTopologyEntityDTOs(
                SearchTopologyEntityDTOsRequest.newBuilder()
                        .addEntityOid(Long.valueOf(uuid))
                        .build()).getTopologyEntityDtosList();
        if (entities.size() > 1) {
            throw new UnknownObjectException("Found " + entities.size() + " entities of same id: " + uuid);
        }
        if (entities.size() == 0) {
            throw new UnknownObjectException("Entity: " + uuid + " not found");
        }
        return entities.get(0);
    }

    private static Link generateLinkTo(@Nonnull Object invocationValue, @Nonnull String relation) {
        final String url =
            ControllerLinkBuilder.linkTo(invocationValue)
                .toString()
                .replace("http://", "https://")
                .replace(REPLACEME, UUID);
        return new Link(url).withRel(relation);
    }

    /**
     * Fetch an entity in {@link TopologyEntityDTO} form, given its oid.
     *
     * @param oid the oid of the entity to fetch.
     * @return the entity.
     * @throws UnknownObjectException if the entity was not found.
     * @throws OperationFailedException if the operation failed.
     * @throws UnauthorizedObjectException if user does not have proper access privileges.
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws AccessDeniedException if user is properly authenticated.
     */
    @Nonnull
    private TopologyEntityDTO getTopologyEntityDTO(long oid)
            throws Exception {
        // get information about this entity from the repository
        final SearchTopologyEntityDTOsResponse searchTopologyEntityDTOsResponse;
        try {
            searchTopologyEntityDTOsResponse =
                searchServiceRpc.searchTopologyEntityDTOs(
                    SearchTopologyEntityDTOsRequest.newBuilder()
                        .addEntityOid(oid)
                        .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
        if (searchTopologyEntityDTOsResponse.getTopologyEntityDtosCount() == 0) {
            final String message = "Error fetching entity with uuid: " + oid;
            logger.error(message);
            throw new UnknownObjectException(message);
        }
        return searchTopologyEntityDTOsResponse.getTopologyEntityDtos(0);
    }

    /**
     * Common code for fetching the neighbors of an entity (using a {@link TraversalDirection}, and
     * performing a computation on them.
     *
     * @param oid the oid of the entity.
     * @param traversalDirection the traversal direction.
     * @param code computation to execute.
     * @throws StatusRuntimeException thrown by the internal gRPC call to the repository.
     */
    private void doForNeighbors(
            long oid,
            @Nonnull TraversalDirection traversalDirection,
            @Nonnull Consumer<List<BaseApiDTO>> code)
            throws StatusRuntimeException {
        final List<TopologyEntityDTO> neighbors =
            searchServiceRpc.searchTopologyEntityDTOs(
                    SearchTopologyEntityDTOsRequest.newBuilder()
                        .addSearchParameters(SearchMapper.neighbors(oid, traversalDirection))
                        .build())
                .getTopologyEntityDtosList();
        code.accept(
            neighbors.stream()
                .map(t -> {
                    final BaseApiDTO baseApiDTO = new BaseApiDTO();
                    ServiceEntityMapper.setBasicFields(baseApiDTO, t);
                    return baseApiDTO;
                })
                .collect(Collectors.toList())
        );
    }
}


