package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ConstraintsMapper;
import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.TagsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
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
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.RelationType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IEntitiesService;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterForEntityRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterForEntityResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingToPolicyName;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTOREST.GroupDTO.ConstraintType;

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

    private final SeverityPopulator severityPopulator;

    private final StatsService statsService;

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final UuidMapper uuidMapper;

    private final StatsHistoryServiceBlockingStub statsHistoryService;

    private final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub;

    private final SettingServiceBlockingStub settingServiceBlockingStub;

    private final SettingsMapper settingsMapper;

    private final ActionSearchUtil actionSearchUtil;

    private final RepositoryApi repositoryApi;

    // Entity types which are not part of Host or Storage Cluster.
    private static final ImmutableSet<String> NON_CLUSTER_ENTITY_TYPES =
            ImmutableSet.of(
                    UIEntityType.CHASSIS.apiStr(),
                    UIEntityType.DATACENTER.apiStr(),
                    UIEntityType.DISKARRAY.apiStr(),
                    UIEntityType.LOGICALPOOL.apiStr(),
                    UIEntityType.STORAGECONTROLLER.apiStr());

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
                    UIEntityType.REGION.apiStr(), 1,
                    UIEntityType.AVAILABILITY_ZONE.apiStr(), 2,
                    UIEntityType.DATACENTER.apiStr(), 2,
                    UIEntityType.CHASSIS.apiStr(), 2,
                    ConstraintType.CLUSTER.toString(), 3);

    private static final Set<String> BREADCRUMB_ENTITIES_TO_FETCH =
            new HashSet<>(Arrays.asList(
                    UIEntityType.REGION.apiStr(),
                    UIEntityType.AVAILABILITY_ZONE.apiStr(),
                    UIEntityType.DATACENTER.apiStr(),
                    UIEntityType.CHASSIS.apiStr(),
                    UIEntityType.PHYSICAL_MACHINE.apiStr()));

    public EntitiesService(
            @Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
            @Nonnull final ActionSpecMapper actionSpecMapper,
            final long realtimeTopologyContextId,
            @Nonnull final SupplyChainFetcherFactory supplyChainFetcher,
            @Nonnull final PaginationMapper paginationMapper,
            @Nonnull final SearchServiceBlockingStub searchServiceRpc,
            @Nonnull final GroupServiceBlockingStub groupServiceClient,
            @Nonnull final EntityAspectMapper entityAspectMapper,
            @Nonnull final SeverityPopulator severityPopulator,
            @Nonnull final StatsService statsService,
            @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
            @Nonnull final UuidMapper uuidMapper,
            @Nonnull final StatsHistoryServiceBlockingStub statsHistoryService,
            @Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub,
            @Nonnull final SettingServiceBlockingStub settingServiceBlockingStub,
            @Nonnull final SettingsMapper settingsMapper,
            @Nonnull final ActionSearchUtil actionSearchUtil,
            @Nonnull final RepositoryApi repositoryApi) {
        this.actionOrchestratorRpcService = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.supplyChainFetcher = Objects.requireNonNull(supplyChainFetcher);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.settingPolicyServiceBlockingStub = Objects.requireNonNull(settingPolicyServiceBlockingStub);
        this.settingServiceBlockingStub = Objects.requireNonNull(settingServiceBlockingStub);
        this.entityAspectMapper = Objects.requireNonNull(entityAspectMapper);
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.statsService = Objects.requireNonNull(statsService);
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.statsHistoryService = Objects.requireNonNull(statsHistoryService);
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.actionSearchUtil = Objects.requireNonNull(actionSearchUtil);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
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
        final long oid = Long.valueOf(uuid);
        final ServiceEntityApiDTO result =
            repositoryApi.getServiceEntityById(oid, includeAspects ? entityAspectMapper : null);

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
        severityPopulator.populate(realtimeTopologyContextId, Collections.singletonList(result));

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
     * @param uuid the unique id of Service Entity for which we are requesting actions.
     * @param inputDto A description of filter options on which actions to fetch.
     * @return a list of ActionApiDTOs for the Service Entity indicated by the given uuid.
     * @throws Exception if there is a communication exception
     */
    @Override
    public ActionPaginationResponse getActionsByEntityUuid(String uuid,
                                       ActionApiInputDTO inputDto,
                                       ActionPaginationRequest paginationRequest) throws Exception {
        return
            actionSearchUtil.getActionsByEntityUuids(
                Collections.singleton(Long.valueOf(uuid)), inputDto, paginationRequest);
    }

    @Override
    @Nonnull
    public ActionApiDTO getActionByEntityUuid(@Nonnull String uuid, @Nonnull String aUuid)
            throws Exception {
        final ActionApiDTO result;
        try {
            // get the action object from the action orchestrator
            // and translate it to an ActionApiDTO object
            result =
                actionSpecMapper.mapActionSpecToActionApiDTO(
                    actionOrchestratorRpcService
                        .getAction(
                            SingleActionRequest.newBuilder()
                                .setActionId(Long.valueOf(aUuid))
                                .setTopologyContextId(realtimeTopologyContextId)
                                .build())
                        .getActionSpec(),
                    realtimeTopologyContextId);
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
        // check if the passed in entity uuid is related to the action
        if (!isRelatedAction(uuid, result)) {
            throw new IllegalArgumentException(String.format("Entity %s in the query is not " +
                "related to the action %s.", uuid, aUuid));
        }
        return result;
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
        GetEntitySettingsRequest request =
                GetEntitySettingsRequest.newBuilder()
                        .setSettingFilter(EntitySettingFilter.newBuilder()
                                .addEntities(Long.valueOf(uuid))
                                .setIncludeSettingPolicies(includePolicies)
                                .build())
                        .build();

        final Optional<SettingsForEntity> entitySettings = SettingDTOUtil.flattenEntitySettings(
            settingPolicyServiceBlockingStub.getEntitySettings(request))
                .filter(settingsForEntity -> settingsForEntity.hasEntityId()
                    && settingsForEntity.getEntityId()==Long.valueOf(uuid))
                .findFirst();

        if (!entitySettings.isPresent()) {
            return Collections.emptyList();
        }

        // Mapping from SettingSpecName -> List of settings of that spec type.
        Map<String, List<SettingToPolicyName>> settingSpecNameToSettingsMap =
                entitySettings.get().getSettingsList()
                        .stream()
                        .collect(Collectors.groupingBy(settingToPolicyName ->
                                settingToPolicyName.getSetting().getSettingSpecName()));

        final Iterable<SettingSpec> specIt = () -> settingServiceBlockingStub.searchSettingSpecs(
                SearchSettingSpecsRequest.newBuilder()
                        .addAllSettingSpecName(settingSpecNameToSettingsMap.keySet())
                        .build());

        final Map<String, SettingSpec> specs = StreamSupport.stream(specIt.spliterator(), false)
                .collect(Collectors.toMap(SettingSpec::getName, Function.identity()));

        return settingsMapper.toManagerDtos(settingSpecNameToSettingsMap, specs);
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

        long entityOid = Long.valueOf(uuid);
        Optional<ServiceEntityApiDTO> entityApiDTO =
                repositoryApi.getServiceEntitiesById(
                        ServiceEntitiesRequest.newBuilder(ImmutableSet.of(entityOid))
                                .build())
                .getOrDefault(entityOid, Optional.empty());


        if (!entityApiDTO.isPresent()) {
            logger.warn("No entity dto found for entity with oid: {}", entityOid);
            return Collections.emptyList();
        }

        long oidToQuery = 1;
        List<BaseApiDTO> result = new ArrayList<>();
        result.add(entityApiDTO.get());

        // Skip supply-chain call for entities which are not consumers(direct/in-direct) of host(PM).
        if (entityApiDTO.get().getClassName().equals(UIEntityType.STORAGE.apiStr())) {
            oidToQuery = entityOid;
        } else if (!NON_CLUSTER_ENTITY_TYPES.contains(entityApiDTO.get().getClassName())) {

            // We are only interested in entities of type : in BREADCRUMB_ENTITIES_TO_FETCH
            // type of the input entity.
            Set<String> entityTypesToFetch = new HashSet<>(BREADCRUMB_ENTITIES_TO_FETCH);
            entityTypesToFetch.add(entityApiDTO.get().getClassName());

            final SupplychainApiDTO supplyChain = supplyChainFetcher.newApiDtoFetcher()
                    .topologyContextId(realtimeTopologyContextId)
                    .addSeedUuids(Lists.newArrayList(uuid))
                    .entityTypes(entityTypesToFetch.stream().collect(Collectors.toList()))
                    .includeHealthSummary(false)
                    .entityDetailType(EntityDetailType.entity)
                    .fetch();

            final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
                    supplyChain.getSeMap().entrySet().stream()
                            .map(Entry::getValue)
                            .flatMap(supplyChainEntryDTO -> supplyChainEntryDTO.getInstances().values().stream())
                            .collect(Collectors.toMap(apiDTO -> Long.valueOf(apiDTO.getUuid()), Function.identity()));

            // Remove PM(Host) entity type from the result if the entity being searched for is not PM.
            result = serviceEntityMap.values()
                    .stream()
                    .filter(serviceEntityApiDTO ->
                            !UIEntityType.PHYSICAL_MACHINE.apiStr().equals(serviceEntityApiDTO.getClassName())
                                    || entityApiDTO.get().getClassName().equals(UIEntityType.PHYSICAL_MACHINE.apiStr()))
                    .collect(Collectors.toList());

            // Find the oid of the Host the entity is connected to.
            for (Entry<Long, ServiceEntityApiDTO> entry : serviceEntityMap.entrySet()) {
                ServiceEntityApiDTO serviceEntityApiDTO = entry.getValue();
                if (serviceEntityApiDTO.getClassName().equals(UIEntityType.PHYSICAL_MACHINE.apiStr())) {
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
            if (response.hasCluster()) {
                final ServiceEntityApiDTO serviceEntityApiDTO = new ServiceEntityApiDTO();
                serviceEntityApiDTO.setDisplayName(GroupProtoUtil.getGroupDisplayName(response.getCluster()));
                serviceEntityApiDTO.setUuid(Long.toString(response.getCluster().getId()));
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
            TagsMapper.convertTagsToApi(
                    repositoryApi.getTopologyEntityDTO(Long.valueOf(s)).getTags().getTagsMap());
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
        GetEntitySettingPoliciesRequest request =
                GetEntitySettingPoliciesRequest.newBuilder()
                        .setEntityOid(Long.valueOf(uuid))
                        .build();

        GetEntitySettingPoliciesResponse response =
                settingPolicyServiceBlockingStub.getEntitySettingPolicies(request);

        return settingsMapper.convertSettingPolicies(response.getSettingPoliciesList());
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

        Optional<String> myEntityType = getEntityType(uuid, supplyChain);
        // Get the consumers of this entity.
        final Set<String> consumerOids = new HashSet<>();
        if (myEntityType.isPresent()) {
            SupplychainEntryDTO supplychainEntryDTO = supplyChain.getSeMap().get(myEntityType.get());
            if (supplychainEntryDTO != null) {
                Set<String> consumerTypes = supplychainEntryDTO.getConnectedConsumerTypes();
                if (consumerTypes != null){
                    consumerOids.addAll(consumerTypes.stream()
                            .map(type -> supplyChain.getSeMap().get(type))
                            .filter(Objects::nonNull)
                            .map(dto -> dto.getInstances())
                            .filter(Objects::nonNull)
                            .flatMap(serviceEntityApiMap -> serviceEntityApiMap.keySet().stream())
                            .collect(Collectors.toSet()));
                }
            }
        }

        // Now query repo to get the TopologyEntityDTO for the current entity and its consumers.
        List<ConstraintApiDTO> constraintApiDtos = new ArrayList<>();
        List<String> oidsToQuery = new ArrayList<>();
        oidsToQuery.addAll(consumerOids);
        oidsToQuery.add(uuid);

        // The constraints are embedded in the commodities. We have to fetch the TopologyEntityDTO
        // to get the commodities info.
        Map<Long, TopologyEntityDTO> entityDtos =
                getTopologyEntityDTOs(oidsToQuery).stream()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        // We don't have to query the providers as we already have the provider info inside
        // the commoditiesBought object.
        // We don't support fully parity with classic. We just populate the ConstraintType(CommodityDTO.Type)
        // and the ConstraintKey(CommodityDTO.Key)
        constraintApiDtos.addAll(ConstraintsMapper.createConstraintApiDTOs(
                Collections.singletonList(entityDtos.get(Long.valueOf(uuid))),
                serviceEntityMap, RelationType.bought));
        constraintApiDtos.addAll(ConstraintsMapper.createConstraintApiDTOs(
                consumerOids.stream()
                        .map(consumerOid -> entityDtos.get(Long.valueOf(consumerOid)))
                        .collect(Collectors.toList()),
                serviceEntityMap, RelationType.sold));
        return constraintApiDtos;
    }

    @Override
    public List<ServiceEntityApiDTO> getPotentialEntitiesByEntity(String uuid, ConstraintApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Get TopologyEntityDTO based on provided oid.
     */
    private List<TopologyEntityDTO> getTopologyEntityDTOs(@Nonnull List<String> uuids)
            throws UnknownObjectException {

        List<TopologyEntityDTO> entities = searchServiceRpc.searchTopologyEntityDTOs(
                SearchTopologyEntityDTOsRequest.newBuilder()
                        .addAllEntityOid(uuids.stream()
                                .map(Long::valueOf)
                                .collect(Collectors.toList()))
                        .build()).getTopologyEntityDtosList();
        return entities;
    }

    /**
     * Get TopologyEntityDTO based on provided oid.
     */
    private TopologyEntityDTO getTopologyEntityDTO(@Nonnull String uuid) throws UnknownObjectException {
        List<TopologyEntityDTO> entityDtos =
                getTopologyEntityDTOs(Collections.singletonList(uuid));

        if (entityDtos.size() > 1) {
            throw new UnknownObjectException("Found " + entityDtos.size() + " entities of same id: " + uuid);
        }
        if (entityDtos.size() == 0) {
            throw new UnknownObjectException("Entity: " + uuid + " not found");
        }
        return entityDtos.get(0);
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
                        .addSearchParameters(SearchProtoUtil.neighbors(oid, traversalDirection))
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

    /**
     *  Get the type of the entity from the given supply chain.
     * @param uuid Oid of the entity.
     * @param supplyChain SupplyChain of the entity.
     * @return
     */
    private Optional<String> getEntityType(String uuid, SupplychainApiDTO supplyChain) {
        for (Entry<String, SupplychainEntryDTO> entry : supplyChain.getSeMap().entrySet()) {
                if (entry.getValue().getInstances().keySet().contains(uuid)) {
                    return Optional.of(entry.getKey());
                }
        }
        return Optional.empty();
    }

    /**
     * Check if the entity is related to the action.
     *
     * @param entityUuid The uuid of the entity.
     * @param action The action API DTO retrieved from action service.
     * @return True if there are related.
     */
    private boolean isRelatedAction(@Nonnull final String entityUuid,
        @Nonnull final ActionApiDTO action) {
        final ServiceEntityApiDTO targetEntity = action.getTarget();
        final ServiceEntityApiDTO currentEntity = action.getCurrentEntity();
        final ServiceEntityApiDTO newEntity = action.getNewEntity();
        return (targetEntity != null && entityUuid.equals(targetEntity.getUuid())) ||
            (currentEntity != null && entityUuid.equals(currentEntity.getUuid())) ||
            (newEntity != null && entityUuid.equals(newEntity.getUuid()));
    }
}


