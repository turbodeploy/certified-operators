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
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ConstraintsMapper;
import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.TagsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
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
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.RelationType;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IEntitiesService;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTOREST.GroupDTO.ConstraintType;

public class EntitiesService implements IEntitiesService {
    // these two constants are used to create hateos links for getEntitiesByUuid only.
    // TODO: discuss hateos with PM.  If we decide to support them, these constants
    // and the utility methods should go to their own class.  If not, these constants
    // and the utility methods should be removed.
    private final static String UUID = "{uuid}";
    private final static String REPLACEME = "#REPLACEME";

    private static final Logger logger = LogManager.getLogger();

    private final ActionsServiceBlockingStub actionOrchestratorRpcService;

    private final ActionSpecMapper actionSpecMapper;

    private final long realtimeTopologyContextId;

    private final SupplyChainFetcherFactory supplyChainFetcher;

    private final GroupServiceBlockingStub groupServiceClient;

    private final EntityAspectMapper entityAspectMapper;

    private final SeverityPopulator severityPopulator;

    private final PriceIndexPopulator priceIndexPopulator;

    private final StatsService statsService;

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final UuidMapper uuidMapper;

    private final StatsHistoryServiceBlockingStub statsHistoryService;

    private final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub;

    private final SettingsMapper settingsMapper;

    private final ActionSearchUtil actionSearchUtil;

    private final RepositoryApi repositoryApi;

    private final EntitySettingQueryExecutor entitySettingQueryExecutor;

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

    /**
     * The breadcrumb entities we are interested in when traversing entities that
     * belong to a cluster.
     */
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
        @Nonnull final GroupServiceBlockingStub groupServiceClient,
        @Nonnull final EntityAspectMapper entityAspectMapper,
        @Nonnull final SeverityPopulator severityPopulator,
        @Nonnull final PriceIndexPopulator priceIndexPopulator,
        @Nonnull final StatsService statsService,
        @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
        @Nonnull final UuidMapper uuidMapper,
        @Nonnull final StatsHistoryServiceBlockingStub statsHistoryService,
        @Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub,
        @Nonnull final SettingsMapper settingsMapper,
        @Nonnull final ActionSearchUtil actionSearchUtil,
        @Nonnull final RepositoryApi repositoryApi, final EntitySettingQueryExecutor entitySettingQueryExecutor) {
        this.actionOrchestratorRpcService = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.supplyChainFetcher = Objects.requireNonNull(supplyChainFetcher);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.settingPolicyServiceBlockingStub = Objects.requireNonNull(settingPolicyServiceBlockingStub);
        this.entityAspectMapper = Objects.requireNonNull(entityAspectMapper);
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.priceIndexPopulator = Objects.requireNonNull(priceIndexPopulator);
        this.statsService = Objects.requireNonNull(statsService);
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.statsHistoryService = Objects.requireNonNull(statsHistoryService);
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.actionSearchUtil = Objects.requireNonNull(actionSearchUtil);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.entitySettingQueryExecutor = entitySettingQueryExecutor;
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
        final SingleEntityRequest req = repositoryApi.entityRequest(oid);
        if (includeAspects) {
            req.useAspectMapper(entityAspectMapper);
        }
        final ServiceEntityApiDTO result = req.getSE()
            .orElseThrow(() -> new UnknownObjectException(uuid));

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

        // populate price index
        priceIndexPopulator.populateRealTimeEntities(Collections.singletonList(result));

        return result;
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
            actionSearchUtil.getActionsByEntityUuids(Collections.singleton(uuidMapper.fromUuid(uuid)),
                inputDto, paginationRequest);
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
        final ApiId id = uuidMapper.fromUuid(uuid);
        final List<SettingsManagerApiDTO> retMgrs =
            entitySettingQueryExecutor.getEntitySettings(id, includePolicies);
        return retMgrs;
    }

    @Override
    public SettingApiDTO getSettingByEntity(final String s, final String s1, final String s2) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<? extends SettingApiDTO<?>> getSettingsByEntityAndManager(final String s, final String s1) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityQuery(String uuid,
                                              StatPeriodApiInputDTO inputDto) throws Exception {
        return statsService.getStatsByEntityQuery(uuid, inputDto);
    }

    /**
     * Get a list of breadcrumb trail for an entity.
     * Scan the supply chain of this entity and determine if it belongs to a cluster. If so, insert
     * the cluster entity into the list of breadcrumbs.
     *
     * Steps:
     * 1. If the input entity is below the host in the supply chain (e.g DC/Chassis/Storage etc..),
     *    there is no cluster to fetch. So just return the entity.
     * 2. If the input entity is a Storage Device, check if it belongs to a Storage Cluster.
     * 3. If the input entity is equal to or above virtual machine, check if it belongs to a
     *    virtual machine cluster (e.g., containers belong to k8s cluster).
     * 4. Otherwise, check if the input entity belongs to a physical machine cluster.
     *
     * Note: If the entity belongs to both virtual machine cluster and physical machine cluster,
     *       only virtual machine cluster will be inserted into the list of breadcrumbs.
     *
     * @param uuid the uuid of the input entity
     * @return A list of sorted breadcrumbs entities
     * @throws Exception
     */
    @Override
    public List<BaseApiDTO> getGroupsByUuid(String uuid,
                                            Boolean path) throws Exception {
        if (!path) {
            // TODO: Get all groups which the service entity is member of
            throw ApiUtils.notImplementedInXL();
        }

        // Get the input entity
        long entityOid = Long.valueOf(uuid);
        Optional<ServiceEntityApiDTO> entityApiDTO =
                repositoryApi.entityRequest(entityOid).getSE();

        if (!entityApiDTO.isPresent()) {
            logger.warn("No entity dto found for entity with oid: {}", entityOid);
            return Collections.emptyList();
        }

        // The input entity cannot be a part of any cluster.
        if (NON_CLUSTER_ENTITY_TYPES.contains(entityApiDTO.get().getClassName())) {
            return Lists.newArrayList(entityApiDTO.get());
        }

        // The input entity is a storage device.
        // Skip supply-chain call for storage as it is not a consumer(direct/in-direct) of any
        // physical machine or virtual machine.
        if (entityApiDTO.get().getClassName().equals(UIEntityType.STORAGE.apiStr())) {
            List<BaseApiDTO> result = Lists.newArrayList();
            getClusterApiDTO(entityOid).ifPresent(result::add);
            result.add(entityApiDTO.get());
            sortBreadCrumbResult(result);
            return result;
        }

        // We are only interested in entities of type defined in BREADCRUMB_ENTITIES_TO_FETCH.
        Set<String> entityTypesToFetch = new HashSet<>(BREADCRUMB_ENTITIES_TO_FETCH);

        // We need to find out if the input entity belongs to any virtual machine cluster.
        // We only fetch virtual machine entities from the supply chain if the input entity
        // is equal to or above the virtual machine in the supply chain. This will not introduce
        // much overhead, as the seed for supply chain generation in this case starts from
        // virtual machine and above.
        if (!BREADCRUMB_ENTITIES_TO_FETCH.contains(entityApiDTO.get().getClassName()) &&
            !entityApiDTO.get().getClassName().equals(UIEntityType.VIRTUAL_DATACENTER.apiStr())) {
            entityTypesToFetch.add(UIEntityType.VIRTUAL_MACHINE.apiStr());
        }
        // Add the input entity type itself.
        entityTypesToFetch.add(entityApiDTO.get().getClassName());

        // Fetch the supply chain with entities of interest.
        final SupplychainApiDTO supplyChain = supplyChainFetcher.newApiDtoFetcher()
                .topologyContextId(realtimeTopologyContextId)
                .addSeedUuids(Lists.newArrayList(uuid))
                .entityTypes(Lists.newArrayList(entityTypesToFetch))
                .includeHealthSummary(false)
                .entityDetailType(EntityDetailType.entity)
                .fetch();
        // Create an unmodifiable serviceEntityMap
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
                supplyChain.getSeMap().values().stream()
                    .flatMap(supplyChainEntryDTO ->
                             supplyChainEntryDTO.getInstances().values().stream())
                    // Drop VDC entities unless it is the same as the input entity.
                    // We do this now because VDC entity is not needed to get a cluster.
                    .filter(serviceEntityApiDTO ->
                            !UIEntityType.VIRTUAL_DATACENTER.apiStr().equals(serviceEntityApiDTO.getClassName())
                            || uuid.equals(serviceEntityApiDTO.getUuid()))
                    .collect(Collectors.toMap(apiDTO ->
                            Long.valueOf(apiDTO.getUuid()), Function.identity()));

        // Check if the input entity belongs to a Virtual Machine Cluster.
        List<BaseApiDTO> result = getVirtualMachineClusterBreadCrumbs(entityApiDTO.get(),
                serviceEntityMap);
        if (!result.isEmpty()) {
            return result;
        }

        // If not, then check if it belongs to a Physical Machine Cluster
        return getClusterBreadCrumbs(entityApiDTO.get(), serviceEntityMap);
    }

    /**
     * Get the list of breadcrumbs entities if the input entity belongs to a physical machine
     * cluster.
     *
     * @param entityApiDTO the input entity
     * @param serviceEntityMap a map of entities that are on the supply chain of the input entity
     *
     * @return A list of breadcrumbs entities including the physical machine cluster that
     * the input entity belongs to.
     */
    private List<BaseApiDTO> getClusterBreadCrumbs(ServiceEntityApiDTO entityApiDTO,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) {
        List<BaseApiDTO> result = serviceEntityMap.values()
                .stream()
                // Remove VM entity from the result unless the input entity is the same VM.
                .filter(serviceEntityApiDTO ->
                        !UIEntityType.VIRTUAL_MACHINE.apiStr().equals(serviceEntityApiDTO.getClassName())
                        || entityApiDTO.getUuid().equals(serviceEntityApiDTO.getUuid()))
                // Remove PM(Host) entity from the result unless the input entity is the same PM.
                .filter(serviceEntityApiDTO ->
                        !UIEntityType.PHYSICAL_MACHINE.apiStr().equals(serviceEntityApiDTO.getClassName())
                        || entityApiDTO.getUuid().equals(serviceEntityApiDTO.getUuid()))
                .collect(Collectors.toList());
        // Find the oid of the Host the entity is connected to.
        for (Entry<Long, ServiceEntityApiDTO> entry : serviceEntityMap.entrySet()) {
            ServiceEntityApiDTO serviceEntityApiDTO = entry.getValue();
            if (serviceEntityApiDTO.getClassName().equals(UIEntityType.PHYSICAL_MACHINE.apiStr())) {
                long oidToQuery = Long.valueOf(serviceEntityApiDTO.getUuid());
                getClusterApiDTO(oidToQuery).ifPresent(result::add);
                // We found the cluster that the PM belongs to, break out of the loop.
                break;
            }
        }
        sortBreadCrumbResult(result);
        return result;
    }

    /**
     * Get the list of breadcrumbs entities if the input entity belongs to a virtual machine
     * cluster.
     *
     * @param entityApiDTO the input entity
     * @param serviceEntityMap a map of entities that are on the supply chain of the input entity
     *
     * @return An empty list if the input entity does not belong to any virtual machine cluster.
     * Otherwise, return a list of breadcrumbs entities including the virtual machine cluster that
     * the input entity belongs to.
     */
    private List<BaseApiDTO> getVirtualMachineClusterBreadCrumbs(ServiceEntityApiDTO entityApiDTO,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) {
        List<BaseApiDTO> result = Lists.newArrayList();
        if (BREADCRUMB_ENTITIES_TO_FETCH.contains(entityApiDTO.getClassName()) ||
                entityApiDTO.getClassName().equals(UIEntityType.VIRTUAL_DATACENTER.apiStr())) {
            // The input entity is below virtual machine.
            // Return an empty list.
            return result;
        }
        // Find the oid of the Virtual Machine to which the input entity is connected
        // either directly or indirectly, then call the group service to find the Virtual
        // Machine cluster that the Virtual Machine belongs to.
        serviceEntityMap.entrySet()
                .stream()
                .filter(entry ->
                        entry.getValue().getClassName().equals(UIEntityType.VIRTUAL_MACHINE.apiStr()))
                .findFirst()
                .ifPresent(entry ->
                        getClusterApiDTO(entry.getKey())
                                .ifPresent(result::add));
        if (result.isEmpty()) {
            // This entity does not belong to any virtual machine cluster.
            // Return an empty list.
            return result;
        }
        // This entity belongs to a virtual machine cluster.
        // Complete the list of breadcrumbs.
        result.addAll(serviceEntityMap.values()
                .stream()
                // Drop the physical machine entity.
                .filter(e -> !UIEntityType.PHYSICAL_MACHINE.apiStr().equals(e.getClassName()))
                // Drop the virtual machine entity if input entity is not a virtual machine.
                // Only keep the virtual machine entity when the input entity itself is a virtual
                // machine.
                // For example, we may generate the following breadcrumbs:
                // 1. The input entity is a kubernetes container
                //    [DataCenter]/[VM Cluster]/[Container]
                // 2. The input entity is a kubernetes node (i.e., a VM)
                //    [DataCenter]/[VM Cluster]/[VM]
                .filter(e -> !UIEntityType.VIRTUAL_MACHINE.apiStr().equals(e.getClassName())
                        || entityApiDTO.getClassName().equals(UIEntityType.VIRTUAL_MACHINE.apiStr()))
                .collect(Collectors.toList()));
        // Sort the order.
        sortBreadCrumbResult(result);
        return result;
    }

    /**
     * Call the group service to query the cluster that the object belongs to.
     * @param oidToQuery the oid of the entity
     * @return The group DTO with a cluster constraint.
     */
    private Optional<ServiceEntityApiDTO> getClusterApiDTO(long oidToQuery) {
        GetGroupForEntityResponse response =
                groupServiceClient.getGroupForEntity(GetGroupForEntityRequest.newBuilder()
                        .setEntityId(oidToQuery)
                        .build());

        Optional<GroupDTO.Grouping> cluster = response.getGroupList()
                        .stream()
                        .filter(group -> group.getDefinition().getType()
                                        == GroupType.COMPUTE_HOST_CLUSTER)
                        .findAny();

        if (cluster.isPresent()) {
            final ServiceEntityApiDTO serviceEntityApiDTO = new ServiceEntityApiDTO();
            serviceEntityApiDTO.setDisplayName(cluster.get().getDefinition().getDisplayName());
            serviceEntityApiDTO.setUuid(Long.toString(cluster.get().getId()));
            serviceEntityApiDTO.setClassName(ConstraintType.CLUSTER.name());
            // Insert the clusterRecord before the Entity record.
            return Optional.of(serviceEntityApiDTO);
        }
        return Optional.empty();
    }

    /**
     * Sort the list of breadcrumbs entities based on predefined order.
     * @param result the list to sort
     */
    private void sortBreadCrumbResult(List<BaseApiDTO> result) {
        // the UI breadcrumb path is of the form:
        // DataCenterName|ChassisName/ClusterName/EntityName
        // For Cloud, we use Region in-place of Cluster and the path is of the form:
        // AvailabilityZone/RegionName/EntityName
        // Sort the result to create the correct path.
        result.sort((dto1, dto2) ->
                BREADCRUMB_ENTITY_PRECEDENCE_MAP.getOrDefault(dto1.getClassName(), Integer.MAX_VALUE)
                        .compareTo(BREADCRUMB_ENTITY_PRECEDENCE_MAP.getOrDefault(dto2.getClassName(),
                                Integer.MAX_VALUE)));
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
        return TagsMapper.convertTagsToApi(repositoryApi.entityRequest(Long.valueOf(s))
            .getEntity()
            .orElseThrow(() -> new UnknownObjectException(s))
            .getTags().getTagsMap());
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
        final TopologyEntityDTO entityDTO = repositoryApi.entityRequest(Long.parseLong(uuid))
            .getFullEntity()
            .orElseThrow(() -> new UnknownObjectException(uuid));
        return entityAspectMapper.getAspectsByEntity(entityDTO)
            .entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getApiName(), Entry::getValue));
    }

    @Override
    public EntityAspect getAspectByEntityUuid(String uuid, String aspectTag) throws UnauthorizedObjectException, UnknownObjectException {
        AspectName aspectName = AspectName.fromString(aspectTag);
        final TopologyEntityDTO entityDTO = repositoryApi.entityRequest(Long.parseLong(uuid))
            .getFullEntity()
            .orElseThrow(() -> new UnknownObjectException(uuid));
        return entityAspectMapper.getAspectByEntity(entityDTO, aspectName);
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
        Set<Long> oidsToQuery = new HashSet<>();
        consumerOids.forEach(oid -> oidsToQuery.add(Long.parseLong(oid)));
        oidsToQuery.add(Long.parseLong(uuid));

        // The constraints are embedded in the commodities. We have to fetch the TopologyEntityDTO
        // to get the commodities info.
        Map<Long, TopologyEntityDTO> entityDtos = repositoryApi.entitiesRequest(oidsToQuery)
            .getFullEntities()
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
        final Stream<MinimalEntity> neighbors = repositoryApi.newSearchRequest(
            SearchProtoUtil.neighbors(oid, traversalDirection)).getMinimalEntities();
        code.accept(neighbors.map(ServiceEntityMapper::toBasicEntity)
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


