package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.tools.Longs;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.TagsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionInputUtil;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.component.external.api.util.cost.CostStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
import com.vmturbo.api.constraints.ConstraintApiDTO;
import com.vmturbo.api.constraints.ConstraintApiInputDTO;
import com.vmturbo.api.constraints.PlacementOptionApiDTO;
import com.vmturbo.api.controller.GroupsController;
import com.vmturbo.api.controller.MarketsController;
import com.vmturbo.api.cost.CostInputApiDTO;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionResourceImpactStatApiInputDTO;
import com.vmturbo.api.dto.entity.EntityDetailsApiDTO;
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
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.RelationType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IEntitiesService;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.RiskUtil;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.DeleteEntityCustomTagRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.DeleteEntityCustomTagsRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.EntityCustomTagsCreateRequest;
import com.vmturbo.common.protobuf.group.EntityCustomTagsServiceGrpc.EntityCustomTagsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.EntityConstraints;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraint;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraintsRequest;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraintsResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacements;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacementsRequest;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacementsResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraintsServiceGrpc.EntityConstraintsServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.Tag.DeleteTagListRequest;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTOREST.GroupDTO.ConstraintType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

public class EntitiesService implements IEntitiesService {
    // these two constants are used to create hateos links for getEntitiesByUuid only.
    // TODO: discuss hateos with PM.  If we decide to support them, these constants
    // and the utility methods should go to their own class.  If not, these constants
    // and the utility methods should be removed.
    private final static String UUID = "{uuid}";
    private final static String REPLACEME = "#REPLACEME";

    private final static String NO_ENTITY_MESSAGE="There is no entity for the uuid specified: %s";

    private final static String NO_ENTITY_GROUP_MESSAGE="There is no valid entity or group for the uuid specified: %s";

    private final static String EMPTY_UUID_MESSAGE="Uuid should not be empty. It is an invalid uuid format.";

    private static final String ILLEGAL_UUID_MESSAGE = "%s is illegal argument. "
            + "It is an invalid uuid format.";

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

    private final EntityConstraintsServiceBlockingStub entityConstraintsRpcService;

    private final PolicyServiceBlockingStub policyRpcService;

    private final ThinTargetCache thinTargetCache;

    private final PaginationMapper paginationMapper;

    private final ServiceEntityMapper serviceEntityMapper;

    private final SettingsManagerMapping settingsManagerMapping;

    private final EntityCustomTagsServiceBlockingStub entityCustomTagsService;

    private final CostStatsQueryExecutor costStatsQueryExecutor;

    // Entity types which are not part of Host or Storage Cluster.
    private static final ImmutableSet<String> NON_CLUSTER_ENTITY_TYPES =
            ImmutableSet.of(
                    ApiEntityType.CHASSIS.apiStr(),
                    ApiEntityType.DATACENTER.apiStr(),
                    ApiEntityType.DISKARRAY.apiStr(),
                    ApiEntityType.LOGICALPOOL.apiStr(),
                    ApiEntityType.STORAGECONTROLLER.apiStr());

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
                    ApiEntityType.REGION.apiStr(), 1,
                    ApiEntityType.AVAILABILITY_ZONE.apiStr(), 2,
                    ApiEntityType.DATACENTER.apiStr(), 2,
                    ApiEntityType.CHASSIS.apiStr(), 2,
                    ConstraintType.CLUSTER.toString(), 3);

    /**
     * The breadcrumb entities we are interested in when traversing entities that
     * belong to a cluster.
     */
    private static final Set<String> BREADCRUMB_ENTITIES_TO_FETCH =
            new HashSet<>(Arrays.asList(
                    ApiEntityType.REGION.apiStr(),
                    ApiEntityType.AVAILABILITY_ZONE.apiStr(),
                    ApiEntityType.DATACENTER.apiStr(),
                    ApiEntityType.CHASSIS.apiStr(),
                    ApiEntityType.PHYSICAL_MACHINE.apiStr()));

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
        @Nonnull final RepositoryApi repositoryApi,
        final EntitySettingQueryExecutor entitySettingQueryExecutor,
        @Nonnull final EntityConstraintsServiceBlockingStub entityConstraintsRpcService,
        @Nonnull final PolicyServiceBlockingStub policyRpcService,
        @Nonnull final ThinTargetCache thinTargetCache,
        @Nonnull final PaginationMapper paginationMapper,
        @Nonnull final ServiceEntityMapper serviceEntityMapper,
        final SettingsManagerMapping settingsManagerMapping,
        @Nonnull final EntityCustomTagsServiceBlockingStub entityCustomTagsService,
        @Nonnull final CostStatsQueryExecutor costStatsQueryExecutor) {
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
        this.entityConstraintsRpcService = entityConstraintsRpcService;
        this.policyRpcService = policyRpcService;
        this.thinTargetCache = thinTargetCache;
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.settingsManagerMapping = settingsManagerMapping;
        this.entityCustomTagsService = entityCustomTagsService;
        this.costStatsQueryExecutor = Objects.requireNonNull(costStatsQueryExecutor);
    }

    @Override
    public ServiceEntityApiDTO getEntities() throws Exception {
        final ServiceEntityApiDTO result = new ServiceEntityApiDTO();
        result.add(
            generateLinkTo(
                ControllerLinkBuilder.methodOn(MarketsController.class).getEntitiesByMarketUuid(REPLACEME, null, null, null, null, null, null, true),
                "Market Entities"));
        result.add(
            generateLinkTo(
                ControllerLinkBuilder.methodOn(GroupsController.class).getEntitiesByGroupUuid(REPLACEME, null, null, null, null),
                "Group entities"));
        return result;
    }

    @Override
    @Nonnull
    public ServiceEntityApiDTO getEntityByUuid(@Nonnull String uuid, boolean includeAspects)
            throws IllegalArgumentException, OperationFailedException, ConversionException,
            InterruptedException, UnknownObjectException {
        final long oid = getEntityOidFromString(uuid);
        // get information about this entity from the repository
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
    public EntityDetailsApiDTO getEntityDetails(@Nonnull final String entityUuid)
            throws Exception {
        final long oid = getEntityOidFromString(entityUuid);
        final SingleEntityRequest req = repositoryApi.entityRequest(oid);
        return req.getEntityDetails()
                .orElseThrow(() -> new UnknownObjectException(entityUuid));
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
        return actionSearchUtil.getActionsByScope(uuidMapper.fromUuid(uuid), inputDto,
                paginationRequest);
    }

    /**
     * Return the Action information given the id for the action.
     *
     * @param uuid        uuid of the entity
     * @param actionUuid  uuid of the action
     * @param detailLevel the level of Action details to be returned
     * @return
     * @throws Exception
     */
    @Override
    @Nonnull
    public ActionApiDTO getActionByEntityUuid(@Nonnull final String uuid,
                                              @Nonnull final String actionUuid,
                                              @Nullable final ActionDetailLevel detailLevel)
            throws Exception {
        getEntityOidFromString(uuid);
        long oid = actionSearchUtil.getActionInstanceId(actionUuid, null).orElseThrow(() -> {
            logger.error("Cannot lookup action as one with ID {} cannot be found.", uuid);
            return new UnknownObjectException("Cannot find action with ID " + uuid);
        });

        final ActionApiDTO result;
        try {
            // get the action object from the action orchestrator
            // and translate it to an ActionApiDTO object
            result =
                    actionSpecMapper.mapActionSpecToActionApiDTO(
                            actionOrchestratorRpcService
                                    .getAction(
                                            SingleActionRequest.newBuilder()
                                                    .setActionId(oid)
                                                    .setTopologyContextId(realtimeTopologyContextId)
                                                    .build())
                                    .getActionSpec(),
                            realtimeTopologyContextId,
                            detailLevel);
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
        // check if the passed in entity uuid is related to the action
        if (!isRelatedAction(uuid, result)) {
            throw new IllegalArgumentException(String.format("Entity %s in the query is not " +
                    "related to the action %s.", uuid, actionUuid));
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
        if (!id.isEntity() && !id.isGroup()) {
            throw new IllegalArgumentException("Uuid '" + uuid
                    + "' does not correspond to an entity or a group.");
        }
        final List<SettingsManagerApiDTO> retMgrs =
            entitySettingQueryExecutor.getEntitySettings(id, includePolicies);
        return retMgrs;
    }

    /**
     * Get a setting by settingUuid and settingsManagerUuid, for the entity with entityUuid.
     *
     * @param entityUuid The uuid of the entity.
     * @param settingsManagerUuid The uuid of the settings manager.
     * @param settingUuid The uuid of the setting.
     * @return A setting dto for the specified entity, populated with the values for that entity.
     * @throws Exception on errors or on bad input.
     */
    @Override
    public SettingApiDTO<?> getSettingByEntity(
            final String entityUuid,
            final String settingsManagerUuid,
            final String settingUuid) throws Exception {
        final ApiId id = uuidMapper.fromUuid(entityUuid);
        validateEntityAndSettingAndManager(id, settingUuid, settingsManagerUuid);
        // get the setting for that entity, wrapped inside its settings manager
        final List<SettingsManagerApiDTO> result = entitySettingQueryExecutor.getEntitySettings(
                id, false, Collections.singletonList(settingUuid), null);
        List<? extends SettingApiDTO<?>> settings = result.stream()
                .filter(sm -> sm.getUuid().equals(settingsManagerUuid))
                .findFirst()
                .orElseThrow(() ->
                        new OperationFailedException("Failed to retrieve settings manager."))
                .getSettings();
        if (settings == null) {
            throw new OperationFailedException("Failed to retrieve settings.");
        }
        return settings.stream()
                .filter(s -> s.getUuid().equals(settingUuid))
                .findFirst()
                .orElseThrow(() -> new OperationFailedException("Failed to retrieve setting."));
    }

    /**
     * Return a list of {@link SettingApiDTO} representing the settings under the specified settings
     * manager for the specified entity.
     *
     * @param entityUuid the uuid of the entity to query for.
     * @param settingsManagerUuid the uuid of the manager to which the setting belongs.
     * @return the settings for that entity under that settingsmanager.
     * @throws OperationFailedException on error or bad input.
     */
    @Override
    public List<? extends SettingApiDTO<?>> getSettingsByEntityAndManager(
            final String entityUuid,
            final String settingsManagerUuid) throws OperationFailedException {
        final ApiId id = uuidMapper.fromUuid(entityUuid);
        validateEntityAndManager(id, settingsManagerUuid);
        // get all settings for that entity, organized by settings manager
        final List<SettingsManagerApiDTO> result = entitySettingQueryExecutor.getEntitySettings(
                id, false, null, null);
        if (result.isEmpty()) {
            throw new OperationFailedException("Failed to retrieve settings manager with uuid '"
                    + settingsManagerUuid + "'.");
        }
        return result.stream()
                .filter(s -> s.getUuid().equals(settingsManagerUuid))
                .findFirst()
                .orElseThrow(() -> new OperationFailedException("Failed to retrieve settings"
                        + " manager with uuid '" + settingsManagerUuid + "' for uuid '" + entityUuid
                        + "'."))
                .getSettings();
    }

    /**
     * Validates that a settings manager uuid corresponds to an existing settings manager.
     *
     * @param entityApiId the entity to check against its type.
     * @param managerUuid the settings manager uuid to validate.
     * @throws OperationFailedException if the entity type cannot be resolved
     */
    private void validateEntityAndManager(@Nonnull final ApiId entityApiId, @Nonnull final String managerUuid)
            throws OperationFailedException {
        validateApiId(entityApiId);
        // Validate that settings manager exists.
        if (!settingsManagerMapping.getManagerInfo(managerUuid).isPresent()) {
            throw new IllegalArgumentException("Invalid settings manager uuid '" + managerUuid
                    + "'.");
        }
        // add a check when validating entity type, since in some cases group uuids might be
        // allowed
        if (entityApiId.isEntity()) {
            // Validate that the settings manager is valid for this entity type.
            // The settings manager is considered valid if it has at least one setting that is being
            // used for the entity type provided.
            ApiEntityType entityType = entityApiId.getCachedEntityInfo()
                    .orElseThrow(() -> new OperationFailedException("Could not fetch the entity "
                            + "type for uuid '" + entityApiId.uuid() + "'."))
                    .getEntityType();
            if (settingsManagerMapping.getManagerInfo(managerUuid).get()
                    .getSettings().stream()
                    .map(SettingsMapper::getSettingSpec)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(SettingSpec::hasEntitySettingSpec)
                    .map(SettingSpec::getEntitySettingSpec)
                    .filter(EntitySettingSpec::hasEntitySettingScope)
                    .map(EntitySettingSpec::getEntitySettingScope)
                    .filter(EntitySettingScope::hasEntityTypeSet)
                    .map(scope -> scope.getEntityTypeSet().getEntityTypeList())
                    .flatMap(Collection::stream)
                    .noneMatch(type -> type.equals(entityType.typeNumber()))) {
                throw new IllegalArgumentException("Invalid settings manager uuid '" + managerUuid
                        + "' for entity type '" + entityType.apiStr() + "'.");
            }
        }
    }

    /**
     * Validates that:
     * 1) the setting uuid corresponds to an existing setting spec.
     * 2) the settings manager uuid corresponds to an existing settings manager.
     * 3) the settings manager contains the setting.
     *
     * @param entityApiId the entity to check against its type.
     * @param settingUuid the setting uuid to validate.
     * @param managerUuid the settings manager uuid to validate.
     */
    private void validateEntityAndSettingAndManager(@Nonnull final ApiId entityApiId,
            @Nonnull final String settingUuid,
            @Nonnull final String managerUuid) {
        validateApiId(entityApiId);
        // Validate that setting exists.
        final SettingSpec settingSpec = SettingsMapper.getSettingSpec(settingUuid).orElseThrow(() ->
                new IllegalArgumentException("Invalid setting uuid '" + settingUuid + "'."));
        // Validate that settings manager exists.
        final SettingsManagerInfo resolvedSettingsManager =
                settingsManagerMapping.getManagerInfo(managerUuid).orElseThrow(() ->
                        new IllegalArgumentException("Invalid settings manager uuid '" + managerUuid
                                + "'.")
                );
        // Validate that the setting is under this settings manager.
        if (!resolvedSettingsManager.getSettings().contains(settingUuid)) {
            throw new IllegalArgumentException("Provided settings manager uuid '" + managerUuid
                    + "' is not valid for setting uuid '" + settingUuid + "'.");
        }
        // add a check when validating entity type, since in some cases group uuids might be
        // allowed
        if (entityApiId.isEntity()) {
            // Validate that the entity type is valid for this setting spec.
            // sanity checks
            if (!settingSpec.hasEntitySettingSpec()) {
                throw createInvalidSettingException(settingUuid, entityApiId.uuid());
            }
            SettingProto.EntitySettingSpec entitySettingSpec = settingSpec.getEntitySettingSpec();
            if (!entitySettingSpec.hasEntitySettingScope()) {
                throw createInvalidSettingException(settingUuid, entityApiId.uuid());
            }
            SettingProto.EntitySettingScope scope =
                    settingSpec.getEntitySettingSpec().getEntitySettingScope();
            if (!scope.hasEntityTypeSet()) {
                throw createInvalidSettingException(settingUuid, entityApiId.uuid());
            }
            // actual check
            if (!scope.getEntityTypeSet().getEntityTypeList().contains(
                    entityApiId.getCachedEntityInfo()
                            .orElseThrow(() ->
                                    createInvalidSettingException(settingUuid, entityApiId.uuid()))
                            .getEntityType().typeNumber())) {
                throw createInvalidSettingException(settingUuid, entityApiId.uuid());
            }
        }
    }

    /**
     * Checks that the {@link ApiId} provided corresponds to either an entity or a group.
     *
     * @param apiId the {@link ApiId} to validate.
     */
    private void validateApiId(@Nonnull final ApiId apiId) {
        if (!apiId.isEntity() && !apiId.isGroup()) {
            throw new IllegalArgumentException("Uuid '" + apiId.uuid()
                    + "' does not correspond to an entity or a group.");
        }
    }

    /**
     * Utility function for {@link EntitiesService#validateEntityAndSettingAndManager}.
     * Creates a new {@link IllegalArgumentException} with an appropriate error message.
     *
     * @param settingUuid the name of the setting, to be included in the error message.
     * @param entityUuid the uuid for the entity (or group), to be included in the error message.
     * @return the new {@link IllegalArgumentException}
     */
    private IllegalArgumentException createInvalidSettingException(String settingUuid,
            String entityUuid) {
        return new IllegalArgumentException("Setting uuid '" + settingUuid + "' is invalid for"
                + " uuid '" + entityUuid + "'.");
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
        // Get the input entity
        long entityOid = uuidMapper.fromUuid(uuid).oid();

        if (!path) {
            return groupServiceClient.getGroupsForEntities(GetGroupsForEntitiesRequest.newBuilder()
                .addEntityId(entityOid)
                .setLoadGroupObjects(true)
                .build())
            .getGroupsList()
            .stream()
            .map(GroupMapper::toBaseApiDTO)
            .collect(Collectors.toList());
        }


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
        if (entityApiDTO.get().getClassName().equals(ApiEntityType.STORAGE.apiStr())) {
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
            !entityApiDTO.get().getClassName().equals(ApiEntityType.VIRTUAL_DATACENTER.apiStr())) {
            entityTypesToFetch.add(ApiEntityType.VIRTUAL_MACHINE.apiStr());
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
                            !ApiEntityType.VIRTUAL_DATACENTER.apiStr().equals(serviceEntityApiDTO.getClassName())
                            || uuid.equals(serviceEntityApiDTO.getUuid()))
                    .collect(Collectors.toMap(apiDTO -> {
                        try {
                            return uuidMapper.fromUuid(apiDTO.getUuid()).oid();
                        } catch (OperationFailedException e) {
                            throw new IllegalArgumentException(
                                    String.format("The uuid %s can not be mapped to oid",
                                            apiDTO.getUuid()), e);
                        }
                    }, Function.identity()));

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
     * @throws OperationFailedException if there is any error mapping the scope.
     */
    private List<BaseApiDTO> getClusterBreadCrumbs(ServiceEntityApiDTO entityApiDTO,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap) throws OperationFailedException {
        List<BaseApiDTO> result = serviceEntityMap.values()
                .stream()
                // Remove VM entity from the result unless the input entity is the same VM.
                .filter(serviceEntityApiDTO ->
                        !ApiEntityType.VIRTUAL_MACHINE.apiStr().equals(serviceEntityApiDTO.getClassName())
                        || entityApiDTO.getUuid().equals(serviceEntityApiDTO.getUuid()))
                // Remove PM(Host) entity from the result unless the input entity is the same PM.
                .filter(serviceEntityApiDTO ->
                        !ApiEntityType.PHYSICAL_MACHINE.apiStr().equals(serviceEntityApiDTO.getClassName())
                        || entityApiDTO.getUuid().equals(serviceEntityApiDTO.getUuid()))
                .collect(Collectors.toList());
        // Find the oid of the Host the entity is connected to.
        for (Entry<Long, ServiceEntityApiDTO> entry : serviceEntityMap.entrySet()) {
            ServiceEntityApiDTO serviceEntityApiDTO = entry.getValue();
            if (serviceEntityApiDTO.getClassName().equals(ApiEntityType.PHYSICAL_MACHINE.apiStr())) {
                long oidToQuery = uuidMapper.fromUuid(serviceEntityApiDTO.getUuid()).oid();
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
                entityApiDTO.getClassName().equals(ApiEntityType.VIRTUAL_DATACENTER.apiStr())) {
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
                        entry.getValue().getClassName().equals(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
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
                .filter(e -> !ApiEntityType.PHYSICAL_MACHINE.apiStr().equals(e.getClassName()))
                // Drop the virtual machine entity if input entity is not a virtual machine.
                // Only keep the virtual machine entity when the input entity itself is a virtual
                // machine.
                // For example, we may generate the following breadcrumbs:
                // 1. The input entity is a kubernetes container
                //    [DataCenter]/[VM Cluster]/[Container]
                // 2. The input entity is a kubernetes node (i.e., a VM)
                //    [DataCenter]/[VM Cluster]/[VM]
                .filter(e -> !ApiEntityType.VIRTUAL_MACHINE.apiStr().equals(e.getClassName())
                        || entityApiDTO.getClassName().equals(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
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
        GetGroupsForEntitiesResponse response =
                groupServiceClient.getGroupsForEntities(GetGroupsForEntitiesRequest.newBuilder()
                        .addEntityId(oidToQuery)
                        .addGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                        .setLoadGroupObjects(true)
                        .build());
        // Response should contain 0 or 1 cluster for this entity
        if (response.getGroupsCount() > 0) {
            final GroupDTO.Grouping cluster = response.getGroups(0);
            final ServiceEntityApiDTO serviceEntityApiDTO = new ServiceEntityApiDTO();
            serviceEntityApiDTO.setDisplayName(cluster.getDefinition().getDisplayName());
            serviceEntityApiDTO.setUuid(Long.toString(cluster.getId()));
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
    public List<StatSnapshotApiDTO> getActionStatsResourceImpactByUuid(
            @Nonnull String uuid, @Nonnull ActionResourceImpactStatApiInputDTO inputDTO) throws Exception {
        try {
            if (CollectionUtils.isEmpty(inputDTO.getActionResourceImpactStatList())) {
                throw new InvalidOperationException("Missing list of ActionResourceImpactStat");
            }

            final ApiId apiId = uuidMapper.fromUuid(uuid);
            return actionStatsQueryExecutor.retrieveActionStats(
                        ImmutableActionStatsQuery.builder()
                                .addScopes(apiId)
                                .actionInput(ActionInputUtil.toActionApiInputDTO(inputDTO))
                                .actionResourceImpactIdentifierSet(ActionInputUtil.toActionResourceImpactIdentifierSet(inputDTO))
                                .build()).get(apiId);
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
    public List<TagApiDTO> getTagsByEntityUuid(final String s)
            throws IllegalArgumentException, OperationFailedException, UnknownObjectException {
        final long oid = getEntityOrGroupOidFromString(s);
        final boolean isGroup = uuidMapper.fromUuid(s).isGroup();
        final Map<String, TagValuesDTO> tagsMap = new HashMap<>();

        // TODO: After a deprecation cycle, this logic should be changed to only support
        // entities and not support groups.  Group tags are supported through the groups endpoints.
        if (isGroup) {
            final GetTagsRequest tagsRequest =
                    GetTagsRequest.newBuilder().addGroupId(oid).build();
            final GetTagsResponse tagsForGroups =
                    groupServiceClient.getTags(tagsRequest);
            final Tags tags = tagsForGroups.getTagsMap().get(oid);
            if (tags != null) {
                tagsMap.putAll(tags.getTagsMap());
            }
        } else {
            tagsMap.putAll(repositoryApi.entityRequest(oid)
                    .getEntity()
                    .orElseThrow(() -> new UnknownObjectException(s))
                    .getTags()
                    .getTagsMap());
        }
        return TagsMapper.convertTagsToApi(tagsMap);
    }

    @Override
    public List<TagApiDTO> createTagsByEntityUuid(final String uuid, final List<TagApiDTO> tagApiDTOs)
            throws OperationFailedException {
        final long oid = getEntityOidFromString(uuid);

        // Convert to Tags and also validate.
        Tags tags = convertToTags(tagApiDTOs);
        final EntityCustomTagsCreateRequest request = EntityCustomTagsCreateRequest.newBuilder()
                .setEntityId(oid)
                .setTags(tags).build();

        try {
            entityCustomTagsService.createTags(request);
        } catch(StatusRuntimeException e) {
            throw new OperationFailedException("Entity service RPC call failed to complete request: "
                    + e.getMessage());
        }

        return tagApiDTOs;
    }

    private static Tags convertToTags(List<TagApiDTO> apiTags)
            throws OperationFailedException {

        final Tags.Builder tags = Tags.newBuilder();
        for(TagApiDTO tag : apiTags) {

            if(StringUtils.isEmpty(tag.getKey())) {
                throw new OperationFailedException("Tag key cannot be empty string");
            }

            if(tag.getValues().isEmpty()) {
                throw new OperationFailedException("Tag values list cannot be empty");
            }

            TagValuesDTO currentValues = tags.getTagsMap().get(tag.getKey());
            TagValuesDTO.Builder newValues = TagValuesDTO.newBuilder()
                    .addAllValues(tag.getValues());
            if(currentValues != null) {
                newValues.addAllValues(currentValues.getValuesList());
            }

            tags.putTags(tag.getKey(), newValues.build()).build();
        }
        return tags.build();
    }

    @Override
    public void deleteTagsByEntityUuid(final String uuid) throws Exception {
        final long oid = getEntityOidFromString(uuid);
        final DeleteEntityCustomTagsRequest request =
                DeleteEntityCustomTagsRequest.newBuilder().setEntityOid(oid).build();
        try {
            entityCustomTagsService.deleteTags(request);
        } catch (StatusRuntimeException e) {
            throw new OperationFailedException("Unable to delete tag for Entity: '" + uuid + "'", e);
        }
    }

    @Override
    public void deleteTagsByEntityUuid(final String uuid, final List<String> tagKeys) throws Exception {
        final long oid = getEntityOidFromString(uuid);
        final DeleteTagListRequest request =
                DeleteTagListRequest.newBuilder()
                        .setOid(oid)
                        .addAllTagKey(tagKeys)
                        .build();
        try {
            entityCustomTagsService.deleteTagList(request);
        } catch (StatusRuntimeException e) {
            throw new OperationFailedException("Unable to delete tag for Entity: '" + uuid + "'", e);
        }
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
        final long oid = getEntityOidFromString(uuid);
        GetEntitySettingPoliciesRequest request =
                GetEntitySettingPoliciesRequest.newBuilder()
                        .addEntityOidList(Long.valueOf(uuid))
                        .setIncludeInactive(true)
                        .build();

        GetEntitySettingPoliciesResponse response =
                settingPolicyServiceBlockingStub.getEntitySettingPolicies(request);

        return settingsMapper.convertSettingPolicies(response.getSettingPoliciesList());
    }

    @Override
    public Map<String, EntityAspect> getAspectsByEntityUuid(String uuid)
            throws UnauthorizedObjectException, UnknownObjectException, OperationFailedException,
            ConversionException, InterruptedException {
        final long oid = getEntityOidFromString(uuid);
        final TopologyEntityDTO entityDTO = repositoryApi.entityRequest(oid)
            .getFullEntity()
            .orElseThrow(() -> new UnknownObjectException(uuid));
        return entityAspectMapper.getAspectsByEntity(entityDTO)
            .entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getApiName(), Entry::getValue));
    }

    @Override
    public EntityAspect getAspectByEntityUuid(String uuid, String aspectTag)
            throws UnauthorizedObjectException, UnknownObjectException, OperationFailedException,
            ConversionException, InterruptedException {
        final long oid = getEntityOidFromString(uuid);
        AspectName aspectName = AspectName.fromString(aspectTag);
        final TopologyEntityDTO entityDTO = repositoryApi.entityRequest(oid)
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
        final long oid = getEntityOidFromString(uuid);
        final EntityConstraintsResponse response = entityConstraintsRpcService.getConstraints(
            EntityConstraintsRequest.newBuilder().setOid(oid).build());
        String discoveringTargets = response.getDiscoveringTargetIdsList().stream()
            .map(targetId -> thinTargetCache.getTargetInfo(targetId))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(thinTargetInfo -> !thinTargetInfo.isHidden())
            .map(thinTargetInfo -> thinTargetInfo.probeInfo().type())
            .distinct()
            .collect(Collectors.joining(", "));

        final List<ConstraintApiDTO> constraintApiDtos = new ArrayList<>(response.getEntityConstraintCount());
        for (EntityConstraint entityConstraint : response.getEntityConstraintList()) {
            ConstraintApiDTO constraintApiDTO = new ConstraintApiDTO();

            constraintApiDTO.setEntityType(ApiEntityType.fromType(entityConstraint.getEntityType()).apiStr());

            // We only care about RelationType.bought.
            constraintApiDTO.setRelation(RelationType.bought);

            constraintApiDTO.setNumPotentialEntities(entityConstraint.getNumPotentialPlacements());

            final ServiceEntityApiDTO serviceEntityApiDTO = new ServiceEntityApiDTO();
            serviceEntityApiDTO.setDisplayName(entityConstraint.getCurrentPlacement().getDisplayName());
            constraintApiDTO.setRelatedEntities(Collections.singletonList(serviceEntityApiDTO));

            final List<PlacementOptionApiDTO> placementOptionApiDTOs =
                new ArrayList<>(entityConstraint.getPotentialPlacementsCount());
            List<Long> policyIdsToFetch = entityConstraint.getPotentialPlacementsList().stream()
                .map(PotentialPlacements::getCommodityType)
                .filter(c -> RiskUtil.POLICY_COMMODITY_TYPES.contains(c.getType())
                    || isCommodityTypeEligibleForMerge(c))
                .filter(c -> c.hasKey())
                .map(c -> Longs.tryParse(c.getKey()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            Map<Long, Policy> policies = Maps.newHashMap();
            if (!policyIdsToFetch.isEmpty()) {
                policyRpcService.getPolicies(PolicyRequest.newBuilder()
                    .addAllPolicyIds(policyIdsToFetch).build())
                    .forEachRemaining(policy -> policies.put(
                        policy.getPolicy().getId(), policy.getPolicy()));
            }

            for (PotentialPlacements potentialPlacement : entityConstraint.getPotentialPlacementsList()) {
                final PlacementOptionApiDTO placementOptionApiDTO = new PlacementOptionApiDTO();
                String source = discoveringTargets;

                placementOptionApiDTO.setConstraintType(ClassicEnumMapper.getCommodityString(
                    CommodityType.forNumber(potentialPlacement.getCommodityType().getType())));
                placementOptionApiDTO.setKey(potentialPlacement.getCommodityType().getKey());

                placementOptionApiDTO.setNumPotentialEntities(potentialPlacement.getNumPotentialPlacements());

                final BaseApiDTO baseApiDTO = new BaseApiDTO();
                baseApiDTO.setDisplayName(initializeScopeDisplayName(potentialPlacement));
                TopologyDTO.CommodityType cType = potentialPlacement.getCommodityType();
                boolean isTurboConstraint = false;
                if (RiskUtil.POLICY_COMMODITY_TYPES.contains(cType.getType())
                    || isCommodityTypeEligibleForMerge(cType)) {
                    Long policyId = Longs.tryParse(potentialPlacement.getCommodityType().getKey());
                    if (policyId != null && policies.containsKey(policyId)) {
                        baseApiDTO.setDisplayName(policies.get(policyId).getPolicyInfo().getDisplayName());
                        if (!policies.get(policyId).hasTargetId()) {
                            // it is a policy created in Turbonomic, so it is a turbonomic constraint
                            isTurboConstraint = true;

                        }
                    }
                }
                placementOptionApiDTO.setScope(baseApiDTO);
                // Only set the target if the constraint is not created in Turbo.
                // If the constraint is created in Turbo, then don't set the target. The UI by
                // default will show "Turbonomic" as the source of the constraint
                if (!isTurboConstraint) {
                    TargetApiDTO target = new TargetApiDTO();
                    target.setType(source);
                    placementOptionApiDTO.setTarget(target);
                }

                placementOptionApiDTOs.add(placementOptionApiDTO);
            }
            constraintApiDTO.setPlacementOptions(placementOptionApiDTOs);

            constraintApiDtos.add(constraintApiDTO);
        }
        return constraintApiDtos;
    }

    private boolean isCommodityTypeEligibleForMerge(TopologyDTO.CommodityType commodityType) {
        return (commodityType.getType() == CommodityType.CLUSTER_VALUE
            || (commodityType.getType() == CommodityType.STORAGE_CLUSTER_VALUE
            && TopologyDTOUtil.isRealStorageClusterCommodityKey(commodityType.getKey()))
            || commodityType.getType() == CommodityType.DATACENTER_VALUE
            || commodityType.getType() == CommodityType.ACTIVE_SESSIONS_VALUE);
    }

    private String initializeScopeDisplayName(PotentialPlacements potentialPlacementRecord) {
        if (!StringUtils.isEmpty(potentialPlacementRecord.getScopeDisplayName())) {
            return potentialPlacementRecord.getScopeDisplayName();
        } else {
            return ActionDTOUtil.getCommodityDisplayName(potentialPlacementRecord.getCommodityType());
        }
    }

    @Override
    public EntityPaginationResponse getPotentialEntitiesByEntity(String uuid, ConstraintApiInputDTO inputDto,
                                                                 EntityPaginationRequest paginationRequest) throws Exception {
        final long entityOid = getEntityOidFromString(uuid);

        final PotentialPlacementsRequest.Builder request =
            PotentialPlacementsRequest.newBuilder()
                .setOid(entityOid)
                .setRelationType(EntityConstraints.RelationType.BOUGHT)
                .addAllCommodityType(inputDto.getPlacementOptions().stream()
                    .filter(option -> !"".equals(option.getKey()))
                    .map(option -> TopologyDTO.CommodityType.newBuilder()
                        .setType(ClassicEnumMapper.commodityType(option.getConstraintType()).getNumber())
                        .setKey(option.getKey()).build())
                    .collect(Collectors.toList()))
                .setPaginationParams(paginationMapper.toProtoParams(paginationRequest));

        if (inputDto.getEntityTypeFilter() != null) {
            // TODO: Add a mapper for API EntityType enum (OM-60615)
            request.addPotentialEntityTypes(ApiEntityType.fromString(
                inputDto.getEntityTypeFilter().name()).typeNumber());
        }

        final PotentialPlacementsResponse response =
            entityConstraintsRpcService.getPotentialPlacements(request.build());

        final List<ServiceEntityApiDTO> nextPage = serviceEntityMapper.toServiceEntityApiDTO(
            response.getEntitiesList());

        return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
            .map(nextCursor -> paginationRequest.nextPageResponse(
                nextPage, nextCursor, response.getPaginationResponse().getTotalRecordCount()))
            .orElseGet(() -> paginationRequest.finalPageResponse(
                nextPage, response.getPaginationResponse().getTotalRecordCount()));
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
        code.accept(neighbors.map(ServiceEntityMapper::toBaseServiceEntityApiDTO)
                .collect(Collectors.toList()));
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

    /**
     * Get the entity's OID.
     *
     * @param uuid the uuid.
     * @throws OperationFailedException if the string is not an entity's OID.
     */
    private long getEntityOidFromString(@Nonnull final String uuid)
            throws OperationFailedException, IllegalArgumentException {
        if(org.apache.commons.lang.StringUtils.isEmpty(uuid)) {
            throw new IllegalArgumentException(String.format(EMPTY_UUID_MESSAGE, uuid));
        }
        ApiId apiId = uuidMapper.fromUuid(uuid);
        if (!apiId.isEntity()) {
            throw new IllegalArgumentException(String.format(NO_ENTITY_MESSAGE, uuid));
        }
        return apiId.oid();
    }

    /**
     * Get the entity's or group's OID.
     *
     * @param uuid the uuid.
     * @throws OperationFailedException if the string is neither an entity's OID nor a group's OID.
     */
    private long getEntityOrGroupOidFromString(@Nonnull final String uuid)
            throws OperationFailedException {
        if(org.apache.commons.lang.StringUtils.isEmpty(uuid)) {
            throw new IllegalArgumentException(String.format(EMPTY_UUID_MESSAGE, uuid));
        }
        ApiId apiId = uuidMapper.fromUuid(uuid);
        if (!apiId.isEntity() && !apiId.isGroup()) {
            throw new IllegalArgumentException(String.format(NO_ENTITY_GROUP_MESSAGE, uuid));
        }
        return apiId.oid();
    }

    /**
     * Get list of cloud cost statistics for given entity.
     * POST /entities/{entity_Uuid}/cost
     *
     * @param cloudEntityUuid uuid of a cloud entity
     * @param costInputApiDTO Filters and groupings applied to cost statistic
     * @return List of {@link StatSnapshotApiDTO} containing cloud cost data
     */
    @Override
    public List<StatSnapshotApiDTO> getEntityCloudCostStats(
            @Nonnull final String cloudEntityUuid,
            @Nullable final CostInputApiDTO costInputApiDTO)
            throws OperationFailedException, UnknownObjectException {
        final long entityOid = getEntityOidFromString(cloudEntityUuid);
        final EntityDTO.EntityType entityType = repositoryApi.entityRequest(entityOid)
                .getEntity()
                .map(ApiPartialEntity::getEntityType)
                .map(EntityType::forNumber)
                .orElseThrow(() -> new UnknownObjectException(cloudEntityUuid));
        return costStatsQueryExecutor.getEntityCostStats(
                entityOid, entityType, costInputApiDTO);
    }
}
