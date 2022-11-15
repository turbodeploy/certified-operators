package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.RepositoryRequestResult;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.GroupFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.TagsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroupProducer;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ObjectsPage;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionInputUtil;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.component.external.api.util.cost.CostStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
import com.vmturbo.api.cost.CostInputApiDTO;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionResourceImpactStatApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.notification.NotificationSettingsApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.Origin;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMemberOrderBy;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMembersPaginationResponse;
import com.vmturbo.api.pagination.GroupPaginationRequest;
import com.vmturbo.api.pagination.GroupPaginationRequest.GroupOrderBy;
import com.vmturbo.api.pagination.GroupPaginationRequest.GroupPaginationResponse;
import com.vmturbo.api.pagination.PaginationRequest;
import com.vmturbo.api.pagination.PaginationResponse;
import com.vmturbo.api.pagination.PaginationUtil;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteTagRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.CreationOriginCase;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.User;
import com.vmturbo.common.protobuf.group.GroupDTO.OriginFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.tag.Tag.DeleteTagListRequest;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Service implementation of Groups functionality.
 **/
public class GroupsService implements IGroupsService {

    /**
     * Map Entity types to be expanded to the RelatedEntityType to retrieve. The entity is a
     * grouping entity in classic.
     */
    private static final Map<Integer, List<String>> GROUPING_ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
        EntityType.DATACENTER_VALUE, ImmutableList.of(ApiEntityType.PHYSICAL_MACHINE.apiStr()),
        EntityType.VIRTUAL_DATACENTER_VALUE, ImmutableList.of(ApiEntityType.VIRTUAL_DATACENTER.apiStr())
    );

    public static final Set<String> NESTED_GROUP_TYPES =
        ImmutableSet.of(StringConstants.CLUSTER, StringConstants.STORAGE_CLUSTER,
                StringConstants.VIRTUAL_MACHINE_CLUSTER);

    /**
     * Map Api Enum Origin to GroupDTO.Origin.
     */
    public static final Map<Origin, Type> API_ORIGIN_TO_GROUPDTO_ORIGIN =
            ImmutableMap.<Origin, Type>builder()
                    .put(Origin.DISCOVERED, Type.DISCOVERED)
                    .put(Origin.USER, Type.USER)
                    .put(Origin.SYSTEM, Type.SYSTEM)
                    .build();

    /**
     * Map Api Enum Origin to GroupDTO.Origin.
     */
    public static final Map<CreationOriginCase, Origin> GROUPDTO_ORIGIN_TO_API_CREATION_ORIGIN =
            ImmutableMap.<CreationOriginCase, Origin>builder()
                    .put(CreationOriginCase.DISCOVERED, Origin.DISCOVERED)
                    .put(CreationOriginCase.USER, Origin.USER)
                    .put(CreationOriginCase.SYSTEM, Origin.SYSTEM)
                    .build();

    /**
     * Hardcoded uuid of the parent group of all user groups.
     */
    @VisibleForTesting
    static final String USER_GROUPS = "GROUP-MyGroups";
    @VisibleForTesting
    static final String CLUSTER_HEADROOM_GROUP_UUID = "GROUP-PhysicalMachineByCluster";
    @VisibleForTesting
    static final String STORAGE_CLUSTER_HEADROOM_GROUP_UUID = "GROUP-StorageByStorageCluster";

    private static final String ILLEGAL_GROUP_UUID_MESSAGE = "%s is an illegal group uuid.";

    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final GroupMapper groupMapper;

    private final GroupExpander groupExpander;

    private final UuidMapper uuidMapper;

    private final EntityAspectMapper entityAspectMapper;

    private final SettingsManagerMapping settingsManagerMapping;

    private final TemplateServiceBlockingStub templateService;

    private final RepositoryApi repositoryApi;

    private final long realtimeTopologyContextId;

    private final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub;

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private StatsService statsService = null;

    private final ActionSearchUtil actionSearchUtil;

    private final SettingsMapper settingsMapper;

    private final ThinTargetCache thinTargetCache;

    private final EntitySettingQueryExecutor entitySettingQueryExecutor;

    private final GroupFilterMapper groupFilterMapper;

    private final Logger logger = LogManager.getLogger();

    private final BusinessAccountRetriever businessAccountRetriever;

    private final ServiceProviderExpander serviceProviderExpander;

    private final PaginationMapper paginationMapper;

    private final UserSessionContext userSessionContext;

    private final CostStatsQueryExecutor costStatsQueryExecutor;

    GroupsService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                  @Nonnull final GroupServiceBlockingStub groupServiceRpc,
                  @Nonnull final GroupMapper groupMapper,
                  @Nonnull final GroupExpander groupExpander,
                  @Nonnull final UuidMapper uuidMapper,
                  @Nonnull final RepositoryApi repositoryApi,
                  final long realtimeTopologyContextId,
                  @Nonnull final SettingsManagerMapping settingsManagerMapping,
                  @Nonnull final TemplateServiceBlockingStub templateService,
                  @Nonnull final EntityAspectMapper entityAspectMapper,
                  @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
                  @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                  @Nonnull final ActionSearchUtil actionSearchUtil,
                  @Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub,
                  @Nonnull final SettingsMapper settingsMapper,
                  @Nonnull final ThinTargetCache thinTargetCache,
                  @Nonnull final EntitySettingQueryExecutor entitySettingQueryExecutor,
                  @Nonnull final GroupFilterMapper groupFilterMapper,
                  @Nonnull final BusinessAccountRetriever businessAccountRetriever,
                  @Nonnull final ServiceProviderExpander serviceProviderExpander,
                  @Nonnull final PaginationMapper paginationMapper,
                  @Nonnull final UserSessionContext userSessionContext,
                  @Nonnull final CostStatsQueryExecutor costStatsQueryExecutor) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.settingsManagerMapping = Objects.requireNonNull(settingsManagerMapping);
        this.templateService = templateService;
        this.entityAspectMapper = entityAspectMapper;
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.settingPolicyServiceBlockingStub = Objects.requireNonNull(settingPolicyServiceBlockingStub);
        this.actionSearchUtil = Objects.requireNonNull(actionSearchUtil);
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);
        this.entitySettingQueryExecutor = entitySettingQueryExecutor;
        this.groupFilterMapper = Objects.requireNonNull(groupFilterMapper);
        this.businessAccountRetriever = Objects.requireNonNull(businessAccountRetriever);
        this.serviceProviderExpander = Objects.requireNonNull(serviceProviderExpander);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.userSessionContext = userSessionContext;
        this.costStatsQueryExecutor = Objects.requireNonNull(costStatsQueryExecutor);
    }

    /**
     * Connect to the stats service.  We use a setter to avoid circular dependencies
     * in the Spring configuration in the API component.
     *
     * @param statsService the stats service.
     */
    public void setStatsService(@Nonnull StatsService statsService) {
        this.statsService = Objects.requireNonNull(statsService);
    }

    /**
     * Get groups from the group component. This method is not optimized in case we need to
     * paginate the results. Consider using {@link #getPaginatedGroupApiDTOs} or
     * {@link #getPaginatedGroups} instead.
     *
     * @return a list of {@link GroupApiDTO}.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if invalid request has been passed
     */
    @Override
    public ResponseEntity<List<GroupApiDTO>> getGroups()
            throws ConversionException, InterruptedException {
        List<GroupApiDTO> results = getGroupApiDTOS(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.getDefaultInstance())
            .build(), true);
        return PaginationUtil.buildResponseEntity(results, null, null, null);
    }

    /**
     * Get groups from the group component. This method is optimized in case we need to
     * paginate the results.
     *
     * @param groupPaginationRequest pagination parameters.
     * @return a {@link GroupPaginationResponse} that contains the list with this page's groups and
     *         pagination related information.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if invalid request has been passed
     */
    @Override
    public GroupPaginationResponse getPaginatedGroups(@Nonnull GroupPaginationRequest groupPaginationRequest,
                                                      @Nullable final Origin groupOrigin)
            throws ConversionException, InterruptedException, InvalidOperationException {
        if (groupPaginationRequest == null) {
            throw new InvalidOperationException("Missing pagination parameters.");
        }

        GroupFilter.Builder groupFilter = GroupFilter.newBuilder();
        if (groupOrigin != null) {
            groupFilter.setOriginFilter(OriginFilter.newBuilder()
                    .addOrigin(API_ORIGIN_TO_GROUPDTO_ORIGIN.get(groupOrigin)));
        }
        /*
         * Use the old implementation (paginate inside api component) in the following cases:
         * - if order_by is set to COST:
         *      Currently group component doesn't support ordering groups by COST.
         */
        GroupPaginationRequest.GroupOrderBy orderBy = groupPaginationRequest.getOrderBy();
        if (orderBy == GroupOrderBy.COST) {
            List<Grouping> groups = new ArrayList<>();
            groupServiceRpc.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(groupFilter)
                    .build()
            ).forEachRemaining(groups::add);
            final ObjectsPage<GroupApiDTO> paginatedGroupApiDTOs =
                    groupMapper.toGroupApiDto(groups, true,
                            paginationMapper.groupToSearchPaginationRequest(groupPaginationRequest),
                            null);
            final int totalRecordCount = paginatedGroupApiDTOs.getTotalCount();
            // Determine if this is the final page
            long nextCursor = paginatedGroupApiDTOs.getNextCursor();
            if (nextCursor == totalRecordCount) {
                return groupPaginationRequest.finalPageResponse(paginatedGroupApiDTOs.getObjects(),
                        totalRecordCount);
            } else {
                return groupPaginationRequest.nextPageResponse(paginatedGroupApiDTOs.getObjects(),
                        Long.toString(nextCursor), totalRecordCount);
            }
        }

        // For all other cases we do a query to group component for paginated results.
        return (GroupPaginationResponse)requestPaginatedGroupsFromGroupComponent(
                groupPaginationRequest,
                GetPaginatedGroupsRequest.newBuilder()
                        .setGroupFilter(groupFilter)
                        .setPaginationParameters(
                                paginationMapper.toProtoParams(groupPaginationRequest))
                        .build());
    }


    @Override
    public GroupApiDTO getGroupByUuid(String uuid, boolean includeAspects)
            throws UnknownObjectException, ConversionException, InterruptedException {
        // The "Nightly Plan Configuration" UI calls this API to populate the list of clusters and
        // their associated templates. The UI uses the uuid "GROUP-PhysicalMachineByCluster" with
        // request.  This UUID is defined in the context of version 6.1 implementation.
        // In order to reuse UI as is, we decided to handle this use case as a special case.
        // If the uuid equals "GROUP-PhysicalMachineByCluster", return null so that UI won't show it.
        // The logic to get a list of clusters in in method getMembersByGroupUuid.
        if (uuid.equals(CLUSTER_HEADROOM_GROUP_UUID)) {
            return null;
        }

        final Optional<Grouping> groupAndMembers = groupExpander.getGroup(uuid);
        if (groupAndMembers.isPresent()) {
            return groupMapper.groupsToGroupApiDto(Collections.singletonList(groupAndMembers.get()),
                    true).values().iterator().next();
        } else {
            final String msg = "Group not found: " + uuid;
            throw new UnknownObjectException(msg);
        }
    }

    /**
     * Get Ids of all entities from a given group by expanding and traversing through its members.
     * @param groupUuid - unique ID for the parent group object
     * @param apiId - derived ApiId from the group
     * @return a Set containing entity ids for the group
     * @throws OperationFailedException - the entity-grouping type is not supported
     */
    @Nonnull
    @VisibleForTesting
    Set<Long> getLeafEntitiesByGroupUuid(@Nonnull String groupUuid, @Nonnull ApiId apiId)
            throws OperationFailedException {

        // first check if it's group
        final Optional<GroupAndMembers> groupAndMembers = groupExpander.getGroupWithMembersAndEntities(groupUuid);
        if (groupAndMembers.isPresent()) {
            return Sets.newHashSet(groupAndMembers.get().entities());
        }

        if (apiId.isTarget()) {
            return expandUuids(Collections.singleton(groupUuid), Collections.emptyList(), null);
        }

        // check if scope is entity, if not, throw exception
        final MinimalEntity entity = repositoryApi.entityRequest(Long.parseLong(groupUuid)).getMinimalEntity()
                .orElseThrow(() -> new UnsupportedOperationException("Scope: " + groupUuid + " is not supported"));
        // check if supported grouping entity
        if (!GROUPING_ENTITY_TYPES_TO_EXPAND.containsKey(entity.getEntityType())) {
            throw new UnsupportedOperationException("Entity: " + groupUuid + " is not supported");
        }
        return expandUuids(Collections.singleton(groupUuid),
                GROUPING_ENTITY_TYPES_TO_EXPAND.get(entity.getEntityType()), null);
    }

    @Override
    public ResponseEntity<List<ServiceEntityApiDTO>> getEntitiesByGroupUuid(@Nonnull String uuid,
                                                                            @Nullable SearchPaginationRequest searchPagination) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        final Search.SearchParameters.Builder searchBuilder = Search.SearchParameters.newBuilder();
        Set<Long> leafEntities = Collections.emptySet();

        if (apiId.isRealtimeMarket()) {
            searchBuilder.setStartingFilter(SearchProtoUtil.entityTypeFilter(SearchProtoUtil.SEARCH_ALL_TYPES));
        } else {
            leafEntities = this.getLeafEntitiesByGroupUuid(uuid, apiId);

            // Special handling for the empty member list, because passing empty to repositoryApi returns all entities.
            if (leafEntities.isEmpty()) {
                return PaginationUtil.buildResponseEntity(Collections.emptyList(), null, null, null);
            }
            searchBuilder.setStartingFilter(SearchProtoUtil.idFilter(leafEntities));
        }

        final RepositoryApi.SearchRequest searchRequest = repositoryApi.newSearchRequest(searchBuilder.build());
        searchRequest.usePriceIndexPopulator(true);

        // Handle paginated request/response
        if (searchPagination != null) {
            final Pagination.PaginationParameters paginationParameters = paginationMapper.toProtoParams(searchPagination);
            return searchRequest.getPaginatedSEList(paginationParameters);
        }

        // Handle non-paginated request/response
        final List<ServiceEntityApiDTO> entities = searchRequest.getSEList();

        int missingEntities = leafEntities.size() - entities.size();
        if (missingEntities > 0) {
            logger.warn("{} entities from scope {} not found in repository.", missingEntities, uuid);
        }

        return PaginationUtil.buildResponseEntity(entities, null, null, null);
    }

    @Override
    public List<LogEntryApiDTO> getNotificationsByGroupUuid(String uuid, String starttime, String endtime, String category) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LogEntryApiDTO getNotificationByGroupUuid(String uuid, String nUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getNotificationCountStatsByUuid(final String uuid,
                                                        final ActionApiInputDTO actionApiInputDTO)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }


    /**
     * Return a list of {@link ActionApiDTO} object for Service Entities in the given group.
     *
     * @param uuid ID of the Group for which ActionApiDTOs should be returned
     * @return a list of {@link ActionApiDTO} object reflecting the Actions stored in the ActionOrchestrator for
     * Service Entities in the given group id.
     */
    @Override
    public ActionPaginationResponse getActionsByGroupUuid(String uuid,
                                      ActionApiInputDTO inputDto,
                                      ActionPaginationRequest paginationRequest) throws Exception {
        return actionSearchUtil.getActionsByScope(uuidMapper.fromUuid(uuid), inputDto,
                paginationRequest);
    }

    @Override
    public ActionApiDTO getActionByGroupUuid(@Nonnull final String uuid,
                                             @Nonnull final String aUuid,
                                             @Nullable final ActionDetailLevel detailLevel)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<PolicyApiDTO> getPoliciesByGroupUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<NotificationSettingsApiDTO> getNotificationSettingsByGroup(@Nonnull final String groupUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<SettingsManagerApiDTO> getSettingsByGroupUuid(String uuid, boolean includePolicies) throws Exception {
        final ApiId id = uuidMapper.fromUuid(uuid);
        if (!id.isGroup()) {
            throw new IllegalArgumentException(uuid + " is not a group uuid.");
        }
        final List<SettingsManagerApiDTO> mgrs =
            entitySettingQueryExecutor.getEntitySettings(id, includePolicies);

        GroupID groupID = GroupID.newBuilder()
                            .setId(Long.valueOf(uuid))
                            .build();
        GetGroupResponse response = groupServiceRpc.getGroup(groupID);

        Grouping group = response.getGroup();
        Template template = getHeadroomTemplate(group.getId());

        SettingsManagerApiDTO settingsManagerApiDto = settingsMapper.toSettingsManagerApiDTO(template);

        mgrs.add(settingsManagerApiDto);

        return mgrs;
    }

    @Override
    public SettingApiDTO getSettingByGroup(final String groupUuid, final String settingManagerUuid,
                                           final String settingUuid) throws Exception {
        Optional<Grouping> maybeGroup = groupExpander.getGroup(groupUuid);
        if (!maybeGroup.isPresent()) {
            throw new IllegalArgumentException(groupUuid + " is not a group id.");
        }
        Grouping group = maybeGroup.get();

        // we don't even need to check the settingManagerUuid, to be honest. the settingUuid should
        // be enough of an ID in XL.
        // if we are requesting the special "headroom template" setting then we don't need to go to the
        // setting service.Grouping
        if (SettingsMapper.CLUSTER_HEADROOM_TEMPLATE_SETTING_UUID.equalsIgnoreCase(settingUuid)) {
            return getTemplateSetting(group.getId());
        } else {
            // for the other managers, we need to get the setting from the setting service. We'll
            // use the entitySettingQueryExecutor for now. We could probably optimize the call a bit
            // since we're only expecting a limited result, but since this is an "undocumented API"
            // right now, not going to worry about performance yet.
            Optional<SettingApiDTO> optionalSetting = entitySettingQueryExecutor
                    .getEntitySettings(
                            uuidMapper.fromUuid(groupUuid),
                            false,
                            Collections.singleton(settingUuid),
                            null)
                    .stream()
                    .map(settingManager -> settingManager.getSettings())
                    .flatMap(List::stream)
                    .map(setting -> (SettingApiDTO)setting)
                    .findFirst();

            if (!optionalSetting.isPresent()) {
                throw new UnknownObjectException("Setting " + settingUuid + " not found for id " + groupUuid);
            }
            return optionalSetting.get();
        }
    }

    @Override
    public List<? extends SettingApiDTO<?>> getSettingsByGroupAndManager(final String groupUuid,
                                                         final String settingManagerUuid) throws Exception {
        Optional<Grouping> maybeGroup = groupExpander.getGroup(groupUuid);
        if (!maybeGroup.isPresent()) {
            throw new IllegalArgumentException(groupUuid + " is not a group id.");
        }

        Grouping group = maybeGroup.get();
        // if we are requesting the special "capacity plan data manager" we won't even go to the
        // setting service.
        if (SettingsMapper.CLUSTER_HEADROOM_SETTINGS_MANAGER.equalsIgnoreCase(settingManagerUuid)) {
            return Collections.singletonList(getTemplateSetting(group.getId()));
        } else {
            // for the other managers, we need to get the settings from the setting service.
            return entitySettingQueryExecutor.getEntitySettings(
                    uuidMapper.fromUuid(groupUuid),
                    false,
                    settingsManagerMapping.getSettingNamesForManagers(Collections.singleton(settingManagerUuid)), null)
                    .stream()
                        .map(SettingsManagerApiDTO::getSettings)
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        }
    }

    private Template getHeadroomTemplate(final long groupId) throws UnknownObjectException {
        // Get the headroom template with the ID in ClusterInfo if available.
        Optional<Template> templateOpt = getClusterHeadroomTemplate(groupId);
        if (templateOpt.isPresent()) {
            return templateOpt.get();
        }

        // If the headroom template ID is not set in clusterInfo, or the template with the ID is
        // not found, get the default headroom template.
        final Optional<Template> headroomTemplates = TemplateProtoUtil.flattenGetResponse(
            templateService.getTemplates(TemplateDTO.GetTemplatesRequest.newBuilder()
                    .setFilter(TemplateDTO.TemplatesFilter.newBuilder()
                        .addTemplateName(CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME))
                    .build()))
                .map(TemplateDTO.SingleTemplateResponse::getTemplate)
                .filter(template -> template.getType().equals(Template.Type.SYSTEM))
                // Stable sort of results. Normally there will only be one.
                .sorted(Comparator.comparingLong(Template::getId))
                .findFirst();
        if (!headroomTemplates.isPresent()) {
            throw new UnknownObjectException("No system headroom VM found!");
        } else {
            return headroomTemplates.get();
        }
    }

    /**
     * Gets the template ID and name from the template service, and return the values in a
     * SettingsApiDTO object.
     *
     * @param groupId the ID of Group object for which the headroom template is to be found.
     * @return the VM headroom template information of the given cluster.
     * @throws UnknownObjectException No headroom template found for this group.
     */
    @Nonnull
    private SettingApiDTO<String> getTemplateSetting(long groupId) throws UnknownObjectException {
        Template headroomTemplate = getHeadroomTemplate(groupId);

        return SettingsMapper.toHeadroomTemplateSetting(headroomTemplate);
    }

    /**
     * Gets the template for the given group Id.
     *
     * @param groupId group ID
     * @return the Template if found. Otherwise, return an empty Optional object.
     */
    private Optional<Template> getClusterHeadroomTemplate(Long groupId) {
        try {
            GetHeadroomTemplateResponse response = templateService.getHeadroomTemplateForCluster(
                GetHeadroomTemplateRequest.newBuilder().setGroupId(groupId).build());
            if (response.hasHeadroomTemplate()) {
                return Optional.of(response.getHeadroomTemplate());
            } else {
                return Optional.empty();
            }
        } catch (StatusRuntimeException e) {
            logger.error("There was exception while getting headroom template for group `{}`",
                groupId, e);
            throw e;
        }
    }

    @Override
    public SettingApiDTO<String> putSettingByUuidAndName(String groupUuid,
                                                 String managerName,
                                                 String settingUuid,
                                                 SettingApiDTO<String> setting)
            throws Exception {
        // Update the cluster headroom template ID
        if (settingUuid.equals(SettingsMapper.CLUSTER_HEADROOM_TEMPLATE_SETTING_UUID) &&
                managerName.equals(SettingsMapper.CLUSTER_HEADROOM_SETTINGS_MANAGER)) {
            try {
                 templateService.updateHeadroomTemplateForCluster(
                     TemplateDTO.UpdateHeadroomTemplateRequest.newBuilder()
                                        .setGroupId(Long.parseLong(groupUuid))
                                        .setTemplateId(Long.parseLong(setting.getValue()))
                                        .build());
                return getTemplateSetting(Long.parseLong(groupUuid));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid group ID or cluster headroom template ID");
            }
        }

        // The implementation for updating settings other than cluster headroom template is not available.
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByGroupUuid(String uuid, String encodedQuery) throws Exception {
        return statsService.getStatsByEntityUuid(uuid, encodedQuery);
    }

    @Override
    public void deleteGroup(String uuid)  throws UnknownObjectException, InvalidOperationException {
        final DeleteGroupResponse res = groupServiceRpc.deleteGroup(
                            GroupID.newBuilder().setId(Long.parseLong(uuid)).build());
        // FIXME Add detailed information to the {@link DeleteGroupResponse} structure about the deletion group status
        // And throw out the correct exceptions declared in the controller groups
        if (!res.getDeleted()) {
            throw new InvalidOperationException("Failed to delete group with uuid " + uuid);
        }
    }

    @Override
    public GroupApiDTO createGroup(GroupApiDTO inputDTO) throws ConversionException,
            InterruptedException, InvalidOperationException, OperationFailedException {
        String username = getUsername();

        final GroupDefinition groupDefinition = groupMapper.toGroupDefinition(inputDTO);
        try {
            final CreateGroupResponse resp = groupServiceRpc.createGroup(CreateGroupRequest.newBuilder()
                            .setGroupDefinition(groupDefinition)
                            .setOrigin(GroupDTO.Origin.newBuilder().setUser(User.newBuilder().setUsername(username)))
                            .build());

            return groupMapper.groupsToGroupApiDto(Collections.singletonList(resp.getGroup()), true)
                    .values()
                    .iterator()
                    .next();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
                // The full "message" has the status pre-pended. Use just the description, if
                // available.
                final String message = e.getStatus().getDescription() == null
                        ? e.getMessage() : e.getStatus().getDescription();
                throw new InvalidOperationException(message);
            } else {
                throw new OperationFailedException(e.getMessage());
            }
        }
    }

    @VisibleForTesting
    protected String getUsername() {
        final Optional<AuthUserDTO> authUser = SAMLUserUtils.getAuthUserDTO();

        if (authUser.isPresent()) {
            return authUser.get().getUser();
        } else {
            logger.error("Cannot get the user name for user creating group");
            throw new IllegalStateException(
                            "An occured when creating group. Please check the logs.");
        }
    }

    @Nonnull
    @Override
    public GroupApiDTO editGroup(@Nonnull String uuid, @Nonnull GroupApiDTO inputDTO)
                    throws UnknownObjectException, ConversionException, InterruptedException,
                    OperationFailedException {

        final GetGroupResponse groupResponse =
                groupServiceRpc.getGroup(GroupID.newBuilder().setId(Long.parseLong(uuid)).build());

        if (!groupResponse.hasGroup()) {
            throw new UnknownObjectException(
                    String.format("Group with UUID %s does not exist", uuid));
        }

        final GroupDefinition groupDefinition = groupMapper.toGroupDefinition(inputDTO);
        final UpdateGroupResponse response = groupServiceRpc
                        .updateGroup(UpdateGroupRequest
                                        .newBuilder()
                                        .setId(Long.parseLong(uuid))
                                        .setNewDefinition(groupDefinition)
                                        .build());
        return groupMapper.groupsToGroupApiDto(
                Collections.singletonList(response.getUpdatedGroup()), true)
                .values()
                .iterator()
                .next();
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(String uuid, ActionApiInputDTO inputDto) throws Exception {
        try {
            // TODO : We need to support cloud stats for all scopes e.g. Stats for a group of 2 AWS VM entities.
            final boolean specialCloudStatsQuery =
                uuid.equals(DefaultCloudGroupProducer.ALL_CLOULD_WORKLOAD_AWS_AND_AZURE_UUID) &&
                !inputDto.getGroupBy().isEmpty() &&
                // For this special query we expect and support a specific way to group results.
                inputDto.getGroupBy().get(0).equals(StringConstants.RISK_SUB_CATEGORY);
            // Handle cloud stats
            if (specialCloudStatsQuery) {
                GetActionCategoryStatsResponse response =
                    actionOrchestratorRpc.getActionCategoryStats(
                        GetActionCategoryStatsRequest.newBuilder()
                            .setTopologyContextId(realtimeTopologyContextId)
                            .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                            .addEntityType(EntityType.DATABASE_VALUE)
                            .addEntityType(EntityType.DATABASE_SERVER_VALUE)
                            .build());
                List<StatSnapshotApiDTO> statSnapshotApiDTOS =
                    ActionCountsMapper.convertActionCategoryStatsToApiStatSnapshot(response.getActionStatsByCategoryList());
                if (inputDto.getStartTime() != null && inputDto.getEndTime() != null) {
                    // If the request is for ProjectedActions, set all numEntities to zero.
                    // This hack is needed because UI expects it in this format.
                    statSnapshotApiDTOS.stream()
                        .flatMap(dto -> dto.getStatistics().stream())
                        .filter(dto -> dto.getName() == StringConstants.NUM_ENTITIES)
                        .forEach(statApiDTO -> {
                            final StatValueApiDTO valueDto = new StatValueApiDTO();
                            float statValue = 0;
                            valueDto.setAvg(statValue);
                            valueDto.setMax(statValue);
                            valueDto.setMin(statValue);
                            valueDto.setTotal(statValue);
                            statApiDTO.setValues(valueDto);
                        });
                }
                return statSnapshotApiDTOS;
            }

            final ApiId apiScopeId = uuidMapper.fromUuid(uuid);
            if (isCloudTabGlobalOptimization(apiScopeId)) {
                // only get cloud related entities
                inputDto.setEnvironmentType(EnvironmentType.CLOUD);
                inputDto.setRelatedEntityTypes(
                    // all types. cannot make it empty because extractMgmtUnitSubgroupFilter will
                    // replace it with the groups entity types.
                    Arrays.stream(ApiEntityType.values()).map(ApiEntityType::apiStr).collect(Collectors.toList()));

                final Map<ApiId, List<StatSnapshotApiDTO>> retStats =
                    actionStatsQueryExecutor.retrieveActionStats(ImmutableActionStatsQuery.builder()
                        .addScopes(apiScopeId)
                        .actionInput(inputDto)
                        .build());

                return retStats.values().stream()
                    .flatMap(listStat -> listStat.stream())
                    .filter(listStat -> !CollectionUtils.isEmpty(listStat.getStatistics()))
                    .collect(Collectors.toList());
            } else {
                final Map<ApiId, List<StatSnapshotApiDTO>> retStats =
                    actionStatsQueryExecutor.retrieveActionStats(ImmutableActionStatsQuery.builder()
                        .addScopes(apiScopeId)
                        .actionInput(inputDto)
                        .build());

                return retStats.values().stream()
                    .flatMap(listStat -> listStat.stream())
                    .filter(listStat -> !CollectionUtils.isEmpty(listStat.getStatistics()))
                    .collect(Collectors.toList());
            }
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                return Collections.emptyList();
            } else {
                throw e;
            }
        }
    }

    @Override
    public List<StatSnapshotApiDTO> getActionStatsResourceImpactByUuid(String uuid, ActionResourceImpactStatApiInputDTO inputDTO) throws Exception {
        try {
            if (org.springframework.util.CollectionUtils.isEmpty(inputDTO.getActionResourceImpactStatList())) {
                throw new InvalidOperationException("Missing list of ActionResourceImpactStat");
            }

            final ApiId apiScopeId = uuidMapper.fromUuid(uuid);
            final Map<ApiId, List<StatSnapshotApiDTO>> retStats =
                    actionStatsQueryExecutor.retrieveActionStats(ImmutableActionStatsQuery.builder()
                            .addScopes(apiScopeId)
                            .actionInput(ActionInputUtil.toActionApiInputDTO(inputDTO))
                            .actionResourceImpactIdentifierSet(ActionInputUtil.toActionResourceImpactIdentifierSet(inputDTO))
                            .build());

            return retStats.values().stream()
                    .flatMap(listStat -> listStat.stream())
                    .filter(listStat -> !CollectionUtils.isEmpty(listStat.getStatistics()))
                    .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                return Collections.emptyList();
            } else {
                throw e;
            }
        }
    }

    private static boolean isCloudTabGlobalOptimization(ApiId apiScopeId) {
        Optional<CachedGroupInfo> opt = apiScopeId.getCachedGroupInfo();
        if (!opt.isPresent()) {
            return false;
        }
        CachedGroupInfo groupInfo = opt.get();
        Set<ApiEntityType> entityTypes = groupInfo.getEntityTypes();
        return apiScopeId.isGlobalTempGroup()
            && (entityTypes.contains(ApiEntityType.REGION) || entityTypes.contains(ApiEntityType.AVAILABILITY_ZONE));
    }

    @Override
    public void validateInput(GroupApiDTO inputDTO, Errors errors) {
        // TODO: Implement validation for groups
    }

    /**
     * In classic, this method would get a sequence of ancestor groups that a group may belong to,
     * and is used to present a "breadcrumb" trail in the UI. In XL, we don't have the same group
     * hierarchy, and the concept of a breadcrumb trail for groups doesn't apply the same way. So
     * in XL, the group breadcrumb trail will only contain two items: "Home" and a link to itself.
     *
     * @param groupUuid uuid of the group for which the breadcrumb list is being requested
     * @param path boolean that according to the API indicates whether to include all parents up to
     *             the root, but in practice does nothing in XL
     * @return a DTO describing the current group
     */
    @Override
    public List<BaseApiDTO> getGroupsByUuid(final String groupUuid, final Boolean path)  {
        // Although classic returns a "breadcrumb trail" here, the sequence returned could be
        // arbitrary -- if a group belonged to multiple groups, one would be chosen as the ancestor
        // irrespective of how the user actually navigated to the group in the UI. Classic also has
        // a folder-like structure for navigating through groups that XL does not.
        //
        // So, in XL, we've decided to only show the current group in the breadcrumb trail.
        Optional<Grouping> optionalGroup = groupExpander.getGroup(groupUuid);
        if (optionalGroup.isPresent()) {
            Grouping group = optionalGroup.get();
            final ServiceEntityApiDTO groupDTO = new ServiceEntityApiDTO();
            groupDTO.setDisplayName(group.getDefinition().getDisplayName());
            groupDTO.setUuid(groupUuid);
            groupDTO.setClassName(StringConstants.GROUP);
            return Collections.singletonList(groupDTO);
        }

        return new ArrayList<>();
    }

    @Override
    public List<SettingsPolicyApiDTO> getSettingPoliciesByGroupUuid(String uuid) throws Exception {
        if (!StringUtils.isNumeric(uuid)) {
            throw new IllegalArgumentException("Group uuid should be numeric: " + uuid);
        }

        // find all the members in the group and get policies associated with each member
        List<Long> members = getGroupMembers(uuid).stream()
                .map(TopologyEntityDTO::getOid).collect(Collectors.toList());
        GetEntitySettingPoliciesRequest request =
            GetEntitySettingPoliciesRequest.newBuilder()
                .addAllEntityOidList(members)
                .setIncludeInactive(true)
                .build();
        GetEntitySettingPoliciesResponse response =
            settingPolicyServiceBlockingStub.getEntitySettingPolicies(request);

        return settingsMapper.convertSettingPolicies(response.getSettingPoliciesList());
    }

    @Override
    public Map<String, EntityAspect> getAspectsByGroupUuid(String uuid)
            throws UnknownObjectException, ConversionException, InterruptedException {
        return entityAspectMapper.getAspectsByGroup(getGroupMembers(uuid))
            .entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getApiName(), Entry::getValue));
    }

    @Override
    public EntityAspect getAspectByGroupUuid(String uuid, String aspectTag)
            throws UnknownObjectException, ConversionException, InterruptedException {
        return entityAspectMapper.getAspectByGroup(getGroupMembers(uuid), aspectTag);
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByGroupUuidAspect(String uuid, String aspectTag, String encodedQuery) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByGroupUuidAspectQuery(String uuid, String aspectTag, StatPeriodApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ActionApiDTO> getCurrentActionsByGroupUuidAspect(String uuid, String aspectTag) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ActionApiDTO> getActionsByGroupUuidAspect(String uuid, String aspectTag, ActionApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ServiceEntityApiDTO> getSettingsPolicyEntitiesBySetting(String groupUuid,
                                                                        String automationManagerUuid,
                                                                        String settingUuid,
                                                                        String settingPolicyUuid) throws Exception {
        // TODO (roman, Aug 8 2019): Modify the entity settings RPC to allow putting in a group
        // UUID directly.
        final Set<Long> groupMembers = groupExpander.expandUuid(groupUuid);

        final Set<Long> entitiesInGroupAffectedByPolicy = SettingDTOUtil.flattenEntitySettings(
            settingPolicyServiceBlockingStub.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setSettingFilter(EntitySettingFilter.newBuilder()
                    // Restrict to entities in the group
                    .addAllEntities(groupMembers)
                    // Affected by the specific policy
                    .setPolicyId(Long.parseLong(settingPolicyUuid))
                    // For a specific setting (note - this is just to reduce the size of the
                    // response).
                    .addSettingName(StringUtils.strip(settingUuid)))
                .build()))
                .flatMap(settingGroup -> settingGroup.getEntityOidsList().stream())
            .collect(Collectors.toSet());

        return repositoryApi.entitiesRequest(entitiesInGroupAffectedByPolicy).getSEList();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByGroupQuery(String uuid,
                                                         StatPeriodApiInputDTO inputDto)
            throws Exception {
        return statsService.getStatsByEntityQuery(uuid, inputDto);
    }

    /**
     * Gets groups for magic UI groupUuid.
     * @param uuid group uuid
     * @param request paginated request
     * @return {@link GroupMembersPaginationResponse} containing groups for uuid
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if invalid request has been passed
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws OperationFailedException when the filters don't match the group type
     */
    @VisibleForTesting
    @Nonnull
    GroupMembersPaginationResponse getMembersForUIMagicGroupUuid(@Nonnull String uuid,
                                                                         @Nonnull final GroupMembersPaginationRequest request)
                    throws InterruptedException, InvalidOperationException, ConversionException,
                    OperationFailedException {
        if (CLUSTER_HEADROOM_GROUP_UUID.equals(uuid)) {
            return request.allResultsResponse(getGroupsByType(GroupType.COMPUTE_HOST_CLUSTER, Collections.emptyList(),
                                                              Collections.emptyList())
                                                              .stream()
                                                              // TODO: The next line is a workaround of a UI limitation. The UI only accepts groups
                                                              // with classname "Group" This line can be removed when bug OM-30381 is fixed.
                                                              .peek(groupApiDTO -> groupApiDTO.setClassName(StringConstants.GROUP))
                                                              .collect(Collectors.toList()));
        } else if (STORAGE_CLUSTER_HEADROOM_GROUP_UUID.equals(uuid)) {
            return request.allResultsResponse(getGroupsByType(GroupType.STORAGE_CLUSTER, Collections.emptyList(), Collections.emptyList())
                                                              .stream()
                                                              // TODO: The next line is a workaround of a UI limitation. The UI only accepts groups
                                                              // with classname "Group" This line can be removed when bug OM-30381 is fixed.
                                                              .peek(groupApiDTO -> groupApiDTO.setClassName(StringConstants.GROUP))
                                                              .collect(Collectors.toList()));
        } else if (USER_GROUPS.equals(uuid)) { // Get all user-created groups
            final Collection<GroupApiDTO> groups = getGroupApiDTOS(GetGroupsRequest.newBuilder()
                                                                                   .setGroupFilter(GroupFilter.newBuilder().setOriginFilter(OriginFilter
                                                                                                                                                            .newBuilder().addOrigin(GroupDTO.Origin.Type.USER)))
                                                                                   .build(), true);
            return request.allResultsResponse(Lists.newArrayList(groups));
        }
        return request.allResultsResponse(Collections.emptyList());
    }

    /**
     * Checks if groupUuid is magic UI string referring to particular set of groups.
     * @param groupUuid groupUuid of focus
     * @return true if groupUuid is magic UI string
     */
    @VisibleForTesting
    boolean isMagicUiStringGroupUuid(@Nonnull String groupUuid) {
        return CLUSTER_HEADROOM_GROUP_UUID.equals(groupUuid)
                || STORAGE_CLUSTER_HEADROOM_GROUP_UUID.equals(groupUuid)
                || USER_GROUPS.equals(groupUuid);
    }

    /**
     * Gets {@link GroupAndMembers} with immediate groups only from groupUuid, or throws error.
     * @param groupUuid groupUuid of focus
     * @return GroupAndMembers group and only immediate members set to members and entities on object
     * @throws IllegalArgumentException invalid groupUuid, unable to get members
     */
    @VisibleForTesting
    GroupAndMembers getGroupWithImmediateMembersOnly(String groupUuid) throws IllegalArgumentException {
        return groupExpander.getGroupWithImmediateMembersOnly(groupUuid)
                        .orElseThrow(() ->
                     new IllegalArgumentException("Can't get members of an invalid group: " + groupUuid));
    }

    /**
     * Get members info for groupUuid.
     * @param groupUuid groupUuid to request
     * @param request paginated request parameters
     * @return {@link GroupMembersPaginationResponse}
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if invalid request has been passed
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws OperationFailedException when the filters don't match the group type
     **/
    @Override
    public GroupMembersPaginationResponse getMembersByGroupUuid(@Nonnull String groupUuid,
                                                                @Nonnull final GroupMembersPaginationRequest request)
                    throws InterruptedException, InvalidOperationException, ConversionException,
                    OperationFailedException {

        if (isMagicUiStringGroupUuid(groupUuid)) {
            return getMembersForUIMagicGroupUuid(groupUuid, request);
        }

        // Get members of the group with the uuid (oid)
        final GroupAndMembers groupAndMembers = getGroupWithImmediateMembersOnly(groupUuid);

        logger.info("Number of members for group {} is {}", groupUuid, groupAndMembers.members().size());

        // If members is empty, there are no groups or entities to return
        if (groupAndMembers.members().isEmpty() ) {
           return request.allResultsResponse( Collections.emptyList());
        }

        if (GroupProtoUtil.isNestedGroup(groupAndMembers.group())) {
            //Check to see if pagination is requested... else use legacy code path with no pagination for backwards compatibility.
            if (request.hasLimit() || request.getCursor().isPresent()) {
                //Pagination applied in group component
                return getGroupsPaginatedCall(groupAndMembers.members(), request);
            }
            final Collection<GroupApiDTO> groups =
                            getGroupApiDTOS(GetGroupsRequest.newBuilder()
                               .setGroupFilter(GroupFilter
                                   .newBuilder()
                                   .addAllId(groupAndMembers.members()))
                               .build(), true);
            return request.allResultsResponse(Lists.newArrayList(groups));
        }
        // Group members are entities or business accounts, call repository component
        final long skipCount;
        if (request.getCursor().isPresent()) {
            try {
                skipCount = Long.parseLong(request.getCursor().get());
                if (skipCount < 0) {
                    throw new InvalidOperationException("Illegal cursor: " +
                        skipCount + ". Must be be a positive integer");
                }
            } catch (NumberFormatException e) {
                throw new InvalidOperationException("Cursor " + request.getCursor() +
                    " is invalid. Should be a number.");
            }

        } else {
            skipCount = 0;
        }
        if (request.getOrderBy() != GroupMemberOrderBy.ID) {
            throw new InvalidOperationException("Order " + request.getOrderBy().name() +
                " is invalid. The only supported order is by id");
        }
        final int memberCount = groupAndMembers.members().size();
        int actualFoundCount = memberCount;
        final List<Long> nextPageIds = groupAndMembers.members().stream()
            .sorted(request.isAscending() ? Comparator.naturalOrder() : Comparator.reverseOrder())
            .skip(skipCount)
            .limit(request.getLimit())
            .collect(Collectors.toList());
        final RepositoryRequestResult entities =
                repositoryApi.getByIds(nextPageIds, Collections.emptySet(), false);
        final Collection<BaseApiDTO> results = new ArrayList<>(
                entities.getBusinessAccounts().size() +
                        entities.getServiceEntities().size());
        results.addAll(entities.getBusinessAccounts());
        results.addAll(entities.getServiceEntities());

        final int missingEntities = nextPageIds.size() - results.size();
        if (missingEntities > 0) {
            logger.warn("{} group members from group {} not found in repository.",
                missingEntities, groupUuid);
            actualFoundCount -= missingEntities;
        }
        Long nextCursor = skipCount + nextPageIds.size();
        if (nextCursor == memberCount) {
            return request.finalPageResponse(Lists.newArrayList(results), actualFoundCount);
        }
        return request.nextPageResponse(Lists.newArrayList(results),
            Long.toString(nextCursor), actualFoundCount);
    }

    @Override
    public List<TagApiDTO> getTagsByGroupUuid(final String s) throws Exception {

        // if s is not a group uuid, it will throw an exception
        final long oid = getGroupOidFromString(s);
        final Map<String, TagValuesDTO> tagsMap = new HashMap<>();

        final GetTagsRequest tagsRequest =
                GetTagsRequest.newBuilder().addGroupId(oid).build();
        final GetTagsResponse tagsForGroups =
                groupServiceRpc.getTags(tagsRequest);
        final Tags tags = tagsForGroups.getTagsMap().get(oid);
        if (tags != null) {
            tagsMap.putAll(tags.getTagsMap());
        }
        return TagsMapper.convertTagsToApi(tagsMap);
    }

    @Override
    public List<TagApiDTO> createTagsByGroupUuid(final String uuid, final List<TagApiDTO> tagApiDTOs)
        throws OperationFailedException {
        final long oid = getGroupOidFromString(uuid);

        // Convert to Tags, and validate, otherwise throws IllegalArgumentException
        Tags tags = convertToTags(tagApiDTOs);
        final CreateTagsRequest request = CreateTagsRequest.newBuilder()
                .setGroupId(oid)
                .setTags(tags).build();

        try {
            groupServiceRpc.createTags(request);
        } catch (StatusRuntimeException e) {
            throw new OperationFailedException("Group service RPC call failed to complete request: "
                    + e.getMessage());
        }

        return tagApiDTOs;
    }

    private static Tags convertToTags(List<TagApiDTO> apiTags) throws OperationFailedException {

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
    public void deleteTagsByGroupUuid(final String uuid) throws Exception {
        final long oid = getGroupOidFromString(uuid);
        final DeleteTagsRequest request = DeleteTagsRequest.newBuilder().setGroupOid(oid).build();
        try {
            groupServiceRpc.deleteTags(request);
        } catch (StatusRuntimeException e) {
            throw new OperationFailedException("Unable to delete tag for Group: '" + uuid + "'", e);
        }
    }

    @Override
    public void deleteTagsByGroupUuid(final String uuid, final List<String> tagKeys) throws Exception {
        final long oid = getGroupOidFromString(uuid);
        final DeleteTagListRequest request = DeleteTagListRequest.newBuilder()
                .setOid(oid)
                .addAllTagKey(tagKeys)
                .build();
        try {
            groupServiceRpc.deleteTagList(request);
        } catch (StatusRuntimeException e) {
            throw new OperationFailedException("Unable to delete tag for Group: '" + uuid + "'", e);
        }
    }

    /**
     * Get the groups matching a {@link GetGroupsRequest} from the group component, and convert
     * them to the associated {@link GroupApiDTO} format.
     *
     * @param groupsRequest The request.
     * @param populateSeverity Whether or not to populate the severity in the response. Populating
     *                         severity requires another relatively expensive RPC call, so use this
     *                         only when necessary.
     * @param environmentType type of the environment to include in response, if null, all are included
     * @return The list of {@link GroupApiDTO} objects.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    private List<GroupApiDTO> getGroupApiDTOS(final GetGroupsRequest groupsRequest,
            final boolean populateSeverity, @Nullable final EnvironmentType environmentType)
            throws ConversionException, InterruptedException {
        //TODO: OM-65316  Group component will support severity soon, can transfer logic of getting
        //severity to group Component side
        final Iterator<Grouping> response = groupServiceRpc.getGroups(groupsRequest);
        final List<Grouping> groups = new ArrayList<>();
        response.forEachRemaining(group -> {
            if (!isHiddenGroup(group)) {
                groups.add(group);
            }
        });
        final ObjectsPage<GroupApiDTO> result;
        try {
            result = groupMapper.toGroupApiDto(groups, populateSeverity, null, environmentType);
        } catch (InvalidOperationException e) {
            throw new ConversionException("Error faced converting groups " +
                groups.stream().map(Grouping::getId).collect(Collectors.toList()), e);
        }
        return result.getObjects();
    }

    /**
     * Get the groups matching a {@link GetGroupsRequest} from the group component, and convert
     * them to the associated {@link GroupApiDTO} format.
     *
     * @param groupsRequest The request.
     * @param populateSeverity Whether or not to populate the severity in the response. Populating
     *                         severity requires another relatively expensive RPC call, so use this
     *                         only when necessary.
     * @return The list of {@link GroupApiDTO} objects.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    public List<GroupApiDTO> getGroupApiDTOS(final GetGroupsRequest groupsRequest,
            final boolean populateSeverity) throws ConversionException, InterruptedException {
        return getGroupApiDTOS(groupsRequest, populateSeverity, null);
    }

    /**
     * Calls group component to get members of groupAndMembers.
     * @param groupUuids groups ids to request and paginate
     * @param groupMembersPaginationRequest pagination request
     * @return paginated response
     * @throws ConversionException Error converting groups
     */
    @Nonnull
    @VisibleForTesting
    GroupMembersPaginationResponse getGroupsPaginatedCall(@Nonnull Collection<Long> groupUuids,
                      @Nonnull final GroupMembersPaginationRequest groupMembersPaginationRequest)
                    throws ConversionException {
        final GroupFilter groupFilter = GroupFilter.newBuilder()
                        .addAllId(groupUuids)
                        .setIncludeHidden(false)
                        .build();

        final PaginationParameters paginationParameters = paginationMapper.toProtoParams(groupMembersPaginationRequest);

        final GetPaginatedGroupsRequest getPaginatedGroupsRequest =
                        GetPaginatedGroupsRequest.newBuilder()
                            .setGroupFilter(groupFilter)
                            .setPaginationParameters(paginationParameters)
                            .build();

        final GetPaginatedGroupsResponse paginatedGroupsResponse =
                        groupServiceRpc.getPaginatedGroups(getPaginatedGroupsRequest);
        List<Grouping> groupings = paginatedGroupsResponse.getGroupsList();

        final ObjectsPage<GroupApiDTO> result;
        try {
            result = groupMapper.toGroupApiDto(groupings, true, null, null);
        } catch (InvalidOperationException | ConversionException | InterruptedException e) {
            throw new ConversionException("Error faced converting groups "
                                          + groupings.stream().map(Grouping::getId).collect(Collectors.toList()), e);
        }
        final List<BaseApiDTO> groupApiDTOS = Lists.newArrayList(result.getObjects());
        return PaginationProtoUtil.getNextCursor(paginatedGroupsResponse.getPaginationResponse())
                        .map(nextCursor -> groupMembersPaginationRequest.nextPageResponse(groupApiDTOS, nextCursor,
                            paginatedGroupsResponse.getPaginationResponse().getTotalRecordCount()))
                        .orElseGet(() -> groupMembersPaginationRequest.finalPageResponse(groupApiDTOS,
                            paginatedGroupsResponse.getPaginationResponse().getTotalRecordCount()));
    }

    /**
     * Get the groups from the group component and convert them to the associated
     * {@link GroupApiDTO} format. Pagination happens in group component except if orderBy is set to
     * COST or scopes parameter is being passed, in which case pagination happens (inefficiently) in
     * api component's memory.
     *
     * @param filterList the list of filter criteria to apply.
     * @param paginationRequest Contains the limit, the order and a potential cursor
     * @param groupType the type of the group
     * @param groupEntityTypes Contains set of entityTypes of the group members
     * @param cloudType cloud type to include in response; if null, all are included
     * @param environmentType type of the environment to include in response, if null, all are included
     * @param scopes all result groups should be within this list of scopes, which can be entity or group
     * @param includeAllGroupClasses true if the search should return all types of groups, not just
     *                               REGULAR.
     *                               filterList is assumed empty if includeAllGroupClasses is true.
     * @param groupOrigin the origin to filter the groups
     *
     * @return The list of {@link GroupApiDTO} objects.
     * @throws InvalidOperationException When the cursor is invalid.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws OperationFailedException when input filters do not apply to group type.
     */
    @Nonnull
    public SearchPaginationResponse getPaginatedGroupApiDTOs(final List<FilterApiDTO> filterList,
                                                             final SearchPaginationRequest paginationRequest,
                                                             @Nullable GroupType groupType,
                                                             @Nullable final Set<String> groupEntityTypes,
                                                             @Nullable EnvironmentType environmentType,
                                                             @Nullable CloudType cloudType,
                                                             @Nullable List<String> scopes,
                                                             final boolean includeAllGroupClasses,
                                                             @Nullable final Origin groupOrigin)
            throws InvalidOperationException, ConversionException, OperationFailedException,
            InterruptedException {
        /*
         * Use the old, inefficient implementation (paginate inside api component) in the following
         * cases:
         * - if order_by is set to COST:
         *      Currently group component doesn't support ordering groups  by COST.
         */
        if (paginationRequest.getOrderBy() == SearchOrderBy.COST) {
            final GetGroupsRequest groupsRequest = getGroupsRequestForFilters(
                    groupType == null ? GroupType.REGULAR : groupType,
                    filterList, scopes, includeAllGroupClasses, groupOrigin).build();
            final List<Grouping> groups = new LinkedList<>();
            if (groupEntityTypes != null && !groupEntityTypes.isEmpty()) {
                final Iterable<Grouping> rawGroups = () -> groupServiceRpc.getGroups(groupsRequest);
                Predicate<GroupDefinition> memberTypePredicate = memberTypePredicate(groupEntityTypes);
                StreamSupport.stream(rawGroups.spliterator(), false)
                        .filter(g -> memberTypePredicate.test(g.getDefinition()))
                        .forEachOrdered(groups::add);
            } else {
                groupServiceRpc.getGroups(groupsRequest).forEachRemaining(groups::add);
            }
            // Paginate the response and return it.
            return nextGroupPage(groups, paginationRequest, environmentType);
        }

        // For all other cases we do a query to group component for paginated results.
        return (SearchPaginationResponse)requestPaginatedGroupsFromGroupComponent(
                paginationRequest,
                createGetPaginatedGroupsRequest(paginationRequest, groupType, filterList,
                        includeAllGroupClasses, groupOrigin, environmentType, cloudType,
                        groupEntityTypes, scopes));
    }

    private GetPaginatedGroupsRequest createGetPaginatedGroupsRequest(
            final SearchPaginationRequest paginationRequest,
            @Nullable GroupType groupType,
            @Nonnull List<FilterApiDTO> filterList,
            boolean includeAllGroupClasses,
            @Nullable Origin groupOrigin,
            @Nullable EnvironmentType environmentType,
            @Nullable CloudType cloudType,
            @Nullable final Set<String> groupEntityTypes,
            @Nullable List<String> scopes)
            throws OperationFailedException, InvalidOperationException {
        GetPaginatedGroupsRequest.Builder requestBuilder = GetPaginatedGroupsRequest.newBuilder();
        final GroupType resolvedGroupType = groupType == null ? GroupType.REGULAR : groupType;
        GroupFilter.Builder groupFilter = groupFilterMapper.apiFilterToGroupFilter(
                resolvedGroupType, LogicalOperator.AND, filterList);
        if (includeAllGroupClasses && resolvedGroupType != GroupType.REGULAR) {
            String errorMessage = "includeAllGroupClasses flag cannot be set to true for group "
                    + "type: " + resolvedGroupType.name();
            throw new OperationFailedException(errorMessage);
        }
        // if we are including all subclasses, clear the group type from the filter
        if (includeAllGroupClasses) {
            groupFilter.clearGroupType();
        }
        if (scopes != null) {
            if (scopes.size() == 1 && scopes.get(0).equals(USER_GROUPS)) {
                // if we are looking for groups created by user, we should also add a origin filter
                groupFilter.setOriginFilter(
                        OriginFilter.newBuilder().addOrigin(GroupDTO.Origin.Type.USER));
            } else {
                // add scopes to filter resulting groups
                requestBuilder.addAllScopes(convertScopes(scopes));
            }
        }
        // groupOrigin if present will override setting origin filter via scopes equals USER_GROUP
        // approach.
        if (groupOrigin != null) {
            groupFilter.setOriginFilter(OriginFilter.newBuilder()
                    .addOrigin(API_ORIGIN_TO_GROUPDTO_ORIGIN.get(groupOrigin)));
        }
        if (environmentType != null) {
            groupFilter.setEnvironmentType(EnvironmentTypeMapper.fromApiToXL(environmentType));
        }
        if (cloudType != null) {
            groupFilter.setCloudType(CloudTypeMapper.fromApiToXlProtoEnum(cloudType));
        }
        if (groupEntityTypes != null && !groupEntityTypes.isEmpty()) {
            for (String memberType : groupEntityTypes) {
                // Determine if the memberType is group or entity, and set the appropriate filter.
                ApiEntityType eType = ApiEntityType.fromString(memberType);
                GroupType gType = GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.get(memberType);
                if (!ApiEntityType.UNKNOWN.equals(eType)) {
                    groupFilter.addDirectMemberTypes(MemberType.newBuilder()
                            .setEntity(eType.typeNumber())
                            .build());
                } else if (gType != null) {
                    groupFilter.addDirectMemberTypes(
                            MemberType.newBuilder().setGroup(gType).build());
                } else {
                    logger.warn("Ignoring invalid group member type: " + memberType);
                }
            }
        }
        requestBuilder.setGroupFilter(groupFilter.build());
        requestBuilder.setPaginationParameters(
                paginationMapper.toProtoParams(
                        // convert the request to GroupPaginationRequest before
                        // converting to proto params so that we get the correct
                        // orderBy value
                        paginationMapper.searchToGroupPaginationRequest(paginationRequest)));
        return requestBuilder.build();
    }

    private PaginationResponse requestPaginatedGroupsFromGroupComponent(
            PaginationRequest inputRequest, GetPaginatedGroupsRequest requestToGroupComponent)
            throws ConversionException, InterruptedException {
        GetPaginatedGroupsResponse response = groupServiceRpc.getPaginatedGroups(
                requestToGroupComponent);
        final List<Grouping> groupings = response.getGroupsList();
        final ObjectsPage<GroupApiDTO> result;
        try {
            result = groupMapper.toGroupApiDto(groupings, true, null, null);
        } catch (InvalidOperationException | ConversionException e) {
            throw new ConversionException("An error occurred during the conversion of "
                    + (groupings.size() == 1
                    ? "group with uuid" + groupings.get(0).getId() + "."
                    : groupings.size() + " groups."), e);
        } finally {
            if (groupings.size() > 1) {
                logger.debug("Uuids of groups in the page which failed to get converted: {}",
                        groupings.stream().map(Grouping::getId).collect(Collectors.toList()));
            }
        }
        final List<BaseApiDTO> groupApiDTOS = Lists.newArrayList(result.getObjects());
        return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                .map(nextCursor -> inputRequest.nextPageResponse(groupApiDTOS,
                        nextCursor, response.getPaginationResponse().getTotalRecordCount()))
                .orElseGet(() -> inputRequest.finalPageResponse(groupApiDTOS,
                        response.getPaginationResponse().getTotalRecordCount()));
    }

    /**
     * Create a predicate which will test whether a {@link GroupDefinition} contains the desired
     * entity types.
     *
     * @param groupEntityTypes Set of entityTypes to map
     * @return The predicate, which will return true if its input contains ANY of the desired types.
     */
    @Nonnull
    private static Predicate<GroupDefinition> memberTypePredicate(@Nonnull final Set<String> groupEntityTypes) {
        final Set<GroupType> acceptableGroupTypes = new HashSet<>();
        final Set<Integer> acceptableEntityTypes = new HashSet<>();
        groupEntityTypes.forEach(groupType -> {
            if (GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.containsKey(groupType)) {
                // group of groups, for example: group of Clusters, group of ResourceGroups
                acceptableGroupTypes.add(GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.get(groupType));
            } else {
                // group of entities
                acceptableEntityTypes.add(ApiEntityType.fromString(groupType).typeNumber());
            }
        });

        return group -> {
            if (group.hasStaticGroupMembers()) {
                // static group
                // ResourceGroup members contain multiple entity types, VMs, Volumes, etc.
                // However, when search for groups for any of these entity types, ResourceGroups
                // should not display. Otherwise it may break certain constraints, for example
                // groups selected in policy can't contain multiple entity types.
                // Return ResourceGroup only when search criteria contains ResourceGroup.
                if (group.getType() == GroupType.RESOURCE) {
                    return acceptableGroupTypes.contains(group.getType());
                }
                return group.getStaticGroupMembers()
                        .getMembersByTypeList()
                        .stream()
                        .anyMatch(staticMembersByType -> {
                            MemberType memberType = staticMembersByType.getType();
                            if (memberType.hasGroup()) {
                                return acceptableGroupTypes.contains(memberType.getGroup());
                            } else {
                                return acceptableEntityTypes.contains(memberType.getEntity());
                            }
                        });
            } else if (group.hasGroupFilters()) {
                // dynamic group of groups
                return group.getGroupFilters()
                        .getGroupFilterList()
                        .stream()
                        .anyMatch(groupFilter -> acceptableGroupTypes.contains(groupFilter.getGroupType()));
            } else if (group.hasEntityFilters()) {
                // dynamic group of entities
                return group.getEntityFilters()
                        .getEntityFilterList()
                        .stream()
                        .anyMatch(entityFilter -> acceptableEntityTypes.contains(entityFilter.getEntityType()));
            } else {
                return false;
            }
        };
    }

    /**
     * Create a GetGroupsRequest based on the given filterList.
     *
     * @param groupType the group type we are creating request for.
     * @param filterList a list of FilterApiDTO to be applied to this group; only "groupsByName" is
     *                   currently supported
     * @return a GetGroupsRequest with the filtering set if an item in the filterList is found
     * @throws OperationFailedException when input filters do not apply to group type.
     * @throws ConversionException on errors converting data to API DTOs
     */
    @VisibleForTesting
    GetGroupsRequest.Builder getGroupsRequestForFilters(@Nonnull GroupType groupType,
            @Nonnull List<FilterApiDTO> filterList)
            throws OperationFailedException, ConversionException {
        return getGroupsRequestForFilters(groupType, filterList, Collections.emptyList(), false, null);
    }

    /**
     * Create a GetGroupsRequest based on the given filterList and scopes. The resulting groups
     * will be within the range of the given scopes.
     *
     * @param groupType the group type we are creating request for.
     * @param filterList a list of FilterApiDTO to be applied to this group; only "groupsByName" is
     *                   currently supported
     * @param scopes list of scopes which are used to filter the resulting groups
     * @param includeAllGroupClasses flag indicating whether or not to include all types of Groups
     *                               like Clusters.  This should only be set to true
     *                               if the groupType is REGULAR and filterList is empty.
     * @param groupOrigin the group origin to filter groups
     * @return a GetGroupsRequest with the filtering set if an item in the filterList is found
     * @throws OperationFailedException when input filters do not apply to group type.
     * @throws ConversionException If the input filters cannot be converted.
     */
    @VisibleForTesting
    GetGroupsRequest.Builder getGroupsRequestForFilters(
            @Nonnull GroupType groupType,
            @Nonnull List<FilterApiDTO> filterList,
            @Nullable List<String> scopes,
            boolean includeAllGroupClasses,
            @Nullable Origin groupOrigin) throws OperationFailedException, ConversionException {
        GetGroupsRequest.Builder request = GetGroupsRequest.newBuilder();
        GroupFilter groupFilter = groupFilterMapper.apiFilterToGroupFilter(
                groupType, LogicalOperator.AND, filterList).build();
        if (includeAllGroupClasses && groupType != GroupType.REGULAR) {
            String errorMessage =
                String.format("includeAllGroupClasses flag cannot be set to true for group type %s.",
                    groupType.name());
            throw new OperationFailedException(errorMessage);
        }
        // if we are including all subclasses, clear the group type from the filter
        if (includeAllGroupClasses) {
            groupFilter = groupFilter.toBuilder().clearGroupType().build();
        }
        request.setGroupFilter(groupFilter);

        if ( scopes != null ) {
            if (scopes.size() == 1 && scopes.get(0).equals(USER_GROUPS)) {
                // if we are looking for groups created by user, we should also add a origin filter
                request.getGroupFilterBuilder().setOriginFilter(
                        OriginFilter.newBuilder()
                                .addOrigin(GroupDTO.Origin.Type.USER));
            } else {
                // add scopes to filter resulting groups
                request.addAllScopes(convertScopes(scopes));
            }
        }

        // groupOrigin if present will override setting origin filter via scopes equals
        // USER_GROUP approach.
        if ( groupOrigin != null ) {
            request.getGroupFilterBuilder().setOriginFilter(
                    OriginFilter.newBuilder().addOrigin(API_ORIGIN_TO_GROUPDTO_ORIGIN.get(groupOrigin))
            );
        }
        return request;
    }

    private Collection<Long> convertScopes(Collection<String> scopeUuids)
                    throws OperationFailedException {
        final Collection<Long> result = new HashSet<>();
        for (String uuid : scopeUuids) {
            result.add(uuidMapper.fromUuid(uuid).oid());
        }
        return Collections.unmodifiableCollection(result);
    }

    /**
     * Return a SearchPaginationResponse containing groups ordered and limited according to the
     * paginationRequest parameters.
     *
     * @param groups The groups to populate and order.
     * @param paginationRequest Contains the parameters for the pagination.
     * @param environmentType type of the environment to include in response, if null, all are included
     * @return The {@link SearchPaginationResponse} containing the groups.
     * @throws InvalidOperationException When the cursor is invalid.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    private SearchPaginationResponse nextGroupPage(final List<Grouping> groups,
                                                   final SearchPaginationRequest paginationRequest,
                                                   @Nullable final EnvironmentType environmentType)
            throws InvalidOperationException, ConversionException, InterruptedException {
        final ObjectsPage<GroupApiDTO> paginatedGroupApiDTOs =
                groupMapper.toGroupApiDto(groups, true, paginationRequest,
                        environmentType);
        final int totalRecordCount = paginatedGroupApiDTOs.getTotalCount();
        final List<BaseApiDTO> retList = new ArrayList<>(paginatedGroupApiDTOs.getObjects());

        // Determine if this is the final page
        long nextCursor = paginatedGroupApiDTOs.getNextCursor();
        if (nextCursor == totalRecordCount) {
            return paginationRequest.finalPageResponse(retList, totalRecordCount);
        } else {
            return paginationRequest.nextPageResponse(retList, Long.toString(nextCursor),
                    totalRecordCount);
        }
    }

    /**
     * Return the groups inside a list of scopes.
     *
     * @param groupType The type of groups to return.
     * @param scopes The scopes.
     * @param filterList The filters to apply to the returned groups.
     * @param environmentType type of the environment to include in response, if null, all are included
     * @return The {@link GroupApiDTO}s returning the groups matching the criteria inside the
     *         provided scopes.
     * @throws OperationFailedException if the filters do not apply to the group type.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if invalid request has been passed
     */
    @Nonnull
    private List<GroupApiDTO> getNestedGroupsInGroups(
            @Nonnull GroupType groupType,
            @Nonnull final List<String> scopes,
            @Nonnull final List<FilterApiDTO> filterList,
            @Nullable final EnvironmentType environmentType)
            throws OperationFailedException, ConversionException, InterruptedException,
            InvalidOperationException {
        final GetGroupsRequest.Builder reqBuilder = getGroupsRequestForFilters(groupType,
                        filterList);
        GroupFilter.Builder builder = GroupFilter.newBuilder(reqBuilder.getGroupFilter());
        scopes.stream()
            .map(groupExpander::getGroupWithMembersAndEntities)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(grAndMem -> {
                if (isNestedGroupOfType(grAndMem, groupType)) {
                    // group of clusters (resource groups, etc.), requested groupType is same type
                    // like: cluster (RG, etc.), can not use user scope framework to handle this
                    grAndMem.members().forEach(builder::addId);
                } else if (groupType == GroupType.RESOURCE && grAndMem.group()
                        .getExpectedTypesList()
                        .contains(GroupDTO.MemberType.newBuilder()
                                .setEntity(EntityType.BUSINESS_ACCOUNT_VALUE)
                                .build())) {
                    // group of business accounts, requested groups can only be resource groups
                    // get resource groups owned by business accounts
                    builder.addPropertyFilters(
                            SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.ACCOUNT_ID,
                                    grAndMem.members().stream().map(Object::toString).collect(Collectors.toList())));
                } else {
                    if (groupType != GroupType.REGULAR &&
                            groupType == grAndMem.group().getDefinition().getType()) {
                        // if scope is special group and requested type is of same type, then add itself
                        // for example: a single resource group, requested groupType is RG; a
                        // single cluster, requested type is cluster
                        builder.addId(grAndMem.group().getId());
                    } else {
                        // scope is normal group or requested groupType is different type from scope
                        // use user scope framework to handle this case (find groups of different
                        // types in group)
                        // for example: group of clusters, requested groupType is storage cluster.
                        reqBuilder.addScopes(grAndMem.group().getId());
                    }
                }
            });
        // if explicitly requesting group ids, it means these are special groups in this context,
        // we should not provide scopes if any, this is used to avoid some issues due to mixed scopes
        // for example: if scopes contains one group of RGs (g1), and the other is group of VMs (g2),
        // requested group type is RG, then it will set id to be members of g1, scopes to be g2,
        // we only want RG and ids are already decided, we should clear scopes, otherwise it will
        // return empty since members of g1 are not within scope g2
        if (builder.getIdCount() > 0) {
            reqBuilder.clearScopes();
        }
        reqBuilder.setGroupFilter(builder);
        return getGroupApiDTOS(reqBuilder.build(), true, environmentType);
    }

    private boolean isNestedGroupOfType(GroupAndMembers groupAndMembers, GroupType groupType) {
        final GroupDefinition group = groupAndMembers.group().getDefinition();
        if (group.hasStaticGroupMembers()) {
            return group.getStaticGroupMembers()
                .getMembersByTypeList()
                .stream()
                .anyMatch(staticMembersByType -> staticMembersByType.getType().hasGroup()
                        && staticMembersByType.getType().getGroup() == groupType);
        } else if (group.hasGroupFilters()) {
            return group
                .getGroupFilters().getGroupFilterList()
                .stream()
                .anyMatch(groupFilter -> groupFilter.getGroupType() == groupType);
        } else {
            return false;
        }
    }

    /**
     * Get {@link GroupApiDTO} describing groups in the system that match certain criteria.
     *
     * @param groupType The type of the group.
     * @param scopes The scopes to look for groups in, which can be either group or entity. usually
     *               the scopes will be handled by user scope framework, they represent supply chain
     *               scopes, all entities in resulting groups will be within the entities accessible
     *               from the scopes using a supply chain traversal. in some special cases (like:
     *               ResourceGroup, BusinessAccount, Cluster, etc.), the scopes are handled separately,
     * @param filterList The list of filters to apply to the groups.
     * @param environmentType type of the environment to include in response, if null, all are included
     * @return the list of groups.
     * @throws OperationFailedException when the filters don't match the group type.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if invalid request has been passed
     */
    @Nonnull
    List<GroupApiDTO> getGroupsByType(@Nonnull GroupType groupType,
            @Nullable final List<String> scopes,
            @Nonnull final List<FilterApiDTO> filterList, @Nullable EnvironmentType environmentType)
            throws OperationFailedException, ConversionException, InterruptedException,
            InvalidOperationException {
        // We assume it's either-or - i.e. either all scopes are groups, or all scopes are entities.
        if (UuidMapper.hasLimitedScope(scopes)) {
            if (scopes.stream().anyMatch(uuid -> {
                try {
                    return uuidMapper.fromUuid(uuid).isGroup();
                } catch (OperationFailedException e) {
                    return false;
                }
            })) {
                return getNestedGroupsInGroups(groupType, scopes, filterList, environmentType);
            } else {
                // get resource groups associated with business account from scope
                if (groupType.equals(GroupType.RESOURCE)) {
                    // handle RG specially since it's the only group owned by an entity (BA)
                    // use BA ids to find owned RGs, if scopes are BusinessAccounts, it will return
                    // owned RGs, if scopes are not BusinessAccounts, it's fine since it will
                    // return empty anyway
                    return getResourceGroupsOwnedByAccount(scopes, environmentType);
                }

                // general case of finding groups in entities (like: find clusters in datacenters)
                // use user scope framework to handle it
                final GetGroupsRequest request = getGroupsRequestForFilters(groupType, filterList,
                        scopes, false, null).build();
                return getGroupApiDTOS(request, true, environmentType);
            }
        } else {
            return getGroupApiDTOS(getGroupsRequestForFilters(groupType, filterList).build(),
                    true, environmentType);
        }
    }

    /**
     * Get resource group owned by account.
     *
     * @param scopes list of account ids
     * @param environmentType type of the environment to include in response, if null, all are
     * included
     * @return the list of groups owned by accounts from scope.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if invalid request has been passed
     */
    @Nonnull
    private List<GroupApiDTO> getResourceGroupsOwnedByAccount(List<String> scopes,
            EnvironmentType environmentType)
            throws ConversionException, InterruptedException, InvalidOperationException {
        final GetGroupsRequest.Builder groupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .addPropertyFilters(
                                SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.ACCOUNT_ID,
                                        scopes))
                        .build());
        return getGroupApiDTOS(groupsRequest.build(), true, environmentType);
    }

    /**
     * Get {@link GroupApiDTO} describing groups in the system that match certain criteria.
     *
     * @param groupType The type of the group.
     * @param scopes The scopes to look for groups in, which can be either group or entity. The
     *               scopes represent supply chain scopes, all entities in resulting groups will be
     *               within the entities accessible from the scopes using a supply chain traversal.
     * @param filterList The list of filters to apply to the groups.
     * @return the list of groups.
     * @throws OperationFailedException when the filters don't match the group type.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if invalid request has been passed
     */
    @Nonnull
    List<GroupApiDTO> getGroupsByType(@Nonnull GroupType groupType,
            @Nullable final List<String> scopes, @Nonnull final List<FilterApiDTO> filterList)
            throws OperationFailedException, ConversionException, InterruptedException,
            InvalidOperationException {
        return getGroupsByType(groupType, scopes, filterList, null);
    }

    /**
     * Check if the returned group is hidden group. We should not show the hidden group to users.
     *
     * @param group The group info fetched from Group Component.
     * @return true if the group is hidden group.
     */
    private boolean isHiddenGroup(@Nonnull final Grouping group) {
        return group.getDefinition().getIsHidden();
    }

    /**
     * Get all members of a given group uuid and return in the form of TopologyEntityDTO.
     */
    private List<TopologyEntityDTO> getGroupMembers(@Nonnull String uuid) throws UnknownObjectException {
        final GroupAndMembers groupAndMembers = groupExpander.getGroupWithMembersAndEntities(uuid)
            .orElseThrow(() -> new UnknownObjectException("Group not found: " + uuid));
        if (GroupProtoUtil.isNestedGroup(groupAndMembers.group())) {
            // The members of a nested group (e.g. group of clusters) don't have TopologyEntityDTO
            // representations.
            return Collections.emptyList();
        } else {
            // Get group members as TopologyEntityDTOs from the repository component
            return repositoryApi.entitiesRequest(Sets.newHashSet(groupAndMembers.members()))
                .getFullEntities()
                .collect(Collectors.toList());
        }
    }

    /**
     * Expand the given scope uuids to entities of related types. This is intended to achieve parity
     * with classic, where there are objects which are EntitiesProvider or Grouping entity, such as
     * Target, DataCenter, VirtualDataCenter, etc. These entities are supported in Search API and
     * getEntitiesByGroupUuid in classic.
     *
     * Currently the following scopes are supported:
     * <ul>
     * <li>target: entities discovered by the target</li>
     * <li>entity(dc/vdc/...): return related entities (vms if relatedEntityType is VirtualMachine)</li>
     * <li>group: return entities which are related to the leaf entities in the group</li>
     * <li>Market": return empty, caller of this function should handle this case</li>
     *</ul>
     *
     * Note: For most of the cases, scopeUuids will contain just one element. It will take more
     * time if more uuids are provided as scope.
     * @param scopeUuids set of oid strings passed in as scopes
     * @param relatedEntityTypes types of related entities to expand to
     * @param environmentType the type of the environment type to include
     * @return set of entity oids belonging to the given scopes
     */
    @Nonnull
    public Set<Long> expandUuids(@Nonnull Set<String> scopeUuids,
                                 @Nullable List<String> relatedEntityTypes,
                                 @Nullable EnvironmentType environmentType)
                throws OperationFailedException {
        // return empty immediately if "Market" exists
        if (scopeUuids.contains(UuidMapper.UI_REAL_TIME_MARKET_STR)) {
            return Collections.emptySet();
        }

        final Set<Long> result = new HashSet<>();
        // since the given scopeUuids can be heterogeneous, we should divide them into different groups
        // one for targets, the other for entities or groups (which will be used as seeds to fetch
        // related entities in supply chain)
        final Set<Long> targetUuids = new HashSet<>();
        final Set<String> seedUuids = new HashSet<>();
        scopeUuids.forEach(scopeUuid -> {
            if (thinTargetCache.getTargetInfo(Long.parseLong(scopeUuid)).isPresent()) {
                targetUuids.add(Long.parseLong(scopeUuid));
            } else {
                seedUuids.add(scopeUuid);
            }
        });

        // if there are targets in scopes, add entities discovered by that target, these entities
        // should not be added to seedUuids since they are all the entities for the target
        if (!targetUuids.isEmpty()) {
            final List<MinimalEntity> targetEntities =
                repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                        SearchProtoUtil.discoveredBy(targetUuids))
                        .build())
                    .getMinimalEntities()
                    .collect(Collectors.toList());
            final Set<Integer> relatedEntityTypesInt = relatedEntityTypes == null
                ? Collections.emptySet()
                : relatedEntityTypes.stream()
                    .map(ApiEntityType::fromString)
                    .map(ApiEntityType::typeNumber)
                    .collect(Collectors.toSet());
            final Predicate<MinimalEntity> filterByType = relatedEntityTypesInt.isEmpty()
                ? entity -> true
                : entity -> relatedEntityTypesInt.contains(entity.getEntityType());
            result.addAll(targetEntities.stream()
                .filter(filterByType)
                .map(MinimalEntity::getOid)
                .collect(Collectors.toSet()));
        }

        // use seedUuids (entity + group) to find entities of related type using supply chain
        if (!seedUuids.isEmpty()) {

            // expand service providers to regions, expandServiceProviders will return a set of all
            // regions and original non serviceProvider scopes. if found an empty list then expand
            // using supplychain
            final Set<Long> expandedScopes = serviceProviderExpander.expand(seedUuids.stream()
                    .map(Long::parseLong)
                    .collect(Collectors.toSet()));
            final Set<String> entityOrGroupsIds = expandedScopes.stream().map(s -> Long.toString(s)).collect(Collectors.toSet());

            final Map<String, SupplyChainNode> supplyChainForScope =
                supplyChainFetcherFactory.newNodeFetcher()
                    .topologyContextId(realtimeTopologyContextId)
                    .addSeedUuids(entityOrGroupsIds)
                    .entityTypes(relatedEntityTypes)
                    .apiEnvironmentType(environmentType)
                    .fetch();
            final Set<Long> relatedEntityOids = supplyChainForScope.values().stream()
                .flatMap(supplyChainNode -> RepositoryDTOUtil.getAllMemberOids(supplyChainNode).stream())
                .collect(Collectors.toSet());
            result.addAll(relatedEntityOids);
        }

        return result;
    }

    /**
     * Get the group's OID.
     *
     * @param uuid the uuid.
     * @throws OperationFailedException if the string is not an groups's OID.
     */
    private long getGroupOidFromString(@Nonnull final String uuid)
            throws OperationFailedException {
        if(StringUtils.isEmpty(uuid)) {
            throw new IllegalArgumentException(String.format(ILLEGAL_GROUP_UUID_MESSAGE, uuid));
        }
        ApiId apiId = uuidMapper.fromUuid(uuid);
        if (!apiId.isGroup()) {
            throw new IllegalArgumentException(String.format(ILLEGAL_GROUP_UUID_MESSAGE, uuid));
        }
        return apiId.oid();
    }

    /**
     * Get list of cloud cost statistics for given group.
     * POST /groups/{group_Uuid}/cost
     *
     * @param groupUuid uuid of the group.  Groups cloud entities will be considered.
     * @param costInputApiDTO Filters and groupings applied to cost statistic
     * @return List of {@link StatSnapshotApiDTO} containing cloud cost data
     * @throws IllegalArgumentException If group cannot be found by UUID.
     */
    @Override
    public List<StatSnapshotApiDTO> getGroupCloudCostStats(@Nonnull String groupUuid,
            @Nullable CostInputApiDTO costInputApiDTO) throws IllegalArgumentException {
        final Optional<Grouping> group = groupExpander.getGroup(groupUuid);
        if (!group.isPresent()) {
            throw new IllegalArgumentException(String.format(ILLEGAL_GROUP_UUID_MESSAGE, groupUuid));
        }
        final GroupAndMembers groupAndMembers = groupExpander.getMembersForGroup(group.get());
        if (groupAndMembers.members().isEmpty()) {
            return Collections.emptyList();
        }
        return costStatsQueryExecutor.getGroupCostStats(groupUuid, groupAndMembers, costInputApiDTO);
    }
}
