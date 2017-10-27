package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.setting.SettingApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClustersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;

/**
 * Service implementation of Groups functionality.
 **/
public class GroupsService implements IGroupsService {

    private static final Collection<String> GLOBAL_SCOPE_SUPPLY_CHAIN = ImmutableList.of(
            "GROUP-VirtualMachine", "GROUP-PhysicalMachineByCluster", "Market");

    private static final String USER_GROUPS = "GROUP-MyGroups";

    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final ClusterServiceBlockingStub clusterServiceRpc;

    private final ActionSpecMapper actionSpecMapper;

    private final GroupMapper groupMapper;

    private final RepositoryApi repositoryApi;

    private final long realtimeTopologyContextId;

    private final Logger logger = LogManager.getLogger();

    GroupsService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                         @Nonnull final GroupServiceBlockingStub groupServiceRpc,
                         @Nonnull final ClusterServiceBlockingStub clusterServiceRpc,
                         @Nonnull final ActionSpecMapper actionSpecMapper,
                         @Nonnull final GroupMapper groupMapper,
                         @Nonnull final RepositoryApi repositoryApi,
                         final long realtimeTopologyContextId) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.clusterServiceRpc = Objects.requireNonNull(clusterServiceRpc);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public List<GroupApiDTO> getGroups()  {
        // Currently, we have only user-created groups.
        return getGroupApiDTOS(GetGroupsRequest.getDefaultInstance());
    }


    @Override
    public GroupApiDTO getGroupByUuid(String uuid, boolean includeAspects) throws UnknownObjectException {
        final long id = Long.parseLong(uuid);

        // TODO (roman, Aug 3, 2017): It's inconvenient that the UI/API knows
        // they want a cluster or group, but want to look them up in a common interface.
        // To deal with the mismatch, we try searching through groups first, and if
        // the group is not found we look through clusters.
        // In the future it makes sense to have a single interface to search all "collections"
        // of entities.
        final GetGroupResponse groupRes = groupServiceRpc.getGroup(GroupID.newBuilder()
                .setId(id)
                .build());
        if (groupRes.hasGroup()) {
            return groupMapper.toGroupApiDto(groupRes.getGroup());
        } else {
            final GetClusterResponse clusterRes = clusterServiceRpc.getCluster(
                    GetClusterRequest.newBuilder()
                        .setClusterId(id)
                        .build());
            if (clusterRes.hasCluster()) {
                return groupMapper.toGroupApiDto(clusterRes.getCluster());
            } else {
                final String msg = "Group not found: " + uuid;
                logger.error(msg);
                throw new UnknownObjectException(msg);
            }
        }
    }

    @Override
    public List<?> getEntitiesByGroupUuid(String uuid) throws Exception {
        // for now, return all entities
        throw ApiUtils.notImplementedInXL();
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
     * Fetch the ActionSpecs from the Action Orchestrator.
     *
     * NOTE: the time window is ignored. The Action Orchestrator has no history.
     *
     * NOTE: the type, state, and mode filter lists are ignored. This filtering would be easy to add.
     *
     * NOTE: the "getCleared" flag is ignored.
     *
     * @param uuid ID of the Group for which ActionApiDTOs should be returned
     * @return a list of {@link ActionApiDTO} object reflecting the Actions stored in the ActionOrchestrator for
     * Service Entities in the given group id.
     */
    @Override
    public List<ActionApiDTO> getActionsByGroupUuid(String uuid,
                                                    ActionApiInputDTO inputDto) throws Exception {
        final ActionQueryFilter filter =
                        actionSpecMapper.createActionFilter(inputDto, getMemberIds(uuid));

        // Note this is a blocking call.
        Iterable<ActionOrchestratorAction> actions = () -> actionOrchestratorRpc.getAllActions(
            FilteredActionRequest.newBuilder()
                .setTopologyContextId(realtimeTopologyContextId)
                .setFilter(filter)
                .build()
        );
        List<ActionSpec> specs = StreamSupport.stream(actions.spliterator(), false)
            .map(ActionOrchestratorAction::getActionSpec)
            .collect(Collectors.toList());
        return actionSpecMapper.mapActionSpecsToActionApiDTOs(specs);
    }

    @Override
    public ActionApiDTO getActionByGroupUuid(String uuid, String aUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<PolicyApiDTO> getPoliciesByGroupUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<SettingsManagerApiDTO> getSettingsByGroupUuid(String uuid, boolean includePolicies) throws Exception {
        // TODO: OM-23672
        return new ArrayList<>();
    }

    @Override
    public SettingApiDTO putSettingByUuidAndName(String groupUuid, String settingUuid, String name, SettingApiInputDTO setting) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByGroupUuid(String uuid, String encodedQuery) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Boolean deleteGroup(String uuid) throws Exception {
        final DeleteGroupResponse res = groupServiceRpc.deleteGroup(
                            GroupID.newBuilder().setId(Long.parseLong(uuid)).build());

        return res.getDeleted();
    }

    @Override
    public GroupApiDTO createGroup(GroupApiDTO inputDTO) throws Exception {
        final GroupInfo request = groupMapper.toGroupInfo(inputDTO);
        final CreateGroupResponse res = groupServiceRpc.createGroup(request);
        return groupMapper.toGroupApiDto(res.getGroup());
    }

    @Override
    public GroupApiDTO editGroup(String uuid, GroupApiDTO inputDTO) throws Exception {
        final GroupInfo info = groupMapper.toGroupInfo(inputDTO);
        UpdateGroupResponse response = groupServiceRpc.updateGroup(UpdateGroupRequest.newBuilder()
            .setId(Long.parseLong(uuid))
            .setNewInfo(info)
            .build());
        return groupMapper.toGroupApiDto(response.getUpdatedGroup());
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(String uuid, ActionApiInputDTO inputDto) throws Exception {
        final ActionQueryFilter filter =
                actionSpecMapper.createActionFilter(inputDto, getMemberIds(uuid));

        try {
            final GetActionCountsResponse response =
                    actionOrchestratorRpc.getActionCounts(GetActionCountsRequest.newBuilder()
                        .setTopologyContextId(realtimeTopologyContextId)
                        .setFilter(filter)
                        .build());
            return ActionCountsMapper.countsByTypeToApi(response.getCountsByTypeList());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                return Collections.emptyList();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void validateInput(Object o, Errors errors) {
        // TODO: Implement validation for groups
    }

    /**
     * Get parent groups in which given group belongs.
     *
     * @param groupUuid uuid of the group for which the parent groups will be returned
     * @param path boolean to include all parents up to the root
     * @return a list of Groups and SE's which are parents of the given group; include all
     * parents up to the root if the 'path' input is true.
     */
    @Override
    public List<BaseApiDTO> getGroupsByUuid(final String groupUuid, final Boolean path)  {
        // TODO: OM-23669
        return new ArrayList<>();
    }

    @Override
    public List<SettingsPolicyApiDTO> getSettingPoliciesByGroupUuid(String uuid) throws Exception {
        // TODO: OM-23671
        return new ArrayList<>();
    }

    @Override
    public Map<String, EntityAspect> getAspectsByGroupUuid(String uuid) throws UnauthorizedObjectException, UnknownObjectException {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public EntityAspect getAspectByGroupUuid(String uuid, String aspectTag) throws UnauthorizedObjectException, UnknownObjectException {
        throw ApiUtils.notImplementedInXL();
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
    public List<ServiceEntityApiDTO> getSettingsPolicyEntitiesBySetting(String uuid,
                                                                        String automationManagerUuid,
                                                                        String settingUuid,
                                                                        String settingPolicyUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByGroupQuery(String uuid,
                                                         StatPeriodApiInputDTO inputDto)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<?> getMembersByGroupUuid(String uuid) throws UnknownObjectException {
        if (USER_GROUPS.equals(uuid)) { // Get all user-created groups
            return getGroupApiDTOS(GetGroupsRequest.newBuilder()
                    .setOriginFilter(Origin.USER)
                    .build());
        } else { // Get members of the group with the uuid (oid)
            final Collection<Long> memberIds = getMemberIds(uuid).orElseThrow(() ->
                    new IllegalArgumentException("Can't get members in invalid group: " + uuid));
            logger.info("Number of members for group {} is {}", uuid, memberIds.size());

            // Get entities of group members from the repository component
            final Map<Long, Optional<ServiceEntityApiDTO>> entities =
                    repositoryApi.getServiceEntitiesById(Sets.newHashSet(memberIds));

            final List<ServiceEntityApiDTO> results = new ArrayList<>();

            entities.forEach((oid, se) -> {
                if (se.isPresent()) {
                    results.add(se.get());
                } else {
                    logger.warn("Cannot find entity with oid " + oid);
                }
            });
            return results;
        }
    }

    @Override
    public List<TagApiDTO> getTagsByGroupUuid(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<TagApiDTO> createTagByGroupUuid(final String s, final TagApiDTO tagApiDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteTagByGroupUuid(final String s, final String s1) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteTagsByGroupUuid(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Get the ID's of entities that are members of a group or cluster.
     *
     * @param groupUuid The UUID of the group or cluster.
     * @return An optional containing the ID's of the entities in the group/cluster.
     *         An empty optional if the group UUID is not valid (e.g. "Market").
     * @throws UnknownObjectException if the UUID is valid, but group with the UUID exists.
     */
    @VisibleForTesting
    Optional<Collection<Long>> getMemberIds(@Nonnull final String groupUuid)
            throws UnknownObjectException {
        Collection<Long> memberIds = null;
        // If the uuid is not for the global, get the group membership from Group component.
        if (!GLOBAL_SCOPE_SUPPLY_CHAIN.contains(groupUuid)) {
            final long id = Long.parseLong(groupUuid);
            // TODO (roman, Aug 3, 2017): It's inconvenient that the UI/API knows
            // they want a cluster or group, but want to look them up in a common interface.
            // To deal with the mismatch, we try searching for group members first, and if
            // the group is not found we look for cluster members.
            // In the future it makes sense to have a single interface to search all "collections"
            // of entities.
            try {
                GetMembersResponse groupResp = groupServiceRpc.getMembers(GetMembersRequest.newBuilder()
                        .setId(id)
                        .build());
                memberIds = groupResp.getMemberIdList();
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                    GetClusterResponse response =
                            clusterServiceRpc.getCluster(GetClusterRequest.newBuilder()
                                .setClusterId(id)
                                .build());
                    if (response.hasCluster()) {
                        memberIds = response.getCluster().getInfo().getMembers().getStaticMemberOidsList();
                    } else {
                        throw new UnknownObjectException("Can't find group " + groupUuid);
                    }
                } else {
                    throw e;
                }
            }
        }
        return Optional.ofNullable(memberIds);
    }

    /**
     * Fetch groups based on a list of {@link FilterApiDTO} criteria.
     *
     * If the list of criteria is empty, return all groups
     *
     * @param filterList the list of filter criteria to apply
     * @return a list of groups that match
     */
    List<GroupApiDTO> getGroupsByFilter(List<FilterApiDTO> filterList) {

        GetGroupsRequest groupsRequest = getGroupsRequestForFilters(filterList);
        return getGroupApiDTOS(groupsRequest);
    }

    /**
     * Fetch groups based on a list of {@link FilterApiDTO} criteria.
     *
     * If the list of criteria is empty, return all groups
     *
     * @param groupOidList the OIDs of groups to fetch
     * @return a list of groups that match
     */
    List<GroupApiDTO> getGroupsByOids(List<Long> groupOidList) {

        GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
                .addAllId(groupOidList)
                .build();
        return getGroupApiDTOS(groupsRequest);
    }

    /**
     * Call the Group Component to look up groups based on an {@link GetGroupsRequest}.
     *
     * The request may, optionally, specify a name-matching regex. If none is supplied,
     * then all GroupApiDTOs are returned.
     *
     * @param groupsRequest a request to specify, optionally, a name-matching regex
     * @return a list of {@link GroupApiDTO}s satisfying the given request
     */
    private List<GroupApiDTO> getGroupApiDTOS(GetGroupsRequest groupsRequest) {
        Iterable<Group> groups = () -> groupServiceRpc.getGroups(groupsRequest);
        return StreamSupport.stream(groups.spliterator(), false)
                .map(groupMapper::toGroupApiDto)
                .collect(Collectors.toList());
    }

    /**
     * Create a GetGroupsRequest based on the given filterList.
     *
     * The only filter supporeted is 'groupsByName'. The name regex for this filter is given by
     * the "expVal" of the FilterApiDTO. The sense of the match is given by the "expType",
     * i.e "EQ" -> 'search for match' vs. anything else -> 'search for non-match'
     *
     * @param filterList a list of FilterApiDTO to be applied to this group; only "groupsByName" is
     *                   currently supported
     * @return a GetGroupsRequest with the filtering set if an item in the filterList is found
     */
    @VisibleForTesting
    GetGroupsRequest getGroupsRequestForFilters(List<FilterApiDTO> filterList) {

        GetGroupsRequest.Builder requestBuilder = GetGroupsRequest.newBuilder();
        for (FilterApiDTO filter : filterList ) {
            if (filter.getFilterType().equals("groupsByName")) {
                requestBuilder.setNameFilter(GroupDTO.NameFilter.newBuilder()
                        .setNameRegex(filter.getExpVal())
                        .setNegateMatch(!filter.getExpType().equals("EQ"))
                );
            }
        }
        return requestBuilder.build();
    }

    /**
     * Fetch clusters based on a list of {@link FilterApiDTO} criteria.
     *
     * If the list of criteria is empty, return all groups
     *
     * @param clusterOidList the OIDs of groups to fetch
     * @return a list of groups that match
     */
    List<GroupApiDTO> getClustersByOids(List<Long> clusterOidList) {

        GetClustersRequest clustersRequest = GetClustersRequest.newBuilder()
                .addAllId(clusterOidList)
                .build();
        return getClusters(clustersRequest);
    }



    /**
     * Fetch a List of all the Clusters of type COMPUTE, i.e. containing HOSTs
     *
     * @return a list of all known COMPUTE clusters, i.e. containing HOSTs (PMs)
     */
    List<GroupApiDTO> getComputeClusters(List<FilterApiDTO> filterList) {

        return getClusters(getClustersFilterRequest(filterList)
                .setTypeFilter(GroupDTO.ClusterInfo.Type.COMPUTE)
                .build());
    }

    /**
     * Fetch a List of all the Clusters of type STORAGE
     *
     * @return a list of all known STORAGE clusters
     */
    List<GroupApiDTO> getStorageClusters(List<FilterApiDTO> filterList) {
        return getClusters(getClustersFilterRequest(filterList)
                .setTypeFilter(GroupDTO.ClusterInfo.Type.STORAGE)
                .build());
    }


    /**
     * Request a list of clusters based on a filter. This filter can specify a list of OIDs,
     * cluster names, or cluster types - the different types of filters are 'AND'ed.
     *
     * @param getClustersRequest the filter to apply to select clusters
     * @return a list of Clusters, represented as GroupApiDTOs, which pass the filter criteria
     */
    private List<GroupApiDTO> getClusters(GetClustersRequest getClustersRequest) {
        final Iterable<Cluster> clusters = () -> clusterServiceRpc.getClusters(getClustersRequest);

        return StreamSupport.stream(clusters.spliterator(), false)
                .map(groupMapper::toGroupApiDto)
                .collect(Collectors.toList());
    }

    /**
     * Create a GetClustersRequest based on the given filterList.
     *
     * The only filter supported is 'clustersByName'. The name regex for this filter is given by
     * the "expVal" of the FilterApiDTO. The sense of the match is given by the "expType",
     * i.e "EQ" -> 'search for match' vs. anything else -> 'search for non-match'
     *
     * @param filterList a list of FilterApiDTO to be applied to this group; only "clustersByName" or
     *                  storageClustersByName are currently supported.
     * @return a GetClustersRequest with the filtering set if an item in the filterList is found
     */
    @VisibleForTesting
    GetClustersRequest.Builder getClustersFilterRequest(List<FilterApiDTO> filterList) {

        GetClustersRequest.Builder requestBuilder = GetClustersRequest.newBuilder();
        for (FilterApiDTO filter : filterList) {
            if (filter.getFilterType().equals("clustersByName") ||
                    filter.getFilterType().equals("storageClustersByName")) {
                requestBuilder.setNameFilter(GroupDTO.NameFilter.newBuilder()
                        .setNameRegex(filter.getExpVal())
                        .setNegateMatch(!filter.getExpType().equals("EQ"))
                );
            }
        }
        return requestBuilder;
    }

}
