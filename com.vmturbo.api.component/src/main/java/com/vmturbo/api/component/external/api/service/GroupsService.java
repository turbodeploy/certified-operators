package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroupProducer;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.notification.NotificationSettingsApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingApiInputDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByNameRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Service implementation of Groups functionality.
 **/
public class GroupsService implements IGroupsService {

    public static final String GROUPS_FILTER_TYPE = "groupsByName";

    private static final Collection<String> GLOBAL_SCOPE_SUPPLY_CHAIN = ImmutableList.of(
            "GROUP-VirtualMachine", "GROUP-PhysicalMachineByCluster", "Market");

    private static final String USER_GROUPS = "GROUP-MyGroups";

    private static final String CLUSTER_HEADROOM_GROUP_UUID = "GROUP-PhysicalMachineByCluster";
    private static final String STORAGE_CLUSTER_HEADROOM_GROUP_UUID = "GROUP-StorageByStorageCluster";
    private static final String CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME = "headroomVM";
    private static final String CLUSTER_HEADROOM_SETTINGS_MANAGER = "capacityplandatamanager";
    private static final String CLUSTER_HEADROOM_TEMPLATE_SETTING_UUID = "templateName";

    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final ActionSpecMapper actionSpecMapper;

    private final GroupMapper groupMapper;

    private final PaginationMapper paginationMapper;

    private final EntityAspectMapper entityAspectMapper;

    private final SettingsManagerMapping settingsManagerMapping;

    private final TemplateServiceBlockingStub templateService;

    private final RepositoryApi repositoryApi;

    private final long realtimeTopologyContextId;

    private final SearchServiceBlockingStub searchServiceBlockingStub;

    private final Logger logger = LogManager.getLogger();

    GroupsService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                         @Nonnull final GroupServiceBlockingStub groupServiceRpc,
                         @Nonnull final ActionSpecMapper actionSpecMapper,
                         @Nonnull final GroupMapper groupMapper,
                         @Nonnull final PaginationMapper paginationMapper,
                         @Nonnull final RepositoryApi repositoryApi,
                         final long realtimeTopologyContextId,
                         @Nonnull final SettingsManagerMapping settingsManagerMapping,
                         @Nonnull final TemplateServiceBlockingStub templateService,
                         @Nonnull final EntityAspectMapper entityAspectMapper,
                         @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.settingsManagerMapping = Objects.requireNonNull(settingsManagerMapping);
        this.templateService = templateService;
        this.entityAspectMapper = entityAspectMapper;
        this.searchServiceBlockingStub = searchServiceBlockingStub;
    }

    @Override
    public List<GroupApiDTO> getGroups()  {
        // Currently, we have only user-created groups.
        return getGroupApiDTOS(GetGroupsRequest.newBuilder()
                .setTypeFilter(Type.GROUP)
                .build());
    }


    @Override
    public GroupApiDTO getGroupByUuid(String uuid, boolean includeAspects) throws UnknownObjectException {
        // The "Nightly Plan Configuration" UI calls this API to populate the list of clusters and
        // their associated templates. The UI uses the uuid "GROUP-PhysicalMachineByCluster" with
        // request.  This UUID is defined in the context of version 6.1 implementation.
        // In order to reuse UI as is, we decided to handle this use case as a special case.
        // If the uuid equals "GROUP-PhysicalMachineByCluster", return a group with the same UUID.
        // The logic to get a list of clusters in in method getMembersByGroupUuid.
        if (uuid.equals(CLUSTER_HEADROOM_GROUP_UUID)) {
            final GroupApiDTO outputDTO = new GroupApiDTO();
            outputDTO.setClassName("Group");
            outputDTO.setDisplayName("Physical Machines by PM Cluster");
            outputDTO.setGroupType(ServiceEntityMapper.toUIEntityType(Type.CLUSTER.getNumber()));
            outputDTO.setUuid(CLUSTER_HEADROOM_GROUP_UUID);
            outputDTO.setEnvironmentType(EnvironmentType.ONPREM);
            return outputDTO;
        }

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
            final String msg = "Group not found: " + uuid;
            logger.error(msg);
            throw new UnknownObjectException(msg);
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
    public ActionPaginationResponse getActionsByGroupUuid(String uuid,
                                      ActionApiInputDTO inputDto,
                                      ActionPaginationRequest paginationRequest) throws Exception {
        final ActionQueryFilter filter =
                        actionSpecMapper.createActionFilter(inputDto, getMemberIds(uuid));

        // Note this is a blocking call.
        final FilteredActionResponse response = actionOrchestratorRpc.getAllActions(
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
    public ActionApiDTO getActionByGroupUuid(String uuid, String aUuid) throws Exception {
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
        Long groupId = null;

        try {
            groupId = Long.parseLong(uuid);
        } catch (NumberFormatException e) {
            // UUID is not a number.
            // FIXME There is a UI bug that sends in "GROUP-PhysicalMachineByCluster". (OM-30275)
            // If value is "GROUP-PhysicalMachineByCluster", ignore the request by returning an empty array.
            // Otherwise, throw exception.
            if (CLUSTER_HEADROOM_GROUP_UUID.equals(uuid)) {
                return new ArrayList<>();
            } else {
                throw new IllegalArgumentException("Cluster uuid is invalid: " + uuid, e);
            }
        }

        Group group = groupServiceRpc.getGroup(GroupID.newBuilder()
                .setId(groupId)
                .build()).getGroup();

        if (group.getType().equals(Type.CLUSTER)) {
            // Group is a cluster.  Return the cluster headroom template as a setting, wrapped in
            // the SettingsManagerApiDTO data structure.
            SettingsManagerInfo managerInfo = settingsManagerMapping
                    .getManagerInfo(CLUSTER_HEADROOM_SETTINGS_MANAGER)
                    .orElseThrow(() -> new UnknownObjectException("Settings manager with uuid "
                            + uuid + " is not found."));

            return getHeadroomSettingsMangerApiDTO(managerInfo, getTemplateSetting(group));
        } else {
            // TODO: OM-23672
            return new ArrayList<>();
        }
    }

    /**
     * Populate the SettingsManagerApiDTO object with data from managerInfo and the template setting.
     *
     * @param managerInfo manager info object
     * @param templateSetting Settings object that contains the template ID
     * @return
     */
    @Nonnull
    private List<SettingsManagerApiDTO> getHeadroomSettingsMangerApiDTO(
            @Nonnull SettingsManagerInfo managerInfo,
            @Nonnull SettingApiDTO templateSetting) {
        final SettingsManagerApiDTO settingsManager = new SettingsManagerApiDTO();
        settingsManager.setUuid(CLUSTER_HEADROOM_SETTINGS_MANAGER);
        settingsManager.setDisplayName(managerInfo.getDisplayName());
        settingsManager.setCategory(managerInfo.getDefaultCategory());
        settingsManager.setSettings(Arrays.asList(templateSetting));
        return Arrays.asList(settingsManager);
    }

    /**
     * Gets the template ID and name from the template service, and return the values in a
     * SettingsApiDTO object.
     *
     * @param group the Group object for which the headroom template is to be found.
     * @return the VM headroom template information of the given cluster.
     * @throws UnknownObjectException No headroom template found for this group.
     */
    @Nonnull
    private SettingApiDTO getTemplateSetting(@Nonnull Group group) throws UnknownObjectException {
        Template headroomTemplate = null;

        // Get the headroom template with the ID in ClusterInfo if available.
        if (group.hasCluster() && group.getCluster().hasClusterHeadroomTemplateId()) {
            Optional<Template> headroomTemplateOpt = getClusterHeadroomTemplate(
                    group.getCluster().getClusterHeadroomTemplateId());
            if (headroomTemplateOpt.isPresent()) {
                headroomTemplate = headroomTemplateOpt.get();
            }
        }
        // If the headroom template ID is not set in clusterInfo, or the template with the ID is
        // not found, get the default headroom template.
        if (headroomTemplate == null) {
            Iterable<Template> templateIter = () -> templateService.getTemplatesByName(
                    GetTemplatesByNameRequest.newBuilder()
                            .setTemplateName(CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME)
                            .build());
            headroomTemplate = StreamSupport.stream(templateIter.spliterator(), false)
                    .filter(template -> template.getType().equals(Template.Type.SYSTEM))
                    .findFirst()
                    .orElseThrow(() -> new UnknownObjectException("No system headroom VM found!"));
        }

        String templateName = headroomTemplate.getTemplateInfo().getName();
        String templateId = Long.toString(headroomTemplate.getId());

        SettingApiDTO setting = new SettingApiDTO();
        setting.setUuid(CLUSTER_HEADROOM_TEMPLATE_SETTING_UUID);
        setting.setValue(templateId);
        setting.setValueDisplayName(templateName);
        setting.setValueType(InputValueType.STRING);
        setting.setEntityType(UIEntityType.PHYSICAL_MACHINE.getValue());

        return setting;
    }

    /**
     * Gets the template with the given template ID.
     *
     * @param templateId template ID
     * @return the Template if found. Otherwise, return an empty Optional object.
     */
    private Optional<Template> getClusterHeadroomTemplate(Long templateId) {
        try {
            return Optional.of(templateService.getTemplate(GetTemplateRequest.newBuilder()
                    .setTemplateId(templateId)
                    .build()));
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                // return empty to indicate that the system headroom plan should be used.
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    @Override
    public SettingApiDTO putSettingByUuidAndName(String groupUuid,
                                                 String managerName,
                                                 String settingUuid,
                                                 SettingApiInputDTO setting)
            throws Exception {
        // Update the cluster headroom template ID
        if (settingUuid.equals(CLUSTER_HEADROOM_TEMPLATE_SETTING_UUID) &&
                managerName.equals(CLUSTER_HEADROOM_SETTINGS_MANAGER)) {
            try {
                UpdateClusterHeadroomTemplateResponse response =
                        groupServiceRpc.updateClusterHeadroomTemplate(
                                UpdateClusterHeadroomTemplateRequest.newBuilder()
                                        .setGroupId(Long.parseLong(groupUuid))
                                        .setClusterHeadroomTemplateId(Long.parseLong(setting.getValue()))
                                        .build());
                return getTemplateSetting(response.getUpdatedGroup());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid group ID or cluster headroom template ID");
            }
        }

        // The implementation for updating settings other than cluster headroom template is not available.
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByGroupUuid(String uuid, String encodedQuery) throws Exception {
        throw ApiUtils.notImplementedInXL();
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
    public GroupApiDTO createGroup(GroupApiDTO inputDTO) throws Exception {
        if (Boolean.TRUE.equals(inputDTO.getTemporary())) {
            final TempGroupInfo tempGroupInfo = groupMapper.toTempGroupProto(inputDTO);
            final CreateTempGroupResponse response = groupServiceRpc.createTempGroup(
                    CreateTempGroupRequest.newBuilder()
                        .setGroupInfo(tempGroupInfo)
                        .build());
            final EnvironmentType environmentType = inputDTO.getEnvironmentType() != null
                    ? inputDTO.getEnvironmentType() : EnvironmentType.ONPREM;
            return groupMapper.toGroupApiDto(response.getGroup(), environmentType);
        } else {
            final GroupInfo request = groupMapper.toGroupInfo(inputDTO);
            final CreateGroupResponse res = groupServiceRpc.createGroup(request);
            return groupMapper.toGroupApiDto(res.getGroup());
        }
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
        return entityAspectMapper.getAspectsByGroup(getGroupMembers(uuid));
    }

    @Override
    public EntityAspect getAspectByGroupUuid(String uuid, String aspectTag) throws UnauthorizedObjectException, UnknownObjectException {
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
        if (CLUSTER_HEADROOM_GROUP_UUID.equals(uuid)) {
            return getClusters(ClusterInfo.Type.COMPUTE);
        } else if (STORAGE_CLUSTER_HEADROOM_GROUP_UUID.equals(uuid)) {
            return getClusters(ClusterInfo.Type.STORAGE);
        } else if (USER_GROUPS.equals(uuid)) { // Get all user-created groups
            return getGroupApiDTOS(GetGroupsRequest.newBuilder()
                    .setTypeFilter(Group.Type.GROUP)
                    .setOriginFilter(Origin.USER)
                    .build());
        } else { // Get members of the group with the uuid (oid)
            final Collection<Long> memberIds = getMemberIds(uuid).orElseThrow(() ->
                    new IllegalArgumentException("Can't get members in invalid group: " + uuid));
            logger.info("Number of members for group {} is {}", uuid, memberIds.size());

            // Get entities of group members from the repository component
            final Map<Long, Optional<ServiceEntityApiDTO>> entities =
                    repositoryApi.getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(
                            Sets.newHashSet(memberIds)).build());

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

    private List<GroupApiDTO> getClusters(ClusterInfo.Type clusterType) {
        final List<GroupApiDTO> groupOfClusters = getGroupApiDTOS(GetGroupsRequest.newBuilder()
                .setTypeFilter(Type.CLUSTER)
                .setClusterFilter(ClusterFilter.newBuilder()
                        .setTypeFilter(clusterType)
                        .build())
                .build());
        // TODO: The next line is a workaround of a UI limitation. The UI only accepts groups
        // with classname "Group" This line can be removed when bug OM-30381 is fixed.
        groupOfClusters.forEach(cluster -> cluster.setClassName("Group"));
        return groupOfClusters;
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
    Optional<Set<Long>> getMemberIds(@Nonnull final String groupUuid)
            throws UnknownObjectException {
        Set<Long> memberIds = null;

        // These magic UI strings currently have no associated group in XL, so they are not valid.
        if (groupUuid.equals(DefaultCloudGroupProducer.ALL_CLOULD_WORKLOAD_AWS_AND_AZURE_UUID) ||
                groupUuid.equals(DefaultCloudGroupProducer.ALL_CLOUD_VM_UUID)) {
            return Optional.empty();
        }

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
                memberIds = Sets.newHashSet(groupResp.getMembers().getIdsList());
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                    throw new UnknownObjectException("Can't find group " + groupUuid);
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
        return getGroupApiDTOS(getGroupsRequestForFilters(filterList)
            .setTypeFilter(Type.GROUP)
            .build());
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
    public List<GroupApiDTO> getGroupApiDTOS(GetGroupsRequest groupsRequest) {
        Iterable<Group> groups = () -> groupServiceRpc.getGroups(groupsRequest);
        return StreamSupport.stream(groups.spliterator(), false)
                .filter(group -> !isHiddenGroup(group))
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
    GetGroupsRequest.Builder getGroupsRequestForFilters(List<FilterApiDTO> filterList) {

        GetGroupsRequest.Builder requestBuilder = GetGroupsRequest.newBuilder();
        for (FilterApiDTO filter : filterList ) {
            if (filter.getFilterType().equals(GROUPS_FILTER_TYPE)) {
                requestBuilder.setNameFilter(GroupDTO.NameFilter.newBuilder()
                        .setNameRegex(filter.getExpVal())
                        .setNegateMatch(!filter.getExpType().equals("EQ"))
                );
            }
        }
        return requestBuilder;
    }


    /**
     * Fetch a List of all the Clusters of type COMPUTE, i.e. containing HOSTs
     *
     * @return a list of all known COMPUTE clusters, i.e. containing HOSTs (PMs)
     */
    List<GroupApiDTO> getComputeClusters(List<FilterApiDTO> filterList) {
        return getGroupApiDTOS(getGroupsRequestForFilters(filterList)
                .setTypeFilter(Type.CLUSTER)
                .setClusterFilter(ClusterFilter.newBuilder()
                        .setTypeFilter(ClusterInfo.Type.COMPUTE)
                        .build())
                .build());
    }

    /**
     * Fetch a List of all the Clusters of type STORAGE
     *
     * @return a list of all known STORAGE clusters
     */
    List<GroupApiDTO> getStorageClusters(List<FilterApiDTO> filterList) {
        return getGroupApiDTOS(getGroupsRequestForFilters(filterList)
                .setTypeFilter(Type.CLUSTER)
                .setClusterFilter(ClusterFilter.newBuilder()
                        .setTypeFilter(ClusterInfo.Type.STORAGE)
                        .build())
                .build());
    }

    /**
     * Check if the returned group is hidden group. We should not show the hidden group to users.
     *
     * @param group The group info fetched from Group Component.
     * @return true if the group is hidden group.
     */
    private boolean isHiddenGroup(@Nonnull final Group group) {
        return group.hasGroup() && group.getGroup().getIsHidden();
    }

    /**
     * Get all members of a given group uuid and return in the form of TopologyEntityDTO.
     */
    private List<TopologyEntityDTO> getGroupMembers(@Nonnull String uuid) throws UnknownObjectException {
        final GetGroupResponse groupRes = groupServiceRpc.getGroup(GroupID.newBuilder()
                .setId(Long.parseLong(uuid))
                .build());
        if (!groupRes.hasGroup()) {
            throw new UnknownObjectException("Group not found: " + uuid);
        }

        final Collection<Long> memberIds = getMemberIds(uuid).orElseThrow(() ->
                new IllegalArgumentException("Can't get members in invalid group: " + uuid));

        // Get group members as TopologyEntityDTOs from the repository component
        final SearchTopologyEntityDTOsResponse response =
                searchServiceBlockingStub.searchTopologyEntityDTOs(
                        SearchTopologyEntityDTOsRequest.newBuilder()
                                .addAllEntityOid(memberIds)
                                .build());
        return response.getTopologyEntityDtosList();
    }
}
