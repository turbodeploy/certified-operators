package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.SearchMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroupProducer;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.GroupExpander.GroupAndMembers;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
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
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateNestedGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateNestedGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterForEntityRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterForEntityResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupPropertyFilterList;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo.TypeCase;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateNestedGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateNestedGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByNameRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesForGroupRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesForGroupResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Service implementation of Groups functionality.
 **/
public class GroupsService implements IGroupsService {

    /**
     * Map Entity types to be expanded to the RelatedEntityType to retrieve. The entity is a
     * grouping entity in classic.
     */
    private static final Map<Integer, List<String>> GROUPING_ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
        EntityType.DATACENTER_VALUE, ImmutableList.of(UIEntityType.PHYSICAL_MACHINE.apiStr()),
        EntityType.VIRTUAL_DATACENTER_VALUE, ImmutableList.of(UIEntityType.VIRTUAL_DATACENTER.apiStr())
    );

    private static final Collection<String> GLOBAL_SCOPE_SUPPLY_CHAIN = ImmutableList.of(
            "GROUP-VirtualMachine", "GROUP-PhysicalMachineByCluster", "Market");

    public static Set<String> NESTED_GROUP_TYPES =
        ImmutableSet.of(StringConstants.CLUSTER, StringConstants.STORAGE_CLUSTER);

    private static final String USER_GROUPS = "GROUP-MyGroups";

    private static final String CLUSTER_HEADROOM_GROUP_UUID = "GROUP-PhysicalMachineByCluster";
    private static final String STORAGE_CLUSTER_HEADROOM_GROUP_UUID = "GROUP-StorageByStorageCluster";
    private static final String CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME = "headroomVM";
    private static final String CLUSTER_HEADROOM_SETTINGS_MANAGER = "capacityplandatamanager";
    private static final String CLUSTER_HEADROOM_TEMPLATE_SETTING_UUID = "templateName";

    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final GroupMapper groupMapper;

    private final GroupExpander groupExpander;

    private final UuidMapper uuidMapper;

    private final PaginationMapper paginationMapper;

    private final EntityAspectMapper entityAspectMapper;

    private final SettingsManagerMapping settingsManagerMapping;

    private final TemplateServiceBlockingStub templateService;

    private final RepositoryApi repositoryApi;

    private final long realtimeTopologyContextId;

    private final SearchServiceBlockingStub searchServiceBlockingStub;

    private final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub;

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final SeverityPopulator severityPopulator;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private StatsService statsService = null;

    private final ActionSearchUtil actionSearchUtil;

    private final SettingsMapper settingsMapper;

    private final TargetsService targetsService;

    private final Logger logger = LogManager.getLogger();

    GroupsService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                  @Nonnull final GroupServiceBlockingStub groupServiceRpc,
                  @Nonnull final GroupMapper groupMapper,
                  @Nonnull final GroupExpander groupExpander,
                  @Nonnull final UuidMapper uuidMapper,
                  @Nonnull final PaginationMapper paginationMapper,
                  @Nonnull final RepositoryApi repositoryApi,
                  final long realtimeTopologyContextId,
                  @Nonnull final SettingsManagerMapping settingsManagerMapping,
                  @Nonnull final TemplateServiceBlockingStub templateService,
                  @Nonnull final EntityAspectMapper entityAspectMapper,
                  @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                  @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
                  @Nonnull final SeverityPopulator severityPopulator,
                  @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                  @Nonnull final ActionSearchUtil actionSearchUtil,
                  @Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub,
                  @Nonnull final SettingsMapper settingsMapper,
                  @Nonnull final TargetsService targetsService) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.settingsManagerMapping = Objects.requireNonNull(settingsManagerMapping);
        this.templateService = templateService;
        this.entityAspectMapper = entityAspectMapper;
        this.searchServiceBlockingStub = searchServiceBlockingStub;
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.settingPolicyServiceBlockingStub = Objects.requireNonNull(settingPolicyServiceBlockingStub);
        this.actionSearchUtil = Objects.requireNonNull(actionSearchUtil);
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.targetsService = Objects.requireNonNull(targetsService);
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

    @Override
    public List<GroupApiDTO> getGroups()  {
        return getGroupApiDTOS(GetGroupsRequest.newBuilder()
            .addTypeFilter(Type.GROUP)
            .addTypeFilter(Type.NESTED_GROUP)
            .addTypeFilter(Type.CLUSTER)
            .build(), true);
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
            outputDTO.setGroupType(UIEntityType.PHYSICAL_MACHINE.apiStr());
            outputDTO.setUuid(CLUSTER_HEADROOM_GROUP_UUID);
            outputDTO.setEnvironmentType(EnvironmentType.ONPREM);
            return outputDTO;
        }

        final Optional<GroupAndMembers> groupAndMembers = groupExpander.getGroupWithMembers(uuid);
        if (groupAndMembers.isPresent()) {
            return groupMapper.toGroupApiDto(groupAndMembers.get(), EnvironmentType.ONPREM);
        } else {
            final String msg = "Group not found: " + uuid;
            logger.error(msg);
            throw new UnknownObjectException(msg);
        }
    }

    @Override
    public List<?> getEntitiesByGroupUuid(String uuid) throws Exception {
        // check if scope is real time market, return all entities in the market
        if (UuidMapper.UI_REAL_TIME_MARKET_STR.equals(uuid)) {
            return Lists.newArrayList(repositoryApi.getSearchResults(null,
                SearchMapper.SEARCH_ALL_TYPES, null));
        }

        final Set<Long> leafEntities;
        // first check if it's group
        final Optional<GroupAndMembers> groupAndMembers = groupExpander.getGroupWithMembers(uuid);
        if (groupAndMembers.isPresent()) {
            leafEntities = Sets.newHashSet(groupAndMembers.get().entities());
        } else if (targetsService.isTarget(uuid)) {
            // check if it's target
            leafEntities = expandUuids(Collections.singleton(uuid), Collections.emptyList(), null);
        } else {
            // check if scope is entity, if not, throw exception
            Search.Entity entity = repositoryApi.fetchEntity(uuid).orElseThrow(() ->
                new UnsupportedOperationException("Scope: " + uuid + " is not supported"));
            // check if supported grouping entity
            if (!GROUPING_ENTITY_TYPES_TO_EXPAND.containsKey(entity.getType())) {
                throw new UnsupportedOperationException("Entity: " + uuid + " is not supported");
            }
            leafEntities = expandUuids(Collections.singleton(uuid),
                GROUPING_ENTITY_TYPES_TO_EXPAND.get(entity.getType()), null);
        }

        // Special handling for the empty member list, because passing empty to repositoryApi returns all entities.
        if (leafEntities.isEmpty()) {
            return Collections.emptyList();
        }

        // Get entities from the repository component
        final Map<Long, Optional<ServiceEntityApiDTO>> entities =
            repositoryApi.getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(leafEntities).build());

        int missingEntities = 0;
        final List<ServiceEntityApiDTO> results = new ArrayList<>();
        for (Optional<ServiceEntityApiDTO> optEntity : entities.values()) {
            if (optEntity.isPresent()) {
                results.add(optEntity.get());
            } else {
                missingEntities++;
            }
        }
        if (missingEntities > 0) {
            logger.warn("{} entities from scope {} not found in repository.", missingEntities, uuid);
        }
        return results;
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
        return
            actionSearchUtil.getActionsByEntityUuids(
                getMemberIds(uuid).orElseGet(Collections::emptySet), inputDto, paginationRequest);
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
            @Nonnull SettingApiDTO<String> templateSetting) {
        final SettingsManagerApiDTO settingsManager = new SettingsManagerApiDTO();
        settingsManager.setUuid(CLUSTER_HEADROOM_SETTINGS_MANAGER);
        settingsManager.setDisplayName(managerInfo.getDisplayName());
        settingsManager.setCategory(managerInfo.getDefaultCategory());
        settingsManager.setSettings(Collections.singletonList(templateSetting));
        return Collections.singletonList(settingsManager);
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
    private SettingApiDTO<String> getTemplateSetting(@Nonnull Group group) throws UnknownObjectException {
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

        SettingApiDTO<String> setting = new SettingApiDTO<>();
        setting.setUuid(CLUSTER_HEADROOM_TEMPLATE_SETTING_UUID);
        setting.setValue(templateId);
        setting.setValueDisplayName(templateName);
        setting.setValueType(InputValueType.STRING);
        setting.setEntityType(UIEntityType.PHYSICAL_MACHINE.apiStr());

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
    public SettingApiDTO<String> putSettingByUuidAndName(String groupUuid,
                                                 String managerName,
                                                 String settingUuid,
                                                 SettingApiDTO<String> setting)
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
        } else if (NESTED_GROUP_TYPES.contains(inputDTO.getGroupType())) {
            final NestedGroupInfo groupInfo = groupMapper.toNestedGroupInfo(inputDTO);
            final CreateNestedGroupResponse response = groupServiceRpc.createNestedGroup(
                CreateNestedGroupRequest.newBuilder()
                    .setGroupInfo(groupInfo)
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

    @Nonnull
    @Override
    public GroupApiDTO editGroup(@Nonnull String uuid, @Nonnull GroupApiDTO inputDTO)
            throws UnknownObjectException, OperationFailedException {

        final GetGroupResponse groupResponse =
                groupServiceRpc.getGroup(GroupID.newBuilder().setId(Long.parseLong(uuid)).build());

        if (!groupResponse.hasGroup()) {
            throw new UnknownObjectException(
                    String.format("Group with UUID %s does not exist", uuid));
        }

        if (groupResponse.getGroup().getType() == Type.NESTED_GROUP) {
            try {
                final NestedGroupInfo info = groupMapper.toNestedGroupInfo(inputDTO);
                final UpdateNestedGroupResponse response =
                    groupServiceRpc.updateNestedGroup(UpdateNestedGroupRequest.newBuilder()
                        .setGroupId(Long.parseLong(uuid))
                        .setNewGroupInfo(info)
                        .build());
                return groupMapper.toGroupApiDto(response.getUpdatedGroup());
            } catch (InvalidOperationException e) {
                throw new OperationFailedException(e);
            }
        } else {
            final GroupInfo info = groupMapper.toGroupInfo(inputDTO);
            UpdateGroupResponse response = groupServiceRpc.updateGroup(UpdateGroupRequest.newBuilder()
                .setId(Long.parseLong(uuid))
                .setNewInfo(info)
                .build());
            return groupMapper.toGroupApiDto(response.getUpdatedGroup());
        }
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
            } else {
                final ApiId apiScopeId = uuidMapper.fromUuid(uuid);
                final Map<ApiId, List<StatSnapshotApiDTO>> retStats =
                    actionStatsQueryExecutor.retrieveActionStats(ImmutableActionStatsQuery.builder()
                        .scopes(Collections.singleton(apiScopeId))
                        .actionInput(inputDto)
                        .build());
                return retStats.getOrDefault(apiScopeId, Collections.emptyList());
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
        if (!StringUtils.isNumeric(uuid)) {
            throw new IllegalArgumentException("Group uuid should be numeric: " + uuid);
        }
        final long groupId = Long.valueOf(uuid);
        GetSettingPoliciesForGroupResponse response =
                settingPolicyServiceBlockingStub.getSettingPoliciesForGroup(
                        GetSettingPoliciesForGroupRequest.newBuilder()
                                .addGroupIds(groupId)
                                .build());

        List<SettingsPolicyApiDTO> settingsPolicyApiDtos = new ArrayList<>();
        if (response.getSettingPoliciesByGroupIdMap().containsKey(groupId)) {
            response.getSettingPoliciesByGroupIdMap().get(groupId).getSettingPoliciesList()
                    .forEach(settingPolicy ->
                            settingsPolicyApiDtos.add(settingsMapper.convertSettingPolicy(settingPolicy)));
        }

        return settingsPolicyApiDtos;
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
        return statsService.getStatsByEntityQuery(uuid, inputDto);
    }

    @Override
    public List<?> getMembersByGroupUuid(String uuid) throws UnknownObjectException {
        if (CLUSTER_HEADROOM_GROUP_UUID.equals(uuid)) {
            return getClusters(ClusterInfo.Type.COMPUTE, Collections.emptyList(), Collections.emptyList())
                .stream()
                // TODO: The next line is a workaround of a UI limitation. The UI only accepts groups
                // with classname "Group" This line can be removed when bug OM-30381 is fixed.
                .peek(groupApiDTO -> groupApiDTO.setClassName(StringConstants.GROUP))
                .collect(Collectors.toList());
        } else if (STORAGE_CLUSTER_HEADROOM_GROUP_UUID.equals(uuid)) {
            return getClusters(ClusterInfo.Type.STORAGE, Collections.emptyList(), Collections.emptyList())
                .stream()
                // TODO: The next line is a workaround of a UI limitation. The UI only accepts groups
                // with classname "Group" This line can be removed when bug OM-30381 is fixed.
                .peek(groupApiDTO -> groupApiDTO.setClassName(StringConstants.GROUP))
                .collect(Collectors.toList());
        } else if (USER_GROUPS.equals(uuid)) { // Get all user-created groups
            return getGroupApiDTOS(GetGroupsRequest.newBuilder()
                    .addTypeFilter(Group.Type.GROUP)
                    .addTypeFilter(Group.Type.NESTED_GROUP)
                    .setOriginFilter(Origin.USER)
                    .build(), true);
        } else { // Get members of the group with the uuid (oid)
            final GroupAndMembers groupAndMembers = groupExpander.getGroupWithMembers(uuid)
                .orElseThrow(() ->
                    new IllegalArgumentException("Can't get members in invalid group: " + uuid));
            logger.info("Number of members for group {} is {}", uuid, groupAndMembers.members().size());

            if (groupAndMembers.group().getType() == Type.NESTED_GROUP) {
                return getGroupApiDTOS(GetGroupsRequest.newBuilder()
                    .addAllId(groupAndMembers.members())
                    .build(), true);
            } else {
                // Special handling for the empty member list, because passing empty to
                // repositoryApi returns all entities.
                if (groupAndMembers.members().isEmpty()) {
                    return Collections.emptyList();
                } else {
                    // Get entities of group members from the repository component
                    final Map<Long, Optional<ServiceEntityApiDTO>> entities =
                        repositoryApi.getServiceEntitiesById(ServiceEntitiesRequest.newBuilder(
                            Sets.newHashSet(groupAndMembers.members())).build());

                    final AtomicLong missingEntities = new AtomicLong(0);
                    final List<ServiceEntityApiDTO> results = entities.values().stream()
                        .filter(optEntity -> {
                            if (!optEntity.isPresent()) {
                                missingEntities.incrementAndGet();
                                return false;
                            } else {
                                return true;
                            }
                        })
                        .map(Optional::get)
                        .collect(Collectors.toList());
                    if (missingEntities.get() > 0) {
                        logger.warn("{} group members from group {} not found in repository.",
                            missingEntities.get(), uuid);
                    }
                    return results;
                }
            }
        }
    }

    private List<GroupApiDTO> getClusters(ClusterInfo.Type clusterType) {
        final List<GroupApiDTO> groupOfClusters = getGroupApiDTOS(GetGroupsRequest.newBuilder()
                .addTypeFilter(Type.CLUSTER)
                .setClusterFilter(ClusterFilter.newBuilder()
                        .setTypeFilter(clusterType)
                        .build())
                .build(), true);
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
            .addTypeFilter(Type.GROUP)
            .addTypeFilter(Type.NESTED_GROUP)
            .build(), true);
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
     */
    @Nonnull
    public List<GroupApiDTO> getGroupApiDTOS(final GetGroupsRequest groupsRequest,
                                             final boolean populateSeverity) {
        final Stream<GroupAndMembers> groupsWithMembers =
            groupExpander.getGroupsWithMembers(groupsRequest);
        final List<GroupApiDTO> retList = groupsWithMembers
            .filter(groupAndMembers -> !isHiddenGroup(groupAndMembers.group()))
            .map(groupAndMembers -> {
                final GroupApiDTO apiDTO = groupMapper.toGroupApiDto(groupAndMembers, EnvironmentType.ONPREM);
                if (populateSeverity && groupAndMembers.entities().size() > 0) {
                    severityPopulator.calculateSeverity(realtimeTopologyContextId, groupAndMembers.entities())
                            .ifPresent(severity -> apiDTO.setSeverity(severity.name()));
                }
                return apiDTO;
            })
            .collect(Collectors.toList());
        return retList;
    }

    /**
     * Create a GetGroupsRequest based on the given filterList.
     *
     * @param filterList a list of FilterApiDTO to be applied to this group; only "groupsByName" is
     *                   currently supported
     * @return a GetGroupsRequest with the filtering set if an item in the filterList is found
     */
    @VisibleForTesting
    GetGroupsRequest.Builder getGroupsRequestForFilters(@Nonnull List<FilterApiDTO> filterList) {
        final GroupPropertyFilterList.Builder groupPropertyFilterListBuilder =
                GroupPropertyFilterList.newBuilder();
        filterList.stream()
                .map(groupMapper::apiFilterToGroupPropFilter)
                .filter(optionalFilter -> optionalFilter.isPresent())
                .map(Optional::get)
                .forEach(groupPropertyFilterListBuilder::addPropertyFilters);
        return
            GetGroupsRequest.newBuilder().setPropertyFilters(groupPropertyFilterListBuilder.build());
    }

    /**
     * Return the clusters inside a list of scopes, assumed to be groups of clusters.
     *
     * @param clusterType The type of clusters to return.
     * @param scopes The scopes - assumed to be groups of clusters.
     * @param filterList The filters to apply to the returned clusters.
     * @return The {@link GroupApiDTO}s returning the clusters matching the criteria inside the
     *         provided scopes.
     */
    @Nonnull
    private List<GroupApiDTO> getClustersInGroups(
            @Nonnull ClusterInfo.Type clusterType,
            @Nonnull final List<String> scopes,
            @Nonnull final List<FilterApiDTO> filterList) {
        final GetGroupsRequest.Builder reqBuilder = getGroupsRequestForFilters(filterList);
        scopes.stream()
            .map(groupExpander::getGroupWithMembers)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(groupAndMembers -> {
                final Group group = groupAndMembers.group();
                return group.getType() == Type.NESTED_GROUP &&
                    group.getNestedGroup().getTypeCase() == TypeCase.CLUSTER &&
                    group.getNestedGroup().getCluster() == clusterType;
            })
            .forEach(clusterAndMembers -> clusterAndMembers.members().forEach(reqBuilder::addId));
        return getGroupApiDTOS(reqBuilder.build(), true);
    }

    /**
     * Return the clusters containing the provided scopes, assumed to be entities.
     *
     * @param clusterType The type of clusters to return.
     * @param scopes The scopes - assumed to be entities.
     * @return The {@link GroupApiDTO}s returning the clusters of the provided entities.
     */
    @Nonnull
    private List<GroupApiDTO> getClustersOfEntities(
            @Nonnull ClusterInfo.Type clusterType,
            @Nullable final List<String> scopes) {
        // As of today (Feb 2019), the scopes object could only have one id (PM oid).
        // If there is PM oid, we want to retrieve the Cluster that the PM belonged to.
        // TODO (Gary, Feb 4, 2019), add a new gRPC service for multiple PM oids when needed.
        final Map<Long, Group> clustersById = scopes.stream()
            .map(uuid -> Long.parseLong(uuid))
            .map(entityId -> {
                final GetClusterForEntityResponse response =
                    groupServiceRpc.getClusterForEntity(GetClusterForEntityRequest.newBuilder()
                        .setEntityId(entityId)
                        .build());
                if (response.hasCluster()) {
                    return response.getCluster();
                } else {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            // Filter according to the provided cluster type.
            .filter(cluster -> cluster.getCluster().getClusterType() == clusterType)
            // Collect to a map to get rid of duplicate clusters (i.e. if scopes specify two entities
            // in the same cluster.
            .collect(Collectors.toMap(Group::getId, Function.identity(), (c1, c2) -> c1));

        return clustersById.values().stream()
            .map(groupExpander::getMembersForGroup)
            .map(clusterAndMembers -> {
                final GroupApiDTO apiDTO = groupMapper.toGroupApiDto(clusterAndMembers, EnvironmentType.ONPREM);
                severityPopulator.calculateSeverity(realtimeTopologyContextId,
                        clusterAndMembers.entities())
                    .ifPresent(severity -> apiDTO.setSeverity(severity.name()));
                return apiDTO;
            })
            .collect(Collectors.toList());
    }

    /**
     * Get {@link GroupApiDTO} describing clusters in the system that match certain criteria.
     *
     * @param clusterType The type of the cluster.
     * @param scopes The scopes to look for clusters in. Could be entities (in which case we look
     *               for the clusters of the entities) or groups-of-clusters (in which case we
     *               look for clusters in the group).
     * @param filterList The list of filters to apply to the clusters.
     * @return
     */
    @Nonnull
    List<GroupApiDTO> getClusters(@Nonnull ClusterInfo.Type clusterType,
                                  @Nullable final List<String> scopes,
                                  @Nonnull final List<FilterApiDTO> filterList) {
        // If we're looking for clusters with a scope there are two possibilities:
        //   - The scope is a group of clusters, in which case we want the clusters in the group.
        //   - The scope is a list of entities, in which case we want the clusters the entities are in.
        // We assume it's either-or - i.e. either all scopes are groups, or all scopes are entities.
        if (UuidMapper.hasLimitedScope(scopes)) {
            if (scopes.stream().anyMatch(uuid -> uuidMapper.fromUuid(uuid).isGroup())) {
                return getClustersInGroups(clusterType, scopes, filterList);
            } else {
                // Note - for now (March 29 2019) we don't have cases where we need to apply the
                // filter list. But in the future we may need to.
                return getClustersOfEntities(clusterType, scopes);
            }
        } else {
            return getGroupApiDTOS(getGroupsRequestForFilters(filterList)
                .addTypeFilter(Type.CLUSTER)
                .setClusterFilter(ClusterFilter.newBuilder()
                    .setTypeFilter(clusterType))
                .build(), true);
        }
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
        final GroupAndMembers groupAndMembers = groupExpander.getGroupWithMembers(uuid)
            .orElseThrow(() -> new UnknownObjectException("Group not found: " + uuid));
        if (groupAndMembers.group().getType() == Type.NESTED_GROUP) {
            // The members of a nested group (e.g. group of clusters) don't have TopologyEntityDTO
            // representations.
            return Collections.emptyList();
        } else {
            // Get group members as TopologyEntityDTOs from the repository component
            final SearchTopologyEntityDTOsResponse response =
                searchServiceBlockingStub.searchTopologyEntityDTOs(
                    SearchTopologyEntityDTOsRequest.newBuilder()
                        .addAllEntityOid(groupAndMembers.members())
                        .build());
            return response.getTopologyEntityDtosList();
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
        final Set<String> targetUuids = new HashSet<>();
        final Set<String> seedUuids = new HashSet<>();
        scopeUuids.forEach(scopeUuid -> {
            if (targetsService.isTarget(scopeUuid)) {
                targetUuids.add(scopeUuid);
            } else {
                seedUuids.add(scopeUuid);
            }
        });

        // if there are targets in scopes, add entities discovered by that target, these entities
        // should not be added to seedUuids since they are all the entities for the target
        if (!targetUuids.isEmpty()) {
            final List<Search.Entity> targetEntities = new ArrayList<>();
            for (String uuid : targetUuids) {
                targetEntities.addAll(targetsService.getTargetEntities(uuid));
            }
            final Set<Integer> relatedEntityTypesInt = relatedEntityTypes == null
                ? Collections.emptySet()
                : relatedEntityTypes.stream()
                    .map(UIEntityType::fromString)
                    .map(UIEntityType::typeNumber)
                    .collect(Collectors.toSet());
            final Predicate<Search.Entity> filterByType = relatedEntityTypesInt.isEmpty()
                ? entity -> true
                : entity -> relatedEntityTypesInt.contains(entity.getType());
            result.addAll(targetEntities.stream()
                .filter(filterByType)
                .map(Search.Entity::getOid)
                .collect(Collectors.toSet()));
        }

        // use seedUuids (entity + group) to find entities of related type using supply chain
        if (!seedUuids.isEmpty()) {
            final Map<String, SupplyChainNode> supplyChainForScope =
                supplyChainFetcherFactory.newNodeFetcher()
                    .topologyContextId(realtimeTopologyContextId)
                    .addSeedUuids(seedUuids)
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
}
