package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.components.common.utils.StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.EnvironmentTypeMapper;
import com.vmturbo.api.component.external.api.mapper.GroupFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
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
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
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
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMemberOrderBy;
import com.vmturbo.api.pagination.GroupMembersPaginationRequest.GroupMembersPaginationResponse;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
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
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesForGroupRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesForGroupResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.components.common.utils.StringConstants;
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
        EntityType.DATACENTER_VALUE, ImmutableList.of(UIEntityType.PHYSICAL_MACHINE.apiStr()),
        EntityType.VIRTUAL_DATACENTER_VALUE, ImmutableList.of(UIEntityType.VIRTUAL_DATACENTER.apiStr())
    );

    private static final Collection<String> GLOBAL_SCOPE_SUPPLY_CHAIN = ImmutableList.of(
            "GROUP-VirtualMachine", "GROUP-PhysicalMachineByCluster", "Market");

    public static Set<String> NESTED_GROUP_TYPES =
        ImmutableSet.of(StringConstants.CLUSTER, StringConstants.STORAGE_CLUSTER,
                StringConstants.VIRTUAL_MACHINE_CLUSTER);

    private static final String USER_GROUPS = "GROUP-MyGroups";

    private static final String CLUSTER_HEADROOM_GROUP_UUID = "GROUP-PhysicalMachineByCluster";
    private static final String STORAGE_CLUSTER_HEADROOM_GROUP_UUID = "GROUP-StorageByStorageCluster";
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

    private final SeverityPopulator severityPopulator;

    private final PriceIndexPopulator priceIndexPopulator;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private StatsService statsService = null;

    private final ActionSearchUtil actionSearchUtil;

    private final SettingsMapper settingsMapper;

    private final ThinTargetCache thinTargetCache;

    private final EntitySettingQueryExecutor entitySettingQueryExecutor;

    private final GroupFilterMapper groupFilterMapper;

    private final Logger logger = LogManager.getLogger();

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
                  @Nonnull final SeverityPopulator severityPopulator,
                  @Nonnull final PriceIndexPopulator priceIndexPopulator,
                  @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                  @Nonnull final ActionSearchUtil actionSearchUtil,
                  @Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub,
                  @Nonnull final SettingsMapper settingsMapper,
                  @Nonnull final ThinTargetCache thinTargetCache,
                  @Nonnull final EntitySettingQueryExecutor entitySettingQueryExecutor,
                  @Nonnull final GroupFilterMapper groupFilterMapper) {
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
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.priceIndexPopulator = Objects.requireNonNull(priceIndexPopulator);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.settingPolicyServiceBlockingStub = Objects.requireNonNull(settingPolicyServiceBlockingStub);
        this.actionSearchUtil = Objects.requireNonNull(actionSearchUtil);
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);
        this.entitySettingQueryExecutor = entitySettingQueryExecutor;
        this.groupFilterMapper = Objects.requireNonNull(groupFilterMapper);
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
     * paginate the results. Consider using {@link #getPaginatedGroupApiDTOS} instead.
     *
     * @return a list of {@link GroupApiDTO}.
     */
    @Override
    public List<GroupApiDTO> getGroups()  {
        return getGroupApiDTOS(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.getDefaultInstance())
            .build(), true);
    }


    @Override
    public GroupApiDTO getGroupByUuid(String uuid, boolean includeAspects) throws UnknownObjectException {
        // The "Nightly Plan Configuration" UI calls this API to populate the list of clusters and
        // their associated templates. The UI uses the uuid "GROUP-PhysicalMachineByCluster" with
        // request.  This UUID is defined in the context of version 6.1 implementation.
        // In order to reuse UI as is, we decided to handle this use case as a special case.
        // If the uuid equals "GROUP-PhysicalMachineByCluster", return null so that UI won't show it.
        // The logic to get a list of clusters in in method getMembersByGroupUuid.
        if (uuid.equals(CLUSTER_HEADROOM_GROUP_UUID)) {
            return null;
        }

        final Optional<GroupAndMembers> groupAndMembers = groupExpander.getGroupWithMembers(uuid);
        if (groupAndMembers.isPresent()) {
            return groupMapper.toGroupApiDto(groupAndMembers.get(),
                    groupMapper.getEnvironmentTypeForGroup(groupAndMembers.get()), true);
        } else {
            final String msg = "Group not found: " + uuid;
            logger.error(msg);
            throw new UnknownObjectException(msg);
        }
    }

    @Override
    public List<?> getEntitiesByGroupUuid(String uuid) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        // check if scope is real time market, return all entities in the market
        if (apiId.isRealtimeMarket()) {
            final List<ServiceEntityApiDTO> entities = repositoryApi.newSearchRequest(
                    SearchProtoUtil.makeSearchParameters(
                            SearchProtoUtil.entityTypeFilter(SearchProtoUtil.SEARCH_ALL_TYPES))
                            .build()).getSEList();
            // populate priceIndex on the entity
            priceIndexPopulator.populateRealTimeEntities(entities);
            // populate severity on the entity
            severityPopulator.populate(realtimeTopologyContextId, entities);
            return entities;
        }

        final Set<Long> leafEntities;
        // first check if it's group
        final Optional<GroupAndMembers> groupAndMembers = groupExpander.getGroupWithMembers(uuid);
        if (groupAndMembers.isPresent()) {
            leafEntities = Sets.newHashSet(groupAndMembers.get().entities());
        } else if (apiId.isTarget()) {
            // check if it's target
            leafEntities = expandUuids(Collections.singleton(uuid), Collections.emptyList(), null);
        } else {
            // check if scope is entity, if not, throw exception
            MinimalEntity entity = repositoryApi.entityRequest(Long.parseLong(uuid)).getMinimalEntity()
                .orElseThrow(() -> new UnsupportedOperationException("Scope: " + uuid + " is not supported"));
            // check if supported grouping entity
            if (!GROUPING_ENTITY_TYPES_TO_EXPAND.containsKey(entity.getEntityType())) {
                throw new UnsupportedOperationException("Entity: " + uuid + " is not supported");
            }
            leafEntities = expandUuids(Collections.singleton(uuid),
                GROUPING_ENTITY_TYPES_TO_EXPAND.get(entity.getEntityType()), null);
        }

        // Special handling for the empty member list, because passing empty to repositoryApi returns all entities.
        if (leafEntities.isEmpty()) {
            return Collections.emptyList();
        }

        // Get entities from the repository component
        final List<ServiceEntityApiDTO> entities = repositoryApi.entitiesRequest(leafEntities)
            .getSEList();

        int missingEntities = leafEntities.size() - entities.size();
        if (missingEntities > 0) {
            logger.warn("{} entities from scope {} not found in repository.", missingEntities, uuid);
        }

        // populate priceIndex on the entity
        priceIndexPopulator.populateRealTimeEntities(entities);
        // populate severity on the entity
        severityPopulator.populate(realtimeTopologyContextId, entities);

        return entities;
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
            actionSearchUtil.getActionsByEntityUuids(Collections.singleton(uuidMapper.fromUuid(uuid)),
                inputDto, paginationRequest);
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
        final ApiId id = uuidMapper.fromUuid(uuid);
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
            Optional<SettingApiDTO> optionalSetting = entitySettingQueryExecutor.getEntitySettings(
                    uuidMapper.fromUuid(groupUuid),
                    false,
                    Collections.singleton(settingUuid))
                    .stream()
                    .map(settingManager -> settingManager.getSettings())
                    .flatMap(List::stream)
                    .map(setting -> (SettingApiDTO) setting)
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
                    settingsManagerMapping.getSettingNamesForManagers(Collections.singleton(settingManagerUuid)))
                    .stream()
                        .map(SettingsManagerApiDTO::getSettings)
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
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
        settingsManager.setUuid(SettingsMapper.CLUSTER_HEADROOM_SETTINGS_MANAGER);
        settingsManager.setDisplayName(managerInfo.getDisplayName());
        settingsManager.setCategory(managerInfo.getDefaultCategory());
        settingsManager.setSettings(Collections.singletonList(templateSetting));
        return Collections.singletonList(settingsManager);
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
    public GroupApiDTO createGroup(GroupApiDTO inputDTO) throws Exception {
        String username = getUsername();

        final GroupDefinition groupDefinition = groupMapper
                        .toGroupDefinition(inputDTO);
        final CreateGroupResponse resp = groupServiceRpc.createGroup(
                        CreateGroupRequest
                            .newBuilder()
                            .setGroupDefinition(groupDefinition)
                            .setOrigin(GroupDTO.Origin.newBuilder()
                                            .setUser(User.newBuilder()
                                                         .setUsername(username)
                                                     )
                                       )
                            .build()
                            );

        return groupMapper.toGroupApiDto(resp.getGroup(), true);
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
            throws UnknownObjectException, OperationFailedException {

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
        return groupMapper.toGroupApiDto(response.getUpdatedGroup(), true);
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

    @Override
    public GroupMembersPaginationResponse getMembersByGroupUuid(String uuid, final GroupMembersPaginationRequest request) throws UnknownObjectException, InvalidOperationException, OperationFailedException {
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
        } else { // Get members of the group with the uuid (oid)
            final GroupAndMembers groupAndMembers =
                groupExpander.getGroupWithMembers(uuid)
                .orElseThrow(() ->
                    new IllegalArgumentException("Can't get members in invalid group: " + uuid));
            logger.info("Number of members for group {} is {}", uuid, groupAndMembers.members().size());

            if (GroupProtoUtil.isNestedGroup(groupAndMembers.group())) {
                final Collection<GroupApiDTO> groups = getGroupApiDTOS(GetGroupsRequest.newBuilder()
                                .setGroupFilter(GroupFilter
                                                .newBuilder()
                                                .addAllId(groupAndMembers.members()))
                    .build(), true);
                return request.allResultsResponse(Lists.newArrayList(groups));

            } else {
                // Special handling for the empty member list, because passing empty to
                // repositoryApi returns all entities.
                if (groupAndMembers.members().isEmpty()) {
                    return request.allResultsResponse(Collections.emptyList());
                } else {
                    // Get entities of group members from the repository component
                    final long skipCount;
                    if (request.getCursor().isPresent()){
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
                    final Set<Long> nextPageIds = groupAndMembers.members().stream()
                        .sorted()
                        .skip(skipCount)
                        .limit(request.getLimit())
                        .collect(Collectors.toSet());
                    final Collection<ServiceEntityApiDTO> results =
                        repositoryApi.entitiesRequest(nextPageIds)
                            .getSEList();

                    final int missingEntities = nextPageIds.size() - results.size();
                    if (missingEntities > 0) {
                        logger.warn("{} group members from group {} not found in repository.",
                            missingEntities, uuid);
                    }
                    Long nextCursor = skipCount + nextPageIds.size();
                    if (nextCursor == groupAndMembers.members().size()) {
                        return request.finalPageResponse(Lists.newArrayList(results));
                    }
                    return request.nextPageResponse(Lists.newArrayList(results),
                        Long.toString(nextCursor));

                }
            }
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
     * Get the groups matching a {@link GetGroupsRequest} from the group component, and convert
     * them to the associated {@link GroupApiDTO} format.
     *
     * @param groupsRequest The request.
     * @param populateSeverity Whether or not to populate the severity in the response. Populating
     *                         severity requires another relatively expensive RPC call, so use this
     *                         only when necessary.
     * @param environmentType type of the environment to include in response, if null, all are included
     * @return The list of {@link GroupApiDTO} objects.
     */
    @Nonnull
    public List<GroupApiDTO> getGroupApiDTOS(final GetGroupsRequest groupsRequest,
            final boolean populateSeverity,
            @Nullable final EnvironmentType environmentType) {
        final Stream<GroupAndMembers> groupsWithMembers =
                groupExpander.getGroupsWithMembers(groupsRequest);
        final List<GroupApiDTO> retList = groupsWithMembers
                .filter(groupAndMembers -> !isHiddenGroup(groupAndMembers.group()))
                .map(groupAndMembers -> groupMapper.toGroupApiDto(groupAndMembers,
                        groupMapper.getEnvironmentTypeForGroup(groupAndMembers), populateSeverity))
                .filter(groupApiDTO -> EnvironmentTypeMapper.matches(environmentType,
                        groupApiDTO.getEnvironmentType()))
                .collect(Collectors.toList());
        return retList;
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
        return getGroupApiDTOS(groupsRequest, populateSeverity, null);
    }

    /**
     * Get the groups matching a {@link GetGroupsRequest} from the group component, convert
     * them to the associated {@link GroupApiDTO} format and paginate them.
     *
     * @param filterList the list of filter criteria to apply.
     * @param paginationRequest Contains the limit, the order and a potential cursor
     * @param groupType Contains the type of the group members
     * @param environmentType type of the environment to include in response, if null, all are included
     *
     * @return The list of {@link GroupApiDTO} objects.
     * @throws InvalidOperationException When the cursor is invalid.
     * @throws OperationFailedException when the input filters do not apply.
     */
    @Nonnull
    public SearchPaginationResponse getPaginatedGroupApiDTOS(final List<FilterApiDTO> filterList,
                                                             final SearchPaginationRequest paginationRequest,
                                                             final String groupType,
                                                             @Nullable EnvironmentType environmentType)
        throws InvalidOperationException, OperationFailedException {

        final GetGroupsRequest groupsRequest = getGroupsRequestForFilters(GroupType.REGULAR, filterList)
            .build();

        List<GroupAndMembers> groupsWithMembers;
        if (groupType != null) {
            MemberType groupMembersType =
                MemberType.newBuilder()
                    .setEntity(UIEntityType.fromString(groupType).typeNumber())
                    .build();
            groupsWithMembers = groupExpander.getGroupsWithMembers(groupsRequest)
                .filter(g -> g.group().getExpectedTypesList().contains(groupMembersType))
                .collect(Collectors.toList());
        } else {
            groupsWithMembers =
                groupExpander.getGroupsWithMembers(groupsRequest).collect(Collectors.toList());

        }

        Map<String, GroupAndMembers> idToGroupAndMembers = new HashMap<>();

        groupsWithMembers.stream()
            .forEach((group) -> idToGroupAndMembers.put(Long.toString(group.group().getId()), group));

        return nextGroupPage(groupsWithMembers, idToGroupAndMembers, paginationRequest, environmentType);
    }

    /**
     * Create a GetGroupsRequest based on the given filterList.
     *
     * @param groupType the group type we are creating request for.
     * @param filterList a list of FilterApiDTO to be applied to this group; only "groupsByName" is
     *                   currently supported
     * @return a GetGroupsRequest with the filtering set if an item in the filterList is found
     * @throws OperationFailedException when input filters do not apply to group type.
     */
    @VisibleForTesting
    GetGroupsRequest.Builder getGroupsRequestForFilters(@Nonnull GroupType groupType,
                    @Nonnull List<FilterApiDTO> filterList) throws OperationFailedException {
        return
            GetGroupsRequest
                .newBuilder()
                .setGroupFilter(groupFilterMapper
                                .apiFilterToGroupFilter(groupType, filterList));
    }

    /**
     * Get a mapping between a group id and its corresponding severity.
     *
     * @param groupsWithMembers a list of groups with their respective members
     * @return a Map containing a group id and its corresponding severity
     */
    private Map<Long, Severity> getGroupsSeverities(List<GroupAndMembers> groupsWithMembers) {
        final Map<Long, Severity> groupSeverity = new HashMap<>();
        Map<Long, Severity> entitiesSeverityMap =
            severityPopulator.calculateSeverities(realtimeTopologyContextId,
                groupsWithMembers.stream().flatMap(groupAndMembers -> groupAndMembers.entities().stream())
                    .collect(Collectors.toSet()));

        groupsWithMembers.stream().forEach(groupAndMembers -> {
            groupSeverity.put(groupAndMembers.group().getId(), groupAndMembers.entities().stream()
                .map(entitiesSeverityMap::get)
                .filter(Objects::nonNull)
                .reduce(Severity.NORMAL, (first, second)
                    -> first.getNumber() > second.getNumber() ? first : second));
        });
        return groupSeverity;
    }

    /**
     * Return a List of {@link GroupApiDTO} with groups containing essential information
     * for the ordering.
     *
     * @param groupsWithMembers The groups to populate with information.
     * @param searchOrderBy Contains the parameters for the ordering.
     * @param environmentType type of the environment to include in response, if null, all are included
     * @return An ordered list of {@link GroupApiDTO}.
     */
    private List<GroupApiDTO> setPrePaginationGroupsInformation(
            @Nonnull List<GroupAndMembers> groupsWithMembers,
            @Nonnull SearchOrderBy searchOrderBy,
            @Nullable EnvironmentType environmentType) {
        final boolean populateSeverity = searchOrderBy == SearchOrderBy.SEVERITY;
        // We only populate severity BEFORE pagination if we are ordering by severity for
        // pagination.
        final Map<Long, Severity> groupSeverities =
            populateSeverity ?
                getGroupsSeverities(groupsWithMembers) : new HashMap<>();

        return groupsWithMembers.stream()
            .filter(groupWithMembers -> !isHiddenGroup(groupWithMembers.group()))
            .map(groupAndMembers -> {
                GroupApiDTO groupApiDTO = groupMapper.toGroupApiDtoWithoutActiveEntities(groupAndMembers,
                    EnvironmentType.UNKNOWN);
                if (populateSeverity) {
                    groupApiDTO.setSeverity(groupSeverities.get(groupAndMembers.group().getId()).name());
                }
                return groupApiDTO;
            })
            .filter(groupApiDTO -> EnvironmentTypeMapper.matches(environmentType,
                    groupApiDTO.getEnvironmentType()))
            .collect(Collectors.toList());
    }

    /**
     * Return a List of {@link BaseApiDTO} containing groups that contains all the missing
     * information
     * after being ordered.
     *
     * @param groupApiDTOs The groups to populate with information.
     * @param searchOrderBy Contains the parameters for the ordering.
     * @param idToGroupAndMembers contains a mapping from a group id to its members.
     * @return An ordered list of {@link BaseApiDTO}.
     */
    private List<BaseApiDTO> setPostPaginationGroupsInformation(List<GroupApiDTO> groupApiDTOs,
                                                     SearchOrderBy searchOrderBy, Map<String,
        GroupAndMembers> idToGroupAndMembers ) {

        if (searchOrderBy != SearchOrderBy.SEVERITY) {
            List<GroupAndMembers> limitedGroupsWithMembers =
                groupApiDTOs.stream().map(groupApiDTO -> idToGroupAndMembers.get(groupApiDTO.getUuid())).collect(Collectors.toList());
            final Map<Long, Severity> limitedGroupSeverities  =
                getGroupsSeverities(limitedGroupsWithMembers);
            groupApiDTOs.forEach(groupApiDTO -> {
                groupApiDTO.setSeverity(limitedGroupSeverities.get(Long.parseLong(groupApiDTO.getUuid())).name());
            });
        }
        List<BaseApiDTO> retList = groupApiDTOs.stream().map(groupApiDTO -> {
            groupApiDTO.setActiveEntitiesCount(groupMapper.getActiveEntitiesCount(idToGroupAndMembers.get(groupApiDTO.getUuid())));
            return groupApiDTO;
        }).collect(Collectors.toList());
        return retList;
    }

    /**
     * Return a SearchPaginationResponse containing groups ordered and limited according to the
     * paginationRequest parameters.
     *
     * @param groupsWithMembers The groups to populate and order.
     * @param idToGroupAndMembers A mapping from a group id to the group with its members.
     * @param paginationRequest Contains the parameters for the pagination.
     * @param environmentType type of the environment to include in response, if null, all are included
     * @return The {@link SearchPaginationResponse} containing the groups.
     * @throws InvalidOperationException When the cursor is invalid.
     */
    private SearchPaginationResponse nextGroupPage(final List<GroupAndMembers> groupsWithMembers,
            final Map<String, GroupAndMembers> idToGroupAndMembers,
            final SearchPaginationRequest paginationRequest,
            @Nullable final EnvironmentType environmentType) throws InvalidOperationException {
        // In this function get information about groups and order them. Since it's an expensive
        // call we need to make sure to retrieve information for all of groups only if they are
        // needed for the sorting. For example, if we sort by severity, we need to fetch and set
        // the severities to the groups before ordering them. If instead we order by name, we can
        // fetch and set the severities only for the groups we are paginating. For this reason
        // two functions that dynamically get the correct information based on the ordering are
        // used: setPrePaginationGroupsInformation and setPostPaginationGroupsInformation.

        final long skipCount;
        if (paginationRequest.getCursor().isPresent()) {
            try {
                skipCount = Long.parseLong(paginationRequest.getCursor().get());
                if (skipCount < 0) {
                    throw new InvalidOperationException("Illegal cursor: " +
                        skipCount + ". Must be be a positive integer");
                }
            } catch (InvalidOperationException e) {
                throw new InvalidOperationException("Cursor " + paginationRequest.getCursor() +
                    " is invalid. Should be a number.");
            }

        } else {
            skipCount = 0;
        }

        final List<GroupApiDTO> groupApiDTOs =
            setPrePaginationGroupsInformation(groupsWithMembers, paginationRequest.getOrderBy(), environmentType);

        final List<GroupApiDTO> paginatedGroupApiDTOs =
            groupApiDTOs.stream()
                .sorted(SearchOrderBy.fromString(
                    paginationRequest.getOrderBy().name()).getComparator(paginationRequest.isAscending()))
                .skip(skipCount)
                .limit(paginationRequest.getLimit())
                .collect(Collectors.toList());

        final List<BaseApiDTO> retList = setPostPaginationGroupsInformation(paginatedGroupApiDTOs,
            paginationRequest.getOrderBy(), idToGroupAndMembers);

        long nextCursor = skipCount + paginatedGroupApiDTOs.size();
        if (nextCursor == idToGroupAndMembers.values().size()) {
            return paginationRequest.finalPageResponse(retList);
        }
        if (nextCursor > idToGroupAndMembers.values().size()) {
            logger.warn("Illegal cursor: the value is bigger than the total amount of groups");
            return paginationRequest.finalPageResponse(retList);
        }
        return paginationRequest.nextPageResponse(retList, Long.toString(nextCursor));
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
     */
    @Nonnull
    private List<GroupApiDTO> getNestedGroupsInGroups(
            @Nonnull GroupType groupType,
            @Nonnull final List<String> scopes,
            @Nonnull final List<FilterApiDTO> filterList,
            @Nullable final EnvironmentType environmentType) throws OperationFailedException {
        final GetGroupsRequest.Builder reqBuilder = getGroupsRequestForFilters(groupType,
                        filterList);
        GroupFilter.Builder builder = GroupFilter.newBuilder(reqBuilder.getGroupFilter());
        scopes.stream()
            .map(groupExpander::getGroupWithMembers)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(grAndMem -> {
                if (isNestedGroupOfType(grAndMem, groupType)) {
                    grAndMem.members().forEach(builder::addId);
                } else {
                    builder.addId(grAndMem.group().getId());
                }
            });
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
     * Return the clusters containing the provided scopes, assumed to be entities.
     *
     * @param clusterType The type of clusters to return.
     * @param scopes The scopes - assumed to be entities.
     * @param environmentType type of the environment to include in response, if null, all are included
     * @return The {@link GroupApiDTO}s returning the clusters of the provided entities.
     */
    @Nonnull
    private List<GroupApiDTO> getClustersOfEntities(
            @Nonnull GroupType clusterType,
            @Nullable final List<String> scopes,
            @Nullable EnvironmentType environmentType) {
        // As of today (Feb 2019), the scopes object could only have one id (PM oid).
        // If there is PM oid, we want to retrieve the Cluster that the PM belonged to.
        // TODO (Gary, Feb 4, 2019), add a new gRPC service for multiple PM oids when needed.
        final Map<Long, Grouping> clustersById = scopes.stream()
            .map(uuid -> Long.parseLong(uuid))
            .map(entityId -> {
                final GetGroupForEntityResponse response =
                    groupServiceRpc.getGroupForEntity(GetGroupForEntityRequest.newBuilder()
                        .setEntityId(entityId)
                        .build());
                return response.getGroupList();
            })
            .flatMap(List::stream)
            .filter(group -> group.getDefinition().getType() == clusterType)
            // Collect to a map to get rid of duplicate clusters (i.e. if scopes specify two entities
            // in the same cluster.
            .collect(Collectors.toMap(Grouping::getId, Function.identity(), (c1, c2) -> c1));

        return clustersById.values().stream()
            .map(groupExpander::getMembersForGroup)
            .map(clusterAndMembers -> groupMapper.toGroupApiDto(clusterAndMembers,
                    groupMapper.getEnvironmentTypeForGroup(clusterAndMembers), true))
            .filter(groupApiDTO -> EnvironmentTypeMapper.matches(environmentType,
                    groupApiDTO.getEnvironmentType()))
            .collect(Collectors.toList());
    }

    /**
     * Get {@link GroupApiDTO} describing groups in the system that match certain criteria.
     *
     * @param groupType The type of the group.
     * @param scopes The scopes to look for groups in. Could be entities (in which case we look
     *               for the groups of the entities) or groups-of-groups (in which case we
     *               look for groups in the group).
     * @param filterList The list of filters to apply to the groups.
     * @param environmentType type of the environment to include in response, if null, all are included
     * @return the list of groups.
     * @throws OperationFailedException when the filters don't match the group type.
     */
    @Nonnull
    List<GroupApiDTO> getGroupsByType(@Nonnull GroupType groupType,
            @Nullable final List<String> scopes,
            @Nonnull final List<FilterApiDTO> filterList,
            @Nullable EnvironmentType environmentType) throws OperationFailedException {
        // If we're looking for clusters with a scope there are two possibilities:
        //   - The scope is a group of clusters, in which case we want the clusters in the group.
        //   - The scope is a list of entities, in which case we want the clusters the entities are in.
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
                // Note - for now (March 29 2019) we don't have cases where we need to apply the
                // filter list. But in the future we may need to.
                return getClustersOfEntities(groupType, scopes, environmentType);
            }
        } else {
            return getGroupApiDTOS(getGroupsRequestForFilters(groupType, filterList).build(),
                    true, environmentType);
        }
    }

    /**
     * Get {@link GroupApiDTO} describing groups in the system that match certain criteria.
     *
     * @param groupType The type of the group.
     * @param scopes The scopes to look for groups in. Could be entities (in which case we look
     *               for the groups of the entities) or groups-of-groups (in which case we
     *               look for groups in the group).
     * @param filterList The list of filters to apply to the groups.
     * @return the list of groups.
     * @throws OperationFailedException when the filters don't match the group type.
     */
    @Nonnull
    List<GroupApiDTO> getGroupsByType(@Nonnull GroupType groupType,
                                  @Nullable final List<String> scopes,
                                  @Nonnull final List<FilterApiDTO> filterList) throws OperationFailedException {
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
        final GroupAndMembers groupAndMembers = groupExpander.getGroupWithMembers(uuid)
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
                    .map(UIEntityType::fromString)
                    .map(UIEntityType::typeNumber)
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
