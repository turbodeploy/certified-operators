package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.GroupProtoUtil.WORKLOAD_ENTITY_TYPES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.GroupExpander.GroupAndMembers;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.group.ResourceGroupApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.OptimizationMetadata;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Maps groups between their API DTO representation and their protobuf representation.
 */
public class GroupMapper {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The set of probe types whose environment type should be treated as CLOUD. This is different
     * from the target category "CLOUD MANAGEMENT". This is also defined in classic in
     * DiscoveryConfigService#cloudTargetTypes.
     */
    public static final Set<String> CLOUD_ENVIRONMENT_PROBE_TYPES = ImmutableSet.of(
            SDKProbeType.AWS.getProbeType(), SDKProbeType.AZURE.getProbeType(), SDKProbeType.AZURE_EA.getProbeType(),
            SDKProbeType.AZURE_SERVICE_PRINCIPAL.getProbeType());


    /**
     * This bimap maps from the class name that use in API level for groups to the
     * group type that we use internally to represent that group.
     */
    public static final BiMap<String, GroupType> API_GROUP_TYPE_TO_GROUP_TYPE =
        ImmutableBiMap.<String, GroupType>builder()
            .put(StringConstants.GROUP, GroupType.REGULAR)
            .put(StringConstants.CLUSTER, GroupType.COMPUTE_HOST_CLUSTER)
            .put(StringConstants.STORAGE_CLUSTER, GroupType.STORAGE_CLUSTER)
            .put(StringConstants.VIRTUAL_MACHINE_CLUSTER, GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER)
            .put(StringConstants.RESOURCE_GROUP, GroupType.RESOURCE)
            .put(StringConstants.BILLING_FAMILY, GroupType.BILLING_FAMILY)
            .build();

    /**
     * This maps className used by external API to default filter type.
     */
    public static final Map<String, String> API_GROUP_TYPE_TO_FILTER_GROUP_TYPE =
        ImmutableMap.<String, String>builder()
            .put(StringConstants.GROUP, GroupFilterMapper.GROUPS_FILTER_TYPE)
            .put(StringConstants.CLUSTER, GroupFilterMapper.CLUSTERS_FILTER_TYPE)
            .put(StringConstants.STORAGE_CLUSTER, GroupFilterMapper.STORAGE_CLUSTERS_FILTER_TYPE)
            .put(StringConstants.VIRTUAL_MACHINE_CLUSTER, GroupFilterMapper.VIRTUALMACHINE_CLUSTERS_FILTER_TYPE)
            .put(StringConstants.RESOURCE_GROUP, GroupFilterMapper.RESOURCE_GROUP_BY_NAME_FILTER_TYPE)
            .put(StringConstants.BILLING_FAMILY, GroupFilterMapper.BILLING_FAMILY_FILTER_TYPE)
            .build();

    /**
     * The API "class types" (as returned by {@link BaseApiDTO#getClassName()}
     * which indicate that the {@link BaseApiDTO} in question is a group.
     */
    public static final Set<String> GROUP_CLASSES = API_GROUP_TYPE_TO_GROUP_TYPE.keySet();

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    private final TopologyProcessor topologyProcessor;

    private final EntityFilterMapper entityFilterMapper;

    private final GroupFilterMapper groupFilterMapper;

    private final SeverityPopulator severityPopulator;

    private final BusinessAccountRetriever businessAccountRetriever;

    private final CostServiceBlockingStub costServiceBlockingStub;

    private final long realtimeTopologyContextId;

    private final ThinTargetCache thinTargetCache;

    private final CloudTypeMapper cloudTypeMapper;

    /**
     * Creates an instance of GroupMapper using all the provided dependencies.
     *  @param supplyChainFetcherFactory for getting supply chain info.
     * @param groupExpander for getting members of groups.
     * @param topologyProcessor for communicating with topology processor.
     * @param repositoryApi for communicating with the api.
     * @param entityFilterMapper for converting between internal and api filter representation.
     * @param groupFilterMapper for converting between internal and api filter representation.
     * @param severityPopulator for get severity information.
     * @param businessAccountRetriever for getting business account information for billing families.
     * @param costServiceBlockingStub for getting information about costs.
     * @param realtimeTopologyContextId the topology context id, used for getting severity.
     * @param thinTargetCache for retrieving targets without making a gRPC call.
     * @param cloudTypeMapper for getting information about cloud mappers.
     */
    public GroupMapper(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                       @Nonnull final GroupExpander groupExpander,
                       @Nonnull final TopologyProcessor topologyProcessor,
                       @Nonnull final RepositoryApi repositoryApi,
                       @Nonnull final EntityFilterMapper entityFilterMapper,
                       @Nonnull final GroupFilterMapper groupFilterMapper,
                       @Nonnull final SeverityPopulator severityPopulator,
                       @Nonnull final BusinessAccountRetriever businessAccountRetriever,
                       @Nonnull final CostServiceBlockingStub costServiceBlockingStub,
                       long realtimeTopologyContextId, ThinTargetCache thinTargetCache, CloudTypeMapper cloudTypeMapper) {
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.entityFilterMapper = entityFilterMapper;
        this.groupFilterMapper = groupFilterMapper;
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.businessAccountRetriever = Objects.requireNonNull(businessAccountRetriever);
        this.costServiceBlockingStub = Objects.requireNonNull(costServiceBlockingStub);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.thinTargetCache = thinTargetCache;
        this.cloudTypeMapper = cloudTypeMapper;
    }

    /**
     * Converts an input API definition of a group represented as a {@link GroupApiDTO} object to a
     * {@link GroupDefinition} object.
     *
     * @param groupDto input API representation of a group object.
     * @return input object converted to a {@link GroupDefinition} object.
     * @throws OperationFailedException if the conversion fails.
     */
    public GroupDefinition toGroupDefinition(@Nonnull final GroupApiDTO groupDto) throws OperationFailedException {
        GroupDefinition.Builder groupBuilder = GroupDefinition.newBuilder()
                        .setDisplayName(groupDto.getDisplayName())
                        .setType(GroupType.REGULAR)
                        .setIsTemporary(Boolean.TRUE.equals(groupDto.getTemporary()));

        final GroupType nestedMemberGroupType = API_GROUP_TYPE_TO_GROUP_TYPE.get(groupDto.getGroupType());

        if (groupDto.getIsStatic()) {
            // for the case static group and static group of groups

            final Set<Long> memberUuids = getGroupMembersAsLong(groupDto);

            if (groupBuilder.getIsTemporary()
                            && !CollectionUtils.isEmpty(groupDto.getScope())) {
                final boolean isGlobalScope = groupDto.getScope().size() == 1 &&
                                groupDto.getScope().get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR);
                final EnvironmentType uiEnvType = getEnvironmentTypeForTempGroup(groupDto.getEnvironmentType());

                final OptimizationMetadata.Builder optimizationMetaData = OptimizationMetadata
                                .newBuilder()
                                .setIsGlobalScope(isGlobalScope);

                if (uiEnvType != null && uiEnvType != EnvironmentType.HYBRID) {
                    optimizationMetaData.setEnvironmentType(uiEnvType == EnvironmentType.CLOUD ?
                        EnvironmentTypeEnum.EnvironmentType.CLOUD : EnvironmentTypeEnum.EnvironmentType.ON_PREM);
                }
                groupBuilder.setOptimizationMetadata(optimizationMetaData);
            }

            groupBuilder.setStaticGroupMembers(getStaticGroupMembers(nestedMemberGroupType,
                            groupDto, memberUuids));

        } else if (nestedMemberGroupType == null) {
            // this means this is dynamic group of entities
            final List<SearchParameters> searchParameters = entityFilterMapper
                            .convertToSearchParameters(groupDto.getCriteriaList(),
                                            groupDto.getGroupType(), null);

            groupBuilder.setEntityFilters(EntityFilters.newBuilder()
                .addEntityFilter(EntityFilter.newBuilder()
                    .setEntityType(UIEntityType.fromString(groupDto.getGroupType()).typeNumber())
                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                        .addAllSearchParameters(searchParameters)))
            );
        } else {
            // this means this a dynamic group of groups
            GroupFilter groupFilter = groupFilterMapper.apiFilterToGroupFilter(nestedMemberGroupType,
                            groupDto.getCriteriaList());
            groupBuilder.setGroupFilters(GroupFilters.newBuilder().addGroupFilter(groupFilter));
        }

        return groupBuilder.build();
    }

    /**
     * Return the environment type and the cloud type for a given group.
     * @param groupAndMembers - the parsed groupAndMembers object for a given group.
     * @return the EnvCloudMapper:
     * EnvironmentType:
     *  - CLOUD if all group members are CLOUD entities or it is empty cloud group or regular
     *          group with cloud group members
     *  - ON_PREM if all group members are ON_PREM entities
     *  - HYBRID if the group contains members both CLOUD entities and ON_PREM entities.
     *  CloudType:
     *  - AWS if all group members are AWS entities
     *  - AZURE if all group members are AZURE entities
     *  - HYBRID if the group contains members both AWS entities and AZURE entities.
     *  - UNKNOWN if the group type cannot be determined
     */
    public EntityEnvironment getEnvironmentAndCloudTypeForGroup(@Nonnull final GroupAndMembers groupAndMembers) {
        // parse the entities members of groupDto
        final Set<CloudType> cloudTypes = EnumSet.noneOf(CloudType.class);
        EnvironmentTypeEnum.EnvironmentType envType = null;
        final Set<Long> targetSet = new HashSet<>(groupAndMembers.entities());
        if (!targetSet.isEmpty()) {
            for (MinimalEntity entity : repositoryApi.entitiesRequest(targetSet)
                    .getMinimalEntities()
                    .collect(Collectors.toList())) {
                if (envType != entity.getEnvironmentType()) {
                    envType = (envType == null) ? entity.getEnvironmentType() : EnvironmentTypeEnum.EnvironmentType.HYBRID;
                }
                // Trying to determine the cloud type
                if (entity.getDiscoveringTargetIdsCount() > 0 && !envType.equals(EnvironmentTypeEnum.EnvironmentType.ON_PREM)) {
                    // The first element is good enough to indicate the cloud type
                    for (Long targetId : entity.getDiscoveringTargetIdsList()) {
                        Optional<ThinTargetCache.ThinTargetInfo> thinInfo = thinTargetCache.getTargetInfo(targetId);
                        if (thinInfo.isPresent() && (!thinInfo.get().isHidden())) {
                            ThinTargetCache.ThinTargetInfo getProbeInfo = thinInfo.get();
                            cloudTypes.add(cloudTypeMapper.fromTargetType(getProbeInfo.probeInfo().type()));
                            break;
                        }
                    }
                    // Once we get more than one, we know it's HYBRID
                    if (cloudTypes.size() > 1) {
                        break;
                    }
                }
            }
        } else {
            final GroupType groupType = groupAndMembers.group().getDefinition().getType();
            final List<MemberType> expectedTypes = groupAndMembers.group().getExpectedTypesList();
            // case for empty cloud groups or regular groups with cloud groups
            if (groupType == GroupType.RESOURCE || groupType == GroupType.BILLING_FAMILY ||
                    expectedTypes.contains(
                            MemberType.newBuilder().setGroup(GroupType.RESOURCE).build()) ||
                    expectedTypes.contains(
                            MemberType.newBuilder().setGroup(GroupType.BILLING_FAMILY).build())) {
                envType = EnvironmentTypeEnum.EnvironmentType.CLOUD;
            }
        }

        final EnvironmentType environmentType = EnvironmentTypeMapper.fromXLToApi(envType != null ? envType
                : EnvironmentTypeEnum.EnvironmentType.ON_PREM ).orElse(EnvironmentType.ONPREM);

        final CloudType cloudType;
        if (cloudTypes.size() == 1) {
            cloudType = cloudTypes.iterator().next();
        } else if (cloudTypes.size() > 1) {
            cloudType = CloudType.HYBRID;
        } else {
            cloudType = CloudType.UNKNOWN;
        }

        return new EntityEnvironment(environmentType, cloudType);
    }

    /**
     * Converts an internal representation of a group represented as a {@Grouping} object
     * to API representation of the object.
     *
     * @param group The internal representation of the object.
     * @param environmentType The environment type to set on the converted object.
     * @return the converted object.
     */
    public GroupApiDTO toGroupApiDto(@Nonnull final Grouping group, EnvironmentType environmentType) {
        return toGroupApiDto(groupExpander.getMembersForGroup(group), environmentType, CloudType.UNKNOWN, false);
    }


    /**
     * Converts an internal representation of a group represented as a {@Grouping} object
     * to API representation of the object.
     *
     * @param group The internal representation of the object.
     * @return the converted object.
     */
    public GroupApiDTO toGroupApiDto(@Nonnull final Grouping group) {
        return toGroupApiDto(group, false);
    }

    /**
     * Converts an internal representation of a group represented as a {@Grouping} object
     * to API representation of the object.
     *
     * @param group The internal representation of the object.
     * @param populateSeverity whether or not to populate severity of the group
     * @return the converted object.
     */
    public GroupApiDTO toGroupApiDto(@Nonnull final Grouping group, boolean populateSeverity) {
        GroupAndMembers groupAndMembers = groupExpander.getMembersForGroup(group);
        EntityEnvironment envCloudType = getEnvironmentAndCloudTypeForGroup(groupAndMembers);

        return toGroupApiDto(groupExpander.getMembersForGroup(group), envCloudType.getEnvironmentType(),
                envCloudType.getCloudType(), populateSeverity);
    }

    /**
     * Converts from {@link GroupAndMembers} to {@link GroupApiDTO}.
     *
     * @param groupAndMembers The {@link GroupAndMembers} object (get it from {@link GroupExpander})
     *                        describing the XL group and its members.
     * @param environmentType The environment type of the group.
     * @param cloudType The cloud type of the group.
     * @param populateSeverity whether or not to populate severity of the group
     * @return The {@link GroupApiDTO} object.
     */
    @SuppressWarnings("checkstyle:RegexpSingleline")
    @Nonnull
    public GroupApiDTO toGroupApiDto(@Nonnull final GroupAndMembers groupAndMembers,
                                     @Nonnull final EnvironmentType environmentType,
                                     @Nonnull final CloudType cloudType,
                                     boolean populateSeverity) {
        final GroupApiDTO outputDTO;
        final Grouping group = groupAndMembers.group();
        outputDTO = convertToGroupApiDto(groupAndMembers);

        outputDTO.setDisplayName(group.getDefinition().getDisplayName());
        outputDTO.setUuid(Long.toString(group.getId()));

        outputDTO.setEnvironmentType(getEnvironmentTypeForTempGroup(environmentType));
        outputDTO.setEntitiesCount(groupAndMembers.entities().size());
        outputDTO.setActiveEntitiesCount(getActiveEntitiesCount(groupAndMembers));

        calculateEstimatedCostForCloudEnv(groupAndMembers, environmentType).ifPresent(
                outputDTO::setCostPrice);

        // only populate severity if required and if the group is not empty, since it's expensive
        if (populateSeverity && !groupAndMembers.entities().isEmpty()) {
            severityPopulator.calculateSeverity(realtimeTopologyContextId, groupAndMembers.entities())
                    .ifPresent(severity -> outputDTO.setSeverity(severity.name()));
        }
        outputDTO.setCloudType(cloudType);
        return outputDTO;
    }

    private Optional<Float> calculateEstimatedCostForCloudEnv(
            @Nonnull GroupAndMembers groupAndMembers, @Nonnull EnvironmentType environmentType) {
        if (environmentType == EnvironmentType.CLOUD) {
            final GetCloudCostStatsResponse cloudCostStatsResponse;
            try {
                cloudCostStatsResponse = costServiceBlockingStub.getCloudCostStats(GetCloudCostStatsRequest.newBuilder()
                    .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setEntityFilter(Cost.EntityFilter.newBuilder()
                            .addAllEntityId(groupAndMembers.members())
                            .build())
                        .build()).build());
            } catch (StatusRuntimeException e) {
                if (Code.UNAVAILABLE == e.getStatus().getCode()) {
                    // Any component may be down at any time. APIs like search should not fail
                    // when the cost component is down. We must log a warning when this happens,
                    // or else it will be difficult for someone to explain why search does not
                    // have cost data.
                    logger.warn("The cost component is not available. As a result, we will not fill in the response with cost details for groupAndMembers={} and environmentType={}",
                        () -> groupAndMembers,
                        () -> environmentType);
                    return Optional.empty();
                } else {
                    // Cost component responded, so it's up an running. We need to make this
                    // exception visible because there might be a bug in the cost component.
                    throw e;
                }
            }
            final List<CloudCostStatRecord> costStatRecordList =
                    cloudCostStatsResponse.getCloudStatRecordList();
            if (!costStatRecordList.isEmpty()) {
                // exclude cost with category STORAGE for VMs, because this cost is duplicate STORAGE cost for volumes
                final double estimatedCost = costStatRecordList.get(0)
                        .getStatRecordsList()
                        .stream()
                        .filter(el -> !(el.getCategory() == CostCategory.STORAGE &&
                                el.getAssociatedEntityType() == EntityType.VIRTUAL_MACHINE_VALUE))
                        .mapToDouble(entityCostStat -> entityCostStat.getValues().getTotal())
                        .sum();
                return Optional.of((float)estimatedCost);
            }
        }
        return Optional.empty();
    }

    private int getMembersCount(GroupAndMembers groupAndMembers) {
        if (groupAndMembers.group().getDefinition().getType() == GroupType.RESOURCE) {
            return groupAndMembers.group()
                .getDefinition()
                .getStaticGroupMembers()
                .getMembersByTypeList()
                .stream()
                .filter(membersByType -> membersByType.getType().hasEntity()
                    && WORKLOAD_ENTITY_TYPES.contains(
                        UIEntityType.fromType(membersByType.getType().getEntity())))
                .map(StaticMembersByType::getMembersCount)
                .mapToInt(Integer::intValue)
                .sum();

        } else {
            return groupAndMembers.members().size();
        }
    }

    /**
     * Converts an internal representation of a group represented as a {@Grouping} object
     * to API representation of the object.
     *
     * @param groupAndMembers The internal representation of the object.
     * @return the converted object with some of the details filled in.
     */
     private GroupApiDTO convertToGroupApiDto(@Nonnull final GroupAndMembers groupAndMembers) {
         final Grouping group = groupAndMembers.group();
         final GroupDefinition groupDefinition = group.getDefinition();
         final GroupApiDTO outputDTO;
         switch (groupDefinition.getType()) {
             case BILLING_FAMILY:
                outputDTO = extractBillingFamilyInfo(groupAndMembers);
                break;
             case RESOURCE:
                 outputDTO = new ResourceGroupApiDTO();
                 if (groupDefinition.hasOwner()) {
                     final long groupOwner = groupDefinition.getOwner();
                     ((ResourceGroupApiDTO)outputDTO).setParentUuid(String.valueOf(groupOwner));
                     repositoryApi.entityRequest(groupOwner)
                            .getMinimalEntity()
                            .ifPresent(el -> ((ResourceGroupApiDTO)outputDTO).setParentDisplayName(
                                    el.getDisplayName()));
                 } else {
                     logger.error("The resource group '{}'({}) doesn't have owner",
                            groupDefinition.getDisplayName(), group.getId());
                 }
                 break;
             default:
                outputDTO = new GroupApiDTO();
         }
         outputDTO.setUuid(String.valueOf(group.getId()));

         outputDTO.setClassName(convertGroupTypeToApiType(groupDefinition.getType()));

         List<String> directMemberTypes = getDirectMemberTypes(groupDefinition);

         if (!directMemberTypes.isEmpty()) {
             if (groupDefinition.getType() == GroupType.RESOURCE) {
                 outputDTO.setGroupType(StringConstants.WORKLOAD);
             } else {
                 outputDTO.setGroupType(String.join(",", directMemberTypes));
             }
         } else {
             outputDTO.setGroupType(UIEntityType.UNKNOWN.apiStr());
         }

         outputDTO.setMemberTypes(directMemberTypes);
         outputDTO.setEntityTypes(group.getExpectedTypesList()
                        .stream()
                        .filter(MemberType::hasEntity)
                        .map(MemberType::getEntity)
                        .map(UIEntityType::fromType)
                        .map(UIEntityType::apiStr)
                        .collect(Collectors.toList()));

         outputDTO.setIsStatic(groupDefinition.hasStaticGroupMembers());
         outputDTO.setTemporary(groupDefinition.getIsTemporary());

         switch (groupDefinition.getSelectionCriteriaCase()) {
             case STATIC_GROUP_MEMBERS:
                 outputDTO.setMemberUuidList(GroupProtoUtil
                                .getStaticMembers(group)
                                .stream()
                                .map(String::valueOf)
                                .collect(Collectors.toList()));
                 break;

             case ENTITY_FILTERS:
                 if (groupDefinition.getEntityFilters().getEntityFilterCount() == 0) {
                     logger.error("The dynamic group does not have any filters. Group {}",
                                    group);
                     break;
                 }
                 // currently api only supports homogeneous dynamic groups
                 if (groupDefinition.getEntityFilters().getEntityFilterCount() > 1) {
                     logger.error("API does not support heterogeneous dynamic groups. Group {}",
                                     group);
                 }

                 outputDTO.setCriteriaList(entityFilterMapper.convertToFilterApis(groupDefinition
                                .getEntityFilters()
                                .getEntityFilter(0)));
                 break;

             case GROUP_FILTERS:
                 if (groupDefinition.getGroupFilters().getGroupFilterCount() == 0) {
                     logger.error("The dynamic group of groups does not have any filters. Group {}",
                                    group);
                     break;
                 }
                 // currently api only supports homogeneous dynamic group of groups. It means
                 // it does not support a dynamic group of resource group and cluster
                 if (groupDefinition.getGroupFilters().getGroupFilterCount() > 1) {
                     logger.error("API does not support heterogeneous dynamic groups of groups. Group {}",
                                    group);
                 }

                 outputDTO.setCriteriaList(groupFilterMapper.groupFilterToApiFilters(groupDefinition
                                .getGroupFilters()
                                .getGroupFilter(0)));
                 break;
             default:
                 break;
         }

         // BillingFamily has custom code for determining the number of members. An undiscovered
         // account is not considered a member in classic.
         if (outputDTO.getMembersCount() == null) {
             outputDTO.setMembersCount(getMembersCount(groupAndMembers));
         }
         outputDTO.setMemberUuidList(groupAndMembers.members().stream()
             .map(oid -> Long.toString(oid))
             .collect(Collectors.toList()));

         return outputDTO;
    }

    private BillingFamilyApiDTO extractBillingFamilyInfo(GroupAndMembers groupAndMembers) {
        BillingFamilyApiDTO billingFamilyApiDTO = new BillingFamilyApiDTO();
        Set<Long> oidsToQuery = new HashSet<>(groupAndMembers.members());
        List<BusinessUnitApiDTO> businessUnitApiDTOList = new ArrayList<>();
        Map<String, String> uuidToDisplayNameMap = new HashMap<>();
        float cost = 0f;
        boolean hasCost = false;
        int discoveredAccounts = 0;

        for (BusinessUnitApiDTO businessUnit : businessAccountRetriever.getBusinessAccounts(oidsToQuery)) {
            Float businessUnitCost = businessUnit.getCostPrice();
            if (businessUnitCost != null) {
                hasCost = true;
                cost += businessUnitCost;
            }

            if (businessUnit.getCloudType() != null) {
                billingFamilyApiDTO.setCloudType(businessUnit.getCloudType());
            }

            String displayName = businessUnit.getDisplayName();
            uuidToDisplayNameMap.put(businessUnit.getUuid(), displayName);

            String accountId = businessUnit.getAccountId();
            if (businessUnit.isMaster()) {
                billingFamilyApiDTO.setMasterAccountUuid(businessUnit.getUuid());
            }

            businessUnitApiDTOList.add(businessUnit);

            // OM-53266: Member count should only consider accounts that are monitored by a probe.
            // Accounts that are only submitted as a member of a BillingFamily should not be counted.
            if (businessUnit.getAssociatedTargetId() != null) {
                discoveredAccounts++;
            }
        }

        if (hasCost) {
            billingFamilyApiDTO.setCostPrice(cost);
        }
        billingFamilyApiDTO.setUuidToNameMap(uuidToDisplayNameMap);
        billingFamilyApiDTO.setBusinessUnitApiDTOList(businessUnitApiDTOList);
        billingFamilyApiDTO.setMembersCount(discoveredAccounts);
        return billingFamilyApiDTO;
    }

    @Nonnull
    private Set<Long> getGroupMembersAsLong(GroupApiDTO groupDto) throws OperationFailedException {
            if (groupDto.getMemberUuidList() == null) {
                return Collections.emptySet();
            } else {
                Set<Long> result = new HashSet<>();
                for (String uuid : groupDto.getMemberUuidList()) {
                    try {
                      //parse the uuids for the members
                      result.add(Long.parseLong(uuid));
                    } catch (NumberFormatException e) {
                        logger.error("Invalid group member uuid in the list of group `{}` members: `{}`. Only long values as uuid are accepted.",
                                        groupDto.getDisplayName(), String.join(",", groupDto.getMemberUuidList()));
                    }
                }
                return result;
            }
    }

    @Nonnull
    private StaticMembers getStaticGroupMembers(@Nullable final GroupType groupType,
                    @Nonnull final GroupApiDTO groupDto, @Nonnull Set<Long> memberUuids) throws OperationFailedException {
        final MemberType memberType;
        if (groupType != null) {
            // if this is group of groups
            memberType = MemberType.newBuilder().setGroup(groupType).build();
        } else {
            // otherwise it is assumed this a group of entities
            memberType = MemberType.newBuilder().setEntity(
                            UIEntityType.fromString(groupDto.getGroupType()).typeNumber()
                        ).build();
        }

        final Set<Long> groupMembers;

        if (!CollectionUtils.isEmpty(groupDto.getScope())) {
            // Derive the members from the scope
            final Map<String, SupplyChainNode> supplyChainForScope =
                    supplyChainFetcherFactory.newNodeFetcher()
                            .addSeedUuids(groupDto.getScope())
                            .entityTypes(Collections.singletonList(groupDto.getGroupType()))
                            .apiEnvironmentType(groupDto.getEnvironmentType())
                            .fetch();
            final SupplyChainNode node = supplyChainForScope.get(groupDto.getGroupType());
            if (node == null) {
                throw new OperationFailedException("Group type: " + groupDto.getGroupType() +
                        " not found in supply chain for scopes: " + groupDto.getScope());
            }
            final Set<Long> entitiesInScope = RepositoryDTOUtil.getAllMemberOids(node);
            // Check if the user only wants a specific set of entities within the scope.
            if (!memberUuids.isEmpty()) {
                groupMembers = Sets.intersection(entitiesInScope, memberUuids);
            } else {
                groupMembers = entitiesInScope;
            }
        } else {
            groupMembers = memberUuids;
        }

        return StaticMembers
                .newBuilder()
                .addMembersByType(StaticMembersByType
                                .newBuilder()
                                .setType(memberType)
                                .addAllMembers(groupMembers)
                                )
                .build();
    }

    private List<String> getDirectMemberTypes(GroupDefinition groupDefinition) {

        switch (groupDefinition.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                return groupDefinition
                                .getStaticGroupMembers()
                                .getMembersByTypeList()
                                .stream()
                                .map(StaticMembersByType::getType)
                                .map(GroupMapper::convertMemberTypeToApiType)
                                .filter(Objects::nonNull)
                                .distinct()
                                .collect(Collectors.toList());
            case ENTITY_FILTERS:
                if (groupDefinition.getEntityFilters().getEntityFilterCount() == 0) {
                    logger.error("The dynamic group does not have any filters. Group {}",
                                    groupDefinition);

                    return Collections.emptyList();
                }
                // currently API only supports homogeneous dynamic groups
                return Collections.singletonList(UIEntityType.fromType(groupDefinition
                                    .getEntityFilters()
                                    .getEntityFilter(0)
                                    .getEntityType()
                                )
                                .apiStr());

            case GROUP_FILTERS:
                if (groupDefinition.getGroupFilters().getGroupFilterCount() == 0) {
                    logger.error("The dynamic group of groups does not have any filters. Group {}",
                                    groupDefinition);
                    return Collections.emptyList();
                }
                 // currently API only supports dynamic groups of single group type
                 return Collections.singletonList(API_GROUP_TYPE_TO_GROUP_TYPE.inverse().get(
                     groupDefinition
                                 .getGroupFilters()
                                 .getGroupFilterList()
                                 .get(0)
                                 .getGroupType()));

            default:
                return Collections.emptyList();
        }
    }

    @Nullable
    private static String convertMemberTypeToApiType(@Nonnull MemberType memberType) {
        switch (memberType.getTypeCase()) {
            case ENTITY:
                return UIEntityType.fromType(memberType.getEntity()).apiStr();
            case GROUP:
                return convertGroupTypeToApiType(memberType.getGroup());
            default:
                logger.error("The member type is `{}` is unknown to API.", memberType);
                return null;
        }
    }

    @Nonnull
    private static String convertGroupTypeToApiType(@Nonnull GroupType type) {
        return API_GROUP_TYPE_TO_GROUP_TYPE
                        .inverse().getOrDefault(type,
                                        type.name());
    }

    /**
     * Converts from {@link GroupAndMembers} to {@link GroupApiDTO} without setting the active
     * entities count.
     *
     * @param groupAndMembers The {@link GroupAndMembers} object (get it from {@link GroupExpander})
     *                        describing the XL group and its members.
     * @param environmentType The environment type of the group.
     * @param cloudType The cloud type of the group.
     * @return The {@link GroupApiDTO} object.
     */
    @Nonnull
    public GroupApiDTO toGroupApiDtoWithoutActiveEntities(@Nonnull final GroupAndMembers groupAndMembers,
                                     @Nonnull EnvironmentType environmentType, @Nonnull CloudType cloudType) {
        final GroupApiDTO outputDTO;
        final Grouping group = groupAndMembers.group();
        outputDTO = toGroupApiDto(groupAndMembers.group());
        outputDTO.setDisplayName(group.getDefinition().getDisplayName());
        outputDTO.setUuid(Long.toString(groupAndMembers.group().getId()));
        outputDTO.setMembersCount(groupAndMembers.members().size());
        outputDTO.setMemberUuidList(groupAndMembers.members().stream()
            .map(oid -> Long.toString(oid))
            .collect(Collectors.toList()));

        if (EnvironmentType.UNKNOWN.equals(environmentType)) {
             EntityEnvironment envCloudType = getEnvironmentAndCloudTypeForGroup(groupAndMembers);
            environmentType = envCloudType.getEnvironmentType();
            cloudType = envCloudType.getCloudType();
        }

        outputDTO.setEnvironmentType(getEnvironmentTypeForTempGroup(environmentType));
        outputDTO.setCloudType(cloudType);
        outputDTO.setEntitiesCount(groupAndMembers.entities().size());

        return outputDTO;
    }

    /**
     * Get the number of active entities from a {@link GroupAndMembers}.
     *
     * @param groupAndMembers The {@link GroupAndMembers} object (get it from {@link GroupExpander})
     *                        describing the XL group and its members.
     * @return The number of active entities
     */
    public int getActiveEntitiesCount(@Nonnull final GroupAndMembers groupAndMembers) {
        // Set the active entities count.
        final Grouping group = groupAndMembers.group();
        try {
            // We need to find the number of active entities in the group. The best way to do that
            // is to do a search, and return only the counts. This minimizes the amount of
            // traffic across the network - although for large groups this is still a lot!
            final PropertyFilter startingFilter;
            if (group.getDefinition().getIsTemporary()
                            && group.getDefinition().hasOptimizationMetadata()
                            && group.getDefinition().getOptimizationMetadata()
                                    .getIsGlobalScope()) {
                // In a global temp group we can just set the environment type.
                startingFilter = SearchProtoUtil.entityTypeFilter(group
                                .getDefinition()
                                .getStaticGroupMembers()
                                .getMembersByType(0)
                                .getType()
                                .getEntity()
                                );
            } else {
                // In any other group we need to send the entity IDs to the search service as the
                // starting filter.
                startingFilter = SearchProtoUtil.idFilter(groupAndMembers.entities());
            }
            return (int)repositoryApi.newSearchRequest(
                SearchProtoUtil.makeSearchParameters(startingFilter)
                    .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                        SearchProtoUtil.stateFilter(UIEntityState.ACTIVE)))
                    .build())
                .count();
        } catch (StatusRuntimeException e) {
            logger.error("Search for active entities in group {} failed. Error: {}",
                group.getId(), e.getMessage());
            // As a fallback, assume every entity is active.
            return groupAndMembers.entities().size();
        }
    }

    /**
     * Get the environment type for temporary group. If it's not created from HYBRID view in UI,
     * return the environment type passed from UI. If it's created from HYBRID view, we should also
     * check added targets, if no cloud targets, set it to ONPREM.
     * Note: We don't want to fetch all entities in this group and go through all of them to decide
     * the environment type like that in classic, since it's expensive for large groups. A better
     * solution will be done on develop.
     *
     * @param environmentTypeFromUI the EnvironmentType passed from UI
     * @return the {@link EnvironmentType} for the temporary group
     */
    private EnvironmentType getEnvironmentTypeForTempGroup(@Nonnull final EnvironmentType environmentTypeFromUI) {
        if (environmentTypeFromUI != EnvironmentType.HYBRID) {
            return environmentTypeFromUI;
        }
        try {
            final Set<Long> addedProbeIds = topologyProcessor.getAllTargets().stream()
                .map(TargetInfo::getProbeId)
                .collect(Collectors.toSet());
            final boolean hasCloudEnvironmentTarget = topologyProcessor.getAllProbes().stream()
                .filter(probeInfo -> addedProbeIds.contains(probeInfo.getId()))
                .anyMatch(probeInfo -> CLOUD_ENVIRONMENT_PROBE_TYPES.contains(probeInfo.getType()));
            return hasCloudEnvironmentTarget ? EnvironmentType.HYBRID : EnvironmentType.ONPREM;
        } catch (CommunicationException e) {
            logger.error("Error fetching targets and probes from topology processor", e);
            return EnvironmentType.HYBRID;
        }
    }


}
