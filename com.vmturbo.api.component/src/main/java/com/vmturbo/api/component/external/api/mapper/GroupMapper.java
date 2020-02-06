package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.GroupProtoUtil.WORKLOAD_ENTITY_TYPES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
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
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator.SeverityMap;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.GroupExpander;
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
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Maps groups between their API DTO representation and their protobuf representation.
 */
public class GroupMapper {
    private static final Logger logger = LogManager.getLogger();
    private static final long REQUEST_TIMEOUT_SEC = 5 * 60;

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

    private final EntityFilterMapper entityFilterMapper;

    private final GroupFilterMapper groupFilterMapper;

    private final SeverityPopulator severityPopulator;

    private final BusinessAccountRetriever businessAccountRetriever;

    private final CostServiceBlockingStub costServiceBlockingStub;

    private final long realtimeTopologyContextId;

    private final ThinTargetCache thinTargetCache;

    private final CloudTypeMapper cloudTypeMapper;

    private final ExecutorService threadPool;

    /**
     * Creates an instance of GroupMapper using all the provided dependencies.
     *  @param supplyChainFetcherFactory for getting supply chain info.
     * @param groupExpander for getting members of groups.
     * @param repositoryApi for communicating with the api.
     * @param entityFilterMapper for converting between internal and api filter representation.
     * @param groupFilterMapper for converting between internal and api filter representation.
     * @param severityPopulator for get severity information.
     * @param businessAccountRetriever for getting business account information for billing families.
     * @param costServiceBlockingStub for getting information about costs.
     * @param realtimeTopologyContextId the topology context id, used for getting severity.
     * @param thinTargetCache for retrieving targets without making a gRPC call.
     * @param cloudTypeMapper for getting information about cloud mappers.
     * @param threadPool thread pool to use for parallel requests
     */
    public GroupMapper(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                       @Nonnull final GroupExpander groupExpander,
                       @Nonnull final RepositoryApi repositoryApi,
                       @Nonnull final EntityFilterMapper entityFilterMapper,
                       @Nonnull final GroupFilterMapper groupFilterMapper,
                       @Nonnull final SeverityPopulator severityPopulator,
                       @Nonnull final BusinessAccountRetriever businessAccountRetriever,
                       @Nonnull final CostServiceBlockingStub costServiceBlockingStub,
                       long realtimeTopologyContextId, ThinTargetCache thinTargetCache, CloudTypeMapper cloudTypeMapper,
                       @Nonnull final ExecutorService threadPool) {
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.entityFilterMapper = entityFilterMapper;
        this.groupFilterMapper = groupFilterMapper;
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.businessAccountRetriever = Objects.requireNonNull(businessAccountRetriever);
        this.costServiceBlockingStub = Objects.requireNonNull(costServiceBlockingStub);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.thinTargetCache = thinTargetCache;
        this.cloudTypeMapper = cloudTypeMapper;
        this.threadPool = Objects.requireNonNull(threadPool);
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

    @Nullable
    private EntityEnvironment getApplianceEnvironment() {
        final Set<Optional<CloudType>> cloudType = thinTargetCache.getAllTargets()
                .stream()
                .map(ThinTargetInfo::probeInfo)
                .map(probe -> cloudTypeMapper.fromTargetType(probe.type()))
                .collect(Collectors.toSet());
        if (cloudType.isEmpty() || cloudType.equals(Collections.singleton(Optional.empty()))) {
            return new EntityEnvironment(EnvironmentType.ONPREM, CloudType.UNKNOWN);
        } else if (cloudType.size() == 1) {
            return new EntityEnvironment(EnvironmentType.CLOUD, cloudType.iterator().next().get());
        } else {
            return null;
        }
    }

    /**
     * Return the environment type and the cloud type for a given group.
     * @param groupAndMembers - the parsed groupAndMembers object for a given group.
     * @param entities map of all the entities (this map contains members of a group, not
     *         vice versa).
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
    private EntityEnvironment getEnvironmentAndCloudTypeForGroup(
            @Nonnull final GroupAndMembers groupAndMembers,
            @Nonnull Map<Long, MinimalEntity> entities) {
        // parse the entities members of groupDto
        final Set<CloudType> cloudTypes = EnumSet.noneOf(CloudType.class);
        EnvironmentTypeEnum.EnvironmentType envType = null;
        final Set<Long> targetSet = new HashSet<>(groupAndMembers.entities());
        if (!targetSet.isEmpty()) {
            for (MinimalEntity entity : groupAndMembers.entities()
                    .stream()
                    .map(entities::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList())) {
                if (envType != entity.getEnvironmentType()) {
                    envType = (envType == null) ? entity.getEnvironmentType() : EnvironmentTypeEnum.EnvironmentType.HYBRID;
                }
                // Trying to determine the cloud type
                if (entity.getDiscoveringTargetIdsCount() > 0 && !envType.equals(EnvironmentTypeEnum.EnvironmentType.ON_PREM)) {
                    // If entity is discovered by several targets iterate over
                    // loop before finding target with cloud type
                    for (Long targetId : entity.getDiscoveringTargetIdsList()) {
                        Optional<ThinTargetCache.ThinTargetInfo> thinInfo = thinTargetCache.getTargetInfo(targetId);
                        if (thinInfo.isPresent() && (!thinInfo.get().isHidden())) {
                            ThinTargetCache.ThinTargetInfo getProbeInfo = thinInfo.get();
                            Optional<CloudType> cloudType = cloudTypeMapper.fromTargetType(getProbeInfo.probeInfo().type());
                            // Multiple targets might have stitched to the cloud entity. For instance,
                            // in OM-54171, AppD stitched to a cloud VM, causing an NPE.
                            if (cloudType.isPresent()) {
                                cloudTypes.add(cloudType.get());
                                break;
                            }
                        }
                    }
                    // Once we get more than one cloudType, we know that cloudType is HYBRID.
                    // Also check that environmentType is already Hybrid.
                    // If two conditions is true we can break loop through the entities in other case continue iterating in
                    // order to not to miss entities with environmentType different from current envType.
                    if (cloudTypes.size() > 1 && envType == EnvironmentTypeEnum.EnvironmentType.HYBRID) {
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
     * Converts an internal representation of a group represented as a {@link Grouping} object
     * to API representation of the object. Resulting groups are guaranteed to be in the same
     * size and order as source {@code group}.
     *
     * @param groups The internal representation of the object.
     * @param populateSeverity whether or not to populate severity of the group
     * @return the converted object.
     */
    @Nonnull
    public Map<Long, GroupApiDTO> groupsToGroupApiDto(@Nonnull final List<Grouping> groups,
            boolean populateSeverity) {
        final List<GroupAndMembers> groupsAndMembers =
                groups.stream().map(groupExpander::getMembersForGroup).collect(Collectors.toList());
        final List<GroupApiDTO> apiGroups = toGroupApiDto(groupsAndMembers, populateSeverity);
        final Map<Long, GroupApiDTO> result = new LinkedHashMap<>(groups.size());
        final Iterator<Grouping> iterator = groups.iterator();
        for (GroupApiDTO apiGroup : apiGroups) {
            result.put(iterator.next().getId(), apiGroup);
        }
        return result;
    }

    /**
     * Converts groups and members to API objects.
     *
     * @param groupsAndMembers source groups to convert
     * @param populateSeverity whether to calculate and set severity values.
     * @return resulting groups. Positions in the resulting list are guaranteed to match positions
     *         in the source list
     */
    @Nonnull
    public List<GroupApiDTO> toGroupApiDto(@Nonnull List<GroupAndMembers> groupsAndMembers,
            boolean populateSeverity) {
        final Set<Long> members = groupsAndMembers.stream()
                .map(GroupAndMembers::entities)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final EntityEnvironment entityEnvironment = getApplianceEnvironment();
        final Future<Set<Long>> activeEntities = threadPool.submit(() -> getActiveMembers(members));
        final Future<Map<Long, String>> ownerDisplayNames =
                threadPool.submit(() -> getOwnerNames(groupsAndMembers));
        final Future<SeverityMap> severityMap = threadPool.submit(
                () -> severityPopulator.getSeverityMap(realtimeTopologyContextId, members));
        final Future<Map<Long, MinimalEntity>> entities;
        if (entityEnvironment == null) {
            entities = threadPool.submit(() -> getEntities(members));
        } else {
            entities = CompletableFuture.completedFuture(null);
        }
        final GroupConversionContext context;
        try {

            context = new GroupConversionContext(
                    activeEntities.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS),
                    ownerDisplayNames.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS),
                    entities.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS), entityEnvironment,
                    severityMap.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to convert groups " +
                    groupsAndMembers.stream().map(GroupAndMembers::group).map(Grouping::getId), e);
        }
        return groupsAndMembers.stream()
                .map(group -> toGroupApiDto(group, context, populateSeverity))
                .collect(Collectors.toList());
    }

    @Nonnull
    private Map<Long, MinimalEntity> getEntities(@Nonnull Set<Long> members) {
        return repositoryApi.entitiesRequest(members)
                .getMinimalEntities()
                .collect(Collectors.toMap(MinimalEntity::getOid, Function.identity()));
    }

    /**
     * Converts from {@link GroupAndMembers} to {@link GroupApiDTO}.
     *
     * @param groupAndMembers The {@link GroupAndMembers} object (get it from {@link GroupExpander})
     *                        describing the XL group and its members.
     * @param conversionContext group conversion context
     * @param populateSeverity whether or not to populate severity of the group
     * @return The {@link GroupApiDTO} object.
     */
    @Nonnull
    private GroupApiDTO toGroupApiDto(@Nonnull final GroupAndMembers groupAndMembers,
            @Nonnull GroupConversionContext conversionContext, boolean populateSeverity) {

        final EntityEnvironment envCloudType = conversionContext.getEntityEnvironment()
                .orElseGet(() -> getEnvironmentAndCloudTypeForGroup(groupAndMembers,
                        conversionContext.getEntities()));
        final GroupApiDTO outputDTO;
        final Grouping group = groupAndMembers.group();
        outputDTO = convertToGroupApiDto(groupAndMembers, conversionContext);

        outputDTO.setDisplayName(group.getDefinition().getDisplayName());
        outputDTO.setUuid(Long.toString(group.getId()));

        outputDTO.setEnvironmentType(getEnvironmentTypeForTempGroup(envCloudType.getEnvironmentType()));
        outputDTO.setEntitiesCount(groupAndMembers.entities().size());
        outputDTO.setActiveEntitiesCount(
                getActiveEntitiesCount(groupAndMembers, conversionContext.getActiveEntities()));

        calculateEstimatedCostForCloudEnv(groupAndMembers, envCloudType.getEnvironmentType()).ifPresent(
                outputDTO::setCostPrice);

        // only populate severity if required and if the group is not empty, since it's expensive
        if (populateSeverity && !groupAndMembers.entities().isEmpty()) {
            outputDTO.setSeverity(conversionContext.getSeverityMap()
                    .calculateSeverity(groupAndMembers.entities())
                    .name());
        }
        outputDTO.setCloudType(envCloudType.getCloudType());
        return outputDTO;
    }

    private Optional<Float> calculateEstimatedCostForCloudEnv(
            @Nonnull GroupAndMembers groupAndMembers, @Nonnull EnvironmentType environmentType) {
        if (environmentType == EnvironmentType.CLOUD && !groupAndMembers.members().isEmpty()) {
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
     * Returns a map from BusinessAccount OID -> BusinessAccount name.
     *
     * @param groupsAndMembers groups to query onwers for
     * @return map
     */
    @Nonnull
    private Map<Long, String> getOwnerNames(@Nonnull Collection<GroupAndMembers> groupsAndMembers) {
        final Set<Long> ownerIds = groupsAndMembers.stream()
                .map(GroupAndMembers::group)
                .map(Grouping::getDefinition)
                .filter(def -> def.getType() == GroupType.RESOURCE)
                .filter(GroupDefinition::hasOwner)
                .map(GroupDefinition::getOwner)
                .collect(Collectors.toSet());
        if (ownerIds.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<Long, String> result = repositoryApi.entitiesRequest(ownerIds)
                .getMinimalEntities()
                .collect(Collectors.toMap(MinimalEntity::getOid, MinimalEntity::getDisplayName));
        return Collections.unmodifiableMap(result);
    }

    /**
     * Converts an internal representation of a group represented as a {@link Grouping} object
     * to API representation of the object.
     *
     * @param groupAndMembers The internal representation of the object.
     * @param conversionContext group conversion context
     * @return the converted object with some of the details filled in.
     */
    private GroupApiDTO convertToGroupApiDto(@Nonnull final GroupAndMembers groupAndMembers,
            @Nonnull GroupConversionContext conversionContext) {
         final Grouping group = groupAndMembers.group();
         final GroupDefinition groupDefinition = group.getDefinition();
         final GroupApiDTO outputDTO;
         switch (groupDefinition.getType()) {
             case BILLING_FAMILY:
                outputDTO = extractBillingFamilyInfo(groupAndMembers);
                break;
             case RESOURCE:
                final ResourceGroupApiDTO resourceGroup = new ResourceGroupApiDTO();
                outputDTO = resourceGroup;
                 if (groupDefinition.hasOwner()) {
                    final String ownerName = conversionContext.getOwnerDisplayName()
                            .get(groupDefinition.getOwner());
                    if (ownerName != null) {
                        resourceGroup.setParentUuid(Long.toString(groupDefinition.getOwner()));
                        resourceGroup.setParentDisplayName(ownerName);
                    }
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
     * Retunrs all the active entities from the specified OIDs.
     *
     * @param members oids to filter
     * @return a subset of {@code members} containing only active entities
     */
    @Nonnull
    private Set<Long> getActiveMembers(@Nonnull Set<Long> members) {
        if (members.isEmpty()) {
            return Collections.emptySet();
        }
        final PropertyFilter startingFilter = SearchProtoUtil.idFilter(members);
        try {
            return repositoryApi.newSearchRequest(
                    SearchProtoUtil.makeSearchParameters(startingFilter)
                            .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                                    SearchProtoUtil.stateFilter(UIEntityState.ACTIVE)))
                            .build()).getOids();
        } catch (StatusRuntimeException e) {
            logger.error("Failed to query repository for active entities " + members, e);
            return members;
        }
    }

    /**
     * Get the number of active entities from a {@link GroupAndMembers}.
     *
     * @param groupAndMembers The {@link GroupAndMembers} object (get it from {@link GroupExpander})
     *                        describing the XL group and its members.
     * @param activeEntities active entities for the current context. They are entities from
     *          different group, so here we have to calculate actual count. Does not work for
     *          temprary groups
     * @return The number of active entities
     */
    private int getActiveEntitiesCount(@Nonnull final GroupAndMembers groupAndMembers,
            @Nonnull Set<Long> activeEntities) {
        // Set the active entities count.
        final Grouping group = groupAndMembers.group();
        // We need to find the number of active entities in the group. The best way to do that
        // is to do a search, and return only the counts. This minimizes the amount of
        // traffic across the network - although for large groups this is still a lot!
        final PropertyFilter startingFilter;
        if (group.getDefinition().getIsTemporary() &&
                group.getDefinition().hasOptimizationMetadata() &&
                group.getDefinition().getOptimizationMetadata().getIsGlobalScope()) {
            // In a global temp group we can just set the environment type.
            startingFilter = SearchProtoUtil.entityTypeFilter(group.getDefinition()
                    .getStaticGroupMembers()
                    .getMembersByType(0)
                    .getType()
                    .getEntity());
            try {
                return (int)repositoryApi.newSearchRequest(
                        SearchProtoUtil.makeSearchParameters(startingFilter)
                                .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                                        SearchProtoUtil.stateFilter(UIEntityState.ACTIVE)))
                                .build()).count();
            } catch (StatusRuntimeException e) {
                logger.error("Search for active entities in group {} failed. Error: {}", group.getId(), e.getMessage());
                // As a fallback, assume every entity is active.
                return groupAndMembers.entities().size();
            }
        } else {
            final Set<Long> entities = new HashSet<>(groupAndMembers.entities());
            entities.retainAll(activeEntities);
            return entities.size();
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
        final Set<ThinProbeInfo> addedProbeTypes = thinTargetCache.getAllTargets()
                .stream()
                .map(ThinTargetInfo::probeInfo)
                .collect(Collectors.toSet());
        final boolean hasCloudEnvironmentTarget = addedProbeTypes.stream()
                .anyMatch(probeInfo -> CLOUD_ENVIRONMENT_PROBE_TYPES.contains(probeInfo.type()));
        return hasCloudEnvironmentTarget ? EnvironmentType.HYBRID : EnvironmentType.ONPREM;
    }

    /**
     * Helper structure, holding all the data necessary for batch conversion of groups.
     */
    private static class GroupConversionContext {
        private final Set<Long> activeEntities;
        private final Map<Long, String> ownerDisplayName;
        private final Map<Long, MinimalEntity> entities;
        private final EntityEnvironment entityEnvironment;
        private final SeverityMap severityMap;

        GroupConversionContext(@Nonnull Set<Long> activeEntities,
                @Nonnull Map<Long, String> ownerDisplayName,
                @Nullable Map<Long, MinimalEntity> entities,
                @Nullable EntityEnvironment entityEnvironment,
                @Nonnull SeverityMap severityMap) {
            this.activeEntities = Objects.requireNonNull(activeEntities);
            this.ownerDisplayName = Objects.requireNonNull(ownerDisplayName);
            this.entities = entities;
            this.entityEnvironment = entityEnvironment;
            this.severityMap = Objects.requireNonNull(severityMap);
        }

        /**
         * Returns a set of active entities OIDs.
         *
         * @return a set of OIDs
         */
        @Nonnull
        public Set<Long> getActiveEntities() {
            return activeEntities;
        }

        /**
         * Returns a map from owner OID -> owner display name.
         *
         * @return map of owner display names
         */
        @Nonnull
        public Map<Long, String> getOwnerDisplayName() {
            return ownerDisplayName;
        }

        /**
         * Returns a map of entities in this request.
         *
         * @return map of entities
         */
        @Nullable
        public Map<Long, MinimalEntity> getEntities() {
            return entities;
        }

        /**
         * Returns global entity environment. If this value is set, this means, that all the
         * entities in the environment have the same environment and cloud types. Either
         * this value or {@link #getEntities()} must be non-null.
         *
         * @return global entity environment
         */
        @Nonnull
        public Optional<EntityEnvironment> getEntityEnvironment() {
            return Optional.ofNullable(entityEnvironment);
        }

        /**
         * Severity map of all the entities in the request.
         *
         * @return severity map
         */
        @Nonnull
        public SeverityMap getSeverityMap() {
            return severityMap;
        }
    }

}
