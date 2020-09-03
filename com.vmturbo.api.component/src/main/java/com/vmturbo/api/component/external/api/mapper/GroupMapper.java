package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.GroupProtoUtil.WORKLOAD_ENTITY_TYPES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.FutureObserver;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator.SeverityMapImpl;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ObjectsPage;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.group.ResourceGroupApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
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
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.utils.StringConstants;
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
            SDKProbeType.AWS.getProbeType(), SDKProbeType.AWS_BILLING.getProbeType(),
            SDKProbeType.GCP.getProbeType(),
            SDKProbeType.AZURE.getProbeType(), SDKProbeType.AZURE_EA.getProbeType(),
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
     * This maps the protobuf logical operators to the API strings used to represent them.
     */
    private static final BiMap<LogicalOperator, String> LOGICAL_OPERATOR_MAPPING =
        ImmutableBiMap.<LogicalOperator, String>builder()
            .put(LogicalOperator.AND, "AND")
            .put(LogicalOperator.OR, "OR")
            .put(LogicalOperator.XOR, "XOR")
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

    private final CostServiceStub costServiceStub;

    private final long realtimeTopologyContextId;

    private final ThinTargetCache thinTargetCache;

    private final CloudTypeMapper cloudTypeMapper;

    private final Map<SearchOrderBy, PaginatorSupplier> paginators;
    private final PaginatorSupplier defaultPaginator;

    /**
     * Creates an instance of GroupMapper using all the provided dependencies.
     *  @param supplyChainFetcherFactory for getting supply chain info.
     * @param groupExpander for getting members of groups.
     * @param repositoryApi for communicating with the api.
     * @param entityFilterMapper for converting between internal and api filter representation.
     * @param groupFilterMapper for converting between internal and api filter representation.
     * @param severityPopulator for get severity information.
     * @param businessAccountRetriever for getting business account information for billing families.
     * @param costServiceStub for getting information about costs.
     * @param realtimeTopologyContextId the topology context id, used for getting severity.
     * @param thinTargetCache for retrieving targets without making a gRPC call.
     * @param cloudTypeMapper for getting information about cloud mappers.
     */
    public GroupMapper(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                       @Nonnull final GroupExpander groupExpander,
                       @Nonnull final RepositoryApi repositoryApi,
                       @Nonnull final EntityFilterMapper entityFilterMapper,
                       @Nonnull final GroupFilterMapper groupFilterMapper,
                       @Nonnull final SeverityPopulator severityPopulator,
                       @Nonnull final BusinessAccountRetriever businessAccountRetriever,
                       @Nonnull final CostServiceStub costServiceStub,
                       long realtimeTopologyContextId, ThinTargetCache thinTargetCache, CloudTypeMapper cloudTypeMapper) {
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.entityFilterMapper = entityFilterMapper;
        this.groupFilterMapper = groupFilterMapper;
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.businessAccountRetriever = Objects.requireNonNull(businessAccountRetriever);
        this.costServiceStub = Objects.requireNonNull(costServiceStub);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.thinTargetCache = thinTargetCache;
        this.cloudTypeMapper = cloudTypeMapper;
        final Map<SearchOrderBy, PaginatorSupplier> paginators = new EnumMap<>(SearchOrderBy.class);
        paginators.put(SearchOrderBy.COST, PaginatorCost::new);
        paginators.put(SearchOrderBy.SEVERITY, PaginatorSeverity::new);
        this.paginators = Collections.unmodifiableMap(paginators);
        this.defaultPaginator = PaginatorName::new;
    }

    /**
     * Converts an input API definition of a group represented as a {@link GroupApiDTO} object to a
     * {@link GroupDefinition} object.
     *
     * @param groupDto input API representation of a group object.
     * @return input object converted to a {@link GroupDefinition} object.
     * @throws ConversionException if the conversion fails.
     * @throws IllegalArgumentException if invalid group type.
     */
    public GroupDefinition toGroupDefinition(@Nonnull final GroupApiDTO groupDto) throws ConversionException, IllegalArgumentException {
        GroupDefinition.Builder groupBuilder = GroupDefinition.newBuilder()
                        .setDisplayName(groupDto.getDisplayName())
                        .setType(GroupType.REGULAR)
                        .setIsTemporary(Boolean.TRUE.equals(groupDto.getTemporary()));

        final GroupType nestedMemberGroupType = API_GROUP_TYPE_TO_GROUP_TYPE.get(groupDto.getGroupType());

        if (groupDto.getIsStatic()) {
            // for the case static group and static group of groups
            errorIfInvalidGroupType(nestedMemberGroupType, groupDto);


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
            errorIfInvalidGroupType(groupDto);

            final List<SearchParameters> searchParameters = entityFilterMapper
                            .convertToSearchParameters(groupDto.getCriteriaList(),
                                            groupDto.getGroupType(), null);

            groupBuilder.setEntityFilters(EntityFilters.newBuilder()
                .addEntityFilter(EntityFilter.newBuilder()
                    .setEntityType(ApiEntityType.fromString(groupDto.getGroupType()).typeNumber())
                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                        .addAllSearchParameters(searchParameters))
                    .setLogicalOperator(mapLogicalOperator(groupDto.getLogicalOperator())))
            );
        } else {

            // this means this a dynamic group of groups
            final GroupFilter groupFilter;
            try {
                groupFilter = groupFilterMapper.apiFilterToGroupFilter(nestedMemberGroupType,
                        mapLogicalOperator(groupDto.getLogicalOperator()),
                        groupDto.getCriteriaList());
            } catch (OperationFailedException e) {
                throw new ConversionException(
                        "Failed to apply filter criteria list for group " + groupDto.getUuid(), e);
            }
            groupBuilder.setGroupFilters(GroupFilters.newBuilder().addGroupFilter(groupFilter));
        }

        return groupBuilder.build();
    }

    /**
     * Check for group types and entity types.
     *
     * @param groupDto input API representation of a group object.
     * @param nestedMemberGroupType takes values from API_GROUP_TYPE_TO_GROUP_TYPE map or null
     * @throws IllegalArgumentException if invalid group type
     */
    private void errorIfInvalidGroupType(@Nullable GroupType nestedMemberGroupType, @Nonnull final GroupApiDTO groupDto) throws IllegalArgumentException {
        if (nestedMemberGroupType == null) {
            errorIfInvalidGroupType(groupDto);
        }
    }

    /**
     * Check only for known entity types.
     *
     * @param groupDto input API representation of a group object.
     * @throws IllegalArgumentException if invalid group type
     */
    private void errorIfInvalidGroupType(@Nonnull final GroupApiDTO groupDto) throws IllegalArgumentException {
        if (ApiEntityType.fromString(groupDto.getGroupType()) == ApiEntityType.UNKNOWN) {
            throw new IllegalArgumentException("Invalid Group Type");
        }
    }

    private LogicalOperator mapLogicalOperator(@Nullable final String logicalOperatorStr) {
        if (logicalOperatorStr == null) {
            return LogicalOperator.AND;
        }
        LogicalOperator ret = LOGICAL_OPERATOR_MAPPING.inverse().get(logicalOperatorStr);
        if (ret == null) {
            ret = LogicalOperator.AND;
            logger.warn("Invalid logical operator {}. Returning default: {}",
                logicalOperatorStr, ret);
        }
        return ret;
    }

    private String logicalOperatorToApiStr(@Nonnull final LogicalOperator logicalOperator) {
        String ret = LOGICAL_OPERATOR_MAPPING.get(logicalOperator);
        if (ret == null) {
            ret = "AND";
            // This most likely indicates a programming error, or we removed a previously-valid
            // enum from the map (which shouldn't happen).
            logger.error("Unhandled logical operator: {}. Returning default: {}", logicalOperator, ret);
        }
        return ret;
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

        final EnvironmentTypeEnum.EnvironmentType environmentType;
        final OptimizationMetadata optMetaData =
                getGroupOptimizationMetaData(groupAndMembers.group());
        if (optMetaData != null && optMetaData.hasEnvironmentType()) {
            environmentType = optMetaData.getEnvironmentType();
        } else if (envType != null) {
            environmentType = envType;
        } else {
            // Case for group of non-cloud entities.
            environmentType = EnvironmentTypeEnum.EnvironmentType.ON_PREM;
        }

        final CloudType cloudType;
        if (cloudTypes.size() == 1) {
            cloudType = cloudTypes.iterator().next();
        } else if (cloudTypes.size() > 1) {
            cloudType = CloudType.HYBRID;
        } else {
            cloudType = CloudType.UNKNOWN;
        }

        return new EntityEnvironment(
                EnvironmentTypeMapper.fromXLToApi(environmentType), cloudType);
    }

    /**
     * Returns a {@link OptimizationMetadata} from a given {@link Grouping} object.
     *
     * @param group The parsed {@link Grouping} object for a given {@link GroupAndMembers}.
     * @return A {@link OptimizationMetadata} if exists, otherwise null.
     */
    private OptimizationMetadata getGroupOptimizationMetaData(
                                        @Nullable final Grouping group) {
        if (group != null && group.hasDefinition()
                && group.getDefinition().hasOptimizationMetadata()) {
            return group.getDefinition().getOptimizationMetadata();
        } else {
            return null;
        }
    }

    /**
     * Converts an internal representation of a group represented as a {@link Grouping} object
     * to API representation of the object. Resulting groups are guaranteed to be in the same
     * size and order as source {@code group}.
     *
     * @param groups The internal representation of the object.
     * @param populateSeverity whether or not to populate severity of the group
     * @return the converted object.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    public Map<Long, GroupApiDTO> groupsToGroupApiDto(@Nonnull final Collection<Grouping> groups,
            boolean populateSeverity) throws ConversionException, InterruptedException {
        final List<GroupApiDTO> apiGroups;
        try {
            apiGroups =
                toGroupApiDto(new ArrayList<>(groups), populateSeverity, null, null).getObjects();
        } catch (InvalidOperationException e) {
            throw new ConversionException("Failed to convert groups "
                + groups.stream().map(Grouping::getId).collect(Collectors.toList()) + ": "
                + e.getLocalizedMessage(), e);
        }

        final Map<Long, GroupApiDTO> result = new LinkedHashMap<>(groups.size());
        for (GroupApiDTO apiGroup : apiGroups) {
            result.put(Long.parseLong(apiGroup.getUuid()), apiGroup);
        }
        return result;
    }

    @Nonnull
    private PaginatorSupplier getPaginator(@Nullable SearchPaginationRequest paginationRequest) {
        if (paginationRequest != null) {
            final PaginatorSupplier paginator = paginators.get(paginationRequest.getOrderBy());
            if (paginator != null) {
                return paginator;
            }
        }
        return defaultPaginator;
    }

    /**
     * Converts groups and members to API objects.
     *
     * @param groups source groups to convert
     * @param populateSeverity whether to calculate and set severity values.
     * @param paginationRequest pagination request, if any
     * @param requestedEnvironment requested environment type, if any
     * @return resulting groups. This list could be shorter then incoming list as some of the groups
     *         could be filtered out
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if pagination request is invalid
     */
    @Nonnull
    public ObjectsPage<GroupApiDTO> toGroupApiDto(@Nonnull List<Grouping> groups,
            boolean populateSeverity, @Nullable SearchPaginationRequest paginationRequest,
            @Nullable EnvironmentType requestedEnvironment)
            throws ConversionException, InterruptedException, InvalidOperationException {
        final PaginatorSupplier paginatorSupplier = getPaginator(paginationRequest);
        final ObjectsPage<GroupAndMembers> groupsPage;
        final GroupConversionContext context;
        try {
            final AbstractPaginator paginator =
                    paginatorSupplier.createPaginator(groups, paginationRequest,
                            requestedEnvironment);
            if (paginator.isEmptyResult()) {
                return ObjectsPage.createEmptyPage();
            }
            groupsPage = paginator.getGroupsPage();
            context = paginator.createContext();
        } catch (ExecutionException | TimeoutException e) {
            throw new ConversionException("Failed to convert groups " +
                groups.stream().map(Grouping::getId), e);
        }
        final List<GroupApiDTO> result = new ArrayList<>(groups.size());
        for (GroupAndMembers group : groupsPage.getObjects()) {
            try {
                result.add(toGroupApiDto(group, context, populateSeverity));
            } catch (ConversionException | RuntimeException e) {
                // We log the error, but we continue converting other groups or else we will
                // return no results when a single group has an error.
                logger.error("Failed to convert group " +
                    group.group().getId()  + " (name: " + group.group().getDefinition().getDisplayName() + ")", e);
            }
        }
        return new ObjectsPage<>(result, groupsPage.getTotalCount(), groupsPage.getNextCursor());
    }

    @Nonnull
    private Future<Map<Long, EntityEnvironment>> getGroupEnvironments(
            @Nonnull Collection<GroupAndMembers> groups, @Nonnull Set<Long> members) {
        final EnvironmentMapObserver observer = new EnvironmentMapObserver(groups);
        repositoryApi.entitiesRequest(members).getMinimalEntities(observer);
        return observer.getFuture();
    }

    /**
     * Converts from {@link GroupAndMembers} to {@link GroupApiDTO}.
     *
     * @param groupAndMembers The {@link GroupAndMembers} object (get it from {@link GroupExpander})
     *                        describing the XL group and its members.
     * @param conversionContext group conversion context
     * @param populateSeverity whether or not to populate severity of the group
     * @return The {@link GroupApiDTO} object.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    private GroupApiDTO toGroupApiDto(@Nonnull final GroupAndMembers groupAndMembers,
                                      @Nonnull GroupConversionContext conversionContext, boolean populateSeverity)
        throws ConversionException, InterruptedException {

        final EntityEnvironment envCloudType =
                conversionContext.getEntityEnvironment(groupAndMembers.group().getId());
        final GroupApiDTO outputDTO;
        final Grouping group = groupAndMembers.group();
        outputDTO = convertToGroupApiDto(groupAndMembers, conversionContext);

        outputDTO.setDisplayName(group.getDefinition().getDisplayName());
        outputDTO.setUuid(Long.toString(group.getId()));

        outputDTO.setEnvironmentType(getEnvironmentTypeForTempGroup(envCloudType.getEnvironmentType()));
        outputDTO.setActiveEntitiesCount(
                getActiveEntitiesCount(groupAndMembers, conversionContext.getActiveEntities()));

        final Float cost = conversionContext.getGroupCosts().get(groupAndMembers.group().getId());
        if (cost != null) {
            outputDTO.setCostPrice(cost);
        }
        // only populate severity if required and if the group is not empty, since it's expensive
        if (populateSeverity && !groupAndMembers.entities().isEmpty()) {
            outputDTO.setSeverity(conversionContext.getSeverityMap()
                .calculateSeverity(groupAndMembers.entities())
                .name());
        }
        outputDTO.setCloudType(envCloudType.getCloudType());
        return outputDTO;
    }

    private Set<Long> getMembersConsideredInMembersCount(GroupAndMembers groupAndMembers) {
        if (groupAndMembers.group().getDefinition().getType() == GroupType.RESOURCE) {
            return groupAndMembers.group()
                .getDefinition()
                .getStaticGroupMembers()
                .getMembersByTypeList()
                .stream()
                .filter(membersByType -> membersByType.getType().hasEntity()
                    && WORKLOAD_ENTITY_TYPES.contains(
                        ApiEntityType.fromType(membersByType.getType().getEntity())))
                .map(StaticMembersByType::getMembersList)
                .flatMap(List::stream)
                .collect(Collectors.toSet());

        } else {
            return Sets.newHashSet(groupAndMembers.members());
        }
    }

    /**
     * Returns a map from BusinessAccount OID -> BusinessAccount name.
     *
     * @param groupsAndMembers groups to query onwers for
     * @return map future
     */
    @Nonnull
    private Future<Map<Long, String>> getOwnerDisplayNames(
            @Nonnull Collection<GroupAndMembers> groupsAndMembers) {
        final Set<Long> ownerIds = groupsAndMembers.stream()
                .map(GroupAndMembers::group)
                .map(Grouping::getDefinition)
                .filter(def -> def.getType() == GroupType.RESOURCE)
                .filter(GroupDefinition::hasOwner)
                .map(GroupDefinition::getOwner)
                .collect(Collectors.toSet());
        if (ownerIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        final MembersDisplayNamesObserver observer = new MembersDisplayNamesObserver();
        repositoryApi.entitiesRequest(ownerIds).getMinimalEntities(observer);
        return observer.getFuture();
    }

    /**
     * Converts an internal representation of a group represented as a {@link Grouping} object
     * to API representation of the object.
     *
     * @param groupAndMembers The internal representation of the object.
     * @param conversionContext group conversion context
     * @return the converted object with some of the details filled in.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    private GroupApiDTO convertToGroupApiDto(@Nonnull final GroupAndMembers groupAndMembers,
            @Nonnull GroupConversionContext conversionContext)
            throws ConversionException, InterruptedException {
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

         List<String> directMemberTypes = GroupProtoUtil.getDirectMemberTypes(groupDefinition)
                 .map(GroupMapper::convertMemberTypeToApiType)
                 .filter(StringUtils::isNotEmpty)
                 .collect(Collectors.toList());

         if (!directMemberTypes.isEmpty()) {
             if (groupDefinition.getType() == GroupType.RESOURCE) {
                 outputDTO.setGroupType(StringConstants.WORKLOAD);
             } else {
                 outputDTO.setGroupType(String.join(",", directMemberTypes));
             }
         } else {
             outputDTO.setGroupType(ApiEntityType.UNKNOWN.apiStr());
         }

         outputDTO.setMemberTypes(directMemberTypes);
         outputDTO.setEntityTypes(group.getExpectedTypesList()
                        .stream()
                        .filter(MemberType::hasEntity)
                        .map(MemberType::getEntity)
                        .map(ApiEntityType::fromType)
                        .map(ApiEntityType::apiStr)
                        .collect(Collectors.toList()));

         outputDTO.setIsStatic(groupDefinition.hasStaticGroupMembers());
         outputDTO.setTemporary(groupDefinition.getIsTemporary());

         switch (groupDefinition.getSelectionCriteriaCase()) {
             case STATIC_GROUP_MEMBERS:
                 if (outputDTO.getMemberUuidList().isEmpty()) {
                     List<Long> groupMembers = GroupProtoUtil.getStaticMembers(group);
                     Set<Long> groupValidEntities;
                     Set<Long> validEntities = conversionContext.getValidEntities();
                     final int missingEntitiesCount;
                     if (GroupProtoUtil.isNestedGroup(groupAndMembers.group())) {
                         // Retain the valid entities of the group.
                         groupValidEntities = Sets.newHashSet(groupAndMembers.entities());
                         groupValidEntities.retainAll(validEntities);
                         missingEntitiesCount =
                                 groupAndMembers.entities().size() - groupValidEntities.size();
                         if (missingEntitiesCount > 0) {
                             logger.warn("{} entities for static group {} not found in repository.",
                                     missingEntitiesCount, groupDefinition.getDisplayName());
                         }
                         // TODO: In case of nested group, we should also only return the valid
                         //       direct members of the group like we only return its valid entities
                         //       See OM-60187
                         outputDTO.setMemberUuidList(groupMembers
                                 .stream()
                                 .map(String::valueOf)
                                 .collect(Collectors.toList()));
                     } else {
                         // Retain valid entities of group members/entities.
                         groupValidEntities = Sets.newHashSet(groupMembers);
                         groupValidEntities.retainAll(validEntities);
                         missingEntitiesCount = groupMembers.size() - groupValidEntities.size();
                         if (missingEntitiesCount > 0) {
                             logger.warn("{} members for static group {} not found in repository.",
                                     missingEntitiesCount, groupDefinition.getDisplayName());
                         }
                         outputDTO.setMemberUuidList(groupValidEntities
                                 .stream()
                                 .map(String::valueOf)
                                 .collect(Collectors.toList()));

                         Set<Long> membersConsideredInMembersCount = getMembersConsideredInMembersCount(groupAndMembers);
                         membersConsideredInMembersCount.retainAll(validEntities);
                         outputDTO.setMembersCount(membersConsideredInMembersCount.size());
                     }
                     outputDTO.setEntitiesCount(groupValidEntities.size());
                 }
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

                 final EntityFilter entityFilter = groupDefinition.getEntityFilters().getEntityFilter(0);
                 outputDTO.setLogicalOperator(logicalOperatorToApiStr(entityFilter.getLogicalOperator()));
                 outputDTO.setCriteriaList(entityFilterMapper.convertToFilterApis(entityFilter));
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

                 final GroupFilter groupFilter = groupDefinition.getGroupFilters().getGroupFilter(0);
                 outputDTO.setLogicalOperator(logicalOperatorToApiStr(groupFilter.getLogicalOperator()));
                 outputDTO.setCriteriaList(groupFilterMapper.groupFilterToApiFilters(groupFilter));
                 break;
             default:
                 break;
         }

         // BillingFamily has custom code for determining the number of members. An undiscovered
         // account is not considered a member in classic.
         if (outputDTO.getMembersCount() == null) {
             outputDTO.setMembersCount(getMembersConsideredInMembersCount(groupAndMembers).size());
         }
        if (outputDTO.getMemberUuidList().isEmpty()) {
             outputDTO.setMemberUuidList(groupAndMembers.members().stream()
                     .map(oid -> Long.toString(oid))
                     .collect(Collectors.toList()));
         }
         if (outputDTO.getEntitiesCount() == null) {
             outputDTO.setEntitiesCount(groupAndMembers.entities().size());
         }

        if (group.hasOrigin() && group.getOrigin().hasDiscovered()) {
            ThinTargetInfo source = null;
            for (long targetId : group.getOrigin().getDiscovered().getDiscoveringTargetIdList()) {
                Optional<ThinTargetInfo> thinTargetInfo = thinTargetCache.getTargetInfo(targetId);
                if (thinTargetInfo.isPresent()) {
                    source = chooseTarget(source, thinTargetInfo.get());
                }
            }
            if (source != null) {
                outputDTO.setSource(createTargetApiDto(source));
            } else {
                logger.debug("Failed to locate source target information for {}", outputDTO.getUuid());
            }
        }
         return outputDTO;
    }

    /**
     * Chooses if we must replace the currently selected target for return.
     * The logic is that non-hidden targets have priority over hidden targets, and in the case that
     * there are only targets with the same visibility then the one with the lowest oid will be
     * returned. In most cases there will be only 1 target, so only the first case will be used.
     * TODO: this is a **convention** used as a temporary workaround to ensure consistency in the
     * case of multiple targets; it should be removed when logic for handling multiple targets is
     * added, see OM-59547.
     * @param current the currently selected target
     * @param candidate the candidate target to be selected
     * @return whether the current target should be replaced.
     */
    private ThinTargetInfo chooseTarget(ThinTargetInfo current, ThinTargetInfo candidate) {
        if (current == null) {
            return  candidate;
        } else if (current.isHidden() && !candidate.isHidden()) {
            return candidate;
        } else if (!current.isHidden() && candidate.isHidden()) {
            return current;
        }
        // same visibility
        return candidate.oid() < current.oid() ? candidate : current;
    }

    /**
     * Creates a {@link TargetApiDTO} and populated its basic fields using the values from
     * thinTargetInfo.
     *
     * @param thinTargetInfo the {@link ThinTargetInfo} object used as input
     * @return the newly created TargetApiDTO
     */
    @Nonnull
    private TargetApiDTO createTargetApiDto(@Nonnull final ThinTargetInfo thinTargetInfo) {
        final TargetApiDTO apiDTO = new TargetApiDTO();
        apiDTO.setType(thinTargetInfo.probeInfo().type());
        apiDTO.setUuid(Long.toString(thinTargetInfo.oid()));
        apiDTO.setDisplayName(thinTargetInfo.displayName());
        apiDTO.setCategory(thinTargetInfo.probeInfo().category());
        return apiDTO;
    }

    private BillingFamilyApiDTO extractBillingFamilyInfo(GroupAndMembers groupAndMembers)
            throws ConversionException, InterruptedException {
        BillingFamilyApiDTO billingFamilyApiDTO = new BillingFamilyApiDTO();
        Set<Long> oidsToQuery = new HashSet<>(groupAndMembers.members());
        List<BusinessUnitApiDTO> businessUnitApiDTOList = new ArrayList<>();
        Map<String, String> uuidToDisplayNameMap = new HashMap<>();
        float cost = 0f;
        boolean hasCost = false;
        List<String> discoveredAccountUuids = Lists.newArrayList();

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

            if (businessUnit.isMaster()) {
                billingFamilyApiDTO.setMasterAccountUuid(businessUnit.getUuid());
            }

            businessUnitApiDTOList.add(businessUnit);

            // OM-53266: Member count should only consider accounts that are monitored by a probe.
            // Accounts that are only submitted as a member of a BillingFamily should not be counted.
            if (businessUnit.getAssociatedTargetId() != null) {
                discoveredAccountUuids.add(businessUnit.getUuid());
            }
        }

        if (hasCost) {
            billingFamilyApiDTO.setCostPrice(cost);
        }
        billingFamilyApiDTO.setUuidToNameMap(uuidToDisplayNameMap);
        billingFamilyApiDTO.setBusinessUnitApiDTOList(businessUnitApiDTOList);
        billingFamilyApiDTO.setMembersCount(discoveredAccountUuids.size());
        billingFamilyApiDTO.setEntitiesCount(discoveredAccountUuids.size());
        billingFamilyApiDTO.setMemberUuidList(discoveredAccountUuids);

        return billingFamilyApiDTO;
    }

    @Nonnull
    private Set<Long> getGroupMembersAsLong(GroupApiDTO groupDto) {
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
            @Nonnull final GroupApiDTO groupDto, @Nonnull Set<Long> memberUuids)
            throws ConversionException {
        final MemberType memberType;
        if (groupType != null) {
            // if this is group of groups
            memberType = MemberType.newBuilder().setGroup(groupType).build();
        } else {
            // otherwise it is assumed this a group of entities
            memberType = MemberType.newBuilder().setEntity(
                            ApiEntityType.fromString(groupDto.getGroupType()).typeNumber()
                        ).build();
        }

        final Set<Long> groupMembers;

        if (!CollectionUtils.isEmpty(groupDto.getScope())) {
            // Derive the members from the scope
            final Map<String, SupplyChainNode> supplyChainForScope;
            try {
                supplyChainForScope = supplyChainFetcherFactory.newNodeFetcher()
                        .addSeedUuids(groupDto.getScope())
                        .entityTypes(Collections.singletonList(groupDto.getGroupType()))
                        .apiEnvironmentType(groupDto.getEnvironmentType())
                        .fetch();
            } catch (OperationFailedException e) {
                throw new ConversionException(
                        "Failed to get static members for group " + groupDto.getUuid(), e);
            }
            final SupplyChainNode node = supplyChainForScope.getOrDefault(groupDto.getGroupType(),
                SupplyChainNode.getDefaultInstance());
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

    @Nullable
    private static String convertMemberTypeToApiType(@Nonnull MemberType memberType) {
        switch (memberType.getTypeCase()) {
            case ENTITY:
                return ApiEntityType.fromType(memberType.getEntity()).apiStr();
            case GROUP:
                return convertGroupTypeToApiType(memberType.getGroup());
            default:
                logger.error("The member type is `{}` is unknown to API.", memberType);
                return null;
        }
    }

    @Nonnull
    public static String convertGroupTypeToApiType(@Nonnull GroupType type) {
        return API_GROUP_TYPE_TO_GROUP_TYPE.inverse().getOrDefault(type, type.name());
    }

    /**
     * Create {@link BaseApiDTO} with uuid, display name, class name fields from {@link Grouping}.
     *
     * @param grouping the {@link Grouping}
     * @return the {@link BaseApiDTO}
     */
    @Nonnull
    public static BaseApiDTO toBaseApiDTO(@Nonnull final Grouping grouping) {
        final BaseApiDTO baseApiDTO = new BaseApiDTO();
        baseApiDTO.setUuid(String.valueOf(grouping.getId()));
        final GroupDTO.GroupDefinition groupDefinition = grouping.getDefinition();
        baseApiDTO.setDisplayName(groupDefinition.getDisplayName());
        baseApiDTO.setClassName(GroupMapper.convertGroupTypeToApiType(groupDefinition.getType()));
        return baseApiDTO;
    }

    /**
     * Returns mapping from oid to entity in repository, based on the specified OIDs.
     *
     * @param oids oids.
     * @return a subset of {@code members} containing only valid entities in repository.
     */
    @Nonnull
    private Future<Map<Long, MinimalEntity>> getMinimalEntities(@Nonnull Set<Long> oids) {
        if (oids.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        final MinimalEntitiesObserver observer = new MinimalEntitiesObserver();
        repositoryApi.entitiesRequest(oids).getMinimalEntities(observer);
        return observer.getFuture();
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
        private final Set<Long> validEntities;
        private final Set<Long> activeEntities;
        private final Map<Long, String> ownerDisplayName;
        private final Map<Long, EntityEnvironment> entities;
        private final EntityEnvironment entityEnvironment;
        private final SeverityMap severityMap;
        private final Map<Long, Float> groupCosts;

        /**
         * Errors encountered during construction of the context.
         * TODO (roman, March 19 2020) OM-56615: Propagate to the user to notify them that the
         * group we are returning from the API may not be complete.
         */
        private final Map<String, Exception> retrievalErrors;

        GroupConversionContext(@Nonnull Set<Long> validEntities,
                @Nonnull Set<Long> activeEntities,
                @Nonnull Map<Long, String> ownerDisplayName,
                @Nullable Map<Long, EntityEnvironment> entities,
                @Nullable EntityEnvironment entityEnvironment,
                @Nonnull SeverityMap severityMap,
                @Nonnull Map<Long, Float> groupCosts,
                @Nonnull final Map<String, Exception> retrievalErrors) {
            this.validEntities = validEntities;
            this.activeEntities = activeEntities;
            this.ownerDisplayName = ownerDisplayName;
            this.entities = entities;
            this.entityEnvironment = entityEnvironment;
            this.retrievalErrors = retrievalErrors;
            this.severityMap = Objects.requireNonNull(severityMap);
            this.groupCosts = Objects.requireNonNull(groupCosts);
            if ((entities == null) == (entityEnvironment == null)) {
                throw new IllegalArgumentException(
                        "Either entities or entity environment is expected to be set for groups " +
                                groupCosts.keySet());
            }
        }

        /**
         * Returns a set of valid entities OIDs.
         *
         * @return a set of OIDs
         */
        @Nonnull
        public Set<Long> getValidEntities() {
            return validEntities;
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
         * Get any errors that were encountered when gathering data for this context.
         * This can be used to notify the user that some parts of the group's information may be missing.
         *
         * @return The map of error description -> {@link Exception} that caused the error.
         */
        @Nonnull
        public Map<String, Exception> getRetrievalErrors() {
            return retrievalErrors;
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
         * Returns global entity environment. If this value is set, this means, that all the
         * entities in the environment have the same environment and cloud types.
         *
         * @param groupOid OID of the group to return environment of
         * @return global entity environment
         */
        @Nonnull
        public EntityEnvironment getEntityEnvironment(long groupOid) {
            if (entityEnvironment != null) {
                return entityEnvironment;
            } else {
                // According to the code, every group will have a record in this map. If the value
                // is absent - it is a error in the code.
                return Objects.requireNonNull(entities.get(groupOid),
                        "entity environment has not been calculated for group " + groupOid);
            }
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

        /**
         * Returns a map of group costs.
         *
         * @return map group OID -> cost (if any)
         */
        @Nonnull
        public Map<Long, Float> getGroupCosts() {
            return groupCosts;
        }
    }

    /**
     * Observer collecting minimal entities into a map, mapping from oid to MinimalEntity.
     */
    private static class MinimalEntitiesObserver extends
            FutureObserver<MinimalEntity, Map<Long, MinimalEntity>> {
        private final Map<Long, MinimalEntity> result = new HashMap<>();

        @Nonnull
        @Override
        protected Map<Long, MinimalEntity> createResult() {
            return Collections.unmodifiableMap(result);
        }

        @Override
        public void onNext(MinimalEntity value) {
            result.put(value.getOid(), value);
        }
    }

    /**
     * Observer collecting minimal entities into a map from OID to entity.
     */
    private static class MembersDisplayNamesObserver extends
            FutureObserver<MinimalEntity, Map<Long, String>> {
        private final Map<Long, String> result = new HashMap<>();

        @Nonnull
        @Override
        protected Map<Long, String> createResult() {
            return Collections.unmodifiableMap(result);
        }

        @Override
        public void onNext(MinimalEntity value) {
            result.put(value.getOid(), value.getDisplayName());
        }
    }

    /**
     * Observer collecting group environment types.
     */
    private class EnvironmentMapObserver extends
            FutureObserver<MinimalEntity, Map<Long, EntityEnvironment>> {
        private final Map<Long, MinimalEntity> entities = new HashMap<>();
        private final Collection<GroupAndMembers> groups;

        EnvironmentMapObserver(@Nonnull Collection<GroupAndMembers> groups) {
            this.groups = Objects.requireNonNull(groups);
        }

        @Nonnull
        @Override
        protected Map<Long, EntityEnvironment> createResult() {
            final Map<Long, EntityEnvironment> entityEnvironmentMap = new HashMap<>(groups.size());
            for (GroupAndMembers group : groups) {
                entityEnvironmentMap.put(group.group().getId(),
                        getEnvironmentAndCloudTypeForGroup(group, entities));
            }
            return Collections.unmodifiableMap(entityEnvironmentMap);
        }

        @Override
        public void onNext(MinimalEntity value) {
            entities.put(value.getOid(), value);
        }
    }

    /**
     * Stream observer to collect groups cost information. It collects cost summarized by groups.
     */
    private class GroupsCostObserver extends
            FutureObserver<GetCloudCostStatsResponse, Map<Long, Float>> {
        private final Map<Long, Float> vmCosts = new HashMap<>();
        private final Collection<GroupAndMembers> groupsAndMembers;

        GroupsCostObserver(@Nonnull Collection<GroupAndMembers> groupsAndMembers) {
            this.groupsAndMembers = Objects.requireNonNull(groupsAndMembers);
        }

        @Nonnull
        @Override
        protected Map<Long, Float> createResult() {
            final Map<Long, Float> groupsCost = new HashMap<>(groupsAndMembers.size());
            for (GroupAndMembers groupAndMembers: groupsAndMembers) {
                final long oid = groupAndMembers.group().getId();
                boolean hasAnyCost = false;
                float cost = 0;
                for (Long member: groupAndMembers.entities()) {
                    Float nextCost = vmCosts.get(member);
                    if (nextCost != null) {
                        cost += nextCost;
                        hasAnyCost = true;
                    }
                }
                if (hasAnyCost) {
                    groupsCost.put(oid, cost);
                }
            }
            return Collections.unmodifiableMap(groupsCost);
        }

        @Override
        public void onNext(GetCloudCostStatsResponse cloudCostStatsResponse) {
            for (CloudCostStatRecord record : cloudCostStatsResponse.getCloudStatRecordList()) {
                for (StatRecord statRecord : record.getStatRecordsList()) {
                    // exclude cost with category STORAGE for VMs, because this cost is duplicate STORAGE cost for volumes
                    if (!(statRecord.getCategory() == CostCategory.STORAGE) &&
                            (statRecord.getAssociatedEntityId() ==
                                    EntityType.VIRTUAL_MACHINE_VALUE)) {
                        final long oid = statRecord.getAssociatedEntityId();
                        final float cost = statRecord.getValues().getTotal();
                        vmCosts.put(oid, vmCosts.getOrDefault(oid, 0F) + cost);
                    }
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            if (Optional.of(Code.UNAVAILABLE).equals(getStatus(t).map(Status::getCode))) {
                // Any component may be down at any time. APIs like search should not fail
                // when the cost component is down. We must log a warning when this happens,
                // or else it will be difficult for someone to explain why search does not
                // have cost data.
                logger.warn("The cost component is not available. As a result, we will not fill in the response with cost details for groups");
                super.onCompleted();
            } else {
                // Cost component responded, so it's up an running. We need to make this
                // exception visible because there might be a bug in the cost component.
                super.onError(t);
            }
        }
    }

    private static int getSkipCount(@Nonnull SearchPaginationRequest paginationRequest)
            throws InvalidOperationException {
        final int skipCount;
        if (paginationRequest.getCursor().isPresent()) {
            try {
                skipCount = Integer.parseInt(paginationRequest.getCursor().get());
                if (skipCount < 0) {
                    throw new InvalidOperationException(
                            "Illegal cursor: " + skipCount + ". Must be be a positive integer");
                }
            } catch (InvalidOperationException e) {
                throw new InvalidOperationException("Cursor " + paginationRequest.getCursor() +
                        " is invalid. Should be a number.");
            }
        } else {
            skipCount = 0;
        }
        return skipCount;
    }

    @Nonnull
    private ObjectsPage<GroupAndMembers> applyPagination(@Nonnull List<GroupAndMembers> groups,
                                                         @Nonnull Comparator<GroupAndMembers> comparator,
                                                         @Nullable SearchPaginationRequest paginationRequest,
                                                         @Nullable Predicate<Grouping> groupFilter) throws InvalidOperationException {
        if (paginationRequest == null && groupFilter == null) {
            return new ObjectsPage<>(groups, groups.size(), groups.size());
        }
        final int skipCount = paginationRequest == null ? 0 : getSkipCount(paginationRequest);
        final int limit = paginationRequest == null ? groups.size() : paginationRequest.getLimit();
        final List<GroupAndMembers> filteredGroups;

        if (groupFilter != null) {
            filteredGroups = groups.stream()
                .filter(groupAndMembers -> groupFilter.test(groupAndMembers.group()))
                .collect(Collectors.toList());
        } else {
            filteredGroups = groups;
        }
        final List<GroupAndMembers> groupsPage;
        if (paginationRequest != null) {
            final Comparator<GroupAndMembers> effectiveComparator =
                paginationRequest.isAscending() ? comparator : Collections.reverseOrder(comparator);
            final List<GroupAndMembers> sortedGroups = new ArrayList<>(filteredGroups);
            sortedGroups.sort(effectiveComparator);
            groupsPage = sortedGroups.subList(getSkipCount(paginationRequest),
                Math.min(limit + skipCount, sortedGroups.size()));
        } else {
            groupsPage = filteredGroups;
        }
        final int nextCursor = Math.min(filteredGroups.size(), skipCount + limit);
        return new ObjectsPage<>(groupsPage, filteredGroups.size(), nextCursor);
    }

    @Nonnull
    private ObjectsPage<GroupAndMembers> applyPaginationWithoutMembers(@Nonnull List<Grouping> groups,
                                                         @Nonnull Comparator<Grouping> comparator,
                                                         @Nullable SearchPaginationRequest paginationRequest,
                                                         @Nullable Predicate<Grouping> groupFilter) throws InvalidOperationException {
        // This is an expensive operation, we should try to pass pagination parameters as much as
        // possible
        if (paginationRequest == null && groupFilter == null) {
            List<GroupAndMembers> groupAndMembers = groupExpander.getMembersForGroups(groups);
            return new ObjectsPage<>(groupAndMembers, groups.size(), groups.size());
        }
        final int skipCount = paginationRequest == null ? 0 : getSkipCount(paginationRequest);
        final int limit = paginationRequest == null ? groups.size() : paginationRequest.getLimit();
        final List<Grouping> filteredGroups;
        if (groupFilter != null) {
            filteredGroups = groups.stream().filter(groupFilter).collect(Collectors.toList());
        } else {
            filteredGroups = groups;
        }
        final List<Grouping> paginatedGroups;
        if (paginationRequest != null) {
            final Comparator<Grouping> effectiveComparator =
                paginationRequest.isAscending() ? comparator : Collections.reverseOrder(comparator);
            final List<Grouping> sortedGroups = new ArrayList<>(filteredGroups);
            sortedGroups.sort(effectiveComparator);
            paginatedGroups = sortedGroups.subList(getSkipCount(paginationRequest),
                Math.min(limit + skipCount, sortedGroups.size()));
        } else {
            paginatedGroups = filteredGroups;
        }
        final int nextCursor = Math.min(filteredGroups.size(), skipCount + limit);
        List<GroupAndMembers> groupAndMembers = groupExpander.getMembersForGroups(paginatedGroups);
        return new ObjectsPage<>(groupAndMembers, filteredGroups.size(), nextCursor);
    }

    @Nonnull
    private Future<SeverityMap> fetchSeverityMap(@Nonnull Set<Long> members) {
        return severityPopulator.getSeverityMap(realtimeTopologyContextId, members);
    }

    @Nonnull
    private Future<Map<Long, Float>> fetchGroupCosts(
            @Nonnull Collection<GroupAndMembers> groupsAndMembers, @Nonnull Set<Long> members) {
        if (members.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        final GroupsCostObserver observer = new GroupsCostObserver(groupsAndMembers);
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setEntityFilter(
                                Cost.EntityFilter.newBuilder().addAllEntityId(members).build())
                        .build())
                .build();
        costServiceStub.getCloudCostStats(request, observer);
        return observer.getFuture();
    }

    /**
     * Abstract paginator holds default behaviour for various paginators.
     */
    private abstract class AbstractPaginator {

        private final EntityEnvironment globalEnvironment;
        private final Future<Map<Long, EntityEnvironment>> groupEnvironments;
        private final EnvironmentType requestedEnvironmentType;
        private Set<Long> allMembers = null;
        private final List<Grouping> groups;
        private List<GroupAndMembers> groupAndMembers = null;
        private final boolean emptyResult;

        protected AbstractPaginator(@Nonnull List<Grouping> groups,
                @Nullable EnvironmentType requestedEnvironmentType) {
            this.groups = groups;
            globalEnvironment = getApplianceEnvironment();
            if ((requestedEnvironmentType == null) ||
                    (requestedEnvironmentType == EnvironmentType.HYBRID)) {
                // If no filtering by environment type is selected
                // of if hybrid environment is selected - we do not apply environment filters
                this.requestedEnvironmentType = null;
                this.groupEnvironments = null;
                this.emptyResult = false;
            } else if (globalEnvironment != null) {
                // If global environment is homogeneous we either return nothing or do not
                // apply filters
                this.emptyResult =
                        globalEnvironment.getEnvironmentType() != requestedEnvironmentType;
                this.requestedEnvironmentType = null;
                this.groupEnvironments = null;
            } else {
                // We still need to filter by environment type
                groupAndMembers = groupExpander.getMembersForGroups(groups);
                allMembers = getMembersFromGroups(groupAndMembers);
                this.emptyResult = false;
                this.groupEnvironments = getGroupsEnvironment(groupAndMembers, allMembers);
                this.requestedEnvironmentType = requestedEnvironmentType;
            }
        }

        @Nullable
        protected final Predicate<Grouping> getFilter()
                throws ExecutionException, InterruptedException, TimeoutException {
            if (requestedEnvironmentType == null) {
                return null;
            }
            final Map<Long, EntityEnvironment> groupsEnvironment =
                    groupEnvironments.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS);
            return group -> groupEnvironmentMatchesRequested(
                    groupsEnvironment.get(group.getId()).getEnvironmentType(),
                    requestedEnvironmentType);
        }

        private boolean groupEnvironmentMatchesRequested(@Nonnull EnvironmentType groupEnv,
                @Nullable EnvironmentType requestedEnvironmentType) {
            return groupEnv == EnvironmentType.HYBRID || groupEnv.equals(requestedEnvironmentType);
        }

        @Nonnull
        protected final Future<Map<Long, EntityEnvironment>> getGroupsEnvironment(
                @Nonnull Collection<GroupAndMembers> groups, @Nonnull Set<Long> members) {
            if (groupEnvironments != null) {
                return groupEnvironments;
            } else {
                return GroupMapper.this.getGroupEnvironments(groups, members);
            }
        }

        public Set<Long> getAllMembers() {
            if (allMembers == null) {
                getGroupAndMembers();
            }
            return allMembers;
        }

        public List<GroupAndMembers> getGroupAndMembers() {
            if (groupAndMembers == null) {
                groupAndMembers = groupExpander.getMembersForGroups(groups);
                allMembers = getMembersFromGroups(groupAndMembers);
            }
            return groupAndMembers;
        }

        @Nonnull
        public abstract ObjectsPage<GroupAndMembers> getGroupsPage();

        @Nonnull
        public GroupConversionContext createContext()
                throws InterruptedException, TimeoutException, ExecutionException {
            final ObjectsPage<GroupAndMembers> groupsPage = getGroupsPage();
            final List<GroupAndMembers> groupsAndMembers = groupsPage.getObjects();
            final Set<Long> members = groupsAndMembers.stream()
                    .map(GroupAndMembers::entities)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            final EntityEnvironment entityEnvironment = getApplianceEnvironment();
            final Future<Map<Long, MinimalEntity>> entitiesFuture = getMinimalEntities(members);
            final Future<Map<Long, String>> ownerEntitiesFuture = getOwnerDisplayNames(groupsAndMembers);
            final Future<SeverityMap> severityMapFuture = getSeverityMap(members);
            final Future<Map<Long, Float>> groupCostsFuture = getCosts(groupsAndMembers, members);

            final GroupConversionContext context;

            // Collect the errors we encounter so we can print all of them.
            final Map<String, Exception> errors = new HashMap<>();
            // We may encounter errors retrieving some of the related information. For example, if
            // the action orchestrator is down we will not be able to get severity information. We
            // try to get as much as we can, and prevent a failure in a single component from
            // stopping ANY group results from being displayed.

            // Wait for the entities.
            Map<Long, MinimalEntity> entitiesFromRepository;
            Set<Long> validEntities;
            Set<Long> activeEntities;
            try {
                entitiesFromRepository = Objects.requireNonNull(entitiesFuture.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS));
                validEntities = Sets.newHashSet(entitiesFromRepository.keySet());
                activeEntities = entitiesFromRepository.values().stream()
                        .filter(e -> e.getEntityState() == EntityState.POWERED_ON)
                        .map(MinimalEntity::getOid).collect(Collectors.toSet());
            } catch (ExecutionException | TimeoutException e) {
                errors.put("Failed to get entities from repository. Error: " + e.getMessage(), e);
                entitiesFromRepository = Collections.emptyMap();
                // fallback values for valid/active entities
                validEntities = Sets.newHashSet(members);
                activeEntities = Sets.newHashSet(members);
            }

            Map<Long, EntityEnvironment> groupEnvironments = null;
            if (entityEnvironment == null) {
                groupEnvironments = new HashMap<>();
                for (GroupAndMembers group : groupsAndMembers) {
                    groupEnvironments.put(group.group().getId(),
                            getEnvironmentAndCloudTypeForGroup(group, entitiesFromRepository));
                }
            }

            // Wait for the owner display names.
            Map<Long, String> ownerDisplayName;
            try {
                ownerDisplayName = ownerEntitiesFuture.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                errors.put("Failed to get owner display name. Error: " + e.getMessage(), e);
                ownerDisplayName = Collections.emptyMap();
            }

            // Wait for the severity map.
            SeverityMap severityMap;
            try {
                severityMap = Objects.requireNonNull(severityMapFuture.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS));
            } catch (ExecutionException | TimeoutException e) {
                errors.put("Failed to get severity information. Error: " + e.getMessage(), e);
                severityMap = SeverityMapImpl.empty();
            }

            // Wait for the group costs.
            Map<Long, Float> groupCosts;
            try {
                groupCosts = Objects.requireNonNull(groupCostsFuture.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS));
            } catch (ExecutionException | TimeoutException e) {
                errors.put("Failed to get cost information. Error: " + e.getMessage(), e);
                groupCosts = Collections.emptyMap();
            }

            if (!errors.isEmpty()) {
                logger.error("Errors constructing group conversion context." +
                    " Some group field may be empty. Turn on debug logging for stack traces. Errors: {}", errors.keySet());
                if (logger.isDebugEnabled()) {
                    errors.forEach(logger::debug);
                }
            }
            context = new GroupConversionContext(validEntities, activeEntities, ownerDisplayName, groupEnvironments,
                    entityEnvironment, severityMap, groupCosts, errors);
            return context;
        }

        @Nonnull
        protected Future<SeverityMap> getSeverityMap(@Nonnull Set<Long> members) {
            return fetchSeverityMap(members);
        }

        @Nonnull
        private Set<Long> getMembersFromGroups(List<GroupAndMembers> groupAndMembers) {
            return Collections.unmodifiableSet(groupAndMembers
                .stream()
                .map(GroupAndMembers::entities)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet()));
        }

        @Nonnull
        protected Future<Map<Long, Float>> getCosts(
                @Nonnull Collection<GroupAndMembers> groupsAndMembers,
                @Nonnull Set<Long> members) {
            return fetchGroupCosts(groupsAndMembers, members);
        }

        /**
         * Returns whether there will be no results in the groups request based on conflict between
         * requested parameters and global environment parameters.
         *
         * @return whether result is empty
         */
        public boolean isEmptyResult() {
            return emptyResult;
        }
    }

    /**
     * Parinator sorting groups by display name.
     */
    private class PaginatorName extends AbstractPaginator {
        private final ObjectsPage<GroupAndMembers> groupsPage;

        /**
         * Constructs display name sorting paginator.
         *
         * @param groups groups to paginate
         * @param paginationRequest pagination request. May be empty - it will mean that no
         *         pagination logic should be applied.
         * @param requestedEnvironmentType requested environment type
         * @throws InterruptedException if current thread interrupted during execution
         * @throws ExecutionException if any of async tasks faled
         * @throws TimeoutException if any of async tasks timed out
         * @throws InvalidOperationException if pagination requet is invalid
         */
        PaginatorName(@Nonnull List<Grouping> groups,
                @Nullable SearchPaginationRequest paginationRequest,
                @Nullable EnvironmentType requestedEnvironmentType)
                throws InterruptedException, ExecutionException, TimeoutException,
                InvalidOperationException {
            super(groups, requestedEnvironmentType);
            // This is much faster, we should try to use the pagination without members as much
            // as possible
            if (requestedEnvironmentType == null) {
                this.groupsPage = applyPaginationWithoutMembers(groups,
                    Comparator.comparing(group -> group.getDefinition().getDisplayName()), paginationRequest, getFilter());
            } else {
                List<GroupAndMembers> groupAndMembers = getGroupAndMembers();
                this.groupsPage = applyPagination(groupAndMembers,
                    Comparator.comparing(group -> group.group().getDefinition().getDisplayName()),
                    paginationRequest, getFilter());
            }

        }

        @Nonnull
        @Override
        public ObjectsPage<GroupAndMembers> getGroupsPage() {
            return groupsPage;
        }
    }

    /**
     * Paginator implementation sorting groups by severity.
     */
    private class PaginatorSeverity extends AbstractPaginator {
        private final ObjectsPage<GroupAndMembers> groupsPage;
        private final Future<SeverityMap> severityMapFuture;

        /**
         * Constructs severity sorting paginator.
         *
         * @param groups groups to paginate
         * @param paginationRequest pagination request. May be empty - it will mean that no
         *         pagination logic should be applied.
         * @param requestedEnvironmentType requested environment type
         * @throws InterruptedException if current thread interrupted during execution
         * @throws ExecutionException if any of async tasks faled
         * @throws TimeoutException if any of async tasks timed out
         * @throws InvalidOperationException if pagination requet is invalid
         */
        PaginatorSeverity(@Nonnull List<Grouping> groups,
                @Nullable SearchPaginationRequest paginationRequest,
                @Nullable EnvironmentType requestedEnvironmentType)
                throws InterruptedException, ExecutionException, TimeoutException,
                InvalidOperationException {
            super(groups, requestedEnvironmentType);
            this.severityMapFuture = fetchSeverityMap(getAllMembers());

            SeverityMap severityMap;
            try {
                severityMap = severityMapFuture.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                logger.error("Failed to get severities from Action Orchestrator. Defaulting to NORMAL.", e);
                severityMap = SeverityMapImpl.empty();
            }
            List<GroupAndMembers> groupAndMembers = getGroupAndMembers();
            final Map<Long, Integer> groupsSeverities = new HashMap<>(groupAndMembers.size());
            for (GroupAndMembers group: groupAndMembers) {
                final int severity;
                if (group.members().isEmpty()) {
                    severity = -1;
                } else {
                    severity = severityMap.calculateSeverity(group.entities()).getNumber();
                }
                groupsSeverities.put(group.group().getId(), severity);
            }
            final Comparator<GroupAndMembers> comparator =
                    Comparator.comparing(group -> groupsSeverities.get(group.group().getId()));
            this.groupsPage = applyPagination(groupAndMembers, comparator, paginationRequest, getFilter());
        }

        @Nonnull
        @Override
        public ObjectsPage<GroupAndMembers> getGroupsPage() {
            return groupsPage;
        }

        @Nonnull
        @Override
        protected Future<SeverityMap> getSeverityMap(@Nonnull Set<Long> members) {
            return severityMapFuture;
        }
    }

    /**
     * Paginator used to sort groups by costs.
     */
    private class PaginatorCost extends AbstractPaginator {

        private final ObjectsPage<GroupAndMembers> groupsPage;
        private final Future<Map<Long, Float>> groupCostsFuture;

        /**
         * Constructs cost sorting paginator.
         *
         * @param groups groups to paginate
         * @param paginationRequest pagination request. May be empty - it will mean that no
         *         pagination logic should be applied.
         * @param requestedEnvironmentType requested environment type
         * @throws InterruptedException if current thread interrupted during execution
         * @throws ExecutionException if any of async tasks faled
         * @throws TimeoutException if any of async tasks timed out
         * @throws InvalidOperationException if pagination requet is invalid
         */
        PaginatorCost(@Nonnull List<Grouping> groups,
                @Nullable SearchPaginationRequest paginationRequest,
                @Nullable EnvironmentType requestedEnvironmentType)
                throws InvalidOperationException, InterruptedException, ExecutionException,
                TimeoutException {
            super(groups, requestedEnvironmentType);
            List<GroupAndMembers> groupAndMembers = getGroupAndMembers();
            this.groupCostsFuture = fetchGroupCosts(groupAndMembers, getAllMembers());
            final Map<Long, Float> groupCosts =
                    groupCostsFuture.get(REQUEST_TIMEOUT_SEC, TimeUnit.SECONDS);
            final Comparator<GroupAndMembers> comparator = Comparator.comparing(
                    group -> groupCosts.getOrDefault(group.group().getId(), 0F));
            groupsPage = applyPagination(groupAndMembers, comparator, paginationRequest, getFilter());
        }

        @Nonnull
        @Override
        public ObjectsPage<GroupAndMembers> getGroupsPage() {
            return groupsPage;
        }

        @Nonnull
        @Override
        protected Future<Map<Long, Float>> getCosts(
                @Nonnull Collection<GroupAndMembers> groupsAndMembers,
                @Nonnull Set<Long> members) {
            return groupCostsFuture;
        }
    }

    /**
     * A function to create a paginator.
     */
    private interface PaginatorSupplier {
        @Nonnull
        AbstractPaginator createPaginator(@Nonnull List<Grouping> groups,
                @Nullable SearchPaginationRequest paginationRequest,
                @Nullable EnvironmentType environmentType)
                throws InvalidOperationException, InterruptedException, ExecutionException,
                TimeoutException;
    }
}
