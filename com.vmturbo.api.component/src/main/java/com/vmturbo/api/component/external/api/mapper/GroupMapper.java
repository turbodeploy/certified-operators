package com.vmturbo.api.component.external.api.mapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;
import jdk.nashorn.internal.ir.annotations.Immutable;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase.GroupUseCaseCriteria;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.GroupExpander.GroupAndMembers;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupPropertyFilterList;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo.TypeCase;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfoOrBuilder;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.PropertyTypeCase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition.VerticesCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingConditionOrBuilder;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Maps groups between their API DTO representation and their protobuf representation.
 */
public class GroupMapper {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The API "class types" (as returned by {@link BaseApiDTO#getClassName()}
     * which indicate that the {@link BaseApiDTO} in question is a group.
     */
    public static final Set<String> GROUP_CLASSES = ImmutableSet.of(StringConstants.GROUP,
        StringConstants.CLUSTER, StringConstants.STORAGE_CLUSTER,
            StringConstants.VIRTUAL_MACHINE_CLUSTER);

    public static final String GROUPS_FILTER_TYPE = "groupsByName";

    public static final String CLUSTERS_FILTER_TYPE = "clustersByName";

    public static final String CLUSTERS_BY_TAGS_FILTER_TYPE = "clustersByTag";

    public static final String STORAGE_CLUSTERS_FILTER_TYPE = "storageClustersByName";

    public static final String VIRTUALMACHINE_CLUSTERS_FILTER_TYPE = "virtualMachineClustersByName";

    public static final Set<String> GROUP_NAME_FILTER_TYPES = ImmutableSet.of(
            GROUPS_FILTER_TYPE, CLUSTERS_FILTER_TYPE,
            STORAGE_CLUSTERS_FILTER_TYPE, VIRTUALMACHINE_CLUSTERS_FILTER_TYPE);

    public static final Set<String> GROUP_TAG_FILTER_TYPES =
        Collections.singleton(CLUSTERS_BY_TAGS_FILTER_TYPE);

    // For normal criteria, user just need to provide a string (like a display name). But for
    // some criteria, UI allow user to choose from list of available options (like tags, state and
    // account id for now). These special criteria are hardcoded in UI side (see
    // "criteriaKeysWithOptions" in filter.registry.service.ts). UI will call another API
    // "/criteria/{elements}/options" to get a list of options for user to select from.
    // Note: these needs to be consistent with UI side and also the "elements" field defined in
    // "groupBuilderUseCases.json". If not, "/criteria/{elements}/options" will not be called
    // since UI gets all criteria from "groupBuilderUseCases.json" and check if the criteria
    // matches that inside "groupBuilderUseCases.json".
    public static final String ACCOUNT_OID = "BusinessAccount:oid:CONNECTED_TO:1";
    public static final String STATE = "state";

    private static final String CONSUMES = "CONSUMES";
    private static final String PRODUCES = "PRODUCES";
    private static final String CONNECTED_TO = "CONNECTED_TO";
    private static final String CONNECTED_FROM = "CONNECTED_FROM";

    // set of supported traversal types, the string should be the same as groupBuilderUsecases.json
    private static Set<String> TRAVERSAL_TYPES = ImmutableSet.of(
            CONSUMES, PRODUCES, CONNECTED_TO, CONNECTED_FROM);

    public static final String ELEMENTS_DELIMITER = ":";
    public static final String NESTED_FIELD_DELIMITER = "\\.";

    public static final String EQUAL = "EQ";
    public static final String NOT_EQUAL = "NEQ";
    public static final String GREATER_THAN = "GT";
    public static final String LESS_THAN = "LT";
    public static final String GREATER_THAN_OR_EQUAL = "GTE";
    public static final String LESS_THAN_OR_EQUAL = "LTE";
    public static final String REGEX_MATCH = "RXEQ";
    public static final String REGEX_NO_MATCH = "RXNEQ";

    // map from the comparison string to the ComparisonOperator enum
    private static final Map<String, ComparisonOperator> COMPARISON_STRING_TO_COMPARISON_OPERATOR =
            ImmutableMap.<String, ComparisonOperator>builder()
                    .put(EQUAL, ComparisonOperator.EQ)
                    .put(NOT_EQUAL, ComparisonOperator.NE)
                    .put(GREATER_THAN, ComparisonOperator.GT)
                    .put(LESS_THAN, ComparisonOperator.LT)
                    .put(GREATER_THAN_OR_EQUAL, ComparisonOperator.GTE)
                    .put(LESS_THAN_OR_EQUAL, ComparisonOperator.LTE)
                    .build();

    // map from the comparison symbol to the ComparisonOperator enum
    // the order matters, since when this is used for checking whether a string contains
    // ">" or ">=", we should check ">=" first
    private static final Map<String, ComparisonOperator> COMPARISON_SYMBOL_TO_COMPARISON_OPERATOR;
    static {
        Map<String, ComparisonOperator> symbolToOperator = new LinkedHashMap<>();
        symbolToOperator.put("!=", ComparisonOperator.NE);
        symbolToOperator.put(">=", ComparisonOperator.GTE);
        symbolToOperator.put("<=", ComparisonOperator.LTE);
        symbolToOperator.put("=", ComparisonOperator.EQ);
        symbolToOperator.put(">", ComparisonOperator.GT);
        symbolToOperator.put("<", ComparisonOperator.LT);
        COMPARISON_SYMBOL_TO_COMPARISON_OPERATOR = Collections.unmodifiableMap(symbolToOperator);
    }

    private static final Map<String, Function<SearchFilterContext, List<SearchFilter>>> FILTER_TYPES_TO_PROCESSORS;
    static {
        final TraversalFilterProcessor traversalFilterProcessor = new TraversalFilterProcessor();
        final ImmutableMap.Builder<String, Function<SearchFilterContext, List<SearchFilter>>>
                filterTypesToProcessors = new ImmutableMap.Builder<>();
        filterTypesToProcessors.put(
                StringConstants.TAGS_ATTR,
                context -> {
                    //TODO: the expression value coming from the UI is currently unsanitized.
                    // It is assumed that the tag keys and values do not contain characters such as = and |.
                    // This is reported as a JIRA issue OM-39039.
                    final String operator = context.getFilter().getExpType();
                    final boolean positiveMatch = isPositiveMatchingOperator(operator);
                    if (isRegexOperator(operator)) {
                        // regex match is required
                        // break input into key and value
                        final String[] keyval = context.getFilter().getExpVal().split("=");
                        final String key = keyval[0];
                        final String value = keyval[1];
                        final PropertyFilter tagsFilter =
                                SearchMapper.mapPropertyFilterForMultimapsRegex(
                                        StringConstants.TAGS_ATTR, key, value, positiveMatch);
                        return Collections.singletonList(SearchProtoUtil.searchFilterProperty(tagsFilter));
                    } else {
                        // exact match is required
                        final PropertyFilter tagsFilter =
                                SearchMapper.mapPropertyFilterForMultimapsExact(
                                        StringConstants.TAGS_ATTR,
                                        context.getFilter().getExpVal(),
                                        positiveMatch);
                        return Collections.singletonList(SearchProtoUtil.searchFilterProperty(tagsFilter));
                    }
                });
        filterTypesToProcessors.put(
                StringConstants.CLUSTER,
                context -> {
                    ClusterMembershipFilter clusterFilter =
                            SearchProtoUtil.clusterFilter(
                                SearchProtoUtil.nameFilterRegex(
                                    context.getFilter().getExpVal(),
                                    context.getFilter().getExpType().equals(REGEX_MATCH),
                                    context.getFilter().getCaseSensitive()));
                    return Collections.singletonList(SearchProtoUtil.searchFilterCluster(clusterFilter));
                });
        filterTypesToProcessors.put(CONSUMES, traversalFilterProcessor);
        filterTypesToProcessors.put(PRODUCES, traversalFilterProcessor);
        filterTypesToProcessors.put(CONNECTED_FROM, traversalFilterProcessor);
        filterTypesToProcessors.put(CONNECTED_TO, traversalFilterProcessor);
        FILTER_TYPES_TO_PROCESSORS = filterTypesToProcessors.build();
    }

    /**
     * The set of probe types whose environment type should be treated as CLOUD. This is different
     * from the target category "CLOUD MANAGEMENT". This is also defined in classic in
     * DiscoveryConfigService#cloudTargetTypes.
     */
    private static final Set<String> CLOUD_ENVIRONMENT_PROBE_TYPES = ImmutableSet.of(
        SDKProbeType.AWS.getProbeType(), SDKProbeType.AZURE.getProbeType());

    private static StoppingCondition.Builder buildStoppingCondition(String currentToken) {
        return StoppingCondition.newBuilder().setStoppingPropertyFilter(
                        SearchProtoUtil.entityTypeFilter(currentToken));
    }

    private final GroupUseCaseParser groupUseCaseParser;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    private final TopologyProcessor topologyProcessor;

    public GroupMapper(@Nonnull final GroupUseCaseParser groupUseCaseParser,
                       @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                       @Nonnull final GroupExpander groupExpander,
                       @Nonnull final TopologyProcessor topologyProcessor,
                       @Nonnull final RepositoryApi repositoryApi) {
        this.groupUseCaseParser = Objects.requireNonNull(groupUseCaseParser);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
    }

    /**
     * Convert a {@link GroupApiDTO} for a temporary group to the associated
     * {@link TempGroupInfo}.
     *
     * @param apiDTO The {@link GroupApiDTO} for a temporary group.
     * @return A {@link TempGroupInfo} to describe the temporary group in XL.
     * @throws OperationFailedException If there is an error obtaining associated information.
     * @throws InvalidOperationException If the {@link GroupApiDTO} is illegal in some way.
     */
    @Nonnull
    public TempGroupInfo toTempGroupProto(@Nonnull final GroupApiDTO apiDTO)
            throws OperationFailedException, InvalidOperationException {
        if (!Boolean.TRUE.equals(apiDTO.getTemporary())) {
            throw new InvalidOperationException("Attempting to create temp group out of " +
                    "non-temp group request.");
        } else if (apiDTO.getDisplayName() == null) {
            throw new InvalidOperationException("No name for temp group!");
        }

        // Save the list of explicitly-requested member OIDs.
        final Set<Long> explicitMemberIds =
            CollectionUtils.emptyIfNull(apiDTO.getMemberUuidList()).stream()
                .map(uuid -> {
                    try {
                        // We expect the memberUuid list to be composed of entity IDs, so
                        // we don't expand them.
                        return Optional.of(Long.parseLong(uuid));
                    } catch (NumberFormatException e) {
                        logger.error("Invalid group member uuid: {}. Expecting numeric members only.", uuid);
                        return Optional.<Long>empty();
                    }
                })
                .filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toSet());

        final Set<Long> groupMembers;
        final boolean isGlobalScopeGroup;
        if (!CollectionUtils.isEmpty(apiDTO.getScope())) {
            // Derive the members from the scope
            final Map<String, SupplyChainNode> supplyChainForScope =
                    supplyChainFetcherFactory.newNodeFetcher()
                            .addSeedUuids(apiDTO.getScope())
                            .entityTypes(Collections.singletonList(apiDTO.getGroupType()))
                            .apiEnvironmentType(apiDTO.getEnvironmentType())
                            .fetch();
            final SupplyChainNode node = supplyChainForScope.get(apiDTO.getGroupType());
            if (node == null) {
                throw new InvalidOperationException("Group type: " + apiDTO.getGroupType() +
                        " not found in supply chain for scopes: " + apiDTO.getScope());
            }
            final Set<Long> entitiesInScope = RepositoryDTOUtil.getAllMemberOids(node);
            // Check if the user only wants a specific set of entities within the scope.
            if (!explicitMemberIds.isEmpty()) {
                groupMembers = Sets.intersection(entitiesInScope, explicitMemberIds);
            } else {
                groupMembers = entitiesInScope;
            }
            // check if the temp group's scope is Market or not.
            // TODO (roman, June 19 2018): We shouldn't need to retrieve members if it's a global
            // scope group. We should just set this flag.
            isGlobalScopeGroup = apiDTO.getScope().size() == 1 &&
                    apiDTO.getScope().get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR);
        } else if (!CollectionUtils.isEmpty(apiDTO.getMemberUuidList())) {
            groupMembers = explicitMemberIds;
            isGlobalScopeGroup = false;
        } else {
            // At the time of this writing temporary groups are always created from some scope or
            // an explicit list of members, so we can't handle the case where neither is provided.
            throw new InvalidOperationException("No scope/member list for temp group " + apiDTO.getDisplayName());
        }

        final TempGroupInfo.Builder tempGroupBuilder = TempGroupInfo.newBuilder()
                .setEntityType(UIEntityType.fromString(apiDTO.getGroupType()).typeNumber())
                .setMembers(StaticGroupMembers.newBuilder()
                        .addAllStaticMemberOids(groupMembers))
                .setName(apiDTO.getDisplayName())
                .setIsGlobalScopeGroup(isGlobalScopeGroup);
        if (apiDTO.getEnvironmentType() != null &&
                apiDTO.getEnvironmentType() != EnvironmentType.HYBRID) {
            tempGroupBuilder.setEnvironmentType(
                apiDTO.getEnvironmentType() == EnvironmentType.CLOUD ?
                    EnvironmentTypeEnum.EnvironmentType.CLOUD :
                    EnvironmentTypeEnum.EnvironmentType.ON_PREM);
        }
        return tempGroupBuilder.build();
    }

    /**
     * Converts from {@link GroupApiDTO} to {@link GroupInfo}.
     *
     * @param groupDto The {@link GroupApiDTO} object
     * @return         The {@link GroupInfo} object
     */
    public GroupInfo toGroupInfo(@Nonnull final GroupApiDTO groupDto) {
        final GroupInfo.Builder requestBuilder = GroupInfo.newBuilder()
                .setName(groupDto.getDisplayName())
                .setEntityType(UIEntityType.fromString(groupDto.getGroupType()).typeNumber());

        if (groupDto.getIsStatic()) {
            requestBuilder.setStaticGroupMembers(
                    StaticGroupMembers.newBuilder().addAllStaticMemberOids(
                            groupDto.getMemberUuidList().stream().map(Long::parseLong).collect(Collectors.toList())));
        } else {
            requestBuilder
                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                            .addAllSearchParameters(convertToSearchParameters(groupDto, groupDto.getGroupType(), null)))
                    .build();
        }
        return requestBuilder.build();
    }

    public NestedGroupInfo toNestedGroupInfo(@Nonnull final GroupApiDTO groupDto) throws InvalidOperationException {
        NestedGroupInfo.Builder nestedGroupBuilder = NestedGroupInfo.newBuilder()
            .setName(groupDto.getDisplayName());
        if (StringUtils.equals(groupDto.getGroupType(), StringConstants.CLUSTER)) {
            nestedGroupBuilder.setCluster(Type.COMPUTE);
        } else if (StringUtils.equals(groupDto.getGroupType(), StringConstants.STORAGE_CLUSTER)) {
            nestedGroupBuilder.setCluster(Type.STORAGE);
        } else if (StringUtils.equals(groupDto.getGroupType(), StringConstants.VIRTUAL_MACHINE_CLUSTER)) {
            nestedGroupBuilder.setCluster(Type.COMPUTE_VIRTUAL_MACHINE);
        } else {
            throw new InvalidOperationException("Nested groups of type: " +
                groupDto.getGroupType() + " not supported.");
        }

        if (groupDto.getIsStatic()) {
            nestedGroupBuilder.setStaticGroupMembers(
                StaticGroupMembers.newBuilder().addAllStaticMemberOids(
                    groupDto.getMemberUuidList().stream().map(Long::parseLong).collect(Collectors.toList())));
        } else {
            final GroupPropertyFilterList groupPropFilters =
                apiFiltersToGroupPropFilters(nestedGroupBuilder, groupDto.getCriteriaList());
            nestedGroupBuilder.setPropertyFilterList(groupPropFilters);
        }
        return nestedGroupBuilder.build();
    }

    @Nonnull
    private GroupApiDTO createGroupApiDto(@Nonnull final Group group) {
        GroupInfo groupInfo = group.getGroup();
        final GroupApiDTO outputDTO = new GroupApiDTO();
        outputDTO.setClassName(StringConstants.GROUP);
        outputDTO.setGroupType(UIEntityType.fromType(GroupProtoUtil.getEntityType(group)).apiStr());

        switch (groupInfo.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                outputDTO.setIsStatic(true);
                break;
            case SEARCH_PARAMETERS_COLLECTION:
                outputDTO.setIsStatic(false);
                outputDTO.setCriteriaList(convertToFilterApis(groupInfo));
                break;
            default:
                // Nothing to do
                logger.error("Unknown groupMembersCase: " +
                        groupInfo.getSelectionCriteriaCase());
        }
        return outputDTO;
    }

    @Nonnull
    private GroupApiDTO createClusterApiDto(@Nonnull final Group cluster) {
        final GroupApiDTO outputDTO = new GroupApiDTO();
        final ClusterInfo clusterInfo = cluster.getCluster();
        if (clusterInfo.getClusterType() == ClusterInfo.Type.COMPUTE) {
            outputDTO.setClassName(StringConstants.CLUSTER);
        } else if (clusterInfo.getClusterType() == ClusterInfo.Type.STORAGE) {
            outputDTO.setClassName(StringConstants.STORAGE_CLUSTER);
        } else if (clusterInfo.getClusterType() == Type.COMPUTE_VIRTUAL_MACHINE) {
            outputDTO.setClassName(StringConstants.VIRTUAL_MACHINE_CLUSTER);
        } else {
            logger.error("Unexpected cluster type: {}. Defaulting to \"CLUSTER\" (compute)",
                clusterInfo.getClusterType());
            outputDTO.setClassName(StringConstants.CLUSTER);
        }
        outputDTO.setIsStatic(true);
        outputDTO.setGroupType(UIEntityType.fromType(GroupProtoUtil.getEntityType(cluster)).apiStr());
        return outputDTO;
    }

    @Nonnull
    private GroupApiDTO createNestedGroupApiDTO(@Nonnull final Group nestedGroup) {
        Preconditions.checkArgument(nestedGroup.hasNestedGroup());
        final GroupApiDTO outputDTO = new GroupApiDTO();
        outputDTO.setClassName(StringConstants.GROUP);

        switch (nestedGroup.getNestedGroup().getTypeCase()) {
            case CLUSTER:
                if (nestedGroup.getNestedGroup().getCluster() == ClusterInfo.Type.COMPUTE) {
                    outputDTO.setGroupType(StringConstants.CLUSTER);
                } else if (nestedGroup.getNestedGroup().getCluster() == ClusterInfo.Type.STORAGE) {
                    outputDTO.setGroupType(StringConstants.STORAGE_CLUSTER);
                } else if (nestedGroup.getNestedGroup().getCluster() == Type.COMPUTE_VIRTUAL_MACHINE) {
                    outputDTO.setGroupType(StringConstants.VIRTUAL_MACHINE_CLUSTER);
                }
                break;
            default:
                // In the future we will probably also need to support:
                //    - Nested discovered groups (e.g. VC folders)
                //    - Groups of resource groups.
                logger.error("Unhandled nested group type: {}", nestedGroup.getNestedGroup().getTypeCase());
                throw new IllegalArgumentException("Unhandled nested group type: " + nestedGroup.getNestedGroup().getTypeCase());
        }

        outputDTO.setIsStatic(nestedGroup.getNestedGroup().getSelectionCriteriaCase() ==
            SelectionCriteriaCase.STATIC_GROUP_MEMBERS);
        if (nestedGroup.getNestedGroup().getSelectionCriteriaCase() == SelectionCriteriaCase.PROPERTY_FILTER_LIST) {
            outputDTO.setCriteriaList(groupPropFiltersToApiFilters(nestedGroup.getNestedGroup()));
        }

        return outputDTO;
    }

    @Nonnull
    private GroupApiDTO createTempGroupApiDTO(@Nonnull final Group tempGroup) {
        Preconditions.checkArgument(tempGroup.hasTempGroup());
        final GroupApiDTO outputDTO = new GroupApiDTO();
        outputDTO.setClassName(StringConstants.GROUP);
        outputDTO.setIsStatic(true);
        outputDTO.setTemporary(true);
        outputDTO.setGroupType(UIEntityType.fromType(GroupProtoUtil.getEntityType(tempGroup)).apiStr());
        return outputDTO;
    }

    /**
     * Converts from {@link Group} to {@link GroupApiDTO} using default EnvironmentType.ONPREM
     * TODO switch to use {@link GroupMapper#toGroupApiDto(Group)} for Cloud group
     *
     * @param group The {@link Group} object
     * @return  The {@link GroupApiDTO} object
     */
    public GroupApiDTO toGroupApiDto(@Nonnull final Group group) {
        return toGroupApiDto(groupExpander.getMembersForGroup(group), EnvironmentType.ONPREM);
    }

    /**
     * Converts from {@link GroupAndMembers} to {@link GroupApiDTO}.
     *
     * @param groupAndMembers The {@link GroupAndMembers} object (get it from {@link GroupExpander})
     *                        describing the XL group and its members.
     * @param environmentType The environment type of the group.
     * @return The {@link GroupApiDTO} object.
     */
    @Nonnull
    public GroupApiDTO toGroupApiDto(@Nonnull final GroupAndMembers groupAndMembers,
                                     @Nonnull final EnvironmentType environmentType) {
        final GroupApiDTO outputDTO;
        final Group group = groupAndMembers.group();
        switch (group.getType()) {
            case GROUP:
                outputDTO = createGroupApiDto(groupAndMembers.group());
                break;
            case CLUSTER:
                outputDTO = createClusterApiDto(groupAndMembers.group());
                break;
            case TEMP_GROUP:
                outputDTO = createTempGroupApiDTO(groupAndMembers.group());
                break;
            case NESTED_GROUP:
                outputDTO = createNestedGroupApiDTO(groupAndMembers.group());
                break;
            default:
                throw new IllegalArgumentException("Unrecognized group type: " + group.getType());
        }

        outputDTO.setDisplayName(GroupProtoUtil.getGroupDisplayName(group));
        outputDTO.setUuid(Long.toString(group.getId()));

        outputDTO.setMembersCount(groupAndMembers.members().size());
        outputDTO.setMemberUuidList(groupAndMembers.members().stream()
            .map(oid -> Long.toString(oid))
            .collect(Collectors.toList()));
        outputDTO.setEnvironmentType(getEnvironmentTypeForTempGroup(environmentType));
        outputDTO.setEntitiesCount(groupAndMembers.entities().size());
        outputDTO.setActiveEntitiesCount(getActiveEntitiesCount(groupAndMembers));

        return outputDTO;
    }

    private int getActiveEntitiesCount(@Nonnull final GroupAndMembers groupAndMembers) {
        // Set the active entities count.
        final Group group = groupAndMembers.group();
        try {
            // We need to find the number of active entities in the group. The best way to do that
            // is to do a search, and return only the counts. This minimizes the amount of
            // traffic across the network - although for large groups this is still a lot!
            final PropertyFilter startingFilter;
            if (group.getType() == Group.Type.TEMP_GROUP && group.getTempGroup().getIsGlobalScopeGroup()) {
                // In a global temp group we can just set the environment type.
                startingFilter = SearchProtoUtil.entityTypeFilter(GroupProtoUtil.getEntityType(group));
            } else {
                // In any other group we need to send the entity IDs to the search service as the
                // starting filter.
                startingFilter = SearchProtoUtil.idFilter(groupAndMembers.entities());
            }
            return (int) repositoryApi.newSearchRequest(
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

   /**
     * Converts from {@link Group} to {@link GroupApiDTO}.
     *
     * @param group The {@link Group} object
     * @param environmentType The {@link EnvironmentType} object
     * @return  The {@link GroupApiDTO} object
     */
    public GroupApiDTO toGroupApiDto(@Nonnull final Group group,
                                     @Nonnull EnvironmentType environmentType) {
        return toGroupApiDto(groupExpander.getMembersForGroup(group), environmentType);
    }

    /**
     * Convert a filter given by the API to a group property filter.
     *
     * @param filter the API filter to convert.
     * @return the equivalent group property filter or {@code null} if {@code filter} is not
     *         a group filter.
     */
    @Nonnull
    public Optional<PropertyFilter> apiFilterToGroupPropFilter(@Nonnull FilterApiDTO filter) {
        if (GROUP_NAME_FILTER_TYPES.contains(filter.getFilterType())) {
            return
                Optional.of(
                        SearchProtoUtil.nameFilterRegex(
                            filter.getExpVal(),
                            isPositiveMatchingOperator(filter.getExpType()),
                            filter.getCaseSensitive()));
        } else if (GROUP_TAG_FILTER_TYPES.contains(filter.getFilterType())) {
            final boolean positiveMatch = isPositiveMatchingOperator(filter.getExpType());
            if (isRegexOperator(filter.getExpType())) {
                final String[] kv = filter.getExpVal().split("=");
                return
                    Optional.of(
                        SearchMapper.mapPropertyFilterForMultimapsRegex(
                                StringConstants.TAGS_ATTR, kv[0], kv[1], positiveMatch));
            } else {
                return
                    Optional.of(
                        SearchMapper.mapPropertyFilterForMultimapsExact(
                                StringConstants.TAGS_ATTR, filter.getExpVal(), positiveMatch));
            }
        }
        return Optional.empty();
    }

    /**
     * Convert a list of {@link FilterApiDTO}s meant to be applied to groups (to create a
     * nested group) to a {@link GroupPropertyFilterList} used inside XL.
     *
     * @param groupInfo The {@link NestedGroupInfoOrBuilder} describing the nested group.
     *                  Used to narrow down which criteria actually apply.
     * @param criteria The list of {@link FilterApiDTO}s to apply.
     * @return The {@link GroupPropertyFilterList}.
     */
    @Nonnull
    public GroupPropertyFilterList apiFiltersToGroupPropFilters(
            @Nonnull final NestedGroupInfoOrBuilder groupInfo,
            @Nullable List<FilterApiDTO> criteria) {
        if (CollectionUtils.isEmpty(criteria)) {
            return GroupPropertyFilterList.getDefaultInstance();
        }

        final GroupPropertyFilterList.Builder filterListBldr = GroupPropertyFilterList.newBuilder();
        criteria.stream()
            .filter(GroupMapper::isGroupFilter)
            .filter(filter -> {
                if (groupInfo.getTypeCase() == TypeCase.CLUSTER) {
                    switch (groupInfo.getCluster()) {
                        case COMPUTE:
                            return
                                filter.getFilterType().equals(CLUSTERS_FILTER_TYPE) ||
                                filter.getFilterType().equals(CLUSTERS_BY_TAGS_FILTER_TYPE);
                        case STORAGE:
                            return filter.getFilterType().equals(STORAGE_CLUSTERS_FILTER_TYPE);
                        case COMPUTE_VIRTUAL_MACHINE:
                            return
                                filter.getFilterType().equals(VIRTUALMACHINE_CLUSTERS_FILTER_TYPE);
                        default:
                            return false;
                    }
                }
                return false;
            })
            .map(this::apiFilterToGroupPropFilter)
            .forEach(optionalFilter -> optionalFilter.map(filterListBldr::addPropertyFilters));
        return filterListBldr.build();
    }

    private static boolean isGroupFilter(@Nonnull FilterApiDTO filterApiDTO) {
        return
            GROUP_NAME_FILTER_TYPES.contains(filterApiDTO.getFilterType()) ||
            GROUP_TAG_FILTER_TYPES.contains(filterApiDTO.getFilterType());
    }

    /**
     * Convert the list of group property filters (used for dynamic nested groups) into the
     * API/UI-compatible list of {@link FilterApiDTO}s.
     *
     * @param groupInfo The {@link NestedGroupInfo} of the nested group.
     * @return The list of {@link FilterApiDTO} objects.
     */
    @Nonnull
    private List<FilterApiDTO> groupPropFiltersToApiFilters(
            @Nonnull final NestedGroupInfo groupInfo) {
        return groupInfo.getPropertyFilterList().getPropertyFiltersList().stream()
            .map(propFilter -> {
                if (propFilter.getPropertyTypeCase() == PropertyTypeCase.STRING_FILTER) {
                    // the property corresponds to the display name of the cluster
                    final StringFilter stringFilter = propFilter.getStringFilter();
                    final FilterApiDTO filterApiDTO = new FilterApiDTO();

                    // remove leading ^ and trailing $ from the regex
                    final String unfilteredRegex = stringFilter.getStringPropertyRegex();
                    filterApiDTO.setExpVal(
                            unfilteredRegex.substring(1, unfilteredRegex.length() - 1));

                    filterApiDTO.setExpType(stringFilter.getPositiveMatch() ? REGEX_MATCH : REGEX_NO_MATCH);
                    filterApiDTO.setCaseSensitive(stringFilter.getCaseSensitive());
                    switch (groupInfo.getCluster()) {
                        case COMPUTE:
                            filterApiDTO.setFilterType(CLUSTERS_FILTER_TYPE);
                            break;
                        case STORAGE:
                            filterApiDTO.setFilterType(STORAGE_CLUSTERS_FILTER_TYPE);
                            break;
                        case COMPUTE_VIRTUAL_MACHINE:
                            filterApiDTO.setFilterType(VIRTUALMACHINE_CLUSTERS_FILTER_TYPE);
                            break;
                        default:
                            // Error.
                            return null;
                    }
                    return filterApiDTO;
                } else if (propFilter.getPropertyTypeCase() == PropertyTypeCase.MAP_FILTER) {
                    // the property corresponds to the tags of the cluster
                    final MapFilter mapFilter = propFilter.getMapFilter();
                    final FilterApiDTO filterApiDTO = new FilterApiDTO();
                    if (mapFilter.hasRegex()) {
                        // regex matching
                        filterApiDTO.setExpVal(
                                mapFilter.getKey() + "=" +
                                mapFilter.getRegex().substring(1, mapFilter.getRegex().length() - 1));
                        filterApiDTO.setExpType(mapFilter.getPositiveMatch() ? REGEX_MATCH : REGEX_NO_MATCH);
                    } else {
                        // exact matching
                        filterApiDTO.setExpVal(
                                mapFilter.getValuesList().stream()
                                        .map(v -> mapFilter.getKey() + "=" + v)
                                        .collect(Collectors.joining("|")));
                        filterApiDTO.setExpType(mapFilter.getPositiveMatch() ? EQUAL : NOT_EQUAL);
                    }
                    filterApiDTO.setCaseSensitive(false);
                    if (groupInfo.getCluster() == Type.COMPUTE) {
                            filterApiDTO.setFilterType(CLUSTERS_BY_TAGS_FILTER_TYPE);
                    } else {
                        // Error.
                        return null;
                    }
                    return filterApiDTO;
                }
                return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /**
     * Convert a {@link GroupApiDTO} to a list of search parameters. Right now the entity type sources
     * have inconsistency between search service with group service requests. For search service
     * request, UI use class name field store entityType, but for creating group request,
     * UI use groupType field store entityType.
     *
     * @param inputDTO received from the UI
     * @param entityType the name of entity type, such as VirtualMachine
     * @param nameQuery user specified search query for entity name. If it is not null, it will be
     *                  converted to a entity name filter.
     * @return list of search parameters to use for querying the repository
     */
    public List<SearchParameters> convertToSearchParameters(@Nonnull GroupApiDTO inputDTO,
                                                            @Nonnull String entityType,
                                                            @Nullable String nameQuery) {
        final List<FilterApiDTO> criteriaList = inputDTO.getCriteriaList();
        Optional<List<FilterApiDTO>> filterApiDTOList =
                (criteriaList != null && !criteriaList.isEmpty())
                        ? Optional.of(criteriaList)
                        : Optional.empty();
        return filterApiDTOList
                .map(filterApiDTOs -> filterApiDTOs.stream()
                        .map(filterApiDTO -> filter2parameters(filterApiDTO, entityType, nameQuery))
                        .collect(Collectors.toList()))
                .orElse(ImmutableList.of(searchParametersForEmptyCriteria(entityType, nameQuery)));
    }

    /**
     * Convert a {@link GroupInfo} to a list of FilterApiDTO.
     *
     * @param groupInfo {@link GroupInfo} A message contains all information to represent a static or
     *                  dynamic group
     * @return a list of FilterApiDTO which contains different filter rules for dynamic group
     */
    public List<FilterApiDTO> convertToFilterApis(GroupInfo groupInfo) {
        return groupInfo.getSearchParametersCollection()
                .getSearchParametersList().stream()
                .map(this::toFilterApiDTO)
                .collect(Collectors.toList());
    }

    /**
     * Generate search parameters for getting all entities under current entity type. It handles the edge case
     * when users create dynamic group, not specify any criteria rules, this dynamic group should
     * contains all entities under selected type.
     *
     * @param entityType entity type from UI
     * @param nameQuery user specified search query for entity name. If it is not null, it will be
     *                  converted to a entity name filter.
     * @return a search parameters object only contains starting filter with entity type
     */
    private SearchParameters searchParametersForEmptyCriteria(@Nonnull final String entityType,
                                                              @Nullable final String nameQuery) {
        PropertyFilter byType = SearchProtoUtil.entityTypeFilter(entityType);
        final SearchParameters.Builder searchParameters = SearchParameters.newBuilder().setStartingFilter(byType);
        if (!StringUtils.isEmpty(nameQuery)) {
            // For the query string, we want to use a "contains"-type query.
            searchParameters.addSearchFilter(SearchProtoUtil.searchFilterProperty(SearchProtoUtil.nameFilterRegex(".*" + nameQuery + ".*")));
        }
        return searchParameters.build();
    }

    /**
     * Convert one filter DTO to search parameters. The byName filter must have a filter type. If
     * it also has an expVal then use it to filter the results.
     *
     * <p>Examples of "elements" with 2 and 3  elements:
     *
     * <p>1. <b>Storage:PRODUCES</b> - start with instances of Storage, traverse to PRODUCES and stop
     * when reaching instances of byNameClass<br>
     * 2. <b>PhysicalMachine:PRODUCES:1</b> - start with instances of PhysicalMachine, traverse to
     * PRODUCES and stop after one hop.
     *
     * TODO: if the filter language keeps getting expanded on, we might want to use an actual
     * grammar and parser. This implementation is pretty loose in regards to checks and assumes the
     * input set is controlled via groupBuilderUsescases.json. We are not checking all boundary
     * conditions. If this becomes necessary then we'll need to update this implementation.
     *
     * @param filter a filter
     * @param entityType class name of the byName filter
     * @param nameQuery user specified search query for entity name. If it is not null, it will be
     *                  converted to a entity name filter.
     * @return parameters full search parameters
     */
    private SearchParameters filter2parameters(@Nonnull FilterApiDTO filter,
                                               @Nonnull String entityType,
                                               @Nullable String nameQuery) {
        GroupUseCaseCriteria useCase = groupUseCaseParser.getUseCasesByFilterType().get(filter.getFilterType());

        if (useCase == null) {
            throw new IllegalArgumentException("Not existing filter type provided: " + filter.getFilterType());
        }
        // build the search parameters based on the filter criteria
        SearchParameters.Builder parametersBuilder = SearchParameters.newBuilder();

        final List<String> elements = Arrays.asList(useCase.getElements().split(ELEMENTS_DELIMITER));

        Iterator<String> iterator = elements.iterator();
        final String firstToken = iterator.next();
        if (UIEntityType.fromString(firstToken) != UIEntityType.UNKNOWN) {
            parametersBuilder.setStartingFilter(SearchProtoUtil.entityTypeFilter(firstToken));
        } else {
            parametersBuilder.setStartingFilter(SearchProtoUtil.entityTypeFilter(entityType));
            iterator = elements.iterator();
        }

        final ImmutableList.Builder<SearchFilter> searchFilters = new ImmutableList.Builder<>();
        while (iterator.hasNext()) {
            searchFilters.addAll(processToken(filter, entityType, iterator, useCase.getInputType(), firstToken));
        }
        if (!StringUtils.isEmpty(nameQuery)) {
            searchFilters.add(SearchProtoUtil.searchFilterProperty(
                SearchProtoUtil.nameFilterRegex(".*" + nameQuery + ".*")));
        }
        parametersBuilder.addAllSearchFilter(searchFilters.build());
        parametersBuilder.setSourceFilterSpecs(toFilterSpecs(filter));
        return parametersBuilder.build();
    }

    /**
     * Process the given tokens which comes from the elements in "groupBuilderUsecases.json" and
     * convert those criteria into list of SearchFilter which will be used by repository to fetch
     * matching entities.
     *
     * @param filter the FilterApiDTO provided by user in UI, which contains the criteria for this group
     * @param entityType the entity type of the group to create
     * @param iterator the Iterator containing all the tokens to process, for example:
     * "PRODUCES:1:VirtualMachine", first is PRODUCES, then 1, and then VirtualMachine
     * @param inputType the type of the input, such as "*" or "#"
     * @param firstToken the first token defined in the "groupBuilderUsecases.json", for example:
     * "PRODUCES:1:VirtualMachine", first is PRODUCES.
     * @return list of SearchFilters for given tokens
     */
    private List<SearchFilter> processToken(@Nonnull FilterApiDTO filter,
                                            @Nonnull String entityType,
                                            @Nonnull Iterator<String> iterator,
                                            @Nonnull String inputType,
                                            @Nonnull String firstToken) {
        final String currentToken = iterator.next();

        final SearchFilterContext filterContext = new SearchFilterContext(filter, iterator,
                entityType, currentToken, firstToken);
        final Function<SearchFilterContext, List<SearchFilter>> filterApiDtoProcessor =
                        FILTER_TYPES_TO_PROCESSORS.get(currentToken);

        if (filterApiDtoProcessor != null) {
            return filterApiDtoProcessor.apply(filterContext);
        } else if (UIEntityType.fromString(currentToken) != UIEntityType.UNKNOWN) {
            return ImmutableList.of(SearchProtoUtil.searchFilterProperty(
                    SearchProtoUtil.entityTypeFilter(currentToken)));
        } else {
            final PropertyFilter propertyFilter = isListToken(currentToken) ?
                    createPropertyFilterForListToken(currentToken, inputType, filter) :
                    createPropertyFilterForNormalToken(currentToken, inputType, filter);
            return ImmutableList.of(SearchProtoUtil.searchFilterProperty(propertyFilter));
        }
    }

    /**
     * Create PropertyFilter for a list token.
     * The list token starts with the name of the property which is a list, and then wrap filter
     * criteria with "[" and "]". Each criteria is a key value pair combined using "=". If the
     * criteria starts with "#", it means this value is numeric, otherwise it is a string. The last
     * criteria may be a special one which doesn't start with "#" or contains "=", it is just a
     * single property whose value and type are provided by UI.
     * For example: currentToken: "commoditySoldList[type=VMem,#used>0,capacity]". It means finds
     * entities whose VMem commodity's used is more than 0 and capacity meets the value defined in
     * FilterApiDTO.
     *
     * @param currentToken the token which contains nested fields
     * @param inputType the type of the input from UI, which can be "*" (string) or "#" (number)
     * @param filter the FilterApiDTO which contains values provided by user in UI
     * @return PropertyFilter
     */
    private PropertyFilter createPropertyFilterForListToken(@Nonnull String currentToken,
            @Nonnull String inputType, @Nonnull FilterApiDTO filter) {
        // list, for example: "commoditySoldList[type=VMem,#used>0,capacity]"
        int left = currentToken.indexOf('[');
        int right = currentToken.lastIndexOf(']');
        // name of the property which is a list, for example: commoditySoldList
        final String listFieldName = currentToken.substring(0, left);

        ListFilter.Builder listFilter = ListFilter.newBuilder();
        // there is no nested property inside list, the list is a list of strings or numbers
        // for example: targetIds[]
        if (left == right) {
            switch (inputType) {
                case "s":
                case "s|*":
                case "*|s":
                case "*":
                    // string matching
                    // the input types "s" and "*" represent exact string matching
                    // and regex matching respectively.  their combination allows for both.
                    // all cases are treated in the same way here (the distinction is only
                    // helpful to the UI side).  we can distinguish the cases by looking at
                    // the operator
                    final boolean positiveMatch = isPositiveMatchingOperator(filter.getExpType());
                    if (isRegexOperator(filter.getExpType())) {
                        listFilter.setStringFilter(
                            SearchProtoUtil.stringFilterRegex(
                                filter.getExpVal(),
                                positiveMatch,
                                false));
                    } else {
                        listFilter.setStringFilter(
                            SearchProtoUtil.stringFilterExact(
                                Arrays.stream(filter.getExpVal().split("\\|"))
                                        .collect(Collectors.toList()),
                                positiveMatch,
                                false));
                    }
                    break;
                case "#":
                    // numeric comparison
                    listFilter.setNumericFilter(SearchProtoUtil.numericFilter(
                        Long.valueOf(filter.getExpVal()),
                            COMPARISON_STRING_TO_COMPARISON_OPERATOR.get(filter.getExpType())));
                    break;
                default:
                    throw new UnsupportedOperationException("Input type: " + inputType +
                            " is not supported for ListFilter");
            }
        } else {
            ObjectFilter.Builder objectFilter = ObjectFilter.newBuilder();
            // for example: "type=VMem,#used>0,capacity"
            final String nestedListField = currentToken.substring(left + 1, right);
            for (String criteria : nestedListField.split(",")) {
                if (isListToken(criteria)) {
                    // create nested list filter recursively
                    objectFilter.addFilters(createPropertyFilterForListToken(criteria,
                            inputType, filter));
                } else if (criteria.startsWith("#")) {
                    // this is numeric, find the symbol (>) in "#used>0"
                    String symbol = null;
                    int indexOfSymbol = 0;
                    for (String sb : COMPARISON_SYMBOL_TO_COMPARISON_OPERATOR.keySet()) {
                        int indexOfSb = criteria.indexOf(sb);
                        if (indexOfSb != -1) {
                            symbol = sb;
                            indexOfSymbol = indexOfSb;
                            break;
                        }
                    }
                    if (symbol == null) {
                        throw new IllegalArgumentException("No comparison symbol found in"
                                + " criteria: " + criteria);
                    }
                    // key: "used"
                    final String key = criteria.substring(1, indexOfSymbol);
                    // value: "2"
                    final String value = criteria.substring(indexOfSymbol + symbol.length());
                    // ComparisonOperator for ">="
                    final ComparisonOperator co = COMPARISON_SYMBOL_TO_COMPARISON_OPERATOR.get(symbol);

                    objectFilter.addFilters(PropertyFilter.newBuilder()
                            .setPropertyName(key)
                            .setNumericFilter(NumericFilter.newBuilder()
                                    .setComparisonOperator(co)
                                    .setValue(Integer.valueOf(value))
                                    .build())
                            .build());
                } else if (criteria.contains("=")) {
                    // if no # provided, it means string by default, for example: "type=VMem"
                    String[] keyValue = criteria.split("=");
                    objectFilter.addFilters(
                        SearchProtoUtil.stringPropertyFilterExact(
                            keyValue[0], Collections.singletonList(keyValue[1]), true, false));
                } else {
                    // if no "=", it means this is final field, whose comparison operator and value
                    // are provided by UI in FilterApiDTO, for example: capacity
                    objectFilter.addFilters(createPropertyFilterForNormalToken(criteria,
                            inputType, filter));
                }
            }
            listFilter.setObjectFilter(objectFilter.build());
        }

        return PropertyFilter.newBuilder()
                .setPropertyName(listFieldName)
                .setListFilter(listFilter.build())
                .build();
    }

    /**
     * Create SearchFilter for a normal token (string/numeric) which may contain nested fields.
     * For example: currentToken: "virtualMachineInfoRepoDTO.numCpus"
     *
     * @param currentToken the token which may contain nested fields
     * @param inputType the type of the input from UI, which can be "*" (string) or "#" (number)
     * @param filter the FilterApiDTO which contains values provided by user in UI
     * @return PropertyFilter
     */
    private PropertyFilter createPropertyFilterForNormalToken(@Nonnull String currentToken,
            @Nonnull String inputType, @Nonnull FilterApiDTO filter) {
        final String[] nestedFields = currentToken.split(NESTED_FIELD_DELIMITER);
        // start from last field, create the innermost PropertyFilter
        String lastField = nestedFields[nestedFields.length - 1];
        PropertyFilter currentFieldPropertyFilter;
        switch (inputType) {
            case "s":
            case "s|*":
            case "*|s":
            case "*":
                // string matching
                // the input types "s" and "*" represent exact string matching
                // and regex matching respectively.  their combination allows for both.
                // all cases are treated in the same way here (the distinction is only
                // helpful to the UI side).  we can distinguish the cases by looking at
                // the operator
                final boolean positiveMatch = isPositiveMatchingOperator(filter.getExpType());
                if (isRegexOperator(filter.getExpType())) {
                    currentFieldPropertyFilter =
                        SearchProtoUtil.stringPropertyFilterRegex(
                            lastField,
                            filter.getExpVal(),
                            positiveMatch,
                            filter.getCaseSensitive());
                } else {
                    currentFieldPropertyFilter =
                        SearchProtoUtil.stringPropertyFilterExact(
                            lastField,
                            Arrays.stream(filter.getExpVal().split("\\|"))
                                    .collect(Collectors.toList()),
                            positiveMatch,
                            filter.getCaseSensitive());
                }
                break;
            case "#":
                // numeric comparison
                currentFieldPropertyFilter = SearchProtoUtil.numericPropertyFilter(lastField,
                        Long.valueOf(filter.getExpVal()),
                        COMPARISON_STRING_TO_COMPARISON_OPERATOR.get(filter.getExpType()));
                break;
            default:
                throw new UnsupportedOperationException("Input type: " + inputType +
                        " is not supported");
        }

        // process nested fields from second last in descending order
        for (int i = nestedFields.length - 2; i >= 0; i--) {
            currentFieldPropertyFilter = PropertyFilter.newBuilder()
                    .setPropertyName(nestedFields[i])
                    .setObjectFilter(ObjectFilter.newBuilder()
                            .addFilters(currentFieldPropertyFilter)
                            .build())
                    .build();
        }
        return currentFieldPropertyFilter;
    }

    private static boolean isRegexOperator(@Nonnull String operator) {
        return operator.equals(REGEX_MATCH) || operator.equals(REGEX_NO_MATCH);
    }

    private static boolean isPositiveMatchingOperator(@Nonnull String operator) {
        return operator.equals(REGEX_MATCH) || operator.equals(EQUAL);
    }

    /**
     * Check whether the token is a list token.
     * For example: "commoditySoldList[type=VMem,capacity]".
     *
     * @param token the token to check list for
     * @return true if the token is a list, otherwise false
     */
    private boolean isListToken(@Nonnull String token) {
        int left = token.indexOf('[');
        int right = token.lastIndexOf(']');
        return left != -1 && right != -1 && left < right;
    }

    private SearchParameters.FilterSpecs toFilterSpecs(FilterApiDTO filter) {
        return SearchParameters.FilterSpecs.newBuilder()
                        .setExpressionType(filter.getExpType())
                        .setExpressionValue(filter.getExpVal())
                        .setFilterType(filter.getFilterType())
                        .setIsCaseSensitive(filter.getCaseSensitive())
                        .build();
    }

    /**
     * Converts SearchParameters object to FilterApiDTO
     *
     * @param searchParameters represent one search query
     * @return The {@link FilterApiDTO} object which contains filter rule for dynamic group
     */
    private FilterApiDTO toFilterApiDTO(@Nonnull SearchParameters searchParameters) {

        final FilterApiDTO filterApiDTO = new FilterApiDTO();
        final SearchParameters.FilterSpecs sourceFilter = searchParameters.getSourceFilterSpecs();
        filterApiDTO.setExpType(Objects.requireNonNull(sourceFilter.getExpressionType()));
        filterApiDTO.setExpVal(Objects.requireNonNull(sourceFilter.getExpressionValue()));
        filterApiDTO.setFilterType(Objects.requireNonNull(sourceFilter.getFilterType()));
        filterApiDTO.setCaseSensitive(Objects.requireNonNull(sourceFilter.getIsCaseSensitive()));
        return filterApiDTO;
    }


    /**
     * Context with parameters which SearchFilterProducer needs for all cases
     */
    private static class SearchFilterContext {

        private final FilterApiDTO filter;

        private final Iterator<String> iterator;

        private final String entityType;

        private final String currentToken;

        // the first token of the elements defined in groupBuilderUsecases.json, for example:
        // "PhysicalMachine:displayName:PRODUCES:1", the first token is "PhysicalMachine"
        private final String firstToken;

        public SearchFilterContext(@Nonnull FilterApiDTO filter, @Nonnull Iterator<String> iterator,
                        @Nonnull String entityType, @Nonnull String currentToken, @Nonnull String firstToken) {
            this.filter = Objects.requireNonNull(filter);
            this.iterator = Objects.requireNonNull(iterator);
            this.entityType = Objects.requireNonNull(entityType);
            this.currentToken = Objects.requireNonNull(currentToken);
            this.firstToken = Objects.requireNonNull(firstToken);
        }

        @Nonnull
        public FilterApiDTO getFilter() {
            return filter;
        }

        @Nonnull
        public Iterator<String> getIterator() {
            return iterator;
        }

        @Nonnull
        public String getEntityType() {
            return entityType;
        }

        @Nonnull
        public String getCurrentToken() {
            return currentToken;
        }

        public boolean isHopCountBasedTraverse(@Nonnull StoppingConditionOrBuilder stopper) {
            return !iterator.hasNext() && stopper.hasNumberHops();
        }

        /**
         * Check if this SearchFilter should filter by number of connected vertices. For example:
         * filter PMs by number of hosted VMs.
         */
        public boolean shouldFilterByNumConnectedVertices() {
            return TRAVERSAL_TYPES.contains(firstToken);
        }
    }

    /**
     * Processor for filter which has PRODUCES type of token
     */
    @Immutable
    private static class TraversalFilterProcessor implements Function<SearchFilterContext, List<SearchFilter>> {

        @Override
        public List<SearchFilter> apply(SearchFilterContext context) {
            // add a traversal filter
            TraversalDirection direction = TraversalDirection.valueOf(context.getCurrentToken());
            final StoppingCondition.Builder stopperBuilder;
            final Iterator<String> iterator = context.getIterator();
            final String entityType = context.getEntityType();
            if (iterator.hasNext()) {
                final String currentToken = iterator.next();
                // An explicit stopper can either be the number of hops, or an
                // entity type. And note that hops number can not contains '+' or '-'.
                if (StringUtils.isNumeric(currentToken)) {
                    // For example: Produces:1:VirtualMachine
                    final int hops = Integer.valueOf(currentToken);
                    if (hops <= 0) {
                        throw new IllegalArgumentException("Illegal hops number " + hops
                                        + "; should be positive.");
                    }
                    stopperBuilder = StoppingCondition.newBuilder().setNumberHops(hops);
                    // set condition for number of connected vertices if required
                    if (context.shouldFilterByNumConnectedVertices()) {
                        setVerticesCondition(stopperBuilder, iterator.next(), context);
                    }
                } else {
                    // For example: Produces:VirtualMachine
                    stopperBuilder = buildStoppingCondition(currentToken);
                    // set condition for number of connected vertices if required
                    if (context.shouldFilterByNumConnectedVertices()) {
                        setVerticesCondition(stopperBuilder, currentToken, context);
                    }
                }
            } else {
                stopperBuilder = buildStoppingCondition(entityType);
            }

            TraversalFilter traversal = TraversalFilter.newBuilder()
                            .setTraversalDirection(direction)
                            .setStoppingCondition(stopperBuilder)
                            .build();
            final ImmutableList.Builder<SearchFilter> searchFilters = ImmutableList.builder();
            searchFilters.add(SearchProtoUtil.searchFilterTraversal(traversal));
            // add a final entity type filter if the last filer is a hop-count based traverse
            // and it's not a filter based on number of connected vertices
            // for example: get all PMs which hosted more than 2 VMs, we've already get all PMs
            // if it's a filter by number of connected vertices, we don't need to filter on PM type again
            if (context.isHopCountBasedTraverse(stopperBuilder) && !context.shouldFilterByNumConnectedVertices()) {
                searchFilters.add(SearchProtoUtil.searchFilterProperty(SearchProtoUtil
                                .entityTypeFilter(entityType)));
            }
            return searchFilters.build();
        }

        /**
         * Add vertices condition to the given StoppingCondition. For example, group of PMs by
         * number of hosted VMs, the stopping condition contains number of hops, which is 1. This
         * function add one more condition: filter by number of vms hosted by this PM.
         *
         * @param stopperBuilder the StoppingCondition builder to add vertices condition to
         * @param stoppingEntityType the entity type to count number of connected vertices for
         * when the traversal stops. for example: PMs by number of hosted VMs, then the
         * stoppingEntityType is the integer value of VM entity type
         * @param context the SearchFilterContext with parameters provided by user for the group
         */
        private void setVerticesCondition(@Nonnull StoppingCondition.Builder stopperBuilder,
                @Nonnull String stoppingEntityType, @Nonnull SearchFilterContext context) {
            int vertexEntityType = UIEntityType.fromString(stoppingEntityType).typeNumber();
            stopperBuilder.setVerticesCondition(VerticesCondition.newBuilder()
                    .setNumConnectedVertices(NumericFilter.newBuilder()
                            .setValue(Long.valueOf(context.getFilter().getExpVal()))
                            .setComparisonOperator(COMPARISON_STRING_TO_COMPARISON_OPERATOR.get(
                                    context.getFilter().getExpType())))
                    .setEntityType(vertexEntityType)
                    .build());
        }
    }
}
