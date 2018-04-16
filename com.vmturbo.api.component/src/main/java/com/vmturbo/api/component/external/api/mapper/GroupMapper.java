package com.vmturbo.api.component.external.api.mapper;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdk.nashorn.internal.ir.annotations.Immutable;

import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase.GroupUseCaseCriteria;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.components.common.ClassicEnumMapper;

/**
 * Maps groups between their API DTO representation and their protobuf representation.
 */
public class GroupMapper {
    private static final Logger logger = LogManager.getLogger();

    public static final String GROUP = "Group";
    public static final String CLUSTER = "Cluster";
    public static final String STORAGE_CLUSTER = "StorageCluster";

    public static final String DISPLAY_NAME = "displayName";

    private static final String CONSUMES = "CONSUMES";
    private static final String PRODUCES = "PRODUCES";

    public static final String ELEMENTS_DELIMITER = ":";

    public static final String EQUAL = "EQ";
    public static final String NOT_EQUAL = "NEQ";
    public static final String STATE = "state";

    private static final Map<String, Function<SearchFilterContext, List<SearchFilter>>> FILTER_TYPES_TO_PROCESSORS;

    static {
        final TraversalFilterProcessor traversalFilterProcessor = new TraversalFilterProcessor();
        FILTER_TYPES_TO_PROCESSORS = ImmutableMap.of(
        DISPLAY_NAME, (context) -> {
            final PropertyFilter propertyFilter = SearchMapper.nameFilter(context.getFilter().getExpVal(),
                            context.getFilter().getExpType().equals(EQUAL));
            return ImmutableList.of(SearchMapper.searchFilterProperty(propertyFilter));
        },
        STATE, (context) -> {
            final PropertyFilter stateFilter = SearchMapper.stateFilter(context.getFilter().getExpVal(),
                            context.getFilter().getExpType().equals(EQUAL));
            return ImmutableList.of(SearchMapper.searchFilterProperty(stateFilter));
        },
        CLUSTER, (context) -> {
            ClusterMembershipFilter clusterFilter = SearchMapper.clusterFilter(
                            SearchMapper.nameFilter(context.getFilter().getExpVal(),
                                            context.getFilter().getExpType().equals(EQUAL)));
            return ImmutableList.of(SearchMapper.searchFilterCluster(clusterFilter));
        },
        CONSUMES, traversalFilterProcessor,
        PRODUCES, traversalFilterProcessor);
    }

    private static StoppingCondition buildStoppingCondition(String currentToken) {
        return StoppingCondition.newBuilder().setStoppingPropertyFilter(
                        SearchMapper.entityTypeFilter(currentToken)).build();
    }

    private final GroupUseCaseParser groupUseCaseParser;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final GroupExpander groupExpander;

    public GroupMapper(@Nonnull final GroupUseCaseParser groupUseCaseParser,
                       @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                       @Nonnull final GroupExpander groupExpander) {
        this.groupUseCaseParser = Objects.requireNonNull(groupUseCaseParser);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.groupExpander = Objects.requireNonNull(groupExpander);
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
        } else if (CollectionUtils.isEmpty(apiDTO.getScope())) {
            // At the time of this writing temporary groups are always created from some scope.
            throw new InvalidOperationException("No scope for temp group " + apiDTO.getDisplayName());
        }

        // Derive the members from the scope
        final Map<String, SupplyChainNode> supplyChainForScope =
                supplyChainFetcherFactory.newNodeFetcher()
                        .addSeedUuids(apiDTO.getScope())
                        .entityTypes(Collections.singletonList(apiDTO.getGroupType()))
                        .fetch();
        final SupplyChainNode node = supplyChainForScope.get(apiDTO.getGroupType());
        if (node == null) {
            throw new InvalidOperationException("Group type: " + apiDTO.getGroupType() +
                " not found in supply chain for scopes: " + apiDTO.getScope());
        }

        TempGroupInfo.Builder tempGroupInfoBuilder = TempGroupInfo.newBuilder()
                .setEntityType(ServiceEntityMapper.fromUIEntityType(apiDTO.getGroupType()))
                .setMembers(StaticGroupMembers.newBuilder()
                        .addAllStaticMemberOids(RepositoryDTOUtil.getAllMemberOids(node)))
                .setName(apiDTO.getDisplayName());
        // check if the temp group's scope is Market or not.
        final boolean isGlobalScopeGroup = apiDTO.getScope().size() == 1 &&
                apiDTO.getScope().get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR);
        tempGroupInfoBuilder.setIsGlobalScopeGroup(isGlobalScopeGroup);
        return tempGroupInfoBuilder.build();
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
                .setEntityType(ServiceEntityMapper.fromUIEntityType(groupDto.getGroupType()));

        if (groupDto.getIsStatic()) {
            requestBuilder.setStaticGroupMembers(
                    StaticGroupMembers.newBuilder().addAllStaticMemberOids(
                            groupDto.getMemberUuidList().stream().map(Long::parseLong).collect(Collectors.toList())));
        } else {
            requestBuilder
                    .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                            .addAllSearchParameters(convertToSearchParameters(groupDto, groupDto.getGroupType())))
                    .build();
        }

        return requestBuilder.build();
    }

    @Nonnull
    private GroupApiDTO createGroupApiDto(@Nonnull final Group group) {
        GroupInfo groupInfo = group.getGroup();
        final GroupApiDTO outputDTO = new GroupApiDTO();
        outputDTO.setClassName(GROUP);
        outputDTO.setEntitiesCount(0);

        switch (groupInfo.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                outputDTO.setIsStatic(true);
                outputDTO.setEntitiesCount(
                    groupInfo.getStaticGroupMembers().getStaticMemberOidsCount());
                outputDTO.setMemberUuidList(
                        groupInfo.getStaticGroupMembers().getStaticMemberOidsList().stream()
                                .map(Object::toString)
                                .collect(Collectors.toList()));
                break;
            case SEARCH_PARAMETERS_COLLECTION:
                List<String> groupMembers =
                    groupExpander.expandUuids(ImmutableSet.of(String.valueOf(group.getId())))
                                    .stream()
                                    .map(member -> Long.toString(member))
                                    .collect(Collectors.toList());
                outputDTO.setIsStatic(false);
                outputDTO.setMemberUuidList(groupMembers);
                outputDTO.setEntitiesCount(groupMembers.size());
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
    private GroupApiDTO createClusterApiDto(@Nonnull final ClusterInfo clusterInfo) {
        final GroupApiDTO outputDTO = new GroupApiDTO();
        outputDTO.setClassName(CLUSTER);
        // Not clear if there should be a difference between these.
        outputDTO.setEntitiesCount(clusterInfo.getMembers().getStaticMemberOidsCount());
        outputDTO.setMembersCount(clusterInfo.getMembers().getStaticMemberOidsCount());
        outputDTO.setIsStatic(true);
        outputDTO.setMemberUuidList(clusterInfo.getMembers().getStaticMemberOidsList().stream()
            .map(Object::toString)
            .collect(Collectors.toList()));
        return outputDTO;
    }

    @Nonnull
    private GroupApiDTO createTempGroupApiDTO(@Nonnull final TempGroupInfo tempGroupInfo) {
        final GroupApiDTO outputDTO = new GroupApiDTO();
        outputDTO.setClassName(GROUP);
        // Not clear if there should be a difference between these.
        outputDTO.setEntitiesCount(tempGroupInfo.getMembers().getStaticMemberOidsCount());
        outputDTO.setMembersCount(tempGroupInfo.getMembers().getStaticMemberOidsCount());
        outputDTO.setIsStatic(true);
        outputDTO.setMemberUuidList(tempGroupInfo.getMembers().getStaticMemberOidsList().stream()
                .map(Object::toString)
                .collect(Collectors.toList()));
        outputDTO.setTemporary(true);
        return outputDTO;
    }

    /**
     * Converts from {@link Group} to {@link GroupApiDTO}.
     *
     * @param group The {@link Group} object
     * @return  The {@link GroupApiDTO} object
     */
    public GroupApiDTO toGroupApiDto(@Nonnull final Group group) {
        final GroupApiDTO outputDTO;
        switch (group.getType()) {
            case GROUP:
                outputDTO = createGroupApiDto(group);
                break;
            case CLUSTER:
                outputDTO = createClusterApiDto(group.getCluster());
                break;
            case TEMP_GROUP:
                outputDTO = createTempGroupApiDTO(group.getTempGroup());
                break;
            default:
                throw new IllegalArgumentException("Unrecognized group type: " + group.getType());
        }

        outputDTO.setDisplayName(GroupProtoUtil.getGroupName(group));
        outputDTO.setGroupType(ServiceEntityMapper.toUIEntityType(GroupProtoUtil.getEntityType(group)));
        outputDTO.setUuid(Long.toString(group.getId()));

        // XL RESTRICTION: Only ONPREM entities for now. see com.vmturbo.platform.VMTRoot.
        //     OperationalEntities.PresentationLayer.Services.impl.ServiceEntityUtilsImpl
        //     #getEntityInformation() to determine environmentType
        outputDTO.setEnvironmentType(EnvironmentType.ONPREM);


        return outputDTO;
    }

    /**
     * Convert a {@link GroupApiDTO} to a list of search parameters. Right now the entity type sources
     * have inconsistency between search service with group service requests. For search service
     * request, UI use class name field store entityType, but for creating group request,
     * UI use groupType field store entityType.
     *
     * @param inputDTO received from the UI
     * @param entityType the name of entity type, such as VirtualMachine
     * @return list of search parameters to use for querying the repository
     */
    public List<SearchParameters> convertToSearchParameters(GroupApiDTO inputDTO, String entityType) {
        final List<FilterApiDTO> criteriaList = inputDTO.getCriteriaList();
        Optional<List<FilterApiDTO>> filterApiDTOList =
                (criteriaList != null && !criteriaList.isEmpty())
                        ? Optional.of(criteriaList)
                        : Optional.empty();
        return filterApiDTOList
                .map(filterApiDTOs -> filterApiDTOs.stream()
                        .map(filterApiDTO -> filter2parameters(filterApiDTO, entityType))
                        .collect(Collectors.toList()))
                .orElse(ImmutableList.of(searchParametersForEmptyCriteria(entityType)));
    }

    /**
     * Convert a {@link GroupInfo} to a list of FilterApiDTO.
     *
     * @param groupInfo {@link GroupInfo} A message contains all information to represent a static or
     *                  dynamic group
     * @return a list of FilterApiDTO which contains different filter rules for dynamic group
     */
    public List<FilterApiDTO> convertToFilterApis(GroupInfo groupInfo) {
        final String entityType = ServiceEntityMapper.toUIEntityType(groupInfo.getEntityType());

        return groupInfo.getSearchParametersCollection()
                .getSearchParametersList().stream()
                .map(searchParameters -> toFilterApiDTO(searchParameters))
                .collect(Collectors.toList());
    }

    /**
     * Generate search parameters for getting all entities under current entity type. It handles the edge case
     * when users create dynamic group, not specify any criteria rules, this dynamic group should
     * contains all entities under selected type.
     *
     * @param entityType entity type from UI
     * @return a search parameters object only contains starting filter with entity type
     */
    private SearchParameters searchParametersForEmptyCriteria(String entityType) {
        PropertyFilter byType = SearchMapper.entityTypeFilter(entityType);
        return SearchParameters.newBuilder().setStartingFilter(byType).build();
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
     * @return parameters full search parameters
     */
    private SearchParameters filter2parameters(FilterApiDTO filter,
                                               String entityType) {
        GroupUseCaseCriteria useCase = groupUseCaseParser.getUseCasesByFilterType().get(filter.getFilterType());

        if (useCase == null) {
            throw new IllegalArgumentException("Not existing filter type provided: " + filter.getFilterType());
        }
        // build the search parameters based on the filter criteria
        SearchParameters.Builder parametersBuilder = SearchParameters.newBuilder();

        final List<String> elements = Arrays.asList(useCase.getElements().split(ELEMENTS_DELIMITER));

        Iterator<String> iterator = elements.iterator();
        final String firstToken = iterator.next();
        if (ClassicEnumMapper.ENTITY_TYPE_MAPPINGS.keySet().contains(firstToken)) {
            parametersBuilder.setStartingFilter(SearchMapper.entityTypeFilter(firstToken));
        } else {
            parametersBuilder.setStartingFilter(SearchMapper.entityTypeFilter(entityType));
            iterator = elements.iterator();
        }

        final ImmutableList.Builder<SearchFilter> searchFilters = new ImmutableList.Builder<>();
        while (iterator.hasNext()) {
            searchFilters.addAll(processToken(filter, entityType, iterator));
        }
        parametersBuilder.addAllSearchFilter(searchFilters.build());
        parametersBuilder.setSourceFilterSpecs(toFilterSpecs(filter));
        return parametersBuilder.build();
    }

    private List<SearchFilter> processToken(@Nonnull FilterApiDTO filter,
                    @Nonnull String entityType, @Nonnull Iterator<String> iterator) {
        final String currentToken = iterator.next();
        final SearchFilterContext filterContext = new SearchFilterContext(filter, iterator, entityType);

        final Function<SearchFilterContext, List<SearchFilter>> filterApiDtoProcessor =
                        FILTER_TYPES_TO_PROCESSORS.get(currentToken);
        return filterApiDtoProcessor != null
                        ? filterApiDtoProcessor.apply(filterContext)
                        : ImmutableList.of(SearchMapper.searchFilterProperty(
                                        SearchMapper.entityTypeFilter(currentToken)));
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

        public SearchFilterContext(@Nonnull FilterApiDTO filter, @Nonnull Iterator<String> iterator,
                        @Nonnull String entityType) {
            this.filter = Objects.requireNonNull(filter);
            this.iterator = Objects.requireNonNull(iterator);
            this.entityType = Objects.requireNonNull(entityType);
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

        public boolean isHopCountBasedTraverse(@Nonnull StoppingCondition stopper) {
            return !iterator.hasNext() && stopper.hasNumberHops();
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
            TraversalDirection direction = TraversalDirection.valueOf(PRODUCES);
            final StoppingCondition stopper;
            final Iterator<String> iterator = context.getIterator();
            final String entityType = context.getEntityType();
            if (iterator.hasNext()) {
                final String currentToken = iterator.next();
                // An explicit stopper can either be the number of hops, or an
                // entity type. And note that hops number can not contains '+' or '-'.
                if (StringUtils.isNumeric(currentToken)) {
                    final int hops = Integer.valueOf(currentToken);
                    if (hops <= 0) {
                        throw new IllegalArgumentException("Illegal hops number " + hops
                                        + "; should be positive.");
                    }
                    stopper = StoppingCondition.newBuilder().setNumberHops(hops).build();
                } else {
                    stopper = buildStoppingCondition(currentToken);
                }
            } else {
                stopper = buildStoppingCondition(entityType);
            }
            TraversalFilter traversal = TraversalFilter.newBuilder()
                            .setTraversalDirection(direction)
                            .setStoppingCondition(stopper)
                            .build();
            final ImmutableList.Builder<SearchFilter> searchFilters = ImmutableList.builder();
            searchFilters.add(SearchMapper.searchFilterTraversal(traversal));
            // add a final entity type filter if the last filer is a hop-count based traverse
            if (context.isHopCountBasedTraverse(stopper)) {
                searchFilters.add(SearchMapper.searchFilterProperty(SearchMapper
                                .entityTypeFilter(entityType)));
            }
            return searchFilters.build();
        }
    }
}
