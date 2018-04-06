package com.vmturbo.api.component.external.api.mapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase;
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
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;

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
                .map(searchParameters -> generateFilterApiDTO(searchParameters, entityType))
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
     * @param className class name of the byName filter
     * @return parameters full search parameters
     */
    private SearchParameters filter2parameters(FilterApiDTO filter,
                                               String className) {
        GroupUseCaseCriteria useCase = groupUseCaseParser.getUseCasesByFilterType().get(filter.getFilterType());

        // build the search parameters based on the filter criteria
        SearchParameters.Builder parametersBuilder = SearchParameters.newBuilder();

        String[] elements = useCase.getElements().split(ELEMENTS_DELIMITER);

        int currentTokenIndex = 0;
        while (currentTokenIndex < elements.length) {
            String currentToken = elements[currentTokenIndex];
            switch(currentToken) {
                case DISPLAY_NAME:
                    PropertyFilter nameFilter = SearchMapper.nameFilter(filter.getExpVal(),
                            filter.getExpType().equals(EQUAL));
                    if (parametersBuilder.hasStartingFilter()) {
                        parametersBuilder.addSearchFilter(SearchMapper.searchFilterProperty(nameFilter));
                    } else {
                        parametersBuilder.setStartingFilter(nameFilter);
                    }
                    currentTokenIndex++;
                    break;
                case CLUSTER:
                    // add a cluster member filter. Currently this assumes a name matcher, but
                    // we may want to revisit this assumption if/when we support other cluster
                    // identification strategies, such as by tags or other properties.
                    // Also, the cluster filter is not expected to be used when the display name
                    // filter is used. Technically there won't be an issue, but since both of these
                    // filters use the same filter expression, combining them will probably lead to
                    // non-sensical results.
                    ClusterMembershipFilter clusterFilter = SearchMapper.clusterFilter(
                            SearchMapper.nameFilter(filter.getExpVal(), filter.getExpType().equals(EQUAL)));
                    // ClusterMembershipFilter could technically work as a start filter, since it
                    // reduces to a PropertyFilter after the membership is resolved. It could produce
                    // a faster query than an entity-type-filter-first. But we'd need to update the
                    // SearchParameters to allow ClusterMembershipFilters as start filters first,
                    // and that is a task for another time. (anotehr option is to remove the idea
                    // of a separate start filter from the data structure altogher and instead rely
                    // on a data validation to restrict or warn about unexpected filter types from
                    // being placed at the head of the search param list.
                    parametersBuilder.addSearchFilter(SearchMapper.searchFilterCluster(clusterFilter));
                    currentTokenIndex++;
                    break;
                case CONSUMES:
                case PRODUCES:
                    // add a traversal filter
                    TraversalDirection direction = TraversalDirection.valueOf(currentToken);
                    currentTokenIndex++;
                    final StoppingCondition stopper;
                    if (currentTokenIndex < elements.length) {
                        // An explicit stopper can either be the number of hops, or an
                        // entity type. And note that hops number can not contains '+' or '-'.
                        if (StringUtils.isNumeric(elements[currentTokenIndex])) {
                            final int hops = Integer.valueOf(elements[currentTokenIndex]);
                            if (hops <= 0) {
                                throw new IllegalArgumentException("Illegal hops number " + hops
                                        + "; should be positive.");
                            }
                            stopper = StoppingCondition.newBuilder().setNumberHops(hops).build();
                        } else {
                            final String stopEntityType = elements[currentTokenIndex];
                            stopper = StoppingCondition.newBuilder()
                                    .setStoppingPropertyFilter(SearchMapper.entityTypeFilter(stopEntityType))
                                    .build();
                        }
                        currentTokenIndex++;
                    } else {
                        stopper = StoppingCondition.newBuilder()
                                .setStoppingPropertyFilter(SearchMapper.entityTypeFilter(className))
                                .build();
                    }
                    TraversalFilter traversal = TraversalFilter.newBuilder()
                            .setTraversalDirection(direction)
                            .setStoppingCondition(stopper)
                            .build();
                    parametersBuilder.addSearchFilter(SearchMapper.searchFilterTraversal(traversal));
                    // add a final entity type filter if the last filer is a hop-count based traverse
                    if (currentTokenIndex >= elements.length && stopper.hasNumberHops()) {
                        parametersBuilder.addSearchFilter(
                                SearchMapper.searchFilterProperty(
                                        SearchMapper.entityTypeFilter(className)));
                    }
                    break;
                default:
                    // the token should map to an entity type. This type will be used as the
                    // starting filter, if one wasn't set yet, otherwise it will be added as a
                    // search param.
                    PropertyFilter entityTypeFilter = SearchMapper.entityTypeFilter(currentToken);
                    if (parametersBuilder.hasStartingFilter()) {
                        parametersBuilder.addSearchFilter(SearchMapper.searchFilterProperty(entityTypeFilter));
                    } else {
                        parametersBuilder.setStartingFilter(entityTypeFilter);
                    }
                    currentTokenIndex++;
                    break;
            }
            if (! parametersBuilder.hasStartingFilter()) {
                // log a warning that no starting filter was set yet.
                logger.warn("Starting filter wasn't specified yet after processing token {} from ({})",
                        currentToken, useCase.getElements());
            }
        }
        return parametersBuilder.build();
    }

    /**
     * Convert one set of search parameters to a filter DTO, use group entity type to get related
     * useCase json map value, and use starting filter's entity type to get filter type. And also
     * through the first byName search filter to get filter expression value.
     *
     * TODO: The logic for deriving the original filter type by reverse engineering the search params
     * list might be fragile in the long-term. As we accumulate new filter types and new filtering
     * options, it will be harder and harder to keep this method and filter2parameters in sync. An
     * optimization that Roman and I (Patrick) discussed was to store the original filter type
     * key in the SearchParameters object and boil this function down to a single lookup of that
     * key in the use cases map. If we come back to make any other changes to these functions, we
     * should make time to apply that simplification.
     *
     * @param searchParameters represent one search query
     * @param entityType the entity type of group
     * @return The {@link FilterApiDTO} object which contains filter rule for dynamic group
     */
    private FilterApiDTO generateFilterApiDTO(SearchParameters searchParameters, final String entityType) {
        // map of initial filter element -> filter name for this entity type. This is what we are trying to rediscover.
        final Map<String, String> useCaseByEntityType = groupUseCaseParser.getUseCases().entrySet().stream()
                .filter(entry -> entry.getKey().equals(entityType))
                .map(Entry::getValue)
                .map(GroupUseCase::getCriteria)
                .flatMap(List::stream)
                .collect(Collectors.toMap(GroupUseCaseCriteria::getElements,
                    GroupUseCaseCriteria::getFilterType));

        // try to regenerate the use case "elements" based on the search filters, so we can use this
        // to do a map lookup to find the original filter type.
        StringJoiner elementsJoiner = new StringJoiner(":");
        // entityValueFilter is used to extract the dynamic part of the filter input -- this filter
        // would contain the property value being filtered on, such as entity name, cluster name,
        // property value threshold, etc.
        Optional<PropertyFilter> entityValueFilter = Optional.empty();

        // first add the starting filter element, which we currently assume to be either display
        // name or entity type.
        PropertyFilter startFilter = searchParameters.getStartingFilter();
        switch (startFilter.getPropertyName()) {
            case SearchMapper.ENTITY_TYPE_PROPERTY:
                // an entity type filter element is the entity type name
                if (startFilter.hasNumericFilter()) {
                    // create an entity type element based on numeric match
                    elementsJoiner.add(ServiceEntityMapper.toUIEntityType(
                            Math.toIntExact(startFilter.getNumericFilter().getValue())));
                } else { // create one based on string matching
                    elementsJoiner.add(startFilter.getStringFilter().getStringPropertyRegex());
                }
                break;
            case SearchMapper.DISPLAY_NAME_PROPERTY:
                // the display name filter element is just 'displayName'
                elementsJoiner.add(SearchMapper.DISPLAY_NAME_PROPERTY);
                break;
        }

        // add elements for each of the search filters
        for(SearchFilter sf : searchParameters.getSearchFilterList()) {
            switch (sf.getFilterTypeCase()) {
                case CLUSTER_MEMBERSHIP_FILTER:
                    // add a cluster filter element and use the expression from the cluster matcher
                    elementsJoiner.add(CLUSTER);
                    entityValueFilter = Optional.of(sf.getClusterMembershipFilter().getClusterSpecifier());
                    break;
                case TRAVERSAL_FILTER:
                    // add a traversal filter element
                    TraversalFilter traversalFilter = sf.getTraversalFilter();
                    String name = traversalFilter.getTraversalDirection().getValueDescriptor().getName();
                    elementsJoiner.add(traversalFilter.getTraversalDirection().getValueDescriptor().getName());
                    // any hops? Add those too
                    if (traversalFilter.getStoppingCondition().hasNumberHops()) {
                        elementsJoiner.add(String.valueOf(traversalFilter.getStoppingCondition().getNumberHops()));
                    }
                    break;
                case PROPERTY_FILTER:
                    PropertyFilter propertyFilter = sf.getPropertyFilter();
                    elementsJoiner.add(propertyFilter.getPropertyName());
                    // we don't expect both a cluster filter and property filter in the same list since
                    // only one would use the expression
                    entityValueFilter = Optional.of(propertyFilter);
                    break;
            }
        }
        String filterElements = elementsJoiner.toString();

        FilterApiDTO filterApiDTO = new FilterApiDTO();

        // try to find the filter name from the use cases map
        final Optional<String> filterType = Optional.ofNullable(useCaseByEntityType.get(filterElements));
        filterApiDTO.setFilterType(filterType.orElse(
                useCaseByEntityType.get(entityType + ELEMENTS_DELIMITER + DISPLAY_NAME))); // default filter

        entityValueFilter.ifPresent(valueFilter -> {
            switch (valueFilter.getPropertyTypeCase()) {
                case STRING_FILTER:
                    final StringFilter stringFilter = valueFilter.getStringFilter();
                    final String expressionType = stringFilter.getMatch() ? EQUAL : NOT_EQUAL;
                    filterApiDTO.setExpType(expressionType);
                    filterApiDTO.setExpVal(stringFilter.getStringPropertyRegex());
                    break;
                case NUMERIC_FILTER:
                    filterApiDTO.setExpType(valueFilter.getNumericFilter().getComparisonOperator().toString());
                    filterApiDTO.setExpVal(String.valueOf(valueFilter.getNumericFilter().getValue()));
                    break;
                default:
                    logger.error("Unknown PropertyTypeCase: " + valueFilter.getPropertyTypeCase());
            }
        });
        return filterApiDTO;
    }
}
