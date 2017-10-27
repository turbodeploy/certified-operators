package com.vmturbo.api.component.external.api.mapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase.GroupUseCaseCriteria;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.FilterTypeCase;
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

    public static final String ELEMENTS_DELIMITER = ":";

    public static final String EQUAL = "EQ";
    public static final String NOT_EQUAL = "NEQ";

    private final GroupUseCaseParser groupUseCaseParser;

    public GroupMapper(GroupUseCaseParser groupUseCaseParser) {
        this.groupUseCaseParser = groupUseCaseParser;
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
    private GroupApiDTO createGroupApiDto(@Nonnull final GroupInfo groupInfo) {
        final GroupApiDTO outputDTO = new GroupApiDTO();
        outputDTO.setClassName(GROUP);
        outputDTO.setEntitiesCount(0);

        switch (groupInfo.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                outputDTO.setIsStatic(true);
                outputDTO.setMemberUuidList(
                        groupInfo.getStaticGroupMembers().getStaticMemberOidsList().stream()
                                .map(Object::toString)
                                .collect(Collectors.toList()));
                break;
            case SEARCH_PARAMETERS_COLLECTION:
                outputDTO.setIsStatic(false);
                outputDTO.setMemberUuidList(Collections.emptyList());
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
                outputDTO = createGroupApiDto(group.getGroup());
                break;
            case CLUSTER:
                outputDTO = createClusterApiDto(group.getCluster());
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
     * @param filter a filter
     * @param className class name of the byName filter
     * @return parameters full search parameters
     */
    private SearchParameters filter2parameters(FilterApiDTO filter,
                                               String className) {

        GroupUseCaseCriteria useCase = groupUseCaseParser.getUseCasesByFilterType().get(filter.getFilterType());
        String elements = useCase.getElements();
        if (DISPLAY_NAME.equals(elements)) {
            final PropertyFilter byType = SearchMapper.entityTypeFilter(className);
            final boolean match = filter.getExpType().equals(EQUAL);
            final PropertyFilter byName = SearchMapper.nameFilter(filter.getExpVal(), match);
            SearchParameters parameters = SearchParameters.newBuilder()
                    .setStartingFilter(byType)
                    .addSearchFilter(SearchMapper.searchFilterProperty(byName))
                    .build();
            return parameters;
        }
        // elements is of the form (for example) "PhysicalMachine:PRODUCES"
        String[] elementsArray = elements.split(ELEMENTS_DELIMITER);
        TraversalDirection direction = TraversalDirection.valueOf(elementsArray[1]);
        PropertyFilter seTypeFilter = SearchMapper.entityTypeFilter(elementsArray[0]);
        PropertyFilter seNameFilter = SearchMapper.nameFilter(filter.getExpVal(),
            filter.getExpType().equals(EQUAL));
        PropertyFilter classNameFilter = SearchMapper.entityTypeFilter(className);
        int hops = elementsArray.length > 2 ? Integer.valueOf(elementsArray[2]) : 0;
        Function<StoppingCondition.Builder, StoppingCondition.Builder> condition =
                builder -> (hops > 0
                        ? builder.setNumberHops(hops)
                        : builder.setStoppingPropertyFilter(classNameFilter));
        StoppingCondition stop = condition.apply(StoppingCondition.newBuilder()).build();
        TraversalFilter traversal = TraversalFilter.newBuilder()
                .setTraversalDirection(direction)
                .setStoppingCondition(stop)
                .build();
        SearchParameters.Builder parametersBuilder = SearchParameters.newBuilder()
                .setStartingFilter(seTypeFilter)
                .addSearchFilter(SearchMapper.searchFilterProperty(seNameFilter))
                .addSearchFilter(SearchMapper.searchFilterTraversal(traversal));
        if (hops > 0) {
            parametersBuilder.addSearchFilter(SearchMapper.searchFilterProperty(classNameFilter));
        }
        return parametersBuilder.build();
    }

    /**
     * Convert one set of search parameters to a filter DTO, use group entity type to get related useCase json map value,
     * and use starting filter's entity type to get filter type. And also through the first byName
     * search filter to get filter expression value.
     *
     * @param searchParameters represent one search query
     * @param entityType the entity type of group
     * @return The {@link FilterApiDTO} object which contains filter rule for dynamic group
     */
    private FilterApiDTO generateFilterApiDTO(SearchParameters searchParameters, final String entityType) {
        final Map<String, String> useCaseByEntityType = groupUseCaseParser.getUseCases().entrySet().stream()
                .filter(entry -> entry.getKey().equals(entityType))
                .map(Entry::getValue)
                .map(GroupUseCase::getCriteria)
                .flatMap(List::stream)
                .collect(Collectors.toMap(map -> map.getElements().split(ELEMENTS_DELIMITER)[0],
                    GroupUseCaseCriteria::getFilterType));

        PropertyFilter startingPropertyFilter = searchParameters.getStartingFilter();
        final String startFilterEntityName = ServiceEntityMapper.toUIEntityType(
                Math.toIntExact(startingPropertyFilter.getNumericFilter().getValue()));
        FilterApiDTO filterApiDTO = new FilterApiDTO();

        final Optional<String> filterType = Optional.ofNullable(useCaseByEntityType.get(startFilterEntityName));
        filterApiDTO.setFilterType(filterType.orElse(useCaseByEntityType.get(DISPLAY_NAME)));
        Optional<PropertyFilter> entityValueFilter = searchParameters.getSearchFilterList().stream()
                .filter(searchFilter -> searchFilter.getFilterTypeCase() == FilterTypeCase.PROPERTY_FILTER)
                .filter(searchFilter -> searchFilter.getPropertyFilter().getPropertyName().equals(DISPLAY_NAME))
                .map(SearchFilter::getPropertyFilter)
                .findFirst();

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
