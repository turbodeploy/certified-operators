package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class GroupFilterMapper {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The filter used in API when filtering based on group display name.
     */
    public static final String GROUPS_FILTER_TYPE = "groupsByName";

    /**
     * The filter used in API when filtering based on PM cluster display name.
     */
    public static final String CLUSTERS_FILTER_TYPE = "clustersByName";

    /**
     * The filter used in API when filtering based on PM cluster tags.
     */
    public static final String CLUSTERS_BY_TAGS_FILTER_TYPE = "clustersByTag";

    /**
     * The filter used in API when filtering based on storage cluster display name.
     */
    public static final String STORAGE_CLUSTERS_FILTER_TYPE = "storageClustersByName";

    /**
     * The filter used in API when filtering based on vm cluster display name.
     */
    public static final String VIRTUALMACHINE_CLUSTERS_FILTER_TYPE = "virtualMachineClustersByName";

    /**
     * The filter used in API when filtering based on resource group display name.
     */
    public static final String RESOURCE_GROUP_BY_NAME_FILTER_TYPE = "resourceGroupByName";

    /**
     * The filter used in API when filtering based on resource group OID.
     */
    public static final String RESOURCE_GROUP_BY_ID_FILTER_TYPE = "resourceGroupByUuid";

    /**
     * The filter used in API when filtering based on resource group owner account.
     */
    public static final String RESOURCE_GROUP_BY_OWNER_FILTER_TYPE = "resourceGroupByBusinessAccountUuid";

    /**
     * The filter used in API when filtering based on resource groups tags.
     */
    public static final String RESOURCE_GROUP_BY_TAG_FILTER_TYPE = "resourceGroupByTag";

    /**
     * The filter used in API when filtering based on BillingFamily display name.
     */
    public static final String BILLING_FAMILY_FILTER_TYPE = "billingFamilyByName";

    /**
     * A map between group type and property to UI filter that we use for that property. We use
     * different filter for same property of different entity types. For example, for filter based
     * on display name of host cluster we use clustersByName while we use storageClustersByName for
     * display name of storage clusters.
     */
    private static final Table<GroupType, String, String> GROUP_TYPE_AND_PROPERTY_TO_FILTER_TYPE =
                    new ImmutableTable.Builder<GroupType, String, String>()
                    .put(GroupType.REGULAR, SearchableProperties.DISPLAY_NAME, GROUPS_FILTER_TYPE)
                    .put(GroupType.COMPUTE_HOST_CLUSTER, SearchableProperties.DISPLAY_NAME, CLUSTERS_FILTER_TYPE)
                    .put(GroupType.COMPUTE_HOST_CLUSTER, StringConstants.TAGS_ATTR, CLUSTERS_BY_TAGS_FILTER_TYPE)
                    .put(GroupType.STORAGE_CLUSTER, SearchableProperties.DISPLAY_NAME, STORAGE_CLUSTERS_FILTER_TYPE)
                    .put(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER, SearchableProperties.DISPLAY_NAME, VIRTUALMACHINE_CLUSTERS_FILTER_TYPE)
                    .put(GroupType.RESOURCE, SearchableProperties.DISPLAY_NAME, RESOURCE_GROUP_BY_NAME_FILTER_TYPE)
                    .put(GroupType.RESOURCE, StringConstants.TAGS_ATTR, RESOURCE_GROUP_BY_TAG_FILTER_TYPE)
                    .put(GroupType.RESOURCE, StringConstants.ACCOUNTID, RESOURCE_GROUP_BY_OWNER_FILTER_TYPE)
                    .put(GroupType.RESOURCE, StringConstants.OID, RESOURCE_GROUP_BY_ID_FILTER_TYPE)
                    .put(GroupType.BILLING_FAMILY, SearchableProperties.DISPLAY_NAME, BILLING_FAMILY_FILTER_TYPE)
                    .build();

    /**
     * This maps a UI filter type to the property for that it acts on.
     */
    private static final Map<String, String> API_FILTER_TO_PROPERTY = GROUP_TYPE_AND_PROPERTY_TO_FILTER_TYPE
                        .rowMap()
                        .values()
                        .stream()
                        .map(x -> x.entrySet())
                        .flatMap(Set::stream)
                        .collect(Collectors.toMap(Entry::getValue, Entry::getKey));

    /**
     * This maps a UI filter to a function which converts it represented as {@link FilterApiDTO} to
     * its corresponding {@link PropertyFilter}.
     */
    private static final Map<String, Function<FilterApiDTO, Optional<PropertyFilter>>> API_FILTER_TO_CONVERTER =
                    new ImmutableMap.Builder<String, Function<FilterApiDTO, Optional<PropertyFilter>>>()
                    .put(GROUPS_FILTER_TYPE, GroupFilterMapper::convertDisplayNameFilterApiToPropertyFilterFunction)
                    .put(CLUSTERS_FILTER_TYPE, GroupFilterMapper::convertDisplayNameFilterApiToPropertyFilterFunction)
                    .put(STORAGE_CLUSTERS_FILTER_TYPE, GroupFilterMapper::convertDisplayNameFilterApiToPropertyFilterFunction)
                    .put(VIRTUALMACHINE_CLUSTERS_FILTER_TYPE, GroupFilterMapper::convertDisplayNameFilterApiToPropertyFilterFunction)
                    .put(RESOURCE_GROUP_BY_NAME_FILTER_TYPE, GroupFilterMapper::convertDisplayNameFilterApiToPropertyFilterFunction)
                    .put(CLUSTERS_BY_TAGS_FILTER_TYPE, GroupFilterMapper::convertTagFilterApiToPropertyFilterFunction)
                    .put(RESOURCE_GROUP_BY_TAG_FILTER_TYPE, GroupFilterMapper::convertTagFilterApiToPropertyFilterFunction)
                    .put(RESOURCE_GROUP_BY_OWNER_FILTER_TYPE, GroupFilterMapper::convertMultiValueFilterApiToPropertyFilterFunction)
                    .put(RESOURCE_GROUP_BY_ID_FILTER_TYPE, GroupFilterMapper::convertMultiValueFilterApiToPropertyFilterFunction)
                    .put(BILLING_FAMILY_FILTER_TYPE, GroupFilterMapper::convertDisplayNameFilterApiToPropertyFilterFunction)
                    .build();


    /**
     * This maps a property to a bifunction when gets a group type and a {@link PropertyFilter} object and converts
     * it to  {@link FilterApiDTO} used in UI.
     */
    private static final Map<String, BiFunction<GroupType, PropertyFilter, Optional<FilterApiDTO>>> GROUP_TYPE_AND_PROPERTY_TO_CONVERTER =
                    new ImmutableMap.Builder<String, BiFunction<GroupType, PropertyFilter, Optional<FilterApiDTO>>>()
                    .put(SearchableProperties.DISPLAY_NAME, GroupFilterMapper::convertDisplayNamePropertyFilterToApiFilterToFunction)
                    .put(StringConstants.TAGS_ATTR, GroupFilterMapper::convertTagPropertyFilterToApiFilterToFunction)
                    .put(StringConstants.OID, GroupFilterMapper::convertMultiValuePropertyFilterToApiFilterToFunction)
                    .put(StringConstants.ACCOUNTID, GroupFilterMapper::convertMultiValuePropertyFilterToApiFilterToFunction)
                    .build();

    /**
     * Gets a group type and a list of {@link FilterApiDTO} and converts it to a
     * {@link GroupFilter} object which can be used to represent a dynamic group.
     *
     * @param groupType The group type which these filter is applied on.
     * @param criteriaList The list of criteria from UI.
     * @return A {@link GroupFilter} object which represent input constraints.
     * @throws OperationFailedException If the input filters cannot be converted.
     */
    @Nonnull
    public GroupFilter apiFilterToGroupFilter(@Nonnull GroupType groupType,
                    @Nonnull final List<FilterApiDTO> criteriaList)
                                    throws OperationFailedException {

        GroupFilter.Builder groupFilter = GroupFilter.newBuilder();
        //Set the group type in the filter
        groupFilter.setGroupType(groupType);

        // Validate the filters sent and convert them
        for (FilterApiDTO filterApiDTO : criteriaList) {
            // Validate the filter
            if (!GROUP_TYPE_AND_PROPERTY_TO_FILTER_TYPE.row(groupType)
                            .values()
                            .contains(filterApiDTO.getFilterType())) {
                final String errMsg = String.format("Group type `%s` does not support filter `%s`.",
                                groupType, filterApiDTO.getFilterType());
                logger.error(errMsg);
                throw new OperationFailedException(errMsg);
            }

            // get the converter for the filter and apply it.
            final Optional<PropertyFilter> convertedFilter = API_FILTER_TO_CONVERTER
                            .getOrDefault(filterApiDTO.getFilterType(),
                                            filter -> Optional.empty())
                            .apply(filterApiDTO);

            if (convertedFilter.isPresent()) {
                groupFilter.addPropertyFilters(convertedFilter.get());
            } else {
                logger.error("Cannot convert property filter {}",
                                filterApiDTO);
                throw new OperationFailedException("Cannot convert the requested criteria.");
            }
        }

        return groupFilter.build();
    }

    /**
     * Converts a {@link GroupFilter} object to a list of {@link FilterApiDTO} used
     * to represent that filter in the UI.
     *
     * @param groupFilter The object to be converted.
     * @return the converted list.
     */
    @Nonnull
    public List<FilterApiDTO> groupFilterToApiFilters(@Nonnull GroupFilter groupFilter) {
        List<FilterApiDTO> filterApiDtos = new ArrayList<>();
        GroupType groupType = groupFilter.getGroupType();

        for (PropertyFilter propertyFilter : groupFilter.getPropertyFiltersList()) {
            BiFunction<GroupType, PropertyFilter, Optional<FilterApiDTO>> converter =
                            GROUP_TYPE_AND_PROPERTY_TO_CONVERTER.get(propertyFilter.getPropertyName());
            if (converter != null) {
                Optional<FilterApiDTO> filterApiDTO = converter.apply(groupType, propertyFilter);

                if (filterApiDTO.isPresent()) {
                    filterApiDtos.add(filterApiDTO.get());
                } else {
                    logger.error("The property filter `{}` cannot be converted to API filter.",
                                    propertyFilter);
                    throw new IllegalStateException("Cannot convert the requested criteria.");
                }
             } else {
                 logger.error("The property filter `{}` cannot be converted to API filter since its converter cannot be found.",
                                 propertyFilter);
                 throw new IllegalStateException("Cannot convert the requested criteria.");
             }
        }

        return filterApiDtos;
    }

    @Nonnull
    private static Optional<PropertyFilter> convertDisplayNameFilterApiToPropertyFilterFunction(FilterApiDTO input) {
        return Optional.of(SearchProtoUtil.nameFilterRegex(
                        input.getExpVal(),
                        EntityFilterMapper.isPositiveMatchingOperator(input.getExpType()),
                        input.getCaseSensitive()));
    }

    @Nonnull
    private static Optional<PropertyFilter> convertTagFilterApiToPropertyFilterFunction(FilterApiDTO input) {
        final boolean positiveMatch = EntityFilterMapper
            .isPositiveMatchingOperator(input.getExpType());
        if (EntityFilterMapper.isRegexOperator(input.getExpType())) {
            return
                Optional.of(EntityFilterMapper.mapPropertyFilterForMultimapsRegex(
                    StringConstants.TAGS_ATTR, input.getExpVal(), positiveMatch));
        } else {
            return
                Optional.of(EntityFilterMapper.mapPropertyFilterForMultimapsExact(
                    StringConstants.TAGS_ATTR, input.getExpVal(), positiveMatch));
        }
    }

    @Nonnull
    private static Optional<PropertyFilter>  convertMultiValueFilterApiToPropertyFilterFunction(FilterApiDTO input) {
        final boolean positiveMatch = EntityFilterMapper.isPositiveMatchingOperator(input.getExpType());
        final List<String> values = EntityFilterMapper.splitWithEscapes(input.getExpVal(), '|');
        final String propertyName = API_FILTER_TO_PROPERTY.get(input.getFilterType());

        if (propertyName != null) {
            return Optional.of(SearchProtoUtil.stringPropertyFilterExact(propertyName, values,
                positiveMatch, true));
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    private static Optional<FilterApiDTO> convertDisplayNamePropertyFilterToApiFilterToFunction(GroupType groupType,
                                                                                                PropertyFilter propFilter) {
        // the property corresponds to the display name of the cluster
        final StringFilter stringFilter = propFilter.getStringFilter();
        final FilterApiDTO filterApiDTO = new FilterApiDTO();

        // remove leading ^ and trailing $ from the regex
        final String unfilteredRegex = stringFilter.getStringPropertyRegex();
        filterApiDTO.setExpVal(
                unfilteredRegex.substring(1, unfilteredRegex.length() - 1));

        filterApiDTO.setExpType(stringFilter.getPositiveMatch() ? EntityFilterMapper.REGEX_MATCH :
            EntityFilterMapper.REGEX_NO_MATCH);
        filterApiDTO.setCaseSensitive(stringFilter.getCaseSensitive());

        return populateFilterType(groupType, propFilter.getPropertyName(), filterApiDTO);
    }

    @Nonnull
    private static Optional<FilterApiDTO> convertTagPropertyFilterToApiFilterToFunction(GroupType groupType,
                    PropertyFilter propFilter) {
        // the property corresponds to the tags of the cluster
        final MapFilter mapFilter = propFilter.getMapFilter();
        final FilterApiDTO filterApiDTO = new FilterApiDTO();
        if (mapFilter.hasRegex()) {
            // regex matching
            filterApiDTO.setExpVal(
                    mapFilter.getKey() + "=" +
                    mapFilter.getRegex().substring(1, mapFilter.getRegex().length() - 1));
            filterApiDTO.setExpType(mapFilter.getPositiveMatch() ?
                            EntityFilterMapper.REGEX_MATCH :
                            EntityFilterMapper.REGEX_NO_MATCH);
        } else {
            // exact matching
            filterApiDTO.setExpVal(
                    mapFilter.getValuesList().stream()
                            .map(v -> mapFilter.getKey() + "=" + v)
                            .collect(Collectors.joining("|")));
            filterApiDTO.setExpType(mapFilter.getPositiveMatch() ?
                            EntityFilterMapper.EQUAL :
                            EntityFilterMapper.NOT_EQUAL);
        }
        filterApiDTO.setCaseSensitive(false);

        return populateFilterType(groupType, propFilter.getPropertyName(), filterApiDTO);
    }

    @Nonnull
    private static Optional<FilterApiDTO> convertMultiValuePropertyFilterToApiFilterToFunction(GroupType groupType,
                    PropertyFilter propFilter) {
        final FilterApiDTO filterApiDTO = new FilterApiDTO();

        filterApiDTO.setExpVal(
                        propFilter.getStringFilter()
                                .getOptionsList()
                                .stream()
                                .collect(Collectors.joining("|")));

        filterApiDTO.setExpType(propFilter.getStringFilter().getPositiveMatch() ?
                                EntityFilterMapper.EQUAL :
                                EntityFilterMapper.NOT_EQUAL);

        filterApiDTO.setCaseSensitive(propFilter.getStringFilter().getCaseSensitive());

        return populateFilterType(groupType, propFilter.getPropertyName(), filterApiDTO);
    }

    @Nonnull
    private static Optional<FilterApiDTO> populateFilterType(@Nonnull GroupType groupType,
                    @Nonnull String propertyName,  @Nonnull FilterApiDTO filterApiDTO) {
        String filterType = GROUP_TYPE_AND_PROPERTY_TO_FILTER_TYPE.get(groupType, propertyName);

        if (filterType != null) {
            filterApiDTO.setFilterType(filterType);
            return Optional.of(filterApiDTO);
        } else {
            logger.error("Cannot find the associated filter type for {} of {} group type.",
                            groupType);
            return Optional.empty();
        }
    }

}
