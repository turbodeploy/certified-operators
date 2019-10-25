package com.vmturbo.components.test.utilities.utils;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A helper for generating groups according to various criteria.
 */
public class GroupGenerator {
    /**
     * Create a group consisting of all entities of the given type whose
     * display name matches the input.
     *
     * @param entityType The entity type of the desired service entity.
     * @param entityDisplayName The display name of the host.
     * @return The group of only the entities with the given display name.
     */
    public GroupDefinition entityWithName(final EntityType entityType,
                                    @Nonnull final String entityDisplayName) {
        return GroupDefinition.newBuilder()
                .setEntityFilters(EntityFilters.newBuilder()
                        .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(entityType.getNumber())
                                .setSearchParametersCollection(
                                        SearchParametersCollection.newBuilder()
                                                .addSearchParameters(SearchParameters.newBuilder()
                                                        .setStartingFilter(
                                                                entityTypeFilter(entityType))
                                                        .addSearchFilter(displayNameFilter(
                                                                entityDisplayName))))))
                .build();
    }

    /**
     * Create a group of all VMs on the host with the given display name.
     *
     * @param hostDisplayName The display name of the host.
     * @return The group of all VMs on the host with the given display name.
     */
    public GroupDefinition vmsOnHost(@Nonnull final String hostDisplayName) {
        final SearchParameters params = SearchParameters.newBuilder()
                .setStartingFilter(entityTypeFilter(EntityType.PHYSICAL_MACHINE))
                .addSearchFilter(displayNameFilter(hostDisplayName))
                .addSearchFilter(traverseToTypeFilter(EntityType.VIRTUAL_MACHINE))
                .build();
        return GroupDefinition.newBuilder()
                .setEntityFilters(EntityFilters.newBuilder()
                        .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .setSearchParametersCollection(
                                        SearchParametersCollection.newBuilder()
                                                .addSearchParameters(params))))
                .build();
    }

    /**
     * Create a group of all VMs on the storage with the given display name.
     *
     * @param storageDisplayName The group of all VMs on the storage with the given display name.
     * @return The group of all VMs on the storage with the given display name.
     */
    public GroupDefinition vmsOnStorage(@Nonnull final String storageDisplayName) {
        final SearchParameters params = SearchParameters.newBuilder()
                .setStartingFilter(entityTypeFilter(EntityType.STORAGE))
                .addSearchFilter(displayNameFilter(storageDisplayName))
                .addSearchFilter(traverseToTypeFilter(EntityType.VIRTUAL_MACHINE))
                .build();
        return GroupDefinition.newBuilder()
                .setEntityFilters(EntityFilters.newBuilder()
                        .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .setSearchParametersCollection(
                                        SearchParametersCollection.newBuilder()
                                                .addSearchParameters(params))))
                .build();
    }


    /**
     * Create a group of all hosts on the datacenter with the given display name.
     *
     * @param dcDisplayName The display name of the desired datacenter.
     * @return The group of all hosts on the datacenter with the given display name.
     */
    public GroupDefinition hostsOnDatacenter(@Nonnull final String dcDisplayName) {
        final SearchParameters params = SearchParameters.newBuilder()
                .setStartingFilter(entityTypeFilter(EntityType.DATACENTER))
                .addSearchFilter(displayNameFilter(dcDisplayName))
                .addSearchFilter(traverseToTypeFilter(EntityType.PHYSICAL_MACHINE))
                .build();
        return GroupDefinition.newBuilder()
                .setEntityFilters(EntityFilters.newBuilder()
                        .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                                .setSearchParametersCollection(
                                        SearchParametersCollection.newBuilder()
                                                .addSearchParameters(params))))
                .build();
    }

    private PropertyFilter entityTypeFilter(final EntityType entityType) {
        return Search.PropertyFilter.newBuilder()
            .setPropertyName("entityType")
            .setNumericFilter(NumericFilter.newBuilder()
                .setComparisonOperator(ComparisonOperator.EQ)
                .setValue(entityType.getNumber()))
            .build();
    }

    private SearchFilter displayNameFilter(@Nonnull final String displayName) {
        return SearchFilter.newBuilder()
            .setPropertyFilter(PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex(displayName)))
            .build();
    }

    private SearchFilter traverseToTypeFilter(final EntityType entityType) {
        return SearchFilter.newBuilder()
            .setTraversalFilter(TraversalFilter.newBuilder()
                .setTraversalDirection(TraversalDirection.PRODUCES)
                .setStoppingCondition(StoppingCondition.newBuilder()
                    .setStoppingPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("entityType")
                        .setNumericFilter(NumericFilter.newBuilder()
                            .setComparisonOperator(ComparisonOperator.EQ)
                            .setValue(entityType.getNumber())))))
            .build();
    }
}
