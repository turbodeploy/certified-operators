package com.vmturbo.search.metadata.utils;

import static com.vmturbo.api.enums.EntityType.ApplicationComponent;
import static com.vmturbo.api.enums.EntityType.BusinessAccount;
import static com.vmturbo.api.enums.EntityType.BusinessApplication;
import static com.vmturbo.api.enums.EntityType.BusinessTransaction;
import static com.vmturbo.api.enums.EntityType.Container;
import static com.vmturbo.api.enums.EntityType.ContainerPod;
import static com.vmturbo.api.enums.EntityType.DataCenter;
import static com.vmturbo.api.enums.EntityType.Database;
import static com.vmturbo.api.enums.EntityType.DatabaseServer;
import static com.vmturbo.api.enums.EntityType.DiskArray;
import static com.vmturbo.api.enums.EntityType.Namespace;
import static com.vmturbo.api.enums.EntityType.PhysicalMachine;
import static com.vmturbo.api.enums.EntityType.Region;
import static com.vmturbo.api.enums.EntityType.Service;
import static com.vmturbo.api.enums.EntityType.ServiceProvider;
import static com.vmturbo.api.enums.EntityType.Storage;
import static com.vmturbo.api.enums.EntityType.StorageTier;
import static com.vmturbo.api.enums.EntityType.Switch;
import static com.vmturbo.api.enums.EntityType.VirtualDataCenter;
import static com.vmturbo.api.enums.EntityType.VirtualMachine;
import static com.vmturbo.api.enums.EntityType.VirtualVolume;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_TRANSACTION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CONTAINER_POD;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATACENTER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DISK_ARRAY;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.LOGICAL_POOL;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.NAMESPACE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.REGION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.SERVICE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.SERVICE_PROVIDER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_TIER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.SWITCH;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.WORKLOAD_CONTROLLER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.EntityType;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * SearchFilters specification for each pair of entity type. There may be multiple for same
 * pair, since the relationship between them may not be consistent. For example:
 *     1. In vSAN, host consumes storage, however in other cases, storage consumes host;
 *     2. If storage target is added, there may be logical pool between storage and disk array;
 *     3. etc.
 * If multiple set of search filters are provided, all will be tried and results will be merged.
 */
public class SearchFiltersMapper {

    /**
     * SearchFilterSpec for finding related BusinessAccount for an entity.
     */
    public static final SearchFilterSpec RELATED_ACCOUNT_AS_OWNER_FILTER =
            SearchFilterSpec.newBuilder()
                    .addSearchFilters(
                            numberOfHops(TraversalDirection.OWNED_BY, 1),
                            entityTypeFilter(BUSINESS_ACCOUNT))
                    .build();

    /**
     * SearchFilterSpec for finding related region for an entity.
     */
    public static final SearchFilterSpec RELATED_REGION_AS_AGGREGATOR_FILTER =
            SearchFilterSpec.newBuilder()
                    .addSearchFilters(
                            traverseToType(TraversalDirection.AGGREGATED_BY, REGION))
                    .build();

    /**
     * SearchFilterSpec for finding related service provider for an entity.
     */
    public static final SearchFilterSpec RELATED_SERVICE_PROVIDER_AS_AGGREGATOR_FILTER =
            SearchFilterSpec.newBuilder()
                    .addSearchFilters(
                            traverseToType(TraversalDirection.AGGREGATED_BY, SERVICE_PROVIDER))
                    .build();

    /**
     * SearchFilterSpec for finding related BusinessApplication for an entity as consumers.
     */
    public static final SearchFilterSpec RELATED_BUSINESS_APPLICATION_AS_CONSUMER_FILTER =
            SearchFilterSpec.newBuilder()
                    .addSearchFilters(
                            traverseToType(TraversalDirection.PRODUCES, BUSINESS_APPLICATION))
                    .build();

    /**
     * SearchFilterSpec for finding related BusinessTransaction for an entity as consumers.
     */
    public static final SearchFilterSpec RELATED_BUSINESS_TRANSACTION_AS_CONSUMER_FILTER =
            SearchFilterSpec.newBuilder()
                    .addSearchFilters(
                            traverseToType(TraversalDirection.PRODUCES, BUSINESS_TRANSACTION))
                    .build();

    /**
     * SearchFilterSpec for finding related Service for an entity as consumers.
     */
    public static final SearchFilterSpec RELATED_SERVICE_AS_CONSUMER_FILTER =
            SearchFilterSpec.newBuilder()
                    .addSearchFilters(
                            traverseToType(TraversalDirection.PRODUCES, SERVICE))
                    .build();

    /**
     * Private constructor.
     */
    private SearchFiltersMapper() {
    }

    private static final Map<EntityType, Map<EntityType, SearchFilterSpec>> SEARCH_FILTER_SPEC_MAP;

    static {
        final ImmutableMap.Builder<EntityType, Map<EntityType, SearchFilterSpec>>
                searchFilterSpecsByEntityType = ImmutableMap.builder();

        searchFilterSpecsByEntityType.put(BusinessAccount, ImmutableMap.of(
                ServiceProvider, RELATED_SERVICE_PROVIDER_AS_AGGREGATOR_FILTER
        ));

        searchFilterSpecsByEntityType.put(BusinessApplication, ImmutableMap.of(
                BusinessTransaction, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(BUSINESS_TRANSACTION))
                        .build()
        ));

        searchFilterSpecsByEntityType.put(BusinessTransaction, ImmutableMap.of(
                BusinessApplication, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(BUSINESS_APPLICATION))
                        .build()
        ));

        // TODO (OM-63757): we could introduce a new type of filter that stands for
        // "at most x hops", then one traversal is enough to get both entities
        searchFilterSpecsByEntityType.put(Service, ImmutableMap.<EntityType, SearchFilterSpec>builder()
                .put(BusinessApplication, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                // there may be a ba consuming service directly, or through bt,
                                // thus not specifying a hop here
                                traverseToType(TraversalDirection.PRODUCES, BUSINESS_APPLICATION))
                        .build())
                .put(BusinessTransaction, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(BUSINESS_TRANSACTION))
                        .build())
                .build()
        );

        searchFilterSpecsByEntityType.put(ApplicationComponent, ImmutableMap.of(
                BusinessApplication, RELATED_BUSINESS_APPLICATION_AS_CONSUMER_FILTER,
                BusinessTransaction, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 2),
                                entityTypeFilter(BUSINESS_TRANSACTION))
                        .build(),
                Service, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(SERVICE))
                        .build()
        ));

        searchFilterSpecsByEntityType.put(Container, ImmutableMap.of(
                BusinessApplication, RELATED_BUSINESS_APPLICATION_AS_CONSUMER_FILTER,
                BusinessTransaction, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 3),
                                entityTypeFilter(BUSINESS_TRANSACTION))
                        .build(),
                Service, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 2),
                                entityTypeFilter(SERVICE))
                        .build(),
                ContainerPod, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(CONTAINER_POD))
                        .build(),
                Namespace, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(CONTAINER_POD),
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(WORKLOAD_CONTROLLER),
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(NAMESPACE))
                        .build()
        ));

        searchFilterSpecsByEntityType.put(ContainerPod, ImmutableMap.of(
                BusinessApplication, RELATED_BUSINESS_APPLICATION_AS_CONSUMER_FILTER,
                BusinessTransaction, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 4),
                                entityTypeFilter(BUSINESS_TRANSACTION))
                        .build(),
                Service, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 3),
                                entityTypeFilter(SERVICE))
                        .build(),
                Namespace, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(WORKLOAD_CONTROLLER),
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(NAMESPACE))
                        .build(),
                VirtualMachine, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(VIRTUAL_MACHINE))
                        .build()
        ));

        searchFilterSpecsByEntityType.put(Database, ImmutableMap.of(
                BusinessApplication, RELATED_BUSINESS_APPLICATION_AS_CONSUMER_FILTER,
                BusinessTransaction, RELATED_BUSINESS_TRANSACTION_AS_CONSUMER_FILTER,
                Service, RELATED_SERVICE_AS_CONSUMER_FILTER,
                BusinessAccount, RELATED_ACCOUNT_AS_OWNER_FILTER,
                Region, RELATED_REGION_AS_AGGREGATOR_FILTER
        ));

        searchFilterSpecsByEntityType.put(DatabaseServer, ImmutableMap.of(
                BusinessApplication, RELATED_BUSINESS_APPLICATION_AS_CONSUMER_FILTER,
                BusinessTransaction, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 3),
                                entityTypeFilter(BUSINESS_TRANSACTION))
                        .build(),
                Service, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 2),
                                entityTypeFilter(SERVICE))
                        .build(),
                BusinessAccount, RELATED_ACCOUNT_AS_OWNER_FILTER,
                Region, RELATED_REGION_AS_AGGREGATOR_FILTER
        ));

        searchFilterSpecsByEntityType.put(VirtualMachine, ImmutableMap.<EntityType, SearchFilterSpec>builder()
                .put(BusinessApplication, RELATED_BUSINESS_APPLICATION_AS_CONSUMER_FILTER)
                .put(BusinessTransaction, RELATED_BUSINESS_TRANSACTION_AS_CONSUMER_FILTER)
                .put(Service, RELATED_SERVICE_AS_CONSUMER_FILTER)
                .put(ApplicationComponent, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(APPLICATION_COMPONENT))
                        // if there is container and containerpod between app and vm
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 3),
                                entityTypeFilter(APPLICATION_COMPONENT))
                        .build())
                //todo: may need to change if vc volume model is changed
                .put(Storage, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(STORAGE))
                        .build())
                .put(DiskArray, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(STORAGE),
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(DISK_ARRAY))
                        .addSearchFilters(
                                // there may be LogicalPool between storage and disk array, if
                                // storage target is added and stitching happens
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(STORAGE),
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(LOGICAL_POOL),
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(DISK_ARRAY))
                        .build())
                .put(PhysicalMachine, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(PHYSICAL_MACHINE))
                        .build())
                .put(DataCenter, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(PHYSICAL_MACHINE),
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(DATACENTER))
                        .build())
                .put(BusinessAccount, RELATED_ACCOUNT_AS_OWNER_FILTER)
                .put(Region, RELATED_REGION_AS_AGGREGATOR_FILTER)
                .build()
        );

        searchFilterSpecsByEntityType.put(VirtualVolume, ImmutableMap.<EntityType, SearchFilterSpec>builder()
                .put(BusinessApplication, RELATED_BUSINESS_APPLICATION_AS_CONSUMER_FILTER)
                .put(BusinessTransaction, RELATED_BUSINESS_TRANSACTION_AS_CONSUMER_FILTER)
                .put(Service, RELATED_SERVICE_AS_CONSUMER_FILTER)
                .put(Storage, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                //todo: may need to change if vc volume model is changed
                                numberOfHops(TraversalDirection.CONNECTED_TO, 1),
                                entityTypeFilter(STORAGE))
                        .build())
                .put(StorageTier, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(STORAGE_TIER))
                        .build())
                //todo: may need to change if vc volume model is changed
                .put(VirtualMachine, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                // onprem
                                numberOfHops(TraversalDirection.CONNECTED_FROM, 1),
                                entityTypeFilter(VIRTUAL_MACHINE))
                        .addSearchFilters(
                                // cloud
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(VIRTUAL_MACHINE))
                        .build())
                .put(BusinessAccount, RELATED_ACCOUNT_AS_OWNER_FILTER)
                .put(Region, RELATED_REGION_AS_AGGREGATOR_FILTER)
                .put(ServiceProvider, RELATED_SERVICE_PROVIDER_AS_AGGREGATOR_FILTER)
                .build()
        );

        searchFilterSpecsByEntityType.put(Storage, ImmutableMap.of(
                VirtualMachine, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                //todo: may need to change if vc volume model is changed
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(VIRTUAL_MACHINE))
                        .build(),
                PhysicalMachine, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                // used for finding compute cluster for a storage
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(EntityDTO.EntityType.PHYSICAL_MACHINE))
                        .addSearchFilters(
                                // used for finding compute cluster for a storage
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(EntityDTO.EntityType.PHYSICAL_MACHINE))
                        .build(),
                DataCenter, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(PHYSICAL_MACHINE),
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(DATACENTER))
                        .addSearchFilters(
                                // for vsan, storage consumes host
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(PHYSICAL_MACHINE),
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(DATACENTER))
                        .build()
        ));

        searchFilterSpecsByEntityType.put(PhysicalMachine, ImmutableMap.of(
                VirtualMachine, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(VIRTUAL_MACHINE))
                        .build(),
                // for vSAN, host consumes storage and produces vSAN storage
                // so both consumer st and provider st are considered related storages for a host
                Storage, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(STORAGE))
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(STORAGE))
                        .build(),
                Switch, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(SWITCH))
                        .build(),
                DataCenter, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.CONSUMES, 1),
                                entityTypeFilter(DATACENTER))
                        .build()
        ));

        searchFilterSpecsByEntityType.put(VirtualDataCenter, ImmutableMap.of(
                VirtualMachine, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(VIRTUAL_MACHINE))
                        .build()
        ));

        searchFilterSpecsByEntityType.put(DataCenter, ImmutableMap.of(
                PhysicalMachine, SearchFilterSpec.newBuilder()
                        .addSearchFilters(
                                numberOfHops(TraversalDirection.PRODUCES, 1),
                                entityTypeFilter(PHYSICAL_MACHINE))
                        .build()
        ));

        SEARCH_FILTER_SPEC_MAP = searchFilterSpecsByEntityType.build();
    }

    /**
     * Get SearchFilterSpec for the given start entity type and stop entity type.
     *
     * @param startEntityType start entity type
     * @param stopEntityType stop entity type
     * @return {@link SearchFilterSpec}
     */
    public static SearchFilterSpec getSearchFilterSpec(EntityType startEntityType, EntityType stopEntityType) {
        return SEARCH_FILTER_SPEC_MAP.getOrDefault(startEntityType, Collections.emptyMap()).get(stopEntityType);
    }

    /**
     * Get all SearchFilterSpec for the given start entity type grouped by end entity type.
     *
     * @param startEntityType start entity type
     * @return map from stop entity type to {@link SearchFilterSpec}.
     */
    public static Map<EntityType, SearchFilterSpec> getSearchFilterSpecs(EntityType startEntityType) {
        return SEARCH_FILTER_SPEC_MAP.getOrDefault(startEntityType, Collections.emptyMap());
    }

    /**
     * Build a {@link SearchFilter} based on given direction and stop entity type.
     *
     * @param direction traversal direction
     * @param entityType stop entity type
     * @return {@link SearchFilter}
     */
    private static SearchFilter traverseToType(TraversalDirection direction, EntityDTO.EntityType entityType) {
        return SearchFilter.newBuilder()
                .setTraversalFilter(SearchProtoUtil.traverseToType(direction, entityType))
                .build();
    }

    /**
     * Build a {@link SearchFilter} based on given direction and number of hops.
     *
     * @param direction traversal direction
     * @param hops number of hops
     * @return {@link SearchFilter}
     */
    private static SearchFilter numberOfHops(TraversalDirection direction, int hops) {
        return SearchFilter.newBuilder()
                .setTraversalFilter(SearchProtoUtil.numberOfHops(direction, hops))
                .build();
    }

    /**
     * Build a {@link SearchFilter} based on given entity type.
     *
     * @param entityType stop entity type
     * @return {@link SearchFilter}
     */
    private static SearchFilter entityTypeFilter(EntityDTO.EntityType entityType) {
        return SearchFilter.newBuilder()
                .setPropertyFilter(SearchProtoUtil.numericPropertyFilter(
                        SearchableProperties.ENTITY_TYPE, entityType.getNumber(), ComparisonOperator.EQ))
                .build();
    }

    /**
     * Contains a list of search filters set whose results should be combined. It's multiple
     * because there may be different relations between same pair of starting entity type and stop
     * entity type.
     * For example: for vsan case, vsan_st1 consumes host1, and host1 consumes st2, then to get
     * related storages (vsan_st1 and st2) for host1, we need to traverse both consumes & produces
     * and combine the results.
     */
    public static class SearchFilterSpec {
        List<List<SearchFilter>> searchFiltersToCombine;

        private SearchFilterSpec(List<List<SearchFilter>> searchFiltersToCombine) {
            this.searchFiltersToCombine = searchFiltersToCombine;
        }

        public List<List<SearchFilter>> getSearchFiltersToCombine() {
            return searchFiltersToCombine;
        }

        /**
         * Get the new builder for {@link SearchFilterSpec}.
         *
         * @return {@link SearchFilterSpec.Builder}
         */
        public static SearchFilterSpec.Builder newBuilder() {
            return new Builder();
        }

        /**
         * Builder for {@link SearchFilterSpec}.
         */
        private static class Builder {

            List<List<SearchFilter>> searchFiltersToCombine = new ArrayList<>();

            public Builder addSearchFilters(SearchFilter... searchFilters) {
                searchFiltersToCombine.add(Arrays.asList(searchFilters));
                return this;
            }

            public SearchFilterSpec build() {
                return new SearchFilterSpec(Collections.unmodifiableList(searchFiltersToCombine));
            }
        }
    }
}
