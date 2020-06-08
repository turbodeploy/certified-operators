package com.vmturbo.history.db;

import static com.vmturbo.history.db.EntityType.UseCase.NON_PRICE_STATS;
import static com.vmturbo.history.db.EntityType.UseCase.NON_ROLLUP_STATS;
import static com.vmturbo.history.db.EntityType.UseCase.PersistEntity;
import static com.vmturbo.history.db.EntityType.UseCase.STANDARD_STATS;
import static com.vmturbo.history.db.EntityType.UseCase.Spend;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.Table;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.history.db.EntityType.UseCase;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Information about all the entity types processed by the history component.
 */
public class EntityTypeDefinitions {
    private EntityTypeDefinitions() {
    }

    // names for entity types that do not have corresponding ApiEntityType instances.
    /** APPLICATION_SPEND entity type name. */
    private static final String APPLICATION_SPEND = "ApplicationSpend";
    /** VIRTUAL_MACHINE_SPEND entity type name. */
    private static final String VIRTUAL_MACHINE_SPEND = "VirtualMachineSpend";

    // following constants are used as aliases and/or in construction of data structures used in
    // this class and/or in tests, so they're defined here with variables and then incorporated by
    // reference in the ENTITY_TYPE_DEFINITIONS list below
    private static final EntityType APPLICATION_ENTITY_TYPE = create(ApiEntityType.APPLICATION, "app_stats", STANDARD_STATS);
    static final EntityType
            APPLICATION_SPEND_ENTITY_TYPE = create(APPLICATION_SPEND, "app_spend", Spend);
    private static final EntityType CONTAINER_POD_ENTITY_TYPE = create(ApiEntityType.CONTAINER_POD, "cpod_stats", STANDARD_STATS);
    // PM must precede DATACENTER for aliasing
    private static final EntityType PHYSICAL_MACHINE_ENTITY_TYPE = create(ApiEntityType.PHYSICAL_MACHINE, "pm_stats", STANDARD_STATS);
    private static final EntityType DATA_CENTER_ENTITY_TYPE = create(ApiEntityType.DATACENTER, PHYSICAL_MACHINE_ENTITY_TYPE, NON_ROLLUP_STATS);
    private static final EntityType VIRTUAL_DATACENTER_ENTITY_TYPE = create(ApiEntityType.VIRTUAL_DATACENTER, "vdc_stats", STANDARD_STATS);
    private static final EntityType VIRTUAL_MACHINE_SPEND_ENTITY_TYPE = create(VIRTUAL_MACHINE_SPEND, "vm_spend", Spend);

    // this is where all the rest of the entity types are defined
    static final List<EntityType> ENTITY_TYPE_DEFINITIONS = ImmutableList.of(
            APPLICATION_ENTITY_TYPE,
            create(ApiEntityType.APPLICATION_SERVER, "app_server_stats", STANDARD_STATS),
            APPLICATION_SPEND_ENTITY_TYPE,
            create(ApiEntityType.AVAILABILITY_ZONE),
            create(ApiEntityType.BUSINESS_ACCOUNT),
            create(ApiEntityType.BUSINESS_APPLICATION, "business_app_stats", STANDARD_STATS),
            create(ApiEntityType.BUSINESS_USER, "bu_stats", STANDARD_STATS),
            create(ApiEntityType.CHASSIS, "ch_stats", STANDARD_STATS),
            create(ApiEntityType.CLOUD_SERVICE, "service_spend", Spend, PersistEntity),
            create(ApiEntityType.COMPUTE_TIER, PersistEntity),
            create(ApiEntityType.CONTAINER, "cnt_stats", STANDARD_STATS),
            CONTAINER_POD_ENTITY_TYPE,
            create(ApiEntityType.CONTAINER_SPEC, "cnt_spec_stats", STANDARD_STATS),
            create(ApiEntityType.DATABASE, "db_stats", STANDARD_STATS),
            create(ApiEntityType.DATABASE_SERVER, "db_server_stats", STANDARD_STATS),
            create(ApiEntityType.DATABASE_SERVER_TIER, PersistEntity),
            create(ApiEntityType.DATABASE_TIER, PersistEntity),
            DATA_CENTER_ENTITY_TYPE,
            create(ApiEntityType.DESKTOP_POOL, "desktop_pool_stats", STANDARD_STATS),
            create(ApiEntityType.DISKARRAY, "da_stats", STANDARD_STATS),
            create(ApiEntityType.DPOD, "dpod_stats", STANDARD_STATS),
            create(ApiEntityType.HYPERVISOR_SERVER),
            create(ApiEntityType.INTERNET),
            create(ApiEntityType.IOMODULE, "iom_stats", STANDARD_STATS),
            create(ApiEntityType.LOAD_BALANCER, "load_balancer_stats", NON_PRICE_STATS),
            create(ApiEntityType.LOGICALPOOL, "lp_stats", STANDARD_STATS),
            create(ApiEntityType.NAMESPACE, "nspace_stats", STANDARD_STATS),
            create(ApiEntityType.NETWORK),
            PHYSICAL_MACHINE_ENTITY_TYPE,
            create(ApiEntityType.REGION),
            create(ApiEntityType.RESERVED_INSTANCE, "ri_stats"),
            create(ApiEntityType.STORAGE, "ds_stats", STANDARD_STATS),
            create(ApiEntityType.STORAGECONTROLLER, "sc_stats", STANDARD_STATS),
            create(ApiEntityType.STORAGE_TIER, PersistEntity),
            create(ApiEntityType.SWITCH, "sw_stats", STANDARD_STATS),
            create(ApiEntityType.VIEW_POD, "view_pod_stats", STANDARD_STATS),
            create(ApiEntityType.VIRTUAL_APPLICATION, "virtual_app_stats", STANDARD_STATS),
            VIRTUAL_DATACENTER_ENTITY_TYPE,
            create(ApiEntityType.VIRTUAL_MACHINE, "vm_stats", STANDARD_STATS),
            VIRTUAL_MACHINE_SPEND_ENTITY_TYPE,
            create(ApiEntityType.VIRTUAL_VOLUME, "virtual_volume_stats", STANDARD_STATS),
            create(ApiEntityType.VPOD, "vpod_stats", STANDARD_STATS),
            create(ApiEntityType.WORKLOAD_CONTROLLER, "wkld_ctl_stats", STANDARD_STATS),
            create(ApiEntityType.SERVICE_PROVIDER)
    );

    // convenience methods for invoking the constructor for different scenarios
    private static EntityType create(ApiEntityType apiEntityType, String tablePrefix, UseCase... useCases) {
        return create(apiEntityType, null, tablePrefix, null, useCases);
    }

    private static EntityType create(ApiEntityType apiEntityType, EntityType aliasee, UseCase... useCases) {
        return create(apiEntityType, null, null, aliasee, useCases);
    }

    private static EntityType create(ApiEntityType apiEntityType, UseCase... useCases) {
        return create(apiEntityType, null, null, null, useCases);
    }

    private static EntityType create(String name, String tablePrefix, UseCase... useCases) {
        return create(null, name, tablePrefix, null, useCases);
    }

    private static EntityType create(ApiEntityType apiEntityType, String name, String tablePrefix,
            EntityType aliasee, UseCase... useCases) {
        return new EntityTypeDefinition(apiEntityType, name, tablePrefix, aliasee, useCases);
    }

    /** Map from entity type name to entity type. */
    static final Map<String, EntityType> NAME_TO_ENTITY_TYPE_MAP = ENTITY_TYPE_DEFINITIONS.stream()
            .collect(ImmutableMap.toImmutableMap(EntityType::getName, Functions.identity()));

    /** Map from entity stats tables (all time frames) to associated entity types. */
    static final Map<Table<?>, EntityType> TABLE_TO_ENTITY_TYPE_MAP = createTableToEntityTypeMap();

    private static Map<Table<?>, EntityType> createTableToEntityTypeMap() {
        final ImmutableMap.Builder<Table<?>, EntityType> builder = ImmutableMap.builder();
        ENTITY_TYPE_DEFINITIONS.stream()
                .map(type -> (EntityTypeDefinition)type)
                .filter(type -> type.aliasee == null)
                .forEach(type -> {
                    Optional.ofNullable(type.latestTable).ifPresent(table -> builder.put(table, type));
                    Optional.ofNullable(type.hourTable).ifPresent(table -> builder.put(table, type));
                    Optional.ofNullable(type.dayTable).ifPresent(table -> builder.put(table, type));
                    Optional.ofNullable(type.monthTable).ifPresent(table -> builder.put(table, type));
                });
        return builder.build();
    }

    /** Map from SDK entity type numbers to associated entity types. */
    static final Map<Integer, EntityType> SDK_TO_ENTITY_TYPE_MAP =
            ImmutableMap.<Integer, EntityType>builder()
                    .putAll(ENTITY_TYPE_DEFINITIONS.stream()
                            .map(type -> Pair.of(type.getSdkEntityType(), type))
                            .filter(pair -> pair.getKey().isPresent())
                            .collect(Collectors.toMap(pair -> pair.getKey().get().getNumber(), Pair::getValue)))
                    .build();

    /**
     * Class that represents a single entity type definition.
     */
    private static class EntityTypeDefinition implements EntityType {
        private final ApiEntityType apiEntityType;
        private final String name;
        private final String tablePrefix;
        private final EntityType aliasee;
        private final Set<UseCase> useCases;

        private final Table<?> latestTable;
        private final Table<?> hourTable;
        private final Table<?> dayTable;
        private final Table<?> monthTable;
        private final EntityDTO.EntityType sdkType;

        /**
         * Create a new entity type definition.
         *
         * @param apiEntityType corresponding API entity type, if one exists
         * @param name         name for this entity type, must appear if there's no API entity type
         * @param tablePrefix  prefix for stats tables for this entity type, if it has tables
         * @param aliasee      another entity type to which this one is aliased, if any
         * @param useCases     use cases to which this entity type applies
         */
        private EntityTypeDefinition(ApiEntityType apiEntityType, String name, String tablePrefix,
                EntityType aliasee, UseCase... useCases) {
            this.apiEntityType = apiEntityType;
            this.name = name != null ? name : apiEntityType.apiStr();
            this.tablePrefix = tablePrefix;
            this.aliasee = aliasee;
            this.useCases = ImmutableSet.copyOf(useCases);
            this.latestTable = computeTable(getTablePrefix(), LATEST_TABLE_SUFFIX);
            this.hourTable = computeTable(getTablePrefix(), BY_HOUR_TABLE_SUFFIX);
            this.dayTable = computeTable(getTablePrefix(), BY_DAY_TABLE_SUFFIX);
            this.monthTable = computeTable(getTablePrefix(), BY_MONTH_TABLE_SUFFIX);
            this.sdkType = Optional.ofNullable(apiEntityType).map(ApiEntityType::sdkType).orElse(null);
        }

        private static final String LATEST_TABLE_SUFFIX = "latest";
        private static final String BY_HOUR_TABLE_SUFFIX = "by_hour";
        private static final String BY_DAY_TABLE_SUFFIX = "by_day";
        private static final String BY_MONTH_TABLE_SUFFIX = "by_month";

        private static Table<?> computeTable(Optional<String> prefix, String suffix) {
            return prefix.map(p -> p + "_" + suffix)
                    .map(Vmtdb.VMTDB::getTable)
                    .orElse(null);
        }

        public String getName() {
            return name;
        }

        public EntityType resolve() {
            EntityTypeDefinition result = this;
            while (result.aliasee != null) {
                result = (EntityTypeDefinition)result.aliasee;
            }
            return result;
        }

        @Override
        public Optional<String> getTablePrefix() {
            return Optional.ofNullable(((EntityTypeDefinition)resolve()).tablePrefix);
        }

        @Override
        public Optional<Table<?>> getLatestTable() {
            return Optional.ofNullable(latestTable);
        }

        @Override
        public Optional<Table<?>> getHourTable() {
            return Optional.ofNullable(hourTable);
        }

        @Override
        public Optional<Table<?>> getDayTable() {
            return Optional.ofNullable(dayTable);
        }

        @Override
        public Optional<Table<?>> getMonthTable() {
            return Optional.ofNullable(monthTable);
        }

        @Override
        public Optional<Table<?>> getTimeFrameTable(final com.vmturbo.commons.TimeFrame timeFrame) throws IllegalArgumentException {
            if (timeFrame != null) {
                switch (timeFrame) {
                    case LATEST:
                        return getLatestTable();
                    case HOUR:
                        return getHourTable();
                    case DAY:
                        return getDayTable();
                    case MONTH:
                        return getMonthTable();
                    default:
                        // fall through and throw
                }
            }
            final String msg = String.format("Invalid entity stats timeframe: %s",
                    timeFrame != null ? timeFrame.name() : null);
            throw new IllegalArgumentException(msg);
        }

        @Override
        public Optional<EntityDTO.EntityType> getSdkEntityType() {
            return Optional.ofNullable(sdkType);
        }

        @Override
        public boolean hasUseCase(final UseCase useCase) {
            return useCases.contains(useCase);
        }

        @Override
        public String toString() {
            return "EntityType[" + getName() + "]";
        }
    }
}
