package com.vmturbo.history.db;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.vmturbo.components.common.utils.StringConstants.INTERNAL_NAME;
import static com.vmturbo.components.common.utils.StringConstants.RECORDED_ON;
import static com.vmturbo.components.common.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.components.common.utils.StringConstants.UUID;
import static com.vmturbo.history.schema.abstraction.Tables.APP_COMPONENT_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.APP_COMPONENT_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.APP_COMPONENT_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.APP_COMPONENT_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SERVER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SERVER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SERVER_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SERVER_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SPEND_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SPEND_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SPEND_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.APP_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.APP_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.APP_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.BUSINESS_APP_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.BUSINESS_APP_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.BUSINESS_APP_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.BUSINESS_APP_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.BUSINESS_TRANSACTION_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.BUSINESS_TRANSACTION_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.BUSINESS_TRANSACTION_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.BUSINESS_TRANSACTION_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.BU_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.BU_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.BU_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.BU_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.CH_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.CH_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CH_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.CNT_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.CNT_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CNT_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.CNT_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.CPOD_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.CPOD_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CPOD_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.CPOD_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.DA_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.DA_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.DA_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.DB_SERVER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.DB_SERVER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.DB_SERVER_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.DB_SERVER_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.DB_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.DB_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.DESKTOP_POOL_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.DESKTOP_POOL_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.DESKTOP_POOL_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.DESKTOP_POOL_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.DPOD_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.DPOD_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.DPOD_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.DS_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.DS_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.DS_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.IOM_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.IOM_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.IOM_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.LOAD_BALANCER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.LOAD_BALANCER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.LOAD_BALANCER_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.LOAD_BALANCER_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.LP_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.LP_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.LP_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.LP_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.RI_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.RI_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.RI_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.RI_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.SC_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.SC_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.SC_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.SERVICE_SPEND_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.SERVICE_SPEND_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.SERVICE_SPEND_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.SERVICE_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.SERVICE_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.SERVICE_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.SERVICE_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.SW_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.SW_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.SW_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.SW_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.VDC_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.VDC_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.VDC_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.VIEW_POD_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.VIEW_POD_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.VIEW_POD_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.VIEW_POD_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.VIRTUAL_APP_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.VIRTUAL_APP_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.VIRTUAL_APP_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.VIRTUAL_APP_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.VM_SPEND_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.VM_SPEND_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.VM_SPEND_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.VPOD_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.VPOD_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.VPOD_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.tables.AppStatsLatest.APP_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.ChStatsLatest.CH_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.DaStatsLatest.DA_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.DbStatsByDay.DB_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.tables.DbStatsByMonth.DB_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.tables.DpodStatsLatest.DPOD_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.DsStatsLatest.DS_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.IomStatsLatest.IOM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.PmStatsLatest.PM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.ScStatsLatest.SC_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.VdcStatsLatest.VDC_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.VmStatsLatest.VM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.VpodStatsLatest.VPOD_STATS_LATEST;

import java.util.Collection;
import java.util.Optional;

import org.jooq.Table;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.abstraction.Tables;

public enum EntityType {
    CLUSTER
            (0, StringConstants.CLUSTER, "cluster", null, null, CLUSTER_STATS_BY_DAY,  CLUSTER_STATS_BY_MONTH),

    //DataCenter stats are stored in the PM stats tables
    DATA_CENTER(
        1, StringConstants.DATA_CENTER,       "pm",  PM_STATS_LATEST, PM_STATS_BY_HOUR,
        PM_STATS_BY_DAY, PM_STATS_BY_MONTH),
    //The DATACENTER Enum defined in CommonDTO.proto, used by SDK probes, has no '_'
    DATACENTER(91, StringConstants.DATA_CENTER,      "pm",  PM_STATS_LATEST, PM_STATS_BY_HOUR, PM_STATS_BY_DAY, PM_STATS_BY_MONTH),

    PHYSICAL_MACHINE
            (2, StringConstants.PHYSICAL_MACHINE,  "pm",   PM_STATS_LATEST, PM_STATS_BY_HOUR,  PM_STATS_BY_DAY,  PM_STATS_BY_MONTH),

    VIRTUAL_MACHINE
            (3, StringConstants.VIRTUAL_MACHINE,   "vm",   VM_STATS_LATEST, VM_STATS_BY_HOUR,  VM_STATS_BY_DAY,  VM_STATS_BY_MONTH),

    STORAGE
            (4, StringConstants.STORAGE,          "ds",   DS_STATS_LATEST, DS_STATS_BY_HOUR,  DS_STATS_BY_DAY,  DS_STATS_BY_MONTH),

    APPLICATION
            (5, StringConstants.APPLICATION,      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    CHASSIS
            (6, StringConstants.CHASSIS,          "ch",   CH_STATS_LATEST, CH_STATS_BY_HOUR,  CH_STATS_BY_DAY,  CH_STATS_BY_MONTH),

    DISK_ARRAY
            (7, StringConstants.DISK_ARRAY,        "da",   DA_STATS_LATEST, DA_STATS_BY_HOUR,  DA_STATS_BY_DAY,  DA_STATS_BY_MONTH),

    IO_MODULE
            (8, StringConstants.IO_MODULE,         "iom",  IOM_STATS_LATEST, IOM_STATS_BY_HOUR, IOM_STATS_BY_DAY, IOM_STATS_BY_MONTH),

    STORAGE_CONTROLLER
            (9, StringConstants.STORAGE_CONTROLLER,"sc",   SC_STATS_LATEST, SC_STATS_BY_HOUR,  SC_STATS_BY_DAY,  SC_STATS_BY_MONTH),

    SWITCH
            (10, StringConstants.SWITCH,          "sw",   SW_STATS_LATEST, SW_STATS_BY_HOUR,  SW_STATS_BY_DAY,  SW_STATS_BY_MONTH),

    VDC
            (11, StringConstants.VDC, "vdc",  VDC_STATS_LATEST, VDC_STATS_BY_HOUR, VDC_STATS_BY_DAY, VDC_STATS_BY_MONTH),

    VPOD
            (12, StringConstants.V_POD,            "vpod", VPOD_STATS_LATEST, VPOD_STATS_BY_HOUR, VPOD_STATS_BY_DAY, VPOD_STATS_BY_MONTH),

    DPOD
            (13, StringConstants.D_POD,            "dpod", DPOD_STATS_LATEST, DPOD_STATS_BY_HOUR, DPOD_STATS_BY_DAY, DPOD_STATS_BY_MONTH),

    CONTAINER
            (14, StringConstants.CONTAINER,        "cnt",  CNT_STATS_LATEST, CNT_STATS_BY_HOUR, CNT_STATS_BY_DAY, CNT_STATS_BY_MONTH),

    SPEND_VIRTUAL_MACHINE
            (15, StringConstants.VIRTUAL_MACHINE,   "vm_spend", null, VM_SPEND_BY_HOUR, VM_SPEND_BY_DAY, VM_SPEND_BY_MONTH),

    CLOUD_SERVICE
            (16, StringConstants.CLOUD_SERVICE,   "service_spend", null, SERVICE_SPEND_BY_HOUR, SERVICE_SPEND_BY_DAY, SERVICE_SPEND_BY_MONTH),

    CONTAINERPOD
            (17, StringConstants.CONTAINERPOD,        "cpod",  CPOD_STATS_LATEST, CPOD_STATS_BY_HOUR, CPOD_STATS_BY_DAY, CPOD_STATS_BY_MONTH),

    LOGICAL_POOL
            (18, StringConstants.LOGICAL_POOL,        "lp",  LP_STATS_LATEST, LP_STATS_BY_HOUR, LP_STATS_BY_DAY, LP_STATS_BY_MONTH),

    SPEND_APP(19, StringConstants.APPLICATION, "app_spend", null, APP_SPEND_BY_HOUR, APP_SPEND_BY_DAY, APP_SPEND_BY_MONTH),

    BUSINESS_APPLICATION(20, StringConstants.BUSINESS_APPLICATION,      "ba",  BUSINESS_APP_STATS_LATEST, BUSINESS_APP_STATS_BY_HOUR, BUSINESS_APP_STATS_BY_DAY, BUSINESS_APP_STATS_BY_MONTH),

    RESERVED_INSTANCE
            (21, StringConstants.RESERVED_INSTANCE, "ri", RI_STATS_LATEST, RI_STATS_BY_HOUR, RI_STATS_BY_DAY, RI_STATS_BY_MONTH),

    LOAD_BALANCER(22, StringConstants.LOAD_BALANCER,      "lb",  LOAD_BALANCER_STATS_LATEST, LOAD_BALANCER_STATS_BY_HOUR, LOAD_BALANCER_STATS_BY_DAY, LOAD_BALANCER_STATS_BY_MONTH),

    DATABASE_SERVER(23, StringConstants.DATABASE_SERVER,      "db_server",  DB_SERVER_STATS_LATEST, DB_SERVER_STATS_BY_HOUR, DB_SERVER_STATS_BY_DAY, DB_SERVER_STATS_BY_MONTH),

    VIRTUAL_APPLICATION(24, StringConstants.VIRTUAL_APPLICATION,      "vapp",  VIRTUAL_APP_STATS_LATEST, VIRTUAL_APP_STATS_BY_HOUR, VIRTUAL_APP_STATS_BY_DAY, VIRTUAL_APP_STATS_BY_MONTH),

    APPLICATION_SERVER(25, StringConstants.APPSRV,      "app_server",  APP_SERVER_STATS_LATEST, APP_SERVER_STATS_BY_HOUR, APP_SERVER_STATS_BY_DAY, APP_SERVER_STATS_BY_MONTH),
    DESKTOP_POOL(26, StringConstants.DESKTOP_POOL, "dp", DESKTOP_POOL_STATS_LATEST, DESKTOP_POOL_STATS_BY_HOUR, DESKTOP_POOL_STATS_BY_DAY, DESKTOP_POOL_STATS_BY_MONTH),
    BUSINESS_USER(27, StringConstants.BUSINESS_USER, "bu", BU_STATS_LATEST, BU_STATS_BY_HOUR, BU_STATS_BY_DAY, BU_STATS_BY_MONTH),
    VIEW_POD(28, StringConstants.VIEW_POD, "view_pod", VIEW_POD_STATS_LATEST, VIEW_POD_STATS_BY_HOUR, VIEW_POD_STATS_BY_DAY, VIEW_POD_STATS_BY_MONTH),

    DATABASE(29, StringConstants.DATABASE, "db",
            DB_STATS_LATEST, DB_STATS_BY_HOUR,
            DB_STATS_BY_DAY, DB_STATS_BY_MONTH),

    SERVICE(30, StringConstants.SERVICE, "service",
            SERVICE_STATS_LATEST, SERVICE_STATS_BY_HOUR,
            SERVICE_STATS_BY_DAY, SERVICE_STATS_BY_MONTH),

    APPLICATION_COMPONENT(31, StringConstants.APPLICATION_COMPONENT, "app",
            APP_COMPONENT_STATS_LATEST, APP_COMPONENT_STATS_BY_HOUR,
            APP_COMPONENT_STATS_BY_DAY, APP_COMPONENT_STATS_BY_MONTH),

    BUSINESS_TRANSACTION(32, StringConstants.BUSINESS_TRANSACTION, "bt",
            BUSINESS_TRANSACTION_STATS_LATEST, BUSINESS_TRANSACTION_STATS_BY_HOUR,
            BUSINESS_TRANSACTION_STATS_BY_DAY, BUSINESS_TRANSACTION_STATS_BY_MONTH);

    private static final Collection<Function<EntityType, ? extends Table<?>>> TABLE_GETTERS =
                    ImmutableSet.of(EntityType::getLatestTable, EntityType::getHourTable,
                                    EntityType::getDayTable, EntityType::getMonthTable);
    protected static final Map<Table<?>, EntityType> TABLE_TO_SPEND_ENTITY_MAP =
                    new ImmutableMap.Builder<Table<?>, EntityType>()
                                    .put(VM_SPEND_BY_HOUR, SPEND_VIRTUAL_MACHINE)
                                    .put(VM_SPEND_BY_DAY, SPEND_VIRTUAL_MACHINE)
                                    .put(VM_SPEND_BY_MONTH, SPEND_VIRTUAL_MACHINE)
                                    .put(APP_SPEND_BY_HOUR, SPEND_APP)
                                    .put(APP_SPEND_BY_DAY, SPEND_APP)
                                    .put(APP_SPEND_BY_MONTH, SPEND_APP)
                                    .put(SERVICE_SPEND_BY_HOUR, CLOUD_SERVICE)
                                    .put(SERVICE_SPEND_BY_DAY, CLOUD_SERVICE)
                                    .put(SERVICE_SPEND_BY_MONTH, CLOUD_SERVICE)
                                    .build();
    private static final Map<Table<?>, EntityType> TABLE_TO_ENTITY_MAP =
                    createTableToEntityMap(TABLE_TO_SPEND_ENTITY_MAP, EntityType.PHYSICAL_MACHINE);
    private static final Map<Integer, EntityType> VALUE_TO_ENTITY_TYPE_MAP =
                    Stream.of(EntityType.values()).collect(Collectors
                                    .toMap(EntityType::getValue, Function.identity()));
    private static final Map<String, EntityType> NAME_TO_SPEND_ENTITY_TYPE_MAP =
                    new ImmutableMap.Builder<String, EntityType>()
                                    .put(StringConstants.VIRTUAL_MACHINE, SPEND_VIRTUAL_MACHINE)
                                    .put(StringConstants.DATABASE, SPEND_APP)
                                    .put(StringConstants.DATABASE_SERVER, SPEND_APP)
                                    .put(StringConstants.CLOUD_SERVICE, CLOUD_SERVICE)
                                    .build();
    private static final Map<String, EntityType> NAME_TO_ENTITY_TYPE_MAP = createNameToEntity(
                    // DC's are intentionally mapped to PM's
                    Collections.singletonMap(StringConstants.DATA_CENTER, PHYSICAL_MACHINE));

    private static final Collection<EntityType> ENTITIES_EXCLUDED_FROM_ROLL_UP =
                    ImmutableSet.of(
                            BUSINESS_APPLICATION,
                            CLOUD_SERVICE,
                            CLUSTER,
                            DATACENTER,
                            DATA_CENTER,
                            LOAD_BALANCER,
                            RESERVED_INSTANCE,
                            SPEND_APP,
                            SPEND_VIRTUAL_MACHINE);

    public static final List<EntityType> ROLLED_UP_ENTITIES = Stream.of(EntityType.values())
                    .filter(item -> !ENTITIES_EXCLUDED_FROM_ROLL_UP.contains(item))
                    .collect(Collectors.toList());

/**
 * Instances of this type provide information that's important to history component behavior,
 * regarding the entity types it may encounter in topologies or in API queries.
 */
public interface EntityType {

    /**
     * Obtain an entity type instance for the given name.
     *
     * <p>Where applicable, names should be consistent with names used by the API, e.g. names
     * appearing in {@link ApiEntityType} values.</p>
     *
     * @param name name of entity type
     * @return corresponding entity type, if it exists
     */
    static Optional<EntityType> named(String name) {
        return Optional.ofNullable(EntityTypeDefinitions.NAME_TO_ENTITY_TYPE_MAP.get(name));
    }

    /**
     * Obtain an entity type instance for the given name.
     *
     * @param name name of entity type
     * @return corresponding entity type, if it exists
     * @throws IllegalArgumentException if no such entity exists
     */
    static EntityType get(String name) throws IllegalArgumentException {
        return EntityType.named(name).orElseThrow(() ->
                new IllegalArgumentException("EntityType " + name + " not found"));
    }

    /**
     * Get the name of this entity type.
     *
     * @return entity type name
     */
    String getName();

    /**
     * Resolve this entity type to the type to which it is aliased.
     *
     * <p>Multiple aliasing steps are followed, if present, so the result will be an un-aliased
     * entity type.</p>
     *
     * @return this entity type if it is not aliased, else the type at the end of its aliasing chain
     */
    EntityType resolve();

    /**
     * Return the stats table that stores latest records for this entity type.
     *
     * @return the latest table, if this entity type has one
     */
    Optional<Table<?>> getLatestTable();

    /**
     * Return the stats table that stores hourly rollup records for this entity type.
     *
     * @return the hourly table, if this entity type has one
     */
    Optional<Table<?>> getHourTable();

    /**
     * Return the stats table that stores daily rollup records for this entity type.
     *
     * @return the daily table, if this entity type has one
     */
    Optional<Table<?>> getDayTable();

    /**
     * Return the stats table that stores monthly rollup records for this entity type.
     *
     * @return the monthly table, if this entity type has one
     */
    Optional<Table<?>> getMonthTable();

    /**
     * Get the table prefix for stats tables for this entity type.
     *
     * <p>This value is joined with the table suffixes for the various time frames, with an
     * intervening underscore, to arrive at the table name.</p>
     *
     * <p>N.B.: For entity stats table, this includes the word "stats" - e.g. the previs for VM
     * tables is "vm_stats", not "vm".</p>
     *
     * @return the table prefix for this entity, if this entity has stats tables
     */
    Optional<String> getTablePrefix();

    /**
     * Get the entity stats table where this table stores data for the given time frame.
     *
     * @param timeFrame desired time frame - "latest" or a rollup time frame
     * @return corresponding stats table, if this entity has one
     * @throws IllegalArgumentException if an invalid timeframe is provided
     */
    Optional<Table<?>> getTimeFrameTable(TimeFrame timeFrame) throws IllegalArgumentException;

    /**
     * Obtain the entity type that uses the given table for history stats (latest or rollups).
     *
     * @param table table
     * @return associated entity type, if any
     */
    static Optional<EntityType> fromTable(Table<?> table) {
        return Optional.ofNullable(EntityTypeDefinitions.TABLE_TO_ENTITY_TYPE_MAP.get(table));
    }

    /**
     * Get the SDK entity type associated with this entity type, if any.
     *
     * @return associated SDK entity type, if any
     */
    Optional<EntityDTO.EntityType> getSdkEntityType();

    /**
     * Get the entity type that's associated with the given SDK entity type, if any.
     *
     * @param sdkEntityType SDK entity type
     * @return corresponding entity type, if any
     */
    static Optional<EntityType> fromSdkEntityType(EntityDTO.EntityType sdkEntityType) {
        return fromSdkEntityType(sdkEntityType.getNumber());
    }

    /**
     * Get the entity type that's associated with the given SDK entity type number, if any.
     *
     * @param sdkEntityTypeNo SDK entity type number
     * @return corresponding entity type, if any
     */
    static Optional<EntityType> fromSdkEntityType(int sdkEntityTypeNo) {
        return Optional.ofNullable(EntityTypeDefinitions.SDK_TO_ENTITY_TYPE_MAP.get(sdkEntityTypeNo));
    }


    /**
     * Check whether the given entity type has the given use case.
     *
     * @param useCase use case to check
     * @return true if the entity type has the use case
     */
    boolean hasUseCase(UseCase useCase);

    /**
     * Check whether entities of this type should be persisted in the {@link Entities} table.
     *
     * @return true if entities should be persisted
     */
    default boolean persistsEntity() {
        return hasUseCase(UseCase.PersistEntity);
    }

    /**
     * Check whether entities of this type should have commodity/attribute stats persisted to
     * stats tables.
     *
     * @return true if entity stats should be persisted
     */
    default boolean persistsStats() {
        return hasUseCase(UseCase.PersistStats);
    }

    /**
     * Check whether entities of this type participate in entity stats rollup processing.
     *
     * @return true if entity stats data should be rolled up
     */
    default boolean rollsUp() {
        return hasUseCase(UseCase.RollUp);
    }

    /**
     * Check whether entities of this type should persist price index data to stats tables.
     *
     * @return true if price index data should be persisted
     */
    default boolean persistsPriceIndex() {
        return getSdkEntityType()
                .map(sdkType -> !StatsUtils.SDK_ENTITY_TYPES_WITHOUT_SAVED_PRICES.contains(
                        sdkType.getNumber()))
                .orElse(false);
    }

    /**
     * Check whether this is a spend entity type.
     *
     * @return true if this is a spend entity type
     */
    default boolean isSpend() {
        return hasUseCase(UseCase.Spend);
    }

    /**
     * Get a collection of all the defined entity types.
     *
     * @return all defined entity types
     */
    static Collection<EntityType> allEntityTypes() {
        return EntityTypeDefinitions.ENTITY_TYPE_DEFINITIONS;
    }

    /**
     * Use-cases that may apply to individual entity types.
     */
    enum UseCase {
        /** Entities are persisted to the entities table. */
        PersistEntity,
        /** Entity attributes and bought/sold commodities are persisted to stats tables. */
        PersistStats,
        /** Price index data is persisted to stats tables. */
        RollUp,
        /** This is a spend entity. */
        Spend;

        static final UseCase[] STANDARD_STATS = new UseCase[]{
                PersistEntity, PersistStats, RollUp
        };

        static final UseCase[] NON_ROLLUP_STATS = new UseCase[]{
                PersistEntity, PersistStats
        };

        static final UseCase[] NON_PRICE_STATS = new UseCase[]{
                PersistEntity, PersistEntity, RollUp
        };
    }
}
