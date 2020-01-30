package com.vmturbo.history.db;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.vmturbo.components.common.utils.StringConstants.INTERNAL_NAME;
import static com.vmturbo.components.common.utils.StringConstants.RECORDED_ON;
import static com.vmturbo.components.common.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.components.common.utils.StringConstants.UUID;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SPEND_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SPEND_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.APP_SPEND_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.APP_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.APP_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.APP_STATS_BY_MONTH;
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
import static com.vmturbo.history.schema.abstraction.Tables.DPOD_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.DPOD_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.DPOD_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.DS_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.DS_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.DS_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.IOM_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.IOM_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.IOM_STATS_BY_MONTH;
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
import static com.vmturbo.history.schema.abstraction.tables.DpodStatsLatest.DPOD_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.DsStatsLatest.DS_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.IomStatsLatest.IOM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.PmStatsLatest.PM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.ScStatsLatest.SC_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.VdcStatsLatest.VDC_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.VmStatsLatest.VM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.VpodStatsLatest.VPOD_STATS_LATEST;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Lists;

import org.jooq.Table;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.abstraction.Tables;

public enum EntityType {
    CLUSTER
            (0, StringConstants.CLUSTER, "cluster", null, null, CLUSTER_STATS_BY_DAY,  CLUSTER_STATS_BY_MONTH),

    //DataCenter stats are stored in the PM stats tables
    DATA_CENTER (
            1, StringConstants.DATA_CENTER,       "pm",  PM_STATS_LATEST, PM_STATS_BY_HOUR, PM_STATS_BY_DAY, PM_STATS_BY_MONTH),
    //The DATACENTER Enum defined in CommonDTO.proto, used by SDK probes, has no '_'
    DATACENTER
            (91, StringConstants.DATA_CENTER,      "pm",  PM_STATS_LATEST, PM_STATS_BY_HOUR, PM_STATS_BY_DAY, PM_STATS_BY_MONTH),

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

    BUSINESS_APPLICATION
            (20, StringConstants.BUSINESS_APPLICATION,      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    RESERVED_INSTANCE
            (21, StringConstants.RESERVED_INSTANCE, "ri", RI_STATS_LATEST, RI_STATS_BY_HOUR, RI_STATS_BY_DAY, RI_STATS_BY_MONTH),

    LOAD_BALANCER
            (22, StringConstants.LOAD_BALANCER,      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    DATABASE_SERVER
            (23, StringConstants.DATABASE_SERVER,      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    VIRTUAL_APPLICATION
            (24, StringConstants.VIRTUAL_APPLICATION,      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    APPLICATION_SERVER
            (25, StringConstants.APPSRV,      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),
    DESKTOP_POOL(26, StringConstants.DESKTOP_POOL, "dp", VDC_STATS_LATEST, VDC_STATS_BY_HOUR, VDC_STATS_BY_DAY, VDC_STATS_BY_MONTH),
    BUSINESS_USER(27, StringConstants.BUSINESS_USER, "bu", BU_STATS_LATEST, BU_STATS_BY_HOUR, BU_STATS_BY_DAY, BU_STATS_BY_MONTH),
    VIEW_POD(28, StringConstants.VIEW_POD, "view_pod", VIEW_POD_STATS_LATEST, VIEW_POD_STATS_BY_HOUR, VIEW_POD_STATS_BY_DAY, VIEW_POD_STATS_BY_MONTH),
    DATABASE(29, StringConstants.DATABASE,      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),
    SERVICE
            (30, StringConstants.SERVICE, "service",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH)
    ;

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

    private final int value;
    private final String clsName;
    private final String shortName;
    private final Table<?> latestTable;
    private final Table<?> hourTable;
    private final Table<?> dayTable;
    private final Table<?> monthTable;

    private EntityType(int value, String clsName, String shortName,
            Table<?> latestTable, Table<?> hourTable, Table<?> dayTable, Table<?> monthTable) {
        this.value = value;
        this.clsName = clsName;
        this.shortName = shortName;
        this.hourTable = hourTable;
        this.latestTable = latestTable;
        this.dayTable = dayTable;
        this.monthTable = monthTable;
    }

    public int getValue(){
        return value;
    }

    public String getClsName(){
        return clsName;
    }

    public String getTblPrfx(){
        return shortName;
    }

    public Table<?> getLatestTable() {
        return latestTable;
    }

    public boolean isSpendEntity() {
        return TABLE_TO_SPEND_ENTITY_MAP.containsKey(hourTable);
    }

    public Table<?> getHourTable() { return hourTable; }

    public Table<?> getDayTable() {
        return dayTable;
    }

    public Table<?> getMonthTable() {
        return monthTable;
    }

    /**
     * Get the entity stats table for the given timeframe.
     *
     * @param timeFrame the desired timeframe
     * @return the corresponding table
     * @throws IllegalArgumentException if an unknown timeframe is provided
     */
    public Table<?> getTimeFrameTable(TimeFrame timeFrame) throws IllegalArgumentException {
        switch (timeFrame) {
            case LATEST:
                return latestTable;
            case HOUR:
                return hourTable;
            case DAY:
                return dayTable;
            case MONTH:
                return monthTable;
            default:
                throw new IllegalArgumentException("Unknown entity stats timeframe: " + timeFrame.name());
        }
    }

    public List<Table<?>> getTables() {
        return Lists.newArrayList(hourTable, dayTable, monthTable);
    }

    /**
     *  Given the class name, return the EntityType associate with that class Name.
     *
     * @param clsName class name
     * @return An optional of the EntityType associated with the class name.
     */
    public static Optional<EntityType> getEntityTypeByClsName(String clsName) {
        for (EntityType type : EntityType.values()) {
            if (type.clsName.equals(clsName)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }

    @Nullable
    public static EntityType valueOf(int value) {
        return VALUE_TO_ENTITY_TYPE_MAP.get(value);
    }

    private static Map<String, EntityType> createNameToEntity(
                    Map<String, EntityType> specialCases) {
        final Map<String, EntityType> result = new HashMap<>();
        Stream.of(EntityType.values()).forEach(entityType -> result
                        .putIfAbsent(entityType.getClsName(), entityType));
        result.putAll(specialCases);
        return ImmutableMap.copyOf(result);
    }

    // This method just get stats entity types, but for spend entity types call getSpendEntity
    public static EntityType get(String clsName){
        return checkNotNull(NAME_TO_ENTITY_TYPE_MAP.get(clsName),
                "Invalid entity class: %s", clsName);
    }

    public static Optional<EntityType> getTypeForName(String clsName) {
        return Optional.ofNullable(NAME_TO_ENTITY_TYPE_MAP.get(clsName));
    }



    public static EntityType getSpendEntity(String clsName){
        return checkNotNull(NAME_TO_SPEND_ENTITY_TYPE_MAP.get(clsName),
                "Invalid entity class: %s", clsName);
    }

    public static Optional<EntityType> getSpendTypeForName(String clsName) {
        return Optional.ofNullable(NAME_TO_SPEND_ENTITY_TYPE_MAP.get(clsName));
    }

    /**
     * Returns the field name containing the times of the updates in statsTableByEntityType table
     * of this EntityType.
     */
    public String getTimeField() {
        switch (this) {
        case CLUSTER:
            return RECORDED_ON;
        default:
            return SNAPSHOT_TIME;
        }
    }

    /**
     * Returns the field name containing the times of the updates in statsTableByEntityType table
     * of this EntityType.
     */
    public String getIdField() {
        switch (this) {
        case CLUSTER:
            return INTERNAL_NAME;
        default:
            return UUID;
        }
    }

    public static boolean isLatest(Table<?> tbl){
        return LATEST_TABLES.contains(tbl);
    }

    public static final Set<Table<?>> LATEST_TABLES = getTables(EntityType::getLatestTable);

    public static boolean isMonthly(Table<?> tbl){
        return MONTHLY_TABLES.contains(tbl);
    }
    public static final Set<Table<?>> MONTHLY_TABLES = getTables(EntityType::getMonthTable);

    public static boolean isDaily(Table<?> tbl){
        return DAILY_TABLES.contains(tbl);
    }

    public static final Set<Table<?>> DAILY_TABLES = getTables(EntityType::getDayTable);

    // note:  CLUSTER_STATS_BY_HOUR intentionally ommited from HOURLY_TABLES
    public static boolean isHourly(Table<?> tbl){
        return HOURLY_TABLES.contains(tbl);
    }

    public static final Set<Table<?>> HOURLY_TABLES =
                    getTables(EntityType::getHourTable, Tables.SYSTEM_LOAD);

    public static EntityType fromTable(Table<?> tbl){
        EntityType et = TABLE_TO_ENTITY_MAP.get(tbl);
        if (et == null){
            return TABLE_TO_SPEND_ENTITY_MAP.get(tbl);
        }
        return et;
    }

    private static Map<Table<?>, EntityType> createTableToEntityMap(
                    Map<Table<?>, EntityType> tableToSpendEntity, EntityType... specialCases) {
        final Map<Table<?>, EntityType> result = new HashMap<>();
        Stream.of(EntityType.values()).forEach(entityType -> TABLE_GETTERS
                        .forEach(tableGetter -> Optional.ofNullable(tableGetter.apply(entityType))
                                        .filter(table -> tableToSpendEntity.get(table) == null)
                                        .ifPresent(table -> result
                                                        .putIfAbsent(table, entityType))));
        Stream.of(specialCases).forEach(sc -> TABLE_GETTERS
                        .forEach(tableGetter -> result.put(tableGetter.apply(sc), sc)));
        return ImmutableMap.copyOf(result);
    }

    private static Set<Table<?>> getTables(Function<EntityType, ? extends Table<?>> tableSupplier,
                    Table<?>... additionalTables) {
        final Builder<Table<?>> builder = ImmutableSet.builder();
        Stream.of(EntityType.values()).map(tableSupplier).filter(Objects::nonNull)
                                        .forEach(builder::add);
        builder.add(additionalTables);
        return builder.build();
    }
}
