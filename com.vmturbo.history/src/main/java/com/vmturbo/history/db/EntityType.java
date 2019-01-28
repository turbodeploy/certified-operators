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
import static com.vmturbo.history.schema.abstraction.Tables.SYSTEM_LOAD;
import static com.vmturbo.history.schema.abstraction.Tables.VDC_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.VDC_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.VDC_STATS_BY_MONTH;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.jooq.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.components.common.utils.StringConstants;

public enum EntityType {
    CLUSTER
            (0, "Cluster", "cluster", null, null, CLUSTER_STATS_BY_DAY,  CLUSTER_STATS_BY_MONTH),

    //DataCenter stats are stored in the PM stats tables
    DATA_CENTER (
            1, "DataCenter",       "pm",  PM_STATS_LATEST, PM_STATS_BY_HOUR, PM_STATS_BY_DAY, PM_STATS_BY_MONTH),
    //The DATACENTER Enum defined in CommonDTO.proto, used by SDK probes, has no '_'
    DATACENTER
            (91, "DataCenter",      "pm",  PM_STATS_LATEST, PM_STATS_BY_HOUR, PM_STATS_BY_DAY, PM_STATS_BY_MONTH),

    PHYSICAL_MACHINE
            (2, "PhysicalMachine",  "pm",   PM_STATS_LATEST, PM_STATS_BY_HOUR,  PM_STATS_BY_DAY,  PM_STATS_BY_MONTH),

    VIRTUAL_MACHINE
            (3, "VirtualMachine",   "vm",   VM_STATS_LATEST, VM_STATS_BY_HOUR,  VM_STATS_BY_DAY,  VM_STATS_BY_MONTH),

    STORAGE
            (4, "Storage",          "ds",   DS_STATS_LATEST, DS_STATS_BY_HOUR,  DS_STATS_BY_DAY,  DS_STATS_BY_MONTH),

    APPLICATION
            (5, "Application",      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    CHASSIS
            (6, "Chassis",          "ch",   CH_STATS_LATEST, CH_STATS_BY_HOUR,  CH_STATS_BY_DAY,  CH_STATS_BY_MONTH),

    DISK_ARRAY
            (7, "DiskArray",        "da",   DA_STATS_LATEST, DA_STATS_BY_HOUR,  DA_STATS_BY_DAY,  DA_STATS_BY_MONTH),

    IO_MODULE
            (8, "IOModule",         "iom",  IOM_STATS_LATEST, IOM_STATS_BY_HOUR, IOM_STATS_BY_DAY, IOM_STATS_BY_MONTH),

    STORAGE_CONTROLLER
            (9, "StorageController","sc",   SC_STATS_LATEST, SC_STATS_BY_HOUR,  SC_STATS_BY_DAY,  SC_STATS_BY_MONTH),

    SWITCH
            (10, "Switch",          "sw",   SW_STATS_LATEST, SW_STATS_BY_HOUR,  SW_STATS_BY_DAY,  SW_STATS_BY_MONTH),

    VDC
            (11, "VirtualDataCenter", "vdc",  VDC_STATS_LATEST, VDC_STATS_BY_HOUR, VDC_STATS_BY_DAY, VDC_STATS_BY_MONTH),

    VPOD
            (12, "VPod",            "vpod", VPOD_STATS_LATEST, VPOD_STATS_BY_HOUR, VPOD_STATS_BY_DAY, VPOD_STATS_BY_MONTH),

    DPOD
            (13, "DPod",            "dpod", DPOD_STATS_LATEST, DPOD_STATS_BY_HOUR, DPOD_STATS_BY_DAY, DPOD_STATS_BY_MONTH),

    CONTAINER
            (14, "Container",        "cnt",  CNT_STATS_LATEST, CNT_STATS_BY_HOUR, CNT_STATS_BY_DAY, CNT_STATS_BY_MONTH),

    SPEND_VIRTUAL_MACHINE
            (15, "VirtualMachine",   "vm_spend", null, VM_SPEND_BY_HOUR, VM_SPEND_BY_DAY, VM_SPEND_BY_MONTH),

    CLOUD_SERVICE
            (16, "CloudService",   "service_spend", null, SERVICE_SPEND_BY_HOUR, SERVICE_SPEND_BY_DAY, SERVICE_SPEND_BY_MONTH),

    CONTAINERPOD
            (17, "ContainerPod",        "cpod",  CPOD_STATS_LATEST, CPOD_STATS_BY_HOUR, CPOD_STATS_BY_DAY, CPOD_STATS_BY_MONTH),

    LOGICAL_POOL
            (18, "LogicalPool",        "lp",  LP_STATS_LATEST, LP_STATS_BY_HOUR, LP_STATS_BY_DAY, LP_STATS_BY_MONTH),

    SPEND_APP(19, "Application", "app_spend", null, APP_SPEND_BY_HOUR, APP_SPEND_BY_DAY, APP_SPEND_BY_MONTH),

    BUSINESS_APPLICATION
            (20, "BusinessApplication",      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    RESERVED_INSTANCE
            (21, "ReservedInstance", "ri", RI_STATS_LATEST, RI_STATS_BY_HOUR, RI_STATS_BY_DAY, RI_STATS_BY_MONTH),

    LOAD_BALANCER
            (22, "LoadBalancer",      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    DATABASE_SERVER
            (23, "DatabaseServer",      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    VIRTUAL_APPLICATION
            (24, "VirtualApplication",      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH),

    APPLICATION_SERVER
            (25, "ApplicationServer",      "app",  APP_STATS_LATEST, APP_STATS_BY_HOUR, APP_STATS_BY_DAY, APP_STATS_BY_MONTH)
    ;

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
        return TABLE_TO_SPEND_ENTITY_MAP.keySet().contains(hourTable);
    }

    public Table<?> getHourTable() { return hourTable; }

    public Table<?> getDayTable() {
        return dayTable;
    }

    public Table<?> getMonthTable() {
        return monthTable;
    }

    public List<Table<?>> getTables() {
        return Lists.<Table<?>>newArrayList(hourTable, dayTable, monthTable);
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

    private static Map<Integer, EntityType> VALUE_TO_ENTITY_TYPE_MAP = new HashMap<>();

    static {
        for (EntityType type : EntityType.values()) {
            VALUE_TO_ENTITY_TYPE_MAP.put(type.getValue(), type);
        }
    }

    public static EntityType valueOf(int value) {
        return VALUE_TO_ENTITY_TYPE_MAP.get(value);
    }

    private static Map<String, EntityType> NAME_TO_ENTITY_TYPE_MAP =
            new ImmutableMap.Builder<String, EntityType>()
            .put(StringConstants.CLUSTER, CLUSTER)
            .put(StringConstants.VIRTUAL_MACHINE, VIRTUAL_MACHINE)
            .put(StringConstants.DATA_CENTER, PHYSICAL_MACHINE) // DC's are intentionally mapped to PM's
            .put(StringConstants.PHYSICAL_MACHINE, PHYSICAL_MACHINE)
            .put(StringConstants.STORAGE, STORAGE)
            .put(StringConstants.APPSRV, APPLICATION_SERVER)
            .put(StringConstants.DATABASE_SERVER, DATABASE_SERVER)
            .put(StringConstants.APPLICATION, APPLICATION)
            .put(StringConstants.VIRTUAL_APPLICATION, VIRTUAL_APPLICATION)
            .put(StringConstants.CHASSIS, CHASSIS)
            .put(StringConstants.DISK_ARRAY, DISK_ARRAY)
            .put(StringConstants.IO_MODULE, IO_MODULE)
            .put(StringConstants.STORAGE_CONTROLLER, STORAGE_CONTROLLER)
            .put(StringConstants.SWITCH, SWITCH)
            .put(StringConstants.VDC, VDC)
            .put(StringConstants.VPOD, VPOD)
            .put(StringConstants.DPOD, DPOD)
            .put(StringConstants.CONTAINER, CONTAINER)
            .put(StringConstants.CONTAINERPOD, CONTAINERPOD)
            .put(StringConstants.LOGICAL_POOL, LOGICAL_POOL)
            .put(StringConstants.BUSINESS_APPLICATION, BUSINESS_APPLICATION)
            .put(StringConstants.RESERVED_INSTANCE, RESERVED_INSTANCE)
            .put(StringConstants.LOAD_BALANCER, LOAD_BALANCER)
            .build();

    // This method just get stats entity types, but for spend entity types call getSpendEntity
    public static EntityType get(String clsName){
        return checkNotNull(NAME_TO_ENTITY_TYPE_MAP.get(clsName),
                "Invalid entity class: %s", clsName);
    }

    public static Optional<EntityType> getTypeForName(String clsName) {
        return Optional.ofNullable(NAME_TO_ENTITY_TYPE_MAP.get(clsName));
    }

    private static Map<String, EntityType> NAME_TO_SPEND_ENTITY_TYPE_MAP =
                    new ImmutableMap.Builder<String, EntityType>()
                    .put(StringConstants.VIRTUAL_MACHINE, SPEND_VIRTUAL_MACHINE)
                    .put(StringConstants.DATABASE, SPEND_APP)
                    .put(StringConstants.DATABASE_SERVER, SPEND_APP)
                    .put(StringConstants.CLOUD_SERVICE, CLOUD_SERVICE)
                    .build();

    public static EntityType getSpendEntity(String clsName){
        return checkNotNull(NAME_TO_SPEND_ENTITY_TYPE_MAP.get(clsName),
                "Invalid entity class: %s", clsName);
    }

    public static Optional<EntityType> getSpendTypeForName(String clsName) {
        return Optional.ofNullable(NAME_TO_SPEND_ENTITY_TYPE_MAP.get(clsName));
    }

    public static final List<EntityType> ROLLED_UP_ENTITIES =
            new ImmutableList.Builder<EntityType>().addAll(rolledUpEntities())
            .build();

    private static List<EntityType> rolledUpEntities() {
        List<EntityType> vals = Lists.newArrayList(values());
        vals.remove(DATA_CENTER);
        vals.remove(DATACENTER);
        vals.remove(CLOUD_SERVICE);
        vals.remove(SPEND_VIRTUAL_MACHINE);
        vals.remove(BUSINESS_APPLICATION);
        vals.remove(LOAD_BALANCER);
        return vals;
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
    public static Set<Table<?>> LATEST_TABLES =
            new ImmutableSet.Builder<Table<?>>()
            .add(PM_STATS_LATEST)
            .add(VM_STATS_LATEST)
            .add(DS_STATS_LATEST)
            .add(APP_STATS_LATEST)
            .add(CH_STATS_LATEST)
            .add(DA_STATS_LATEST)
            .add(IOM_STATS_LATEST)
            .add(SC_STATS_LATEST)
            .add(SW_STATS_LATEST)
            .add(VDC_STATS_LATEST)
            .add(VPOD_STATS_LATEST)
            .add(DPOD_STATS_LATEST)
            .add(CNT_STATS_LATEST)
            .add(CPOD_STATS_LATEST)
            .add(LP_STATS_LATEST)
            .add(RI_STATS_LATEST)
            .build();

    public static boolean isMonthly(Table<?> tbl){
        return MONTHLY_TABLES.contains(tbl);
    }
    public static Set<Table<?>> MONTHLY_TABLES =
            new ImmutableSet.Builder<Table<?>>()
            .add(PM_STATS_BY_MONTH)
            .add(VM_STATS_BY_MONTH)
            .add(DS_STATS_BY_MONTH)
            .add(APP_STATS_BY_MONTH)
            .add(CH_STATS_BY_MONTH)
            .add(DA_STATS_BY_MONTH)
            .add(IOM_STATS_BY_MONTH)
            .add(SC_STATS_BY_MONTH)
            .add(SW_STATS_BY_MONTH)
            .add(VDC_STATS_BY_MONTH)
            .add(VPOD_STATS_BY_MONTH)
            .add(DPOD_STATS_BY_MONTH)
            .add(CLUSTER_STATS_BY_MONTH)
            .add(CNT_STATS_BY_MONTH)
            .add(CPOD_STATS_BY_MONTH)
            .add(VM_SPEND_BY_MONTH)
                                    .add(APP_SPEND_BY_MONTH)
            .add(SERVICE_SPEND_BY_MONTH)
            .add(LP_STATS_BY_MONTH)
            .add(RI_STATS_BY_MONTH)
            .build();

    public static boolean isDaily(Table<?> tbl){
        return DAILY_TABLES.contains(tbl);
    }
    public static Set<Table<?>> DAILY_TABLES =
            new ImmutableSet.Builder<Table<?>>()
            .add(PM_STATS_BY_DAY)
            .add(VM_STATS_BY_DAY)
            .add(DS_STATS_BY_DAY)
            .add(APP_STATS_BY_DAY)
            .add(CH_STATS_BY_DAY)
            .add(DA_STATS_BY_DAY)
            .add(IOM_STATS_BY_DAY)
            .add(SC_STATS_BY_DAY)
            .add(SW_STATS_BY_DAY)
            .add(VDC_STATS_BY_DAY)
            .add(VPOD_STATS_BY_DAY)
            .add(DPOD_STATS_BY_DAY)
            .add(CLUSTER_STATS_BY_DAY)
            .add(CNT_STATS_BY_DAY)
            .add(CPOD_STATS_BY_DAY)
            .add(VM_SPEND_BY_DAY)
            .add(APP_SPEND_BY_DAY)
            .add(SERVICE_SPEND_BY_DAY)
            .add(LP_STATS_BY_DAY)
            .add(RI_STATS_BY_DAY)
            .build();

    // note:  CLUSTER_STATS_BY_HOUR intentionally ommited from HOURLY_TABLES
    public static boolean isHourly(Table<?> tbl){
        return HOURLY_TABLES.contains(tbl);
    }
    public static Set<Table<?>> HOURLY_TABLES =
            new ImmutableSet.Builder<Table<?>>()
            .add(PM_STATS_BY_HOUR)
            .add(VM_STATS_BY_HOUR)
            .add(DS_STATS_BY_HOUR)
            .add(APP_STATS_BY_HOUR)
            .add(CH_STATS_BY_HOUR)
            .add(DA_STATS_BY_HOUR)
            .add(IOM_STATS_BY_HOUR)
            .add(SC_STATS_BY_HOUR)
            .add(SW_STATS_BY_HOUR)
            .add(VDC_STATS_BY_HOUR)
            .add(VPOD_STATS_BY_HOUR)
            .add(DPOD_STATS_BY_HOUR)
            .add(CNT_STATS_BY_HOUR)
            .add(CPOD_STATS_BY_HOUR)
            .add(SYSTEM_LOAD)
            .add(VM_SPEND_BY_HOUR)
            .add(APP_SPEND_BY_HOUR)
            .add(SERVICE_SPEND_BY_HOUR)
            .add(LP_STATS_BY_HOUR)
            .add(RI_STATS_BY_HOUR)
            .build();


    public static EntityType fromTable(Table<?> tbl){
        EntityType et = TABLE_TO_ENTITY_MAP.get(tbl);
        if (et == null){
            return TABLE_TO_SPEND_ENTITY_MAP.get(tbl);
        }
        return et;
    }

    public static Map<Table<?>, EntityType> TABLE_TO_ENTITY_MAP =
            new ImmutableMap.Builder<Table<?>, EntityType>()
            .put(PM_STATS_LATEST, PHYSICAL_MACHINE)
            .put(PM_STATS_BY_HOUR, PHYSICAL_MACHINE)
            .put(PM_STATS_BY_DAY, PHYSICAL_MACHINE)
            .put(PM_STATS_BY_MONTH, PHYSICAL_MACHINE)
            .put(VM_STATS_LATEST, VIRTUAL_MACHINE)
            .put(VM_STATS_BY_HOUR, VIRTUAL_MACHINE)
            .put(VM_STATS_BY_DAY, VIRTUAL_MACHINE)
            .put(VM_STATS_BY_MONTH, VIRTUAL_MACHINE)
            .put(DS_STATS_LATEST, STORAGE)
            .put(DS_STATS_BY_HOUR, STORAGE)
            .put(DS_STATS_BY_DAY, STORAGE)
            .put(DS_STATS_BY_MONTH, STORAGE)
            .put(APP_STATS_LATEST, APPLICATION)
            .put(APP_STATS_BY_HOUR, APPLICATION)
            .put(APP_STATS_BY_DAY, APPLICATION)
            .put(APP_STATS_BY_MONTH, APPLICATION)
            .put(CH_STATS_LATEST, CHASSIS)
            .put(CH_STATS_BY_HOUR, CHASSIS)
            .put(CH_STATS_BY_DAY, CHASSIS)
            .put(CH_STATS_BY_MONTH, CHASSIS)
            .put(DA_STATS_LATEST, DISK_ARRAY)
            .put(DA_STATS_BY_HOUR, DISK_ARRAY)
            .put(DA_STATS_BY_DAY, DISK_ARRAY)
            .put(DA_STATS_BY_MONTH, DISK_ARRAY)
            .put(IOM_STATS_LATEST, IO_MODULE)
            .put(IOM_STATS_BY_HOUR, IO_MODULE)
            .put(IOM_STATS_BY_DAY, IO_MODULE)
            .put(IOM_STATS_BY_MONTH, IO_MODULE)
            .put(SC_STATS_LATEST, STORAGE_CONTROLLER)
            .put(SC_STATS_BY_HOUR, STORAGE_CONTROLLER)
            .put(SC_STATS_BY_DAY, STORAGE_CONTROLLER)
            .put(SC_STATS_BY_MONTH, STORAGE_CONTROLLER)
            .put(SW_STATS_LATEST, SWITCH)
            .put(SW_STATS_BY_HOUR, SWITCH)
            .put(SW_STATS_BY_DAY, SWITCH)
            .put(SW_STATS_BY_MONTH, SWITCH)
            .put(VDC_STATS_LATEST, VDC)
            .put(VDC_STATS_BY_HOUR, VDC)
            .put(VDC_STATS_BY_DAY, VDC)
            .put(VDC_STATS_BY_MONTH, VDC)
            .put(VPOD_STATS_LATEST, VPOD)
            .put(VPOD_STATS_BY_HOUR, VPOD)
            .put(VPOD_STATS_BY_DAY, VPOD)
            .put(VPOD_STATS_BY_MONTH, VPOD)
            .put(DPOD_STATS_LATEST, DPOD)
            .put(DPOD_STATS_BY_HOUR, DPOD)
            .put(DPOD_STATS_BY_DAY, DPOD)
            .put(DPOD_STATS_BY_MONTH, DPOD)
            .put(CNT_STATS_LATEST, CONTAINER)
            .put(CNT_STATS_BY_HOUR, CONTAINER)
            .put(CNT_STATS_BY_DAY, CONTAINER)
            .put(CNT_STATS_BY_MONTH, CONTAINER)
            .put(CPOD_STATS_LATEST, CONTAINERPOD)
            .put(CPOD_STATS_BY_HOUR, CONTAINERPOD)
            .put(CPOD_STATS_BY_DAY, CONTAINERPOD)
            .put(CPOD_STATS_BY_MONTH, CONTAINERPOD)
            .put(CLUSTER_STATS_BY_DAY, CLUSTER)
            .put(CLUSTER_STATS_BY_MONTH, CLUSTER)
            .put(LP_STATS_LATEST, LOGICAL_POOL)
            .put(LP_STATS_BY_HOUR, LOGICAL_POOL)
            .put(LP_STATS_BY_DAY, LOGICAL_POOL)
            .put(LP_STATS_BY_MONTH, LOGICAL_POOL)
            .put(RI_STATS_LATEST, RESERVED_INSTANCE)
            .put(RI_STATS_BY_HOUR, RESERVED_INSTANCE)
            .put(RI_STATS_BY_DAY, RESERVED_INSTANCE)
            .put(RI_STATS_BY_MONTH, RESERVED_INSTANCE)
            .build();

    public static Map<Table<?>, EntityType> TABLE_TO_SPEND_ENTITY_MAP =
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
}
