package com.vmturbo.extractor.models;

import java.sql.Timestamp;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.tables.Entity;
import com.vmturbo.extractor.schema.tables.Metric;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Definitions of models, tables, and columns used in topology ingestion.
 */
public class ModelDefinitions {

    private ModelDefinitions() {
    }

    /** TIME column. */
    public static final Column<Timestamp> TIME = Column.timestampColumn("time");
    /** ENTITY_OID column. */
    public static final Column<Long> ENTITY_OID = Column.longColumn("entity_oid");
    /** ENTITY_OID, named just "oid". */
    public static final Column<Long> ENTITY_OID_AS_OID = Column.longColumn("oid");
    /** ENTITY_HASH column. */
    public static final Column<Long> ENTITY_HASH = Column.longColumn("entity_hash");
    /** ENTITY_HASH column, named just "hash". */
    public static final Column<Long> ENTITY_HASH_AS_HASH = Column.longColumn("hash");
    /** ENTITY_NAME column. */
    public static final Column<String> ENTITY_NAME = Column.stringColumn("name");
    /** ENTITY_TYPE column, named just "type". */
    public static final Column<EntityType> ENTITY_TYPE_AS_TYPE = Column.entityTypeColumn(Entity.ENTITY.TYPE.getName());
    /** ENTITY_STATE column. */
    public static final Column<EntityState> ENTITY_STATE = Column.entityStateColumn(Entity.ENTITY.STATE.getName());
    /** ENVIRONMENT_TYPE column. */
    public static final Column<EnvironmentType> ENVIRONMENT_TYPE = Column.environmentTypeColumn(Entity.ENTITY.ENVIRONMENT.getName());
    /** ATTRS column. */
    public static final Column<JsonString> ATTRS = Column.jsonColumn("attrs");
    /** SCOPED_OIDS column. */
    public static final Column<Long[]> SCOPED_OIDS = Column.longSetColumn("scoped_oids");
    /** FIRST_SEEN column. */
    public static final Column<Timestamp> FIRST_SEEN = Column.timestampColumn("first_seen");
    /** LAST_SEEN column. */
    public static final Column<Timestamp> LAST_SEEN = Column.timestampColumn("last_seen");
    /** COMMODITY_TYPE column. */
    public static final Column<MetricType> COMMODITY_TYPE = new Column<>(Metric.METRIC.TYPE, ColType.METRIC_TYPE);
    /** COMMODITY_KEY column. */
    public static final Column<String> COMMODITY_KEY = Column.stringColumn("key");
    /** COMMODITY_CURRENT column. */
    public static final Column<Double> COMMODITY_CURRENT = Column.doubleColumn("current");
    /** COMMODITY_CAPACITY column. */
    public static final Column<Double> COMMODITY_CAPACITY = Column.doubleColumn("capacity");
    /** COMMODITY_UTILIZATION column. */
    public static final Column<Double> COMMODITY_UTILIZATION = Column.doubleColumn("utilization");
    /** COMMODITY_CONSUMED column. */
    public static final Column<Double> COMMODITY_CONSUMED = Column.doubleColumn("consumed");
    /** COMMODITY_PROVIDER column. */
    public static final Column<Long> COMMODITY_PROVIDER = Column.longColumn("provider_oid");

    /** ENTITY_TABLE. */
    public static final Table ENTITY_TABLE = Table.named("entity")
            .withColumns(ENTITY_OID_AS_OID, ENTITY_HASH_AS_HASH, ENTITY_TYPE_AS_TYPE, ENTITY_NAME,
                    ENVIRONMENT_TYPE, ENTITY_STATE, ATTRS, SCOPED_OIDS, FIRST_SEEN, LAST_SEEN)
            .build();

    /** METRIC_TABLE. */
    public static final Table METRIC_TABLE = Table.named("metric")
            .withColumns(TIME, ENTITY_OID, ENTITY_HASH, COMMODITY_TYPE,
                    COMMODITY_CURRENT, COMMODITY_CAPACITY, COMMODITY_UTILIZATION, COMMODITY_CONSUMED,
                    COMMODITY_PROVIDER,
                    COMMODITY_KEY)
            .build();

    /** Column for file path. */
    public static final Column<String> FILE_PATH = Column.stringColumn("path");
    /** Column for last modification time. */
    public static final Column<Timestamp> MODIFICATION_TIME = Column.timestampColumn("modification_time");
    /** Column for file size. */
    public static final Column<Long> FILE_SIZE = Column.longColumn("file_size_kb");
    /** Column for storage oid. */
    public static final Column<Long> STORAGE_OID = Column.longColumn("storage_oid");
    /** Column for storage displayName. */
    public static final Column<String> STORAGE_NAME = Column.stringColumn("storage_name");

    /** wasted_file table. */
    public static final Table WASTED_FILE_TABLE = Table.named("wasted_file")
            .withColumns(FILE_PATH, FILE_SIZE, MODIFICATION_TIME, STORAGE_OID, STORAGE_NAME)
            .build();

    /** REPORTING_MODEL. */
    public static final Model REPORTING_MODEL = Model.named("reporting")
            .withTables(ENTITY_TABLE, METRIC_TABLE, WASTED_FILE_TABLE)
            .build();

    /**
     * Default whitelisted commodity types for reporting.
     *
     * <p>Commodity metrics for other types are not recorded.</p>
     */
    public static final Set<CommodityType> REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST =
            ImmutableSet.<CommodityType>builder()
                    .add(CommodityType.ACTIVE_SESSIONS)
                    .add(CommodityType.BALLOONING)
                    .add(CommodityType.BUFFER_COMMODITY)
                    .add(CommodityType.CONNECTION)
                    .add(CommodityType.CPU)
                    .add(CommodityType.CPU_ALLOCATION)
                    .add(CommodityType.CPU_PROVISIONED)
                    .add(CommodityType.DB_CACHE_HIT_RATE)
                    .add(CommodityType.DB_MEM)
                    .add(CommodityType.EXTENT)
                    .add(CommodityType.FLOW)
                    .add(CommodityType.FLOW_ALLOCATION)
                    .add(CommodityType.HEAP)
                    .add(CommodityType.IMAGE_CPU)
                    .add(CommodityType.IMAGE_MEM)
                    .add(CommodityType.IMAGE_STORAGE)
                    .add(CommodityType.IO_THROUGHPUT)
                    .add(CommodityType.MEM)
                    .add(CommodityType.MEM_ALLOCATION)
                    .add(CommodityType.MEM_PROVISIONED)
                    .add(CommodityType.NET_THROUGHPUT)
                    .add(CommodityType.POOL_CPU)
                    .add(CommodityType.POOL_MEM)
                    .add(CommodityType.POOL_STORAGE)
                    .add(CommodityType.PORT_CHANEL)
                    .add(CommodityType.Q1_VCPU)
                    .add(CommodityType.Q2_VCPU)
                    .add(CommodityType.Q3_VCPU)
                    .add(CommodityType.Q4_VCPU)
                    .add(CommodityType.Q5_VCPU)
                    .add(CommodityType.Q6_VCPU)
                    .add(CommodityType.Q7_VCPU)
                    .add(CommodityType.Q8_VCPU)
                    .add(CommodityType.Q16_VCPU)
                    .add(CommodityType.Q32_VCPU)
                    .add(CommodityType.Q64_VCPU)
                    .add(CommodityType.QN_VCPU)
                    .add(CommodityType.REMAINING_GC_CAPACITY)
                    .add(CommodityType.RESPONSE_TIME)
                    .add(CommodityType.SLA_COMMODITY)
                    .add(CommodityType.STORAGE_ACCESS)
                    .add(CommodityType.STORAGE_ALLOCATION)
                    .add(CommodityType.STORAGE_AMOUNT)
                    .add(CommodityType.STORAGE_LATENCY)
                    .add(CommodityType.STORAGE_PROVISIONED)
                    .add(CommodityType.SWAPPING)
                    .add(CommodityType.THREADS)
                    .add(CommodityType.TRANSACTION)
                    .add(CommodityType.TRANSACTION_LOG)
                    .add(CommodityType.VCPU)
                    .add(CommodityType.VCPU_LIMIT_QUOTA)
                    .add(CommodityType.VCPU_REQUEST)
                    .add(CommodityType.VCPU_REQUEST_QUOTA)
                    .add(CommodityType.VMEM)
                    .add(CommodityType.VMEM_LIMIT_QUOTA)
                    .add(CommodityType.VMEM_REQUEST)
                    .add(CommodityType.VMEM_REQUEST_QUOTA)
                    .add(CommodityType.VSTORAGE)
                    .add(CommodityType.TOTAL_SESSIONS)
                    .build();

    /**
     * Commodity types for which we write a separate metric record for each provided commodity key,
     * as opposed to summing metric values across all commodity keys for the same commodity type
     * sold by a given seller.
     *
     * <p>In some cases, the treatment is dependent on the entity type that is  selling the
     * commodity. This map lists all the entity types for which a given commodity type should not be
     * aggregated. When a commodity is unaggregated for all selling entity types, use
     * `EntityType.values()` in the builder row for that commodity type.</p>
     */
    public static final Multimap<CommodityType, EntityDTO.EntityType> UNAGGREGATED_KEYED_COMMODITY_TYPES =
            ImmutableSetMultimap.<CommodityType, EntityDTO.EntityType>builder()
                    // TODO 59460 fill in with correct values
                    .build();

    /** ENTITY STATE enum column. */
    public static final Column<EntityState> ENTITY_STATE_ENUM = Column.entityStateColumn("state");
    /** ENVIRONMENT TYPE enum column. */
    public static final Column<EnvironmentType> ENVIRONMENT_TYPE_ENUM = Column.environmentTypeColumn("environment");
    /** ENTITY TYPE enum column. */
    public static final Column<EntityType> ENTITY_TYPE_ENUM = Column.entityTypeColumn("type");
    /** ENTITY SEVERITY enum column. */
    public static final Column<Severity> SEVERITY_ENUM = Column.severityColumn("severity");
    /** ACTIONS COUNT enum column. */
    public static final Column<Integer> NUM_ACTIONS = Column.intColumn("num_actions");

    /** SEARCH_ENTITY_TABLE. */
    public static final Table SEARCH_ENTITY_TABLE = Table.named("search_entity")
            .withColumns(ENTITY_OID_AS_OID, ENTITY_TYPE_ENUM, ENTITY_NAME, ENVIRONMENT_TYPE_ENUM,
                    ENTITY_STATE_ENUM, SEVERITY_ENUM, NUM_ACTIONS, ATTRS)
            .build();

    /** SEARCH_MODEL. */
    public static final Model SEARCH_MODEL = Model.named("search")
            .withTables(SEARCH_ENTITY_TABLE)
            .build();

}
