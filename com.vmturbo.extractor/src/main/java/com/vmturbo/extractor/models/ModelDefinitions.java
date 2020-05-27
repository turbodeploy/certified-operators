package com.vmturbo.extractor.models;

import static com.vmturbo.extractor.models.HashUtil.XXHASH_FACTORY;
import static com.vmturbo.extractor.models.HashUtil.XXHASH_SEED;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.sql.Timestamp;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.extractor.models.Column.Builder;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Definitions of models, tables, and columns used in topology ingestion.
 */
public class ModelDefinitions {

    private ModelDefinitions() {
    }

    /** TIME column. */
    public static final Column<Timestamp> TIME = Column.timestampColumn("time").build();
    /** ENTITY_OID column. */
    public static final Column<Long> ENTITY_OID = Column.longColumn("entity_oid").build();
    /** ENTITY_OID, named just "oid". */
    public static final Column<Long> ENTITY_OID_AS_OID = Column.longColumn("oid").build();
    /** ENTITY_HASH column. */
    public static final Column<Long> ENTITY_HASH = Column.longColumn("entity_hash").build();
    /** ENTITY_HASH column, named just "hash". */
    public static final Column<Long> ENTITY_HASH_AS_HASH = Column.longColumn("hash").build();
    /** ENTITY_NAME column. */
    public static final Column<String> ENTITY_NAME = Column.stringColumn("name").build();
    /** ENTITY_TYPE column. */
    static final Column<String> ENTITY_TYPE = Column.stringColumn("entity_type").build();
    /** ENTITY_TYPE column, named just "type". */
    public static final Column<String> ENTITY_TYPE_AS_TYPE = Column.stringColumn("type").build();
    /** ENTITY_STATE column. */
    public static final Column<String> ENTITY_STATE = Column.stringColumn("state").build();
    /** ENVIRONMENT_TYPE column. */
    public static final Column<String> ENVIRONMENT_TYPE = Column.stringColumn("environment").build();
    /** ATTRS column. */
    public static final Column<JsonString> ATTRS = Column.jsonColumn("attrs").build();
    /** SCOPED_OIDS column. */
    public static final Column<Long[]> SCOPED_OIDS = Column.longArrayColumn("scoped_oids")
            .withHashFunc(scope -> scopeHash((Long[])scope)).build();
    /** FIRST_SEEN column. */
    public static final Column<Timestamp> FIRST_SEEN = Column.timestampColumn("first_seen").build();
    /** LAST_SEEN column. */
    public static final Column<Timestamp> LAST_SEEN = Column.timestampColumn("last_seen").build();
    /** COMMODITY_TYPE column. */
    public static final Column<String> COMMODITY_TYPE = Column.stringColumn("type").build();
    /** COMMODITY_CURRENT column. */
    public static final Column<Double> COMMODITY_CURRENT = Column.doubleColumn("current").build();
    /** COMMODITY_CAPACITY column. */
    public static final Column<Double> COMMODITY_CAPACITY = Column.doubleColumn("capacity").build();
    /** COMMODITY_UTILIZATION column. */
    public static final Column<Double> COMMODITY_UTILIZATION = Column.doubleColumn("utilization").build();
    /** COMMODITY_CONSUMED column. */
    public static final Column<Double> COMMODITY_CONSUMED = Column.doubleColumn("consumed").build();
    /** COMMODITY_PROVIDER column. */
    public static final Column<Long> COMMODITY_PROVIDER = Column.longColumn("provider_oid").build();

    /** ENTITY_TABLE. */
    public static final Table ENTITY_TABLE = Table.named("entity")
            .withColumns(ENTITY_OID_AS_OID, ENTITY_HASH_AS_HASH, ENTITY_TYPE_AS_TYPE, ENTITY_NAME,
                    ENVIRONMENT_TYPE, ENTITY_STATE, ATTRS, SCOPED_OIDS, FIRST_SEEN, LAST_SEEN)
            .build();

    /** METRIC_TABLE. */
    public static final Table METRIC_TABLE = Table.named("metric")
            .withColumns(TIME, ENTITY_OID, ENTITY_HASH, COMMODITY_TYPE,
                    COMMODITY_CURRENT, COMMODITY_CAPACITY, COMMODITY_UTILIZATION, COMMODITY_CONSUMED,
                    COMMODITY_PROVIDER)
            .build();


    /** REPORTING_MODEL. */
    public static final Model REPORTING_MODEL = Model.named("reporting")
            .withTables(ENTITY_TABLE, METRIC_TABLE)
            .build();

    /**
     * Whitelisted commodity types.
     *
     * <p>Commodity metrics for other types are not recorded.</p>
     */
    public static final Set<CommodityType> COMMODITY_TYPES_WHITELIST = ImmutableSet.<CommodityType>builder()
            .add(CommodityType.BALLOONING)
            .add(CommodityType.REMAINING_GC_CAPACITY)
            .add(CommodityType.COOLING)
            .add(CommodityType.CPU)
            .add(CommodityType.CPU_ALLOCATION)
            .add(CommodityType.CPU_PROVISIONED)
            .add(CommodityType.EXTENT)
            .add(CommodityType.HEAP)
            .add(CommodityType.IO_THROUGHPUT)
            .add(CommodityType.MEM)
            .add(CommodityType.MEM_ALLOCATION)
            .add(CommodityType.MEM_PROVISIONED)
            .add(CommodityType.NET_THROUGHPUT)
            .add(CommodityType.PORT_CHANEL)
            .add(CommodityType.POWER)
            .add(CommodityType.Q16_VCPU)
            .add(CommodityType.Q1_VCPU)
            .add(CommodityType.Q2_VCPU)
            .add(CommodityType.Q32_VCPU)
            .add(CommodityType.Q4_VCPU)
            .add(CommodityType.Q64_VCPU)
            .add(CommodityType.Q8_VCPU)
            .add(CommodityType.SLA_COMMODITY)
            .add(CommodityType.SPACE)
            .add(CommodityType.STORAGE_ACCESS)
            .add(CommodityType.STORAGE_AMOUNT)
            .add(CommodityType.STORAGE_LATENCY)
            .add(CommodityType.STORAGE_PROVISIONED)
            .add(CommodityType.SWAPPING)
            .add(CommodityType.THREADS)
            .add(CommodityType.VCPU)
            .add(CommodityType.VMEM)
            .add(CommodityType.VSTORAGE)
            .build();

    private static final ByteBuffer longBytes = ByteBuffer.allocate(Long.BYTES);
    private static final LongBuffer longBuffer = longBytes.asLongBuffer();

    /**
     * Method to compute a hash value for an entity scope value.
     *
     * <p>A scope value is an array of longs, and we want the hash to be order-independent. So we
     * compute individual hash values for the elements (to get good dispersion), and then XOR those
     * hash values ot arrive at an overall hash for the scope.</p>
     *
     * @param scope oids of entities/groups in this entity's scope scope
     * @return hash value
     */
    private static byte[] scopeHash(Long[] scope) {
        long hash = 0L;
        for (final Long oid : scope) {
            if (oid != null) {
                longBuffer.put(0, oid);
                final long oidHash = XXHASH_FACTORY.hash64().hash(
                        longBytes, 0, Long.BYTES, XXHASH_SEED);
                hash = hash ^ oidHash;
            }
        }
        return Column.toBytes(hash);
    }
}
