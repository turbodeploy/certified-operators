package com.vmturbo.history.stats;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import org.jooq.Record;
import org.jooq.Table;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Static utility methods to support stats class testing.
 **/
public class StatsTestUtils {
    public static final Table<?> PM_LATEST_TABLE = EntityType.PHYSICAL_MACHINE.getLatestTable();
    public static final Table<?> APP_LATEST_TABLE = EntityType.APPLICATION.getLatestTable();
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    public static final String TEST_VM_PATH = "topology/tak_test1_vm_dto.json";
    public static final String TEST_APP_PATH = "topology/guestload_tak_test1_app_dto.json";
    public static final String TEST_APP_WITHOUT_PROVIDER_PATH = "topology/test2_app_without_provider_dto.json";
    public static final String TEST_PM_PATH = "topology/hp-esx_pm_dto.json";
    private static final CommodityType CPU_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.CPU_VALUE).build();
    private static final CommodityType DSPMA_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS_VALUE).build();
    private static final CommodityType Q1_VCPU_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.Q1_VCPU_VALUE).build();
    private static TopologyDTO.CommodityBoughtDTO cpuBought = TopologyDTO.CommodityBoughtDTO.newBuilder()
            .setCommodityType(CPU_COMMODITY_TYPE)
            .build();
    private static final CommodityType FLOW_0_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.FLOW_VALUE)
            .setKey("Flow-0")
            .build();
    private static final CommodityType FLOW_1_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.FLOW_VALUE)
            .setKey("Flow-1")
            .build();
    private static final double CPU_CAPACITY = 111.111;
    private static final double CPU_PERCENTILE = 0.111;
    private static final double DSPMA_CAPACITY = 100000;
    private static final double FLOW_0_CAPACITY = (double)Float.MAX_VALUE;
    private static final double FLOW_1_CAPACITY = 100_000_000.0;

    public static TopologyEntityDTO vm(long oid, long providerId) {
        return TopologyEntityDTO.newBuilder()
                        .setOid(oid)
                        .setDisplayName("VM-" + oid)
                        .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                        // 999 is the provider id. Don't care that it doesn't exist.
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(providerId)
                            .addCommodityBought(cpuBought))
                        .build();
    }

    public static TopologyEntityDTO pm(long oid, double cpuUsed) {
        return TopologyEntityDTO.newBuilder()
                        .setOid(oid)
                        .setDisplayName("PM-" + oid)
                        .setEntityType(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommoditySoldList(cpu(cpuUsed)).build();
    }

    /**
     * Create a PM that sells multiple flow commodities.
     * @param oid entity OID
     * @param flow0Used flow-0 used by this PM
     * @param flow1Used flow-1 used by this PM
     * @return the constructed PM entity
     */
    public static TopologyEntityDTO pm(long oid, double flow0Used, double flow1Used) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName("PM-" + oid)
                .setEntityType(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE)
                .addCommoditySoldList(flow0(flow0Used))
                .addCommoditySoldList(flow1(flow1Used))
                .build();
    }

    public static TopologyEntityDTO vm(long oid, long sellerOid, double cpuUsed) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName("VM-" + oid)
                .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(sellerOid)
                        .addCommodityBought(cpuBought(cpuUsed)))
                .build();
    }

    /**
     * Create a new VM entity that buys flow commodities.
     *
     * @param oid       entity OID for new VM
     * @param sellerOid OID of entity selling flows
     * @param flow0Used flow-0 commodity used value
     * @param flow1Used flow-1 commodity used value
     * @return newly created VM entity
     */
    public static TopologyEntityDTO vm(long oid, long sellerOid, double flow0Used, double flow1Used) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setDisplayName("VM-" + oid)
                .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(sellerOid)
                        .addCommodityBought(flow0Bought(flow0Used))
                        .addCommodityBought(flow1Bought(flow1Used)))
                .build();
    }

    /**
     * Create Cpu sold commodity.
     *
     * @param used a value for the amount of the commodity used.
     * @return cpu sold commodity type {@link com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO}
     */
    public static TopologyDTO.CommoditySoldDTO cpu(double used) {
        return TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(CPU_COMMODITY_TYPE)
                .setUsed(used)
                .setCapacity(CPU_CAPACITY).build();
    }

    public static TopologyDTO.CommoditySoldDTO dspma(double used) {
        return TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(DSPMA_COMMODITY_TYPE)
                        .setUsed(used)
                        .setCapacity(DSPMA_CAPACITY).build();
    }

    public static TopologyDTO.CommodityBoughtDTO cpuBought(double used) {
        return TopologyDTO.CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CPU_COMMODITY_TYPE)
                        .setUsed(used).build();
    }

    public static TopologyDTO.CommoditySoldDTO q1_vcpu(double used) {
        return TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(Q1_VCPU_COMMODITY_TYPE)
                .setUsed(used)
                .setCapacity(CPU_CAPACITY)
                .setHistoricalUsed(HistoricalValues.newBuilder().addTimeSlot(5).addTimeSlot(10)
                .setPercentile(CPU_PERCENTILE))
                .build();
    }

    /**
     * Construct a sold flow-0 commodity.
     *
     * @param flow0Used flow-0 used by seller
     * @return the constructed sold commodity
     */
    public static TopologyDTO.CommoditySoldDTO flow0(double flow0Used) {
        return TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(FLOW_0_COMMODITY_TYPE)
                .setUsed(flow0Used)
                .setCapacity(FLOW_0_CAPACITY).build();
    }

    /**
     * Construct a sold flow-1 commodity.
     *
     * @param flow1Used flow-1 used by seller
     * @return the constructed sold commodity
     */
    public static TopologyDTO.CommoditySoldDTO flow1(double flow1Used) {
        return TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(FLOW_1_COMMODITY_TYPE)
                .setCapacity(FLOW_1_CAPACITY).build();
    }

    /**
     * Construct a bought flow-0 commodity.
     *
     * @param flow0Used flow-0 used by buyer
     * @return constructed bought commodity
     */
    public static TopologyDTO.CommodityBoughtDTO flow0Bought(double flow0Used) {
        return TopologyDTO.CommodityBoughtDTO.newBuilder()
                .setCommodityType(FLOW_0_COMMODITY_TYPE)
                .setUsed(flow0Used).build();
    }

    /**
     * Construct a bought flow-0 commodity.
     *
     * @param flow1Used flow-1 used by buyer
     * @return the constructed bought commodity
     */
    public static TopologyDTO.CommodityBoughtDTO flow1Bought(double flow1Used) {
        return TopologyDTO.CommodityBoughtDTO.newBuilder()
                .setCommodityType(FLOW_1_COMMODITY_TYPE)
                .setUsed(flow1Used).build();
    }

    /**
     * Use GSON to read a single TopologyDTO from a json file.
     *
     * @param testDTOFilePath the path to the resource to read for the TopologyDTO .json
     * @return a {@link TopologyDTO}s read from the given JSON test file
     * @throws Exception not supposed to happen
     */
    public static TopologyEntityDTO generateEntityDTO(String testDTOFilePath)
            throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource(testDTOFilePath).getFile());
        final InputStream dtoInputStream = new FileInputStream(file);
        InputStreamReader inputStreamReader = new InputStreamReader(dtoInputStream);
        JsonReader topologyReader = new JsonReader(inputStreamReader);
        return GSON.fromJson(topologyReader, TopologyEntityDTO.class);
    }

    /**
     * Use GSON to read the test file TopologyDTO's into a collection.
     *
     * @param testTopologyPath the path to the resource to read for the test topology .json
     * @param testTopologyFileName the test file name
     * @return a collection of {@link TopologyDTO}s read from the given JSON test file
     * @throws Exception not supposed to happen
     */
    public static Collection<TopologyEntityDTO> generateEntityDTOs(String testTopologyPath,
                                                                   String testTopologyFileName)
            throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL zipUrl = classLoader.getResource(testTopologyPath);
        ZipFile zip = new ZipFile(new File(zipUrl.toURI()));
        final ZipEntry zipEntry = zip.getEntry(testTopologyFileName);
        final InputStream topologyInputStream = zip.getInputStream(zipEntry);
        InputStreamReader inputStreamReader = new InputStreamReader(topologyInputStream);
        JsonReader topologyReader = new JsonReader(inputStreamReader);
        return Arrays.asList(GSON.fromJson(topologyReader, TopologyEntityDTO[].class));
    }

    /**
     * Create a Record to use in a response list. Use a PmStatsLatestRecord just as an example -
     * the type of the Record is not important. All of the different _stats_latest records have the
     * same schema.
     *
     * @param snapshotTime the time this stat was recorded
     * @param testValue the value of the stat
     * @param propType the property type for this stat
     * @param propSubType the property subtype for this stat
     */
    @Nonnull
    public static Record newStatRecord(@Nonnull final Timestamp snapshotTime,
                                final float testValue,
                                @Nonnull final String propType,
                                @Nonnull final String propSubType) {
        return newStatRecordWithKey(snapshotTime, testValue, propType, propSubType, null);
    }

    /**
     * Create a Record to use in a response list, with specific commodity key and a default
     * effective capacity percentage of 100%.
     *
     * @param snapshotTime the time this stat was recorded
     * @param testValue the value of the stat
     * @param propType the property type for this stat
     * @param propSubType the property subtype for this stat
     * @param commodityKey the commodity key for this stat
     */
    @Nonnull
    public static Record newStatRecordWithKey(@Nonnull final Timestamp snapshotTime,
                                       final float testValue,
                                       @Nonnull final String propType,
                                       @Nonnull final String propSubType,
                                       @Nullable String commodityKey) {
        return newStatRecordWithKeyAndEffectiveCapacity(snapshotTime, testValue, 1.0,
                propType, propSubType, commodityKey);
    }


    /**
     * Create a record with a specific key and effective capacity percentage.
     *
     *  Use a PmStatsLatestRecord just as an example - the type of the Record is not important. All
     *  of the different _stats_latest records have the same schema.
     *
     * @param snapshotTime
     * @param testValue
     * @param effectiveCapacityPercentage
     * @param propType
     * @param propSubType
     * @param commodityKey
     * @return
     */
    @Nonnull
    public static Record newStatRecordWithKeyAndEffectiveCapacity(@Nonnull final Timestamp snapshotTime,
                                              final float testValue,
                                              final double effectiveCapacityPercentage,
                                              @Nonnull final String propType,
                                              @Nonnull final String propSubType,
                                              @Nullable String commodityKey) {
        return newStatRecordWithKeyAndEffectiveCapacityAndProducerUuid(snapshotTime, testValue,
            effectiveCapacityPercentage, propType, propSubType, commodityKey, null);
    }

    @Nonnull
    public static Record newStatRecordWithProducerUuid(@Nonnull final Timestamp snapshotTime,
                                                       final float testValue,
                                                       @Nonnull final String propType,
                                                       @Nonnull final String propSubType,
                                                       @Nonnull String producerUuid) {
        return newStatRecordWithKeyAndEffectiveCapacityAndProducerUuid(snapshotTime, testValue,
            1.0, propType, propSubType, null, producerUuid);
    }

    @Nonnull
    public static Record newMarketStatRecordWithEntityType(@Nonnull final Timestamp snapshotTime,
                                                       final double testValue,
                                                       @Nonnull final String propType,
                                                       @Nonnull final String propSubType,
                                                       @Nonnull final RelationType relation,
                                                       @Nullable String entityType) {
        final MarketStatsLatestRecord statsRecord = new MarketStatsLatestRecord();
        statsRecord.setSnapshotTime(snapshotTime);
        statsRecord.setPropertyType(propType);
        statsRecord.setPropertySubtype(propSubType);
        statsRecord.setRelation(relation);
        statsRecord.setAvgValue(testValue);
        statsRecord.setMinValue(testMinValue(testValue));
        statsRecord.setMaxValue(testMaxValue(testValue));
        statsRecord.setCapacity(testCapacity(testValue));
        if (entityType != null) {
            statsRecord.setEntityType(entityType);
        }
        return statsRecord;
    }

    @Nonnull
    private static Record newStatRecordWithKeyAndEffectiveCapacityAndProducerUuid(
                @Nonnull final Timestamp snapshotTime,
                final float testValue,
                final double effectiveCapacityPercentage,
                @Nonnull final String propType,
                @Nonnull final String propSubType,
                @Nullable String commodityKey,
                @Nullable String producerUuid) {
        final PmStatsLatestRecord statsRecord = new PmStatsLatestRecord();
        statsRecord.setSnapshotTime(snapshotTime);
        statsRecord.setPropertyType(propType);
        statsRecord.setPropertySubtype(propSubType);
        statsRecord.setAvgValue((double)testValue);
        statsRecord.setMinValue(testMinValue(testValue));
        statsRecord.setMaxValue(testMaxValue(testValue));
        statsRecord.setCapacity(testCapacity(testValue));
        // we'll simulate a usage throttle of 75%
        statsRecord.setEffectiveCapacity(statsRecord.getCapacity() * effectiveCapacityPercentage);
        if (commodityKey != null) {
            statsRecord.setCommodityKey(commodityKey);
        }
        if (producerUuid != null) {
            statsRecord.setProducerUuid(producerUuid);
        }
        return statsRecord;
    }

    private static double testMaxValue(final double testValue) {
        return testValue * 2;
    }

    private static double testMinValue(final double testValue) {
        return testValue / 2;
    }

    private static double testCapacity(final double testValue) {
        return testValue * 3;
    }

    @Nonnull
    private static StatValue expectedStatsValue(float testValue) {
        return StatValue.newBuilder()
            .setAvg(testValue)
            .setMin((float)testMinValue(testValue))
            .setMax((float)testMaxValue(testValue))
            .setTotal(testValue)
            .build();
    }

    @Nonnull
    public static StatRecord expectedStatRecord(final String commodityName,
                                                final float testValue,
                                                final RelationType relation,
                                                final String relatedEntityType) {
        // testCapacity(1), testMaxValue(1), expectedStatsValue(1),
        final StatValue expectedValue = expectedStatsValue(testValue);
        return StatRecord.newBuilder()
            .setName(commodityName)
            .setCapacity(StatValue.newBuilder()
                .setMin((float)testCapacity(testValue))
                .setMax((float)testCapacity(testValue))
                .setAvg((float)testCapacity(testValue))
                .setTotal((float)testCapacity(testValue))
                .build())
            .setReserved(0)
            .setCurrentValue((float)testMaxValue(testValue))
            .setValues(expectedValue)
            .setUsed(expectedValue)
            .setPeak(expectedValue)
            .setRelation(relation.toString())
            .setRelatedEntityType(relatedEntityType)
            .build();
    }

    /**
     * Create aggregated entities map based on given seedEntities, the derived entities only
     * include the seedEntity.
     *
     * @param seedEntities set of entities ids to create EntityGroup map for
     * @return mapping from seed entity to derived entities
     */
    public static Map<Long, Set<Long>> createEntityGroupsMap(@Nonnull Set<Long> seedEntities) {
        return seedEntities.stream()
            .collect(Collectors.toMap(Function.identity(), Collections::singleton));
    }
}
