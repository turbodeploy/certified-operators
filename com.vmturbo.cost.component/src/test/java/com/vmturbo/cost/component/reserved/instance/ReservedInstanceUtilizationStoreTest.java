package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationLatestRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class ReservedInstanceUtilizationStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static final double DELTA = 0.000001;

    private DSLContext dsl = dbConfig.getDslContext();

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private ReservedInstanceCostCalculator reservedInstanceCostCalculator;

    private PriceTableStore priceTableStore = Mockito.mock(PriceTableStore.class);

    final EntityRICoverageUpload coverageOne = EntityRICoverageUpload.newBuilder()
            .setEntityId(123L)
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testOne")
                    .setCoveredCoupons(10))
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testTwo")
                    .setCoveredCoupons(20))
            .build();

    final EntityRICoverageUpload coverageTwo = EntityRICoverageUpload.newBuilder()
            .setEntityId(124L)
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testOne")
                    .setCoveredCoupons(30))
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testThree")
                    .setCoveredCoupons(50))
            .build();

    final ReservedInstanceBoughtInfo riInfoOne = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(123L)
            .setProbeReservedInstanceId("testOne")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(100L)
            .setStartTime(System.currentTimeMillis())
            .setEndTime(LocalDateTime.now().plus(Period.ofYears(1))
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli())
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                    .setNumberOfCoupons(100))
            .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                            .newBuilder()
                            .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(15))
                            .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
            .setNumBought(10)
            .build();

    final ReservedInstanceBoughtInfo riInfoTwo = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(456)
            .setProbeReservedInstanceId("testTwo")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(100L)
            .setStartTime(System.currentTimeMillis())
            .setEndTime(LocalDateTime.now().plus(Period.ofYears(1))
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli())
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                    .setNumberOfCoupons(100))
            .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                            .newBuilder()
                            .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(15))
                            .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
            .setNumBought(20)
            .build();

    final ReservedInstanceBoughtInfo riInfoThree = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(789)
            .setProbeReservedInstanceId("testThree")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(50L)
            .setStartTime(System.currentTimeMillis())
            .setEndTime(LocalDateTime.now().plus(Period.ofYears(1))
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli())
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                    .setNumberOfCoupons(100))
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                                    .newBuilder()
                                    .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(15))
                                    .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
            .setNumBought(30)
            .build();

    @Before
    public void setup() throws Exception {
        Map<Long, PricingDTO.ReservedInstancePrice> map = new HashMap<>();
        ReservedInstancePriceTable riPriceTable = ReservedInstancePriceTable.newBuilder()
                .putAllRiPricesBySpecId(map).build();
        Mockito.when(priceTableStore.getMergedRiPriceTable()).thenReturn(riPriceTable);
        reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl, new IdentityProvider(0), 10);
        reservedInstanceCostCalculator = new ReservedInstanceCostCalculator(reservedInstanceSpecStore);
        reservedInstanceBoughtStore = new ReservedInstanceBoughtStore(dsl,
                        new IdentityProvider(0), reservedInstanceCostCalculator, priceTableStore);
        entityReservedInstanceMappingStore = new EntityReservedInstanceMappingStore(dsl);
        reservedInstanceUtilizationStore = new ReservedInstanceUtilizationStore(dsl, reservedInstanceBoughtStore,
                reservedInstanceSpecStore, entityReservedInstanceMappingStore);
        insertDefaultReservedInstanceSpec();
    }

    @Test
    public void testUpdateReservedInstanceUtilization() {
        final List<ReservedInstanceBoughtInfo> reservedInstancesBoughtInfo =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree);
        final List<EntityRICoverageUpload> entityCoverageLists =
                Arrays.asList(coverageOne, coverageTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstancesBoughtInfo);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl,
                stichRIOidToCoverageUploads(entityCoverageLists));
        reservedInstanceUtilizationStore.updateReservedInstanceUtilization(dsl);
        List<ReservedInstanceUtilizationLatestRecord> records =
                dsl.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).fetch();
        List<ReservedInstanceBoughtRecord> riBought = dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT).fetch();
        Map<String, Long> riProbeIdMap = riBought.stream()
                .collect(Collectors.toMap(ReservedInstanceBoughtRecord::getProbeReservedInstanceId,
                        ReservedInstanceBoughtRecord::getId));
        assertEquals(3L, records.size());
        final Optional<ReservedInstanceUtilizationLatestRecord> firstRI =
                records.stream()
                        .filter(record -> record.getId().equals(riProbeIdMap.get("testOne")))
                        .findFirst();
        final Optional<ReservedInstanceUtilizationLatestRecord> secondRI =
                records.stream()
                        .filter(record -> record.getId().equals(riProbeIdMap.get("testTwo")))
                        .findFirst();
        final Optional<ReservedInstanceUtilizationLatestRecord> thirdRI =
                records.stream()
                        .filter(record -> record.getId().equals(riProbeIdMap.get("testThree")))
                        .findFirst();
        assertTrue(firstRI.isPresent());
        assertTrue(secondRI.isPresent());
        assertTrue(thirdRI.isPresent());
        assertEquals(100.0, firstRI.get().getTotalCoupons(), DELTA);
        assertEquals(40.0, firstRI.get().getUsedCoupons(), DELTA);
        assertEquals(100.0, secondRI.get().getTotalCoupons(), DELTA);
        assertEquals(20.0, secondRI.get().getUsedCoupons(), DELTA);
        assertEquals(100.0, thirdRI.get().getTotalCoupons(), DELTA);
        assertEquals(50.0, thirdRI.get().getUsedCoupons(), DELTA);
    }

    @Test
    public void testGetRIUtilizationStatsRecords() {
        final List<ReservedInstanceBoughtInfo> reservedInstancesBoughtInfo =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree);
        final List<EntityRICoverageUpload> entityCoverageLists =
                Arrays.asList(coverageOne, coverageTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstancesBoughtInfo);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl,
                stichRIOidToCoverageUploads(entityCoverageLists));
        reservedInstanceUtilizationStore.updateReservedInstanceUtilization(dsl);
        // get all ri utilization records
        final ReservedInstanceUtilizationFilter filter = ReservedInstanceUtilizationFilter.newBuilder()
                .build();
        final List<ReservedInstanceStatsRecord> riStatsRecords =
                reservedInstanceUtilizationStore.getReservedInstanceUtilizationStatsRecords(filter);
        assertEquals(1L, riStatsRecords.size());
        final ReservedInstanceStatsRecord riStatRecord = riStatsRecords.get(0);
        assertEquals(300L, riStatRecord.getCapacity().getTotal(), DELTA);
        assertEquals(100L, riStatRecord.getCapacity().getMax(), DELTA);
        assertEquals(100L, riStatRecord.getCapacity().getMin(), DELTA);
        assertEquals(100L, riStatRecord.getCapacity().getAvg(), DELTA);
        assertEquals(110L, riStatRecord.getValues().getTotal(), DELTA);
        assertEquals(20L, riStatRecord.getValues().getMin(), DELTA);
        assertEquals(50L, riStatRecord.getValues().getMax(), DELTA);

        final Collection<ReservedInstanceStatsRecord> riLatestStatsRecords =
                reservedInstanceUtilizationStore.getReservedInstanceUtilizationStatsRecords(filter);
        assertEquals(1, riLatestStatsRecords.size());
    }

    private void insertDefaultReservedInstanceSpec() {
        final CloudCostDTO.ReservedInstanceType riType1 = CloudCostDTO.ReservedInstanceType.newBuilder().setTermYears(1).setOfferingClass(
                        CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD).setPaymentOption(
                        CloudCostDTO.ReservedInstanceType.PaymentOption.ALL_UPFRONT).build();

        final CloudCostDTO.ReservedInstanceType riType2 = CloudCostDTO.ReservedInstanceType.newBuilder().setTermYears(2).setOfferingClass(
                        CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD).setPaymentOption(
                        CloudCostDTO.ReservedInstanceType.PaymentOption.ALL_UPFRONT).build();
        final ReservedInstanceSpecRecord specRecordOne = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(99L,
                        OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getValue(),
                        1,
                        Tenancy.DEDICATED.getValue(),
                        OSType.LINUX.getValue(),
                        88L,
                        77L,
                        ReservedInstanceSpecInfo.newBuilder().setRegionId(77L).setType(riType1).build()));
        final ReservedInstanceSpecRecord specRecordTwo = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(100L,
                        OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getValue(),
                        2,
                        Tenancy.HOST.getValue(),
                        OSType.LINUX.getValue(),
                        90L,
                        78L,
                        ReservedInstanceSpecInfo.newBuilder().setRegionId(78L).setType(riType2).build()));
        dsl.batchInsert(Arrays.asList(specRecordOne, specRecordTwo)).execute();
    }

    /**
     * This mirrors the behavior in RIAndExpenseUploadRpcService::updateCoverageWithLocalRIBoughtIds
     * @param entityRICoverageUploads
     * @return
     */
    private List<EntityRICoverageUpload> stichRIOidToCoverageUploads(
            List<EntityRICoverageUpload> entityRICoverageUploads) {

        final Map<String, Long> riProbeIdToOid = reservedInstanceBoughtStore
                .getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter.SELECT_ALL_FILTER)
                .stream()
                .filter(ReservedInstanceBought::hasReservedInstanceBoughtInfo)
                .collect(
                        Collectors.toMap(
                                ri -> ri.getReservedInstanceBoughtInfo()
                                        .getProbeReservedInstanceId(),
                                ReservedInstanceBought::getId
                        ));

        return entityRICoverageUploads.stream()
                .map(entityRICoverage -> EntityRICoverageUpload.newBuilder(entityRICoverage))
                // update the ReservedInstanceId for each Coverage record, mapping through
                // the ProbeReservedInstanceId
                .peek(entityRiCoverageBuilder -> entityRiCoverageBuilder
                        .getCoverageBuilderList().stream()
                        .forEach(coverageBuilder -> coverageBuilder
                                .setReservedInstanceId(riProbeIdToOid.getOrDefault(
                                        coverageBuilder.getProbeReservedInstanceId(), 0L))))
                .map(EntityRICoverageUpload.Builder::build)
                .collect(Collectors.toList());

    }
}
