package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.AccountRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationLatestRecord;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.savings.EntitySavingsException;
import com.vmturbo.cost.component.topology.IngestedTopologyStore;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

@RunWith(Parameterized.class)
public class ReservedInstanceUtilizationStoreTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    private static final Logger logger = LogManager.getLogger();


    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect to use
     * @throws SQLException if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    public ReservedInstanceUtilizationStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost", TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Rule chain to manage db provisioning and lifecycle.
     */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private static final double DELTA = 0.000001;

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private final PriceTableStore priceTableStore = Mockito.mock(PriceTableStore.class);

    private AccountRIMappingStore accountRIMappingStore;

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
            .setReservedInstanceBoughtCoupons(
                    ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(100))
            .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                            .setFixedCost(CurrencyAmount.newBuilder().setAmount(15))
                            .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
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
            .setReservedInstanceBoughtCoupons(
                    ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(100))
            .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                            .setFixedCost(CurrencyAmount.newBuilder().setAmount(15))
                            .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
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
            .setReservedInstanceBoughtCoupons(
                    ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(100)).setReservedInstanceBoughtCost(
                    ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                            .setFixedCost(CurrencyAmount.newBuilder().setAmount(15))
                            .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
            .setNumBought(30)
            .build();
    private IngestedTopologyStore ingestedTopologyStore;
    private final MutableFixedClock clock = new MutableFixedClock(1_000_000_000);


    /**
     * Set up before each test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void setup() throws SQLException, UnsupportedDialectException, InterruptedException {
        Map<Long, PricingDTO.ReservedInstancePrice> map = new HashMap<>();
        ReservedInstancePriceTable riPriceTable =
                ReservedInstancePriceTable.newBuilder().putAllRiPricesBySpecId(map).build();
        Mockito.when(priceTableStore.getMergedRiPriceTable()).thenReturn(riPriceTable);
        ReservedInstanceSpecStore reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl,
                new DefaultIdentityProvider(0), 10);
        ReservedInstanceCostCalculator reservedInstanceCostCalculator =
                new ReservedInstanceCostCalculator(reservedInstanceSpecStore);

        entityReservedInstanceMappingStore = new EntityReservedInstanceMappingStore(dsl);
        accountRIMappingStore = new AccountRIMappingStore(dsl);
        reservedInstanceBoughtStore = new SQLReservedInstanceBoughtStore(dsl,
                new DefaultIdentityProvider(0), reservedInstanceCostCalculator, priceTableStore,
                entityReservedInstanceMappingStore, accountRIMappingStore, new BusinessAccountHelper());

        reservedInstanceUtilizationStore = new ReservedInstanceUtilizationStore(dsl,
                reservedInstanceBoughtStore, reservedInstanceSpecStore, true, 1f);
        insertDefaultReservedInstanceSpec();
        ingestedTopologyStore = new IngestedTopologyStore(mock(ThreadPoolTaskScheduler.class),
                Duration.parse("PT23H"), dsl);
    }

    @Test
    public void testUpdateReservedInstanceUtilization() {
        final List<ReservedInstanceBoughtInfo> reservedInstancesBoughtInfo = Arrays.asList(riInfoOne, riInfoTwo, riInfoThree);
        final List<EntityRICoverageUpload> entityCoverageLists = Arrays.asList(coverageOne,
                coverageTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstancesBoughtInfo);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl,
                stichRIOidToCoverageUploads(entityCoverageLists));
        final List<ReservedInstanceBoughtRecord> riBought = dsl.selectFrom(
                Tables.RESERVED_INSTANCE_BOUGHT).fetch();
        final Map<String, Long> riProbeIdMap = riBought.stream().collect(
                Collectors.toMap(ReservedInstanceBoughtRecord::getProbeReservedInstanceId,
                        ReservedInstanceBoughtRecord::getId));
        accountRIMappingStore.updateAccountRICoverageMappings(
                Arrays.asList(AccountRICoverageUpload.newBuilder()
                        .setAccountId(9000)
                        .addCoverage(Coverage.newBuilder()
                                .setCoveredCoupons(50)
                                .setReservedInstanceId(riProbeIdMap.get("testOne"))
                                .setProbeReservedInstanceId("testOne")
                                .setRiCoverageSource(RICoverageSource.BILLING)
                                .setUsageStartTimestamp(System.currentTimeMillis())
                                .setUsageEndTimestamp(System.currentTimeMillis()))
                        .build(), AccountRICoverageUpload.newBuilder()
                        .setAccountId(9001)
                        .addCoverage(Coverage.newBuilder()
                                .setCoveredCoupons(25)
                                .setReservedInstanceId(riProbeIdMap.get("testOne"))
                                .setProbeReservedInstanceId("testOne")
                                .setRiCoverageSource(RICoverageSource.BILLING)
                                .setUsageStartTimestamp(System.currentTimeMillis())
                                .setUsageEndTimestamp(System.currentTimeMillis()))
                        .addCoverage(Coverage.newBuilder()
                                .setCoveredCoupons(30)
                                .setReservedInstanceId(riProbeIdMap.get("testThree"))
                                .setProbeReservedInstanceId("testThree")
                                .setRiCoverageSource(RICoverageSource.BILLING)
                                .setUsageStartTimestamp(System.currentTimeMillis())
                                .setUsageEndTimestamp(System.currentTimeMillis()))
                        .build()));
        reservedInstanceUtilizationStore.updateReservedInstanceUtilization(dsl, Instant.now());
        List<ReservedInstanceUtilizationLatestRecord> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).fetch();
        assertEquals(3L, records.size());
        final Optional<ReservedInstanceUtilizationLatestRecord> firstRI = records.stream().filter(
                record -> record.getId().equals(riProbeIdMap.get("testOne"))).findFirst();
        final Optional<ReservedInstanceUtilizationLatestRecord> secondRI = records.stream().filter(
                record -> record.getId().equals(riProbeIdMap.get("testTwo"))).findFirst();
        final Optional<ReservedInstanceUtilizationLatestRecord> thirdRI = records.stream().filter(
                record -> record.getId().equals(riProbeIdMap.get("testThree"))).findFirst();
        assertTrue(firstRI.isPresent());
        assertTrue(secondRI.isPresent());
        assertTrue(thirdRI.isPresent());
        assertEquals(100.0, firstRI.get().getTotalCoupons(), DELTA);
        assertEquals(115.0, firstRI.get().getUsedCoupons(), DELTA);
        assertEquals(100.0, secondRI.get().getTotalCoupons(), DELTA);
        assertEquals(20.0, secondRI.get().getUsedCoupons(), DELTA);
        assertEquals(100.0, thirdRI.get().getTotalCoupons(), DELTA);
        assertEquals(80.0, thirdRI.get().getUsedCoupons(), DELTA);
    }

    @Test
    public void testGetRIUtilizationStatsRecords() {
        final List<ReservedInstanceBoughtInfo> reservedInstancesBoughtInfo = Arrays.asList(riInfoOne, riInfoTwo, riInfoThree);
        final List<EntityRICoverageUpload> entityCoverageLists = Arrays.asList(coverageOne,
                coverageTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstancesBoughtInfo);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl,
                stichRIOidToCoverageUploads(entityCoverageLists));
        reservedInstanceUtilizationStore.updateReservedInstanceUtilization(dsl, Instant.now());
        // get all ri utilization records
        final ReservedInstanceUtilizationFilter filter =
                ReservedInstanceUtilizationFilter.newBuilder().build();
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
        final CloudCostDTO.ReservedInstanceType riType1 =
                CloudCostDTO.ReservedInstanceType.newBuilder().setTermYears(1).setOfferingClass(
                        CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD).setPaymentOption(
                        PaymentOption.ALL_UPFRONT).build();

        final CloudCostDTO.ReservedInstanceType riType2 =
                CloudCostDTO.ReservedInstanceType.newBuilder().setTermYears(2).setOfferingClass(
                        CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD).setPaymentOption(
                        PaymentOption.ALL_UPFRONT).build();
        final ReservedInstanceSpecRecord specRecordOne = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(99L, OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getNumber(), 1, Tenancy.DEDICATED.getValue(),
                        OSType.LINUX.getValue(), 88L, 77L,
                        ReservedInstanceSpecInfo.newBuilder().setRegionId(77L).setType(riType1).build()));
        final ReservedInstanceSpecRecord specRecordTwo = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(100L, OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getNumber(), 2, Tenancy.HOST.getValue(),
                        OSType.LINUX.getValue(), 90L, 78L,
                        ReservedInstanceSpecInfo.newBuilder().setRegionId(78L).setType(riType2).build()));
        dsl.batchInsert(Arrays.asList(specRecordOne, specRecordTwo)).execute();
    }

    /**
     * This mirrors the behavior in RIAndExpenseUploadRpcService::updateCoverageWithLocalRIBoughtIds
     *
     * @param entityRICoverageUploads
     * @return
     */
    private List<EntityRICoverageUpload> stichRIOidToCoverageUploads(
            List<EntityRICoverageUpload> entityRICoverageUploads) {

        final Map<String, Long> riProbeIdToOid = reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(
                ReservedInstanceBoughtFilter.SELECT_ALL_FILTER).stream().filter(
                ReservedInstanceBought::hasReservedInstanceBoughtInfo).collect(
                        Collectors.toMap(ri -> ri.getReservedInstanceBoughtInfo().getProbeReservedInstanceId(),
                                ReservedInstanceBought::getId));

        return entityRICoverageUploads.stream().map(entityRICoverage -> EntityRICoverageUpload.newBuilder(entityRICoverage))
                // update the ReservedInstanceId for each Coverage record, mapping through
                // the ProbeReservedInstanceId
                .peek(entityRiCoverageBuilder -> entityRiCoverageBuilder.getCoverageBuilderList().stream()
                        .forEach(coverageBuilder -> coverageBuilder.setReservedInstanceId(
                                riProbeIdToOid.getOrDefault(
                                        coverageBuilder.getProbeReservedInstanceId(), 0L)))).map(
                        EntityRICoverageUpload.Builder::build).collect(Collectors.toList());
    }

    /**
     * Test that entity cost data is correctly rolled up when Postgres is enabled.
     *
     * @throws EntitySavingsException if there's a problem with the store
     */
    @Test
    public void rollupToHourlyDailyAndMonthly() {
        LocalDateTime originalTime = LocalDateTime.now();
        originalTime = originalTime.truncatedTo(ChronoUnit.SECONDS);
        List<LocalDateTime> times = Arrays.asList(originalTime, originalTime.plus(1,
            ChronoUnit.MINUTES), originalTime.plus(1, ChronoUnit.HOURS), originalTime.plus(1, ChronoUnit.DAYS));
        List<Double> couponValues = Arrays.asList(1d, 2d, 3d, 4d);

        final Long arbitraryId = 123L;
        int expectedLatest = 0;
        int expectedHourly = 1;
        int expectedDaily = 1;
        int expectedMonthly = 1;
        Double currentAvg = couponValues.get(0);
        for (int i = 0; i < times.size(); i += 1) {
            LocalDateTime time = times.get(i);
            Double couponValue = couponValues.get(i);
            ingestTopology(time);

            String hourKey = ReservedInstanceUtil.createHourKey(time, arbitraryId,
                    arbitraryId, arbitraryId, arbitraryId);

            String dayKey = ReservedInstanceUtil.createDayKey(time, arbitraryId,
                    arbitraryId, arbitraryId, arbitraryId);

            String monthKey = ReservedInstanceUtil.createMonthKey(time, arbitraryId,
                    arbitraryId, arbitraryId, arbitraryId);

            ReservedInstanceUtilizationLatestRecord riUtilizationRecord = dsl.newRecord(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST,
                new ReservedInstanceUtilizationLatestRecord(time, arbitraryId, arbitraryId, arbitraryId, arbitraryId, couponValue,
                    couponValue, hourKey, dayKey, monthKey));

            DSL.using(dsl.configuration())
                .insertInto(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST)
                .set(riUtilizationRecord).execute();

            performRollup(time);
            // This is to count how many expected records we have for each table
            if (expectedDaily >= 2) {
                expectedMonthly += 1;
            }
            if (expectedHourly >= 2) {
                expectedDaily += 1;
            }
            if (expectedLatest >= 2) {
                expectedHourly += 1;
            }
            expectedLatest += 1;

            assertLatestTable(expectedLatest, currentAvg);
            assertHourlyTable(expectedHourly, currentAvg);
            assertDailyTable(expectedDaily, currentAvg);
            assertMonthlyTable(expectedMonthly, currentAvg);

            if (i < times.size() - 1) {
                currentAvg = (currentAvg + couponValues.get(i + 1)) / 2;
            }
        }
    }

    private void performRollup(LocalDateTime time) {
        reservedInstanceUtilizationStore.performRollup(RollupDurationType.HOURLY,
            Collections.singletonList(time));
        reservedInstanceUtilizationStore.performRollup(RollupDurationType.DAILY,
            Collections.singletonList(time));
        reservedInstanceUtilizationStore.performRollup(RollupDurationType.MONTHLY,
            Collections.singletonList(time));
    }

    private void assertLatestTable(final int expectedCount,
                                   final Double expectedAggregatedValue) {
        Result<?> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).fetch();
        Assert.assertEquals(expectedCount, records.size());
        if (expectedCount == 1) {
            Assert.assertEquals(expectedAggregatedValue,
                (Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).TOTAL_COUPONS.get(records.get(0)));
            Assert.assertEquals(expectedAggregatedValue,
                (Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).USED_COUPONS.get(records.get(0)));
        }
    }

    private void assertHourlyTable(final int expectedCount,
                                   final Double expectedAggregatedValue) {
        Result<?> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR).fetch();
        Assert.assertEquals(expectedCount, records.size());
        if (expectedCount == 1) {
            Assert.assertEquals(expectedAggregatedValue,
                (Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR).TOTAL_COUPONS.get(records.get(0)));
            Assert.assertEquals(expectedAggregatedValue,
                (Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR).USED_COUPONS.get(records.get(0)));
        }
    }

    private void assertDailyTable(final int expectedCount,
                                   final Double expectedAggregatedValue) {
        Result<?> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY).fetch();
        Assert.assertEquals(expectedCount, records.size());
        if (expectedCount == 1) {
            Assert.assertEquals(expectedAggregatedValue,
                (Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY).TOTAL_COUPONS.get(records.get(0)));
            Assert.assertEquals(expectedAggregatedValue,
                (Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY).USED_COUPONS.get(records.get(0)));
        }
    }

    private void assertMonthlyTable(final int expectedCount,
                                   final Double expectedAggregatedValue) {
        Result<?> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH).fetch();
        Assert.assertEquals(expectedCount, records.size());
        if (expectedCount == 1) {
            Assert.assertEquals(expectedAggregatedValue,
                (Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH).TOTAL_COUPONS.get(records.get(0)));
            Assert.assertEquals(expectedAggregatedValue,
                (Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH).USED_COUPONS.get(records.get(0)));
        }
    }


    private void ingestTopology(LocalDateTime time) {
        TopologyInfo info = TopologyInfo.newBuilder()
                .setTopologyContextId(777)
                .setTopologyId(777)
                .setCreationTime(time.toEpochSecond(ZoneOffset.UTC))
                .setTopologyType(TopologyType.REALTIME)
                .build();
        ingestedTopologyStore.recordIngestedTopology(info);
    }
}
