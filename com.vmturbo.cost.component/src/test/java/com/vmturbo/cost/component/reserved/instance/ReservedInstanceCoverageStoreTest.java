package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.savings.EntitySavingsException;
import com.vmturbo.cost.component.topology.IngestedTopologyStore;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

@RunWith(Parameterized.class)
public class ReservedInstanceCoverageStoreTest extends MultiDbTestBase {
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

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public ReservedInstanceCoverageStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost", TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private final static double DELTA = 0.000001;

    private ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private PriceTableStore priceTableStore;

    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    private ReservedInstanceCostCalculator reservedInstanceCostCalculator;

    private IngestedTopologyStore ingestedTopologyStore;

    private final ServiceEntityReservedInstanceCoverageRecord firstEntity =
            ServiceEntityReservedInstanceCoverageRecord.newBuilder()
                    .setId(123)
                    .setBusinessAccountId(12345L)
                    .setAvailabilityZoneId(100L)
                    .setRegionId(101L)
                    .setUsedCoupons(10)
                    .setTotalCoupons(100)
                    .build();
    private final ServiceEntityReservedInstanceCoverageRecord secondEntity =
            ServiceEntityReservedInstanceCoverageRecord.newBuilder()
                    .setId(124)
                    .setBusinessAccountId(12345L)
                    .setAvailabilityZoneId(100L)
                    .setRegionId(101L)
                    .setUsedCoupons(20)
                    .setTotalCoupons(200)
                    .build();
    private final ServiceEntityReservedInstanceCoverageRecord thirdEntity =
            ServiceEntityReservedInstanceCoverageRecord.newBuilder()
                    .setId(125)
                    .setBusinessAccountId(12345L)
                    .setAvailabilityZoneId(100L)
                    .setRegionId(101L)
                    .setUsedCoupons(30)
                    .setTotalCoupons(50)
                    .build();
    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore = Mockito.mock(EntityReservedInstanceMappingStore.class);
    private AccountRIMappingStore accountRIMappingStore = Mockito.mock(AccountRIMappingStore.class);

    @Before
    public void setup() throws Exception {
        reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl, new DefaultIdentityProvider(0), 10);
        reservedInstanceCostCalculator = new ReservedInstanceCostCalculator(reservedInstanceSpecStore);
        reservedInstanceBoughtStore = new SQLReservedInstanceBoughtStore(dsl,
                new DefaultIdentityProvider(0), reservedInstanceCostCalculator, priceTableStore,
                entityReservedInstanceMappingStore, accountRIMappingStore, new BusinessAccountHelper());
        reservedInstanceCoverageStore = new ReservedInstanceCoverageStore(dsl, true, 1f);
        ingestedTopologyStore = new IngestedTopologyStore(mock(ThreadPoolTaskScheduler.class),
                Duration.parse("PT23H"), dsl);
    }

    @Test
    public void testUpdateReservedInstanceCoverageStore() {
        reservedInstanceCoverageStore.updateReservedInstanceCoverageStore(dsl,
                Instant.now(),
                Arrays.asList(firstEntity, secondEntity, thirdEntity));
        final List<ReservedInstanceCoverageLatestRecord> records =
                dsl.selectFrom(Tables.RESERVED_INSTANCE_COVERAGE_LATEST).fetch();
        assertEquals(3, records.size());
        final Optional<ReservedInstanceCoverageLatestRecord> firstRecord = records.stream()
                .filter(ri -> ri.getEntityId().equals(123L))
                .findFirst();
        final Optional<ReservedInstanceCoverageLatestRecord> secondRecord = records.stream()
                .filter(ri -> ri.getEntityId().equals(124L))
                .findFirst();
        final Optional<ReservedInstanceCoverageLatestRecord> thirdRecord = records.stream()
                .filter(ri -> ri.getEntityId().equals(125L))
                .findFirst();
        assertTrue(firstRecord.isPresent());
        assertTrue(secondRecord.isPresent());
        assertTrue(thirdRecord.isPresent());
        assertEquals(100.0, firstRecord.get().getTotalCoupons(), DELTA);
        assertEquals(10.0, firstRecord.get().getUsedCoupons(), DELTA);
        assertEquals(200.0, secondRecord.get().getTotalCoupons(), DELTA);
        assertEquals(20.0, secondRecord.get().getUsedCoupons(), DELTA);
        assertEquals(50.0, thirdRecord.get().getTotalCoupons(), DELTA);
        assertEquals(30.0, thirdRecord.get().getUsedCoupons(), DELTA);
    }

    @Test
    public void testGetReservedInstanceCoverageStatsRecords() {
        reservedInstanceCoverageStore.updateReservedInstanceCoverageStore(dsl,
                Instant.now(),
                Arrays.asList(firstEntity, secondEntity, thirdEntity));
        List<Long> ids = new ArrayList<>();
        ids.add(123L);
        ids.add(124L);
        ids.add(125L);
        final ReservedInstanceCoverageFilter filter = ReservedInstanceCoverageFilter
                .newBuilder()
                .timeFrame(TimeFrame.LATEST)
                .entityFilter(EntityFilter.newBuilder()
                        .addAllEntityId(ids)
                        .build())
                .build();
        final List<ReservedInstanceStatsRecord> riStatsRecords =
                reservedInstanceCoverageStore.getReservedInstanceCoverageStatsRecords(filter);
        assertEquals(1, riStatsRecords.size());
        final ReservedInstanceStatsRecord riStatsRecord = riStatsRecords.get(0);
        assertEquals(200L, riStatsRecord.getCapacity().getMax(), DELTA);
        assertEquals(50L, riStatsRecord.getCapacity().getMin(), DELTA);
        assertEquals(350L, riStatsRecord.getCapacity().getTotal(), DELTA);
        assertEquals(30L, riStatsRecord.getValues().getMax(), DELTA);
        assertEquals(10L, riStatsRecord.getValues().getMin(), DELTA);
        assertEquals(60L, riStatsRecord.getValues().getTotal(), DELTA);

        final Collection<ReservedInstanceStatsRecord> riLatestStatsRecords =
                reservedInstanceCoverageStore.getReservedInstanceCoverageStatsRecords(filter);
        assertEquals(1, riLatestStatsRecords.size());
    }

    /**
     * Test that entity cost data is correctly rolled up when Postgres is enabled.
     *
     * @throws EntitySavingsException if there's a problem with the store
     */
    @Test
    public void rollupToHourlyDailyAndMonthly() {
        // make sure to use a time that has the following properties: 1) adding a minute doesn't increase the hour.
        // 2) Adding an hour doesn't increase by one day 3) Adding one day doesn't increase by one the month
        LocalDateTime originalTime = LocalDateTime.parse("2022-02-09T01:01:01");
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

            ReservedInstanceCoverageLatestRecord riCoverageRecord = dsl.newRecord(Tables.RESERVED_INSTANCE_COVERAGE_LATEST,
                    new ReservedInstanceCoverageLatestRecord(time, arbitraryId, arbitraryId, arbitraryId, arbitraryId, couponValue,
                            couponValue, hourKey, dayKey, monthKey));

            DSL.using(dsl.configuration())
                    .insertInto(Tables.RESERVED_INSTANCE_COVERAGE_LATEST)
                    .set(riCoverageRecord).execute();

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
        reservedInstanceCoverageStore.performRollup(RollupDurationType.HOURLY,
                Collections.singletonList(time));
        reservedInstanceCoverageStore.performRollup(RollupDurationType.DAILY,
                Collections.singletonList(time));
        reservedInstanceCoverageStore.performRollup(RollupDurationType.MONTHLY,
                Collections.singletonList(time));
    }

    private void assertLatestTable(final int expectedCount,
            final Double expectedAggregatedValue) {
        Result<?> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_COVERAGE_LATEST).fetch();
        Assert.assertEquals(expectedCount, records.size());
        if (expectedCount == 1) {
            Assert.assertEquals(expectedAggregatedValue,
                    (Tables.RESERVED_INSTANCE_COVERAGE_LATEST).TOTAL_COUPONS.get(records.get(0)));
            Assert.assertEquals(expectedAggregatedValue,
                    (Tables.RESERVED_INSTANCE_COVERAGE_LATEST).USED_COUPONS.get(records.get(0)));
        }
    }

    private void assertHourlyTable(final int expectedCount,
            final Double expectedAggregatedValue) {
        Result<?> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR).fetch();
        Assert.assertEquals(expectedCount, records.size());
        if (expectedCount == 1) {
            Assert.assertEquals(expectedAggregatedValue,
                    (Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR).TOTAL_COUPONS.get(records.get(0)));
            Assert.assertEquals(expectedAggregatedValue,
                    (Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR).USED_COUPONS.get(records.get(0)));
        }
    }

    private void assertDailyTable(final int expectedCount,
            final Double expectedAggregatedValue) {
        Result<?> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY).fetch();
        Assert.assertEquals(expectedCount, records.size());
        if (expectedCount == 1) {
            Assert.assertEquals(expectedAggregatedValue,
                    (Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY).TOTAL_COUPONS.get(records.get(0)));
            Assert.assertEquals(expectedAggregatedValue,
                    (Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY).USED_COUPONS.get(records.get(0)));
        }
    }

    private void assertMonthlyTable(final int expectedCount,
            final Double expectedAggregatedValue) {
        Result<?> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH).fetch();
        Assert.assertEquals(expectedCount, records.size());
        if (expectedCount == 1) {
            Assert.assertEquals(expectedAggregatedValue,
                    (Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH).TOTAL_COUPONS.get(records.get(0)));
            Assert.assertEquals(expectedAggregatedValue,
                    (Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH).USED_COUPONS.get(records.get(0)));
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
