package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class ReservedInstanceCoverageStoreTest {

    private final static double DELTA = 0.000001;

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    private ReservedInstanceCostCalculator reservedInstanceCostCalculator;

    private DSLContext dsl;

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

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl, new IdentityProvider(0), 10);
        reservedInstanceCostCalculator = new ReservedInstanceCostCalculator(reservedInstanceSpecStore);
        reservedInstanceBoughtStore = new ReservedInstanceBoughtStore(dsl,
                new IdentityProvider(0), reservedInstanceCostCalculator);
        reservedInstanceCoverageStore = new ReservedInstanceCoverageStore(dsl);
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testUpdateReservedInstanceCoverageStore() {
        reservedInstanceCoverageStore.updateReservedInstanceCoverageStore(dsl,
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


}
