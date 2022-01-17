package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * TODO GARY: SQL exception while executing the query insert into reserved_instance_coverage_latest (snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons, hour_key, day_key, month_key) values (cast(? as timestamp), ?, ?, ?, ?, ?, ?, ?, ?, ?):
 * java.sql.BatchUpdateException: Batch entry 0 insert into reserved_instance_coverage_latest (snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons, hour_key, day_key, month_key) values (cast('2022-01-11 16:23:24.527-05'::timestamp as timestamp), 123, 101, 100, 12345, 100.0, 10.0, NULL, NULL, NULL) was aborted: ERROR: null value in column "hour_key" violates not-null constraint
 *   Detail: Failing row contains (2022-01-11 16:23:24.527, 123, 101, 100, 12345, 100, 10, null, null, null).  Call getNextException to see other errors in the batch.
 *   Need to fix `CREATE TRIGGER reserved_instance_utilization_keys BEFORE INSERT ON reserved_instance_utilization_latest`, see OM-77148
 */
@RunWith(Parameterized.class)
public class ReservedInstanceCoverageStoreTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.DBENDPOINT_CONVERTED_PARAMS;
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


}
