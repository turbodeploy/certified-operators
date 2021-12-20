package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestCostDbEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class ReservedInstanceCoverageStoreTest {

    @Autowired(required = false)
    private TestCostDbEndpointConfig dbEndpointConfig;

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

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("cost");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    private final static double DELTA = 0.000001;

    private DSLContext dsl;

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
        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.costEndpoint());
            dsl = dbEndpointConfig.costEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }
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
