package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.PlanReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Tests for the {@link PlanReservedInstanceStore}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestCostDbEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class PlanReservedInstanceStoreTest {

    @Autowired(required = false)
    private TestCostDbEndpointConfig dbEndpointConfig;

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(com.vmturbo.cost.component.db.Cost.COST);

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

    private static final long PLAN_ID = 112345L;
    private static final double DELTA = 0.01;

    private static final long riId_1 = 1L;
    private static final long riId_2 = 2L;
    private static final long tierId1 = 101L;
    private static final String tierName1 = "t101.small";
    private static final long tierId2 = 102L;
    private static final String tierName2 = "t102.large";

    private static final ReservedInstanceBoughtInfo RI_INFO_1 = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(123L)
                    .setProbeReservedInstanceId("bar")
                    .setReservedInstanceSpec(tierId1)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(10)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                                    .setFixedCost(CurrencyAmount.newBuilder().setAmount(0))
                                    .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
                    .setDisplayName(tierName1)
                    .build();

    private static final ReservedInstanceBoughtInfo RI_INFO_2 = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(456L)
                    .setProbeReservedInstanceId("foo")
                    .setReservedInstanceSpec(tierId2)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(20)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                                    .newBuilder()
                                    .setFixedCost(CurrencyAmount.newBuilder().setAmount(15))
                                    .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
                    .setDisplayName(tierName2)
                    .build();

    private static final Map<Long, Long> TIER_ID_TO_COUNT_MAP = createTierIdToCountMap();

    private DSLContext dsl;

    private ReservedInstanceSpecStore reservedInstanceSpecStore;
    private ReservedInstanceCostCalculator reservedInstanceCostCalculator;
    private BusinessAccountHelper businessAccountHelper;
    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;
    private AccountRIMappingStore accountRIMappingStore;
    private PlanReservedInstanceStore planReservedInstanceStore;

    /**
     * Initialize instances before test.
     *
     * @throws Exception if something goes wrong.
     */
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
        businessAccountHelper = new BusinessAccountHelper();
        entityReservedInstanceMappingStore = new EntityReservedInstanceMappingStore(dsl);
        accountRIMappingStore = new AccountRIMappingStore(dsl);
        planReservedInstanceStore =
                new PlanReservedInstanceStore(dsl, new DefaultIdentityProvider(0),
                        reservedInstanceCostCalculator, businessAccountHelper,
                        entityReservedInstanceMappingStore, accountRIMappingStore);
        insertDefaultReservedInstanceSpec();
        final List<ReservedInstanceBought> reservedInstanceBoughtInfos =
                Arrays.asList(ReservedInstanceBought.newBuilder().setId(riId_1).setReservedInstanceBoughtInfo(RI_INFO_1).build(),
                        ReservedInstanceBought.newBuilder().setId(riId_2).setReservedInstanceBoughtInfo(RI_INFO_2)
                                .build());
        planReservedInstanceStore.insertPlanReservedInstanceBought(reservedInstanceBoughtInfos, PLAN_ID);
    }

    private static Map<Long, Long> createTierIdToCountMap() {
        final Map<Long, Long> result = new HashMap<>();
        result.put(tierId1, 10L);
        result.put(tierId2, 20L);
        return Collections.unmodifiableMap(result);
    }

    /**
     * Tests records insertion.
     */
    @Test
    public void testInsertPlanReservedInstanceBought() {
        final List<PlanReservedInstanceBoughtRecord> records = dsl.selectFrom(Tables.PLAN_RESERVED_INSTANCE_BOUGHT).fetch();
        Assert.assertEquals(2, records.size());
        Assert.assertEquals(Sets.newHashSet(10, 20), records.stream()
            .map(PlanReservedInstanceBoughtRecord::getCount)
            .collect(Collectors.toSet()));
        for (PlanReservedInstanceBoughtRecord record : records) {
            Assert.assertEquals(PLAN_ID, record.getPlanId().longValue());
        }
    }

    /**
     * Tests get plan tier to RI map.
     */
    @Test
    public void testGetPlanReservedInstanceCountByRISpecIdMap() {
        final Map<Long, Long> tierToCountMap = planReservedInstanceStore.getPlanReservedInstanceCountByRISpecIdMap(PLAN_ID);
        Assert.assertEquals(TIER_ID_TO_COUNT_MAP, tierToCountMap);
    }

    /**
     * Tests records deletion.
     */
    @Test
    public void testDeletePlanReservedInstanceStats() {
        final int rowsCount = planReservedInstanceStore.deletePlanReservedInstanceStats(PLAN_ID);
        Assert.assertEquals(2, rowsCount);
        final List<PlanReservedInstanceBoughtRecord> records = dsl.selectFrom(Tables.PLAN_RESERVED_INSTANCE_BOUGHT).fetch();
        Assert.assertTrue(records.isEmpty());
    }

    /**
     * Tests get plan RI aggregated costs.
     */
    @Test
    public void testGetReservedInstanceAggregatedCosts() {
        final Cost.ReservedInstanceCostStat stats = planReservedInstanceStore.getPlanReservedInstanceAggregatedCosts(PLAN_ID);
        Assert.assertEquals(300, stats.getFixedCost(), DELTA);
        Assert.assertEquals(7.5, stats.getRecurringCost(), DELTA);
        Assert.assertEquals(7.517, stats.getAmortizedCost(), DELTA);
    }

    /**
     * Test listing plan IDs in the store.
     */
    @Test
    public void testListPlanIds() {
        assertThat(planReservedInstanceStore.getPlanIds(), containsInAnyOrder(PLAN_ID));
    }

    private void insertDefaultReservedInstanceSpec() {
        final CloudCostDTO.ReservedInstanceType riType1 = CloudCostDTO.ReservedInstanceType.newBuilder().setTermYears(1).setOfferingClass(
                        CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD).setPaymentOption(
                        PaymentOption.ALL_UPFRONT).build();

        final CloudCostDTO.ReservedInstanceType riType2 = CloudCostDTO.ReservedInstanceType.newBuilder().setTermYears(2).setOfferingClass(
                        CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD).setPaymentOption(
                        PaymentOption.ALL_UPFRONT).build();

        final ReservedInstanceSpecRecord specRecordOne = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(101L,
                        OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getNumber(),
                        1,
                        Tenancy.DEDICATED.getValue(),
                        OSType.LINUX.getValue(),
                        88L,
                        77L,
                        ReservedInstanceSpecInfo.newBuilder().setType(riType1).build()));
        final ReservedInstanceSpecRecord specRecordTwo = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(102L,
                        OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getNumber(),
                        2,
                        Tenancy.HOST.getValue(),
                        OSType.LINUX.getValue(),
                        90L,
                        78L,
                        ReservedInstanceSpecInfo.newBuilder().setType(riType2).build()));
        dsl.batchInsert(Arrays.asList(specRecordOne, specRecordTwo)).execute();
    }

}
