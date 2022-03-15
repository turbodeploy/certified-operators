package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
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
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Tests for the {@link PlanReservedInstanceStore}.
 */
@RunWith(Parameterized.class)
public class PlanReservedInstanceStoreTest extends MultiDbTestBase {
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
    public PlanReservedInstanceStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(com.vmturbo.cost.component.db.Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

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
        reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl, new DefaultIdentityProvider(0), 10);
        reservedInstanceCostCalculator = new ReservedInstanceCostCalculator(reservedInstanceSpecStore);
        businessAccountHelper = Mockito.mock(BusinessAccountHelper.class);
        entityReservedInstanceMappingStore = Mockito.mock(EntityReservedInstanceMappingStore.class);
        accountRIMappingStore = Mockito.mock(AccountRIMappingStore.class);
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

    /**
     * Test get RI Bought for Analysis.
     *
     * Test Setting:
     * RI1: capacity = 10 coupons, used = 8 coupons
     * RI usage from undiscovered accounts = 3 coupons
     * RI usage from discovered accounts = 5 coupons
     * RI usage from Business Account in scope = 3 coupons
     *
     * Final RI capacity = RI Capacity - RI usage from undiscovered account - RI usage from out-of-scope account == (10 - 3 - 2) = 5 coupons
     * Final RI utilization = RI Utilization - RI usage from undiscovered account - RI usage from out-of-scope account == (8 - 3 - 2) = 3 coupons
     */
    @Test
    public void testGetReservedInstanceBoughtForAnalysis() {
        long PLAN_ID = 2222l;
        long RI_ID1 = 1000l;
        long VM1_OID = 1l;
        long BA_OID = 123L;
        Set<Long> vmOidSet = Collections.singleton(VM1_OID);

        ReservedInstanceBoughtInfo riBoughtInfo1 = ReservedInstanceBoughtInfo.newBuilder()
                .setBusinessAccountId(BA_OID)
                .setProbeReservedInstanceId("smoke")
                .setReservedInstanceSpec(tierId1)
                .setAvailabilityZoneId(100L)
                .setNumBought(1)
                .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                        .setFixedCost(CurrencyAmount.newBuilder().setAmount(0))
                        .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
                .setDisplayName(tierName1)
                .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons.newBuilder()
                        .setNumberOfCoupons(10)
                        .setNumberOfCouponsUsed(8).build())
                .build();
        final List<ReservedInstanceBought> reservedInstanceBoughtInfos =
                Arrays.asList(ReservedInstanceBought.newBuilder().setId(RI_ID1).setReservedInstanceBoughtInfo(riBoughtInfo1).build());
        planReservedInstanceStore.insertPlanReservedInstanceBought(reservedInstanceBoughtInfos, PLAN_ID);

        final ImmutableMap<Long, Double> riToDiscoveredUsageMap = ImmutableMap.<Long, Double>builder().put(RI_ID1, 5.0D).build();
        when(entityReservedInstanceMappingStore.getReservedInstanceUsedCouponsMapByFilter(any())).thenReturn(riToDiscoveredUsageMap);

        final Coverage riCoverage1 = Coverage.newBuilder().setReservedInstanceId(RI_ID1).setCoveredCoupons(3.0D).build();
        Set<Coverage> coverageSet1 = Collections.singleton(riCoverage1);

        final ImmutableMap<Long, Set<Coverage>> riCoverageByEntityMap = ImmutableMap.<Long, Set<Coverage>>builder().put(VM1_OID, coverageSet1).build();
        when(entityReservedInstanceMappingStore.getRICoverageByEntity(any())).thenReturn(riCoverageByEntityMap);

        final ImmutableSet baList = ImmutableSet.builder().add(BA_OID).build();
        when(businessAccountHelper.getDiscoveredBusinessAccounts()).thenReturn(baList);

        final ImmutableMap<Long, Double> riUsageFronUndisocveredAccounts = ImmutableMap.<Long, Double>builder().put(RI_ID1, 3.0D).build();
        when(accountRIMappingStore.getUndiscoveredAccountUsageForRI()).thenReturn(riUsageFronUndisocveredAccounts);

        final List<ReservedInstanceBought> reservedInstanceBoughtForAnalysis = planReservedInstanceStore.getReservedInstanceBoughtForAnalysis(PLAN_ID, vmOidSet);

        Assert.assertFalse(reservedInstanceBoughtForAnalysis.isEmpty());
        final ReservedInstanceBought reservedInstanceBought = reservedInstanceBoughtForAnalysis.get(0);
        final ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons reservedInstanceBoughtCoupons = reservedInstanceBought
                .getReservedInstanceBoughtInfo().getReservedInstanceBoughtCoupons();
        Assert.assertEquals(5.0D, reservedInstanceBoughtCoupons.getNumberOfCoupons(), DELTA);
        Assert.assertEquals(3.0D, reservedInstanceBoughtCoupons.getNumberOfCouponsUsed(), DELTA);
    }
}
