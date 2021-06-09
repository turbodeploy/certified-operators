package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_BOUGHT;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_SPEC;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.AccountReferenceFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceDerivedCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCostFilter;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Class to test ReservedInstanceBoughtStore methods.
 */
public class ReservedInstanceBoughtStoreTest {
    private static final long BA_1 = 123L;
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

    private DSLContext dsl = dbConfig.getDslContext();

    private ReservedInstanceSpecStore reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl, new DefaultIdentityProvider(0), 10);

    private ReservedInstanceCostCalculator reservedInstanceCostCalculator = new ReservedInstanceCostCalculator(reservedInstanceSpecStore);

    private PriceTableStore priceTableStore = Mockito.mock(PriceTableStore.class);
    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore = Mockito.mock(EntityReservedInstanceMappingStore.class);
    private AccountRIMappingStore accountRIMappingStore = Mockito.mock(AccountRIMappingStore.class);
    private BusinessAccountHelper businessAccountHelper = Mockito.mock(BusinessAccountHelper.class);

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore = new SQLReservedInstanceBoughtStore(dsl,
                new DefaultIdentityProvider(0), reservedInstanceCostCalculator, priceTableStore,
            entityReservedInstanceMappingStore, accountRIMappingStore, businessAccountHelper);

    private static final int REGION_VALUE = 54;
    private static final int AVAILABILITYZONE_VALUE = 55;
    private static final int BUSINESS_ACCOUNT_VALUE = 28;
    private static final int NO_OF_MONTHS = 12;
    private static final int MONTHLY_TO_HOURLY_CONVERSION = 730;

    final ReservedInstanceBoughtInfo riInfoOne = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(123L)
                    .setProbeReservedInstanceId("bar")
                    .setReservedInstanceSpec(101L)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(10)
                    .setStartTime(System.currentTimeMillis())
                    .setEndTime(LocalDateTime.now().plus(Period.ofYears(1))
                            .toInstant(ZoneOffset.UTC)
                            .toEpochMilli())
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                                    .setFixedCost(CurrencyAmount.newBuilder().setAmount(0))
                                    .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
                    .build();

    final ReservedInstanceBoughtInfo riInfoTwo = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(456L)
                    .setProbeReservedInstanceId("foo")
                    .setReservedInstanceSpec(102L)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(20)
                    .setStartTime(System.currentTimeMillis())
                    .setEndTime(LocalDateTime.now().plus(Period.ofYears(1))
                            .toInstant(ZoneOffset.UTC)
                            .toEpochMilli())
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                                    .newBuilder()
                                    .setFixedCost(CurrencyAmount.newBuilder().setAmount(15))
                                    .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
                    .build();

    final ReservedInstanceBoughtInfo riInfoThree = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(789L)
                    .setProbeReservedInstanceId("test")
                    .setReservedInstanceSpec(102L)
                    .setAvailabilityZoneId(50L)
                    .setNumBought(30)
                    .setStartTime(System.currentTimeMillis())
                    .setEndTime(LocalDateTime.now().plus(Period.ofYears(1))
                            .toInstant(ZoneOffset.UTC)
                            .toEpochMilli())
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                                    .newBuilder()
                                    .setFixedCost(CurrencyAmount.newBuilder().setAmount(15))
                                    .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
                    .build();

    final ReservedInstanceBoughtInfo riInfoFour = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(789L)
                    .setProbeReservedInstanceId("qux")
                    .setReservedInstanceSpec(101L)
                    .setAvailabilityZoneId(50L)
                    .setNumBought(40)
                    .setStartTime(System.currentTimeMillis())
                    .setEndTime(LocalDateTime.now().plus(Period.ofYears(1))
                            .toInstant(ZoneOffset.UTC)
                            .toEpochMilli())
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                                    .setFixedCost(CurrencyAmount.newBuilder().setAmount(0))
                                    .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0.25)))
                    .build();

    @Before
    public void setup() throws Exception {
        Map<Long, PricingDTO.ReservedInstancePrice> map = new HashMap<>();
        ReservedInstancePriceTable riPriceTable = ReservedInstancePriceTable.newBuilder()
                .putAllRiPricesBySpecId(map).build();
        Mockito.when(priceTableStore.getMergedRiPriceTable()).thenReturn(riPriceTable);
        insertDefaultReservedInstanceSpec();

    }

    @Test
    public void updateReservedInstanceBoughtOnlyAdd() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceBoughtInfos =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceBoughtInfos);
        final List<ReservedInstanceBoughtRecord> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT).fetch();
        verifyAmortizedCosts(reservedInstanceBoughtInfos, records);
        assertEquals(3, records.size());
        assertEquals(Sets.newHashSet("bar", "foo", "test"), records.stream()
            .map(ReservedInstanceBoughtRecord::getProbeReservedInstanceId)
            .collect(Collectors.toSet()));
    }

    @Test
    public void updateReservedInstanceBoughtWithUpdate() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos = Arrays.asList(riInfoOne, riInfoTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        final List<Long> recordIds =
                dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT).fetch().stream()
                    .map(ReservedInstanceBoughtRecord::getId)
                    .collect(Collectors.toList());
        final ReservedInstanceBoughtInfo newRiInfoOne = ReservedInstanceBoughtInfo.newBuilder(riInfoOne)
                .setReservedInstanceSpec(102L)
                .build();
        final ReservedInstanceBoughtInfo newRiInfoTwo = ReservedInstanceBoughtInfo.newBuilder(riInfoTwo)
                .setReservedInstanceSpec(101L)
                .build();
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl,
                Arrays.asList(newRiInfoOne, newRiInfoTwo));
        final List<ReservedInstanceBoughtRecord> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT)
                .where(Tables.RESERVED_INSTANCE_BOUGHT.ID.in(recordIds))
                .fetch();
        verifyAmortizedCosts(Arrays.asList(newRiInfoOne, newRiInfoTwo), records);
        assertEquals(2, records.size());
        assertEquals(Sets.newHashSet("bar", "foo"), records.stream()
                .map(ReservedInstanceBoughtRecord::getProbeReservedInstanceId)
                .collect(Collectors.toSet()));
        assertEquals(Sets.newHashSet(102L, 101L), records.stream()
                .map(ReservedInstanceBoughtRecord::getReservedInstanceSpecId)
                .collect(Collectors.toSet()));
    }

    @Test
    public void updateReservedInstanceBoughtWithDelete() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos = Arrays.asList(riInfoOne, riInfoTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        final ReservedInstanceBoughtRecord fooReservedInstanceRecord =
                dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT)
                    .where(Tables.RESERVED_INSTANCE_BOUGHT.PROBE_RESERVED_INSTANCE_ID.eq("foo"))
                .fetchOne();
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl,
                Arrays.asList(riInfoTwo, riInfoThree, riInfoFour));

        final List<ReservedInstanceBoughtRecord> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT).fetch();
        assertEquals(3, records.size());
        assertEquals(Sets.newHashSet("test", "foo", "qux"), records.stream()
                .map(ReservedInstanceBoughtRecord::getProbeReservedInstanceId)
                .collect(Collectors.toSet()));

        final ReservedInstanceBoughtRecord updateFooReservedInstanceRecord =
                dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT)
                        .where(Tables.RESERVED_INSTANCE_BOUGHT.PROBE_RESERVED_INSTANCE_ID.eq("foo"))
                        .fetchOne();
        assertEquals(fooReservedInstanceRecord, updateFooReservedInstanceRecord);
    }

    @Test
    public void testGetReservedInstanceByAZFilter() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos = Arrays.asList(riInfoOne, riInfoTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        List<Long> scopeIds = new ArrayList<>();
        scopeIds.add(0L);
        final ReservedInstanceBoughtFilter zeroAzFilter = ReservedInstanceBoughtFilter.newBuilder()
                .availabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                        .addAllAvailabilityZoneId(scopeIds)
                        .build())
                .build();
        final List<ReservedInstanceBought> reservedInstancesByZeroAzFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(zeroAzFilter);
        assertEquals(0, reservedInstancesByZeroAzFilter.size());

        scopeIds.add(100L);
        final ReservedInstanceBoughtFilter azFilter = ReservedInstanceBoughtFilter.newBuilder()
                .availabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                        .addAllAvailabilityZoneId(scopeIds)
                        .build())
                .build();
        final List<ReservedInstanceBought> reservedInstancesByAzFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(azFilter);
        assertEquals(2, reservedInstancesByAzFilter.size());
    }


    @Test
    public void testGetReservedInstanceByRegionFilter() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos = Arrays.asList(riInfoOne, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        List<Long> scopeIds = new ArrayList<>();
        scopeIds.add(0L);
        final ReservedInstanceBoughtFilter zeroRegionIdFilter = ReservedInstanceBoughtFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeIds)
                        .build())
                .build();
        final List<ReservedInstanceBought> reservedInstancesByZeroRegionIdFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(zeroRegionIdFilter);
        assertEquals(0, reservedInstancesByZeroRegionIdFilter.size());

        scopeIds.add(77L);
        final ReservedInstanceBoughtFilter regionIdFilter = ReservedInstanceBoughtFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeIds)
                        .build())
                .build();
        final List<ReservedInstanceBought> reservedInstancesByRegionIdFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(regionIdFilter);
        assertEquals(2, reservedInstancesByRegionIdFilter.size());
        assertEquals("bar", reservedInstancesByRegionIdFilter.get(0)
                .getReservedInstanceBoughtInfo().getProbeReservedInstanceId());
    }

    /**
     * Tests getUndiscoveredUnusedReservedInstancesInScope.
     */
    @Test
    public void testGetUndiscoveredUnusedReservedInstance() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos = Arrays.asList(riInfoOne, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        List<Long> scopeIds = new ArrayList<>();
        scopeIds.add(0L);

        final ReservedInstanceBoughtFilter regionIdFilter = ReservedInstanceBoughtFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeIds)
                        .build())
                .build();
        Mockito.when(businessAccountHelper.getDiscoveredBusinessAccounts()).thenReturn(ImmutableSet.of(123L, 456L));
        final List<ReservedInstanceBought> reservedInstancesByRegionIdFilter =
                reservedInstanceBoughtStore.getUndiscoveredUnusedReservedInstancesInScope(regionIdFilter);
        assertEquals(1, reservedInstancesByRegionIdFilter.size());
        assertEquals("qux", reservedInstancesByRegionIdFilter.get(0)
                .getReservedInstanceBoughtInfo().getProbeReservedInstanceId());
    }

    @Test
    public void testGetReservedInstanceByRIFilter() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos = Arrays.asList(
                riInfoOne,
                riInfoTwo,
                riInfoThree,
                riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);

        // find the newly inserted RI id
        long riTwoId = dsl.select(RESERVED_INSTANCE_BOUGHT.ID)
                .from(RESERVED_INSTANCE_BOUGHT)
                .where(RESERVED_INSTANCE_BOUGHT.PROBE_RESERVED_INSTANCE_ID.eq(
                        riInfoTwo.getProbeReservedInstanceId()))
                .fetch()
                .get(0).value1();

        // create the RI Filter
        final ReservedInstanceBoughtFilter riFilter = ReservedInstanceBoughtFilter.newBuilder()
                .riBoughtFilter(Cost.ReservedInstanceBoughtFilter
                        .newBuilder()
                        .setExclusionFilter(false)
                        .addRiBoughtId(riTwoId)
                        .build())
                .build();
        /*
        Invoke SUT
         */
        final List<ReservedInstanceBought> actualRIBoughtInstances =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(riFilter);

        /*
        Assertions
         */
        assertThat(actualRIBoughtInstances, hasSize(1));
        final double fixedCost = actualRIBoughtInstances.get(0).getReservedInstanceBoughtInfo()
                .getReservedInstanceBoughtCost().getFixedCost().getAmount();
        final double recurringCost = actualRIBoughtInstances.get(0).getReservedInstanceBoughtInfo()
                .getReservedInstanceBoughtCost().getRecurringCostPerHour().getAmount();
        final double expectedAmortizedCost = calculateExpectedAmortizedCost(fixedCost, recurringCost, 2);
        assertThat(actualRIBoughtInstances.get(0).getReservedInstanceBoughtInfo(), equalTo(
                riInfoTwo.toBuilder()
                        // This will be stitched to ReservedInstanceUtilizationStore, which
                        // will return 0
                        .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                                .setNumberOfCouponsUsed(0)
                                .build())
                        .setReservedInstanceDerivedCost(ReservedInstanceDerivedCost.newBuilder()
                                .setAmortizedCostPerHour(CurrencyAmount.newBuilder().setAmount(expectedAmortizedCost).build()).build())
                        .build()));
    }

    /**
     * Test getting bought (existing) RIs by Account filter.
     *
     * <p> The RIs returned should be RIs from all sub-accounts and the master account for the Billing Family.
     */
    @Test
    public void testGetReservedInstanceByAccountFilter() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos = Arrays.asList(riInfoOne, riInfoTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        List<Long> scopeIds = new ArrayList<>();
        scopeIds.add(0L);
        final ReservedInstanceBoughtFilter zeroAccountIdFilter = ReservedInstanceBoughtFilter.newBuilder()
                .accountFilter(AccountReferenceFilter.newBuilder()
                        .addAllAccountId(scopeIds)
                        .build())
                .build();
        final List<ReservedInstanceBought> reservedInstancesByZeroAccountIdFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(zeroAccountIdFilter);
        assertEquals(0, reservedInstancesByZeroAccountIdFilter.size());

        scopeIds.add(123L);
        scopeIds.add(456L);
        final ReservedInstanceBoughtFilter accountIdFilter = ReservedInstanceBoughtFilter.newBuilder()
                .accountFilter(AccountReferenceFilter.newBuilder()
                        .addAllAccountId(scopeIds)
                        .build())
                .build();
        final List<ReservedInstanceBought> reservedInstancesByAccountIdFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(accountIdFilter);
        assertEquals(2, reservedInstancesByAccountIdFilter.size());
        assertEquals("bar", reservedInstancesByAccountIdFilter.get(0)
                .getReservedInstanceBoughtInfo().getProbeReservedInstanceId());

    }

    @Test
    public void testGetReservedInstanceCountMap() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder()
                .build();
        final Map<Long, Long> riCountMap = reservedInstanceBoughtStore.getReservedInstanceCountMap(filter);
        final long countForSpecOne = riCountMap.get(88L);
        final long countForSpecTwo = riCountMap.get(90L);

        assertEquals(2, riCountMap.size());
        assertEquals(50L, countForSpecOne);
        assertEquals(50L, countForSpecTwo);
    }

    @Test
    public void testGetReservedInstanceCountMapFilterByRegion() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        List<Long> scopeIds = new ArrayList<>();
        scopeIds.add(77L);
        final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeIds)
                        .build())
                .build();
        final Map<Long, Long> riCountMap = reservedInstanceBoughtStore.getReservedInstanceCountMap(filter);
        final long countForSpecOne = riCountMap.get(88L);

        assertEquals(1, riCountMap.size());
        assertEquals(50L, countForSpecOne);
    }

    @Test
    public void testGetReservedInstanceCountMapFilterByAZ() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        List<Long> scopeIds = new ArrayList<>();
        scopeIds.add(100L);
        final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder()
                .availabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                        .addAllAvailabilityZoneId(scopeIds)
                        .build())
                .build();
        final Map<Long, Long> riCountMap = reservedInstanceBoughtStore.getReservedInstanceCountMap(filter);
        final long countForSpecOne = riCountMap.get(88L);

        assertEquals(2, riCountMap.size());
        assertEquals(10L, countForSpecOne);
    }

    /**
     * Test case to verify retrieving aggregated amortized cost by scoping based on availability zones.
     */
    @Test
    public void testGetReservedInstanceAggregatedAmortizedCostFilterByAZ() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos =
                        Arrays.asList(riInfoOne, riInfoTwo, riInfoThree, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        List<Long> scopeIds = new ArrayList<>();
        scopeIds.add(100L);
        final ReservedInstanceCostFilter filter = ReservedInstanceCostFilter.newBuilder()
                .availabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                        .addAllAvailabilityZoneId(scopeIds)
                        .build())
                .build();
        final List<ReservedInstanceBoughtRecord> reservedInstanceBoughtRecords =
                        dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT)
                                        .where(filter.generateConditions()).fetch();
        final double expectedAggregatedAmortizedCost = calculateExpectedAggregatedAmortizedCosts(reservedInstanceBoughtRecords);
        final Cost.ReservedInstanceCostStat reservedInstanceAggregatedCosts =
                        reservedInstanceBoughtStore.getReservedInstanceAggregatedCosts(filter);
        assertEquals(expectedAggregatedAmortizedCost, reservedInstanceAggregatedCosts.getAmortizedCost(), 0D);
    }

    /**
     * Test case to verify retrieving aggregated amortized cost by scoping based on regions.
     */
    @Test
    public void testGetReservedInstanceAggregatedAmortizedCostFilterByRegion() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos =
                        Arrays.asList(riInfoOne, riInfoTwo, riInfoThree, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        List<Long> scopeIds = new ArrayList<>();
        scopeIds.add(77L);
        final ReservedInstanceCostFilter filter = ReservedInstanceCostFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeIds)
                        .build())
                .build();
        final Result<ReservedInstanceBoughtRecord> reservedInstanceBoughtRecords =
                        dsl.select(RESERVED_INSTANCE_BOUGHT.fields()).from(RESERVED_INSTANCE_BOUGHT)
                                        .join(RESERVED_INSTANCE_SPEC)
                                        .on(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID
                                                        .eq(RESERVED_INSTANCE_SPEC.ID))
                                        .where(filter.generateConditions()).fetch().into(RESERVED_INSTANCE_BOUGHT);
        final double expectedAggregatedAmortizedCost = calculateExpectedAggregatedAmortizedCosts(reservedInstanceBoughtRecords);
        final Cost.ReservedInstanceCostStat reservedInstanceAggregatedCosts =
                        reservedInstanceBoughtStore.getReservedInstanceAggregatedCosts(filter);
        assertEquals(expectedAggregatedAmortizedCost, reservedInstanceAggregatedCosts.getAmortizedCost(), 0D);
    }

    /**
     * Test case to verify retrieving aggregated amortized cost by scoping based on business accounts.
     */
    @Test
    public void testGetReservedInstanceAggregatedAmortizedCostFilterByBusinessAccount() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos =
                        Arrays.asList(riInfoOne, riInfoTwo, riInfoThree, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        List<Long> scopeIds = new ArrayList<>();
        scopeIds.add(789L);
        final ReservedInstanceCostFilter filter = ReservedInstanceCostFilter.newBuilder()
                .accountFilter(AccountReferenceFilter.newBuilder()
                        .addAllAccountId(scopeIds)
                        .build())
                .build();
        final List<ReservedInstanceBoughtRecord> reservedInstanceBoughtRecords =
                        dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT)
                                        .where(filter.generateConditions()).fetch();
        final double expectedAggregatedAmortizedCost = calculateExpectedAggregatedAmortizedCosts(reservedInstanceBoughtRecords);
        final Cost.ReservedInstanceCostStat reservedInstanceAggregatedCosts =
                        reservedInstanceBoughtStore.getReservedInstanceAggregatedCosts(filter);
        assertEquals(expectedAggregatedAmortizedCost, reservedInstanceAggregatedCosts.getAmortizedCost(), 0D);
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

    private void verifyAmortizedCosts(List<ReservedInstanceBoughtInfo> reservedInstanceBoughtInfos, List<ReservedInstanceBoughtRecord> records) {
        final Set<Long> riSpecIDSet =
                        reservedInstanceBoughtInfos.stream().map(a -> a.getReservedInstanceSpec())
                                        .collect(Collectors.toSet());
        final Map<Long, Integer> riSpecToTermMap =
                        reservedInstanceSpecStore.getReservedInstanceSpecByIds(riSpecIDSet).stream()
                                        .collect(Collectors.toMap(ReservedInstanceSpec::getId,
                                                        riSpec -> riSpec.getReservedInstanceSpecInfo()
                                                                        .getType().getTermYears()));
        final Map<String, Double> riToAmortizedCostMap = records.stream().collect(Collectors
                        .toMap(ReservedInstanceBoughtRecord::getProbeReservedInstanceId,
                                        ReservedInstanceBoughtRecord::getPerInstanceAmortizedCostHourly));
        for (ReservedInstanceBoughtInfo reservedInstanceBoughtInfo : reservedInstanceBoughtInfos) {
            final double expectedAmortizedCost = calculateExpectedAmortizedCost(reservedInstanceBoughtInfo
                                            .getReservedInstanceBoughtCost()
                                            .getFixedCost()
                                            .getAmount(),
                            reservedInstanceBoughtInfo
                                            .getReservedInstanceBoughtCost()
                                            .getRecurringCostPerHour()
                                            .getAmount(),
                            riSpecToTermMap.get(reservedInstanceBoughtInfo.getReservedInstanceSpec()));
            assertEquals(expectedAmortizedCost, riToAmortizedCostMap.get(reservedInstanceBoughtInfo.getProbeReservedInstanceId()), 0D);
        }
    }

    private double calculateExpectedAmortizedCost(double fixedCost, double recurringCost, int term) {
        return ((fixedCost / (term * NO_OF_MONTHS * MONTHLY_TO_HOURLY_CONVERSION)) + recurringCost);
    }

    private double calculateExpectedAggregatedAmortizedCosts(List<ReservedInstanceBoughtRecord> reservedInstanceBoughtRecords) {
        double aggregatedAmortizedCostPerRI = 0D;
        for (ReservedInstanceBoughtRecord reservedInstanceBoughtRecord : reservedInstanceBoughtRecords) {
            aggregatedAmortizedCostPerRI += reservedInstanceBoughtRecord.getPerInstanceAmortizedCostHourly()
                            * reservedInstanceBoughtRecord.getCount();
        }
        return aggregatedAmortizedCostPerRI;
    }
}
