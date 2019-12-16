package com.vmturbo.cost.component.reserved.instance;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PlanReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.Tenancy;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Tests for the {@link PlanReservedInstanceStore}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestSQLDatabaseConfig.class})
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class PlanReservedInstanceStoreTest {
    private static final long PLAN_ID = 12345L;

    private static final ReservedInstanceBoughtInfo RI_INFO_1 = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(123L)
                    .setProbeReservedInstanceId("bar")
                    .setReservedInstanceSpec(101L)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(10)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                                    .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0))
                                    .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
                    .setDisplayName("t101.small")
                    .build();

    private static final ReservedInstanceBoughtInfo RI_INFO_2 = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(456L)
                    .setProbeReservedInstanceId("foo")
                    .setReservedInstanceSpec(102L)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(20)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                                    .newBuilder()
                                    .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(15))
                                    .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
                    .setDisplayName("t102.large")
                    .build();

    private static final Map<String, Long> TIER_TO_COUNT_MAP = createTierToCountMap();

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private PlanReservedInstanceStore planReservedInstanceStore;

    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    private DSLContext dsl;

    /**
     * Initialize instances before test.
     *
     * @throws Exception if something goes wrong.
     */
    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl, new IdentityProvider(0), 10);
        planReservedInstanceStore = new PlanReservedInstanceStore(dsl, new IdentityProvider(0));
        insertDefaultReservedInstanceSpec();
        final List<ReservedInstanceBought> reservedInstanceBoughtInfos =
                        Arrays.asList(ReservedInstanceBought.newBuilder().setReservedInstanceBoughtInfo(RI_INFO_1).build(),
                                        ReservedInstanceBought.newBuilder().setReservedInstanceBoughtInfo(RI_INFO_2)
                                                        .build());
        planReservedInstanceStore.insertPlanReservedInstanceBought(reservedInstanceBoughtInfos, PLAN_ID);
    }

    private static Map<String, Long> createTierToCountMap() {
        final Map<String, Long> result = new HashMap<>();
        result.put("t101.small", 10L);
        result.put("t102.large", 20L);
        return Collections.unmodifiableMap(result);
    }

    /**
     * Clean up instances after test.
     */
    @After
    public void teardown() {
        flyway.clean();
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
        final Map<String, Long> tierToCountMap = planReservedInstanceStore.getPlanReservedInstanceCountByRISpecIdMap(PLAN_ID);
        Assert.assertEquals(TIER_TO_COUNT_MAP, tierToCountMap);
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

    private void insertDefaultReservedInstanceSpec() {
        final CloudCostDTO.ReservedInstanceType riType1 = CloudCostDTO.ReservedInstanceType.newBuilder().setTermYears(1).setOfferingClass(
                        CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD).setPaymentOption(
                        CloudCostDTO.ReservedInstanceType.PaymentOption.ALL_UPFRONT).build();

        final CloudCostDTO.ReservedInstanceType riType2 = CloudCostDTO.ReservedInstanceType.newBuilder().setTermYears(2).setOfferingClass(
                        CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD).setPaymentOption(
                        CloudCostDTO.ReservedInstanceType.PaymentOption.ALL_UPFRONT).build();

        final ReservedInstanceSpecRecord specRecordOne = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(101L,
                        OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getValue(),
                        1,
                        Tenancy.DEDICATED.getValue(),
                        OSType.LINUX.getValue(),
                        88L,
                        77L,
                        ReservedInstanceSpecInfo.newBuilder().setType(riType1).build()));
        final ReservedInstanceSpecRecord specRecordTwo = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(102L,
                        OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getValue(),
                        2,
                        Tenancy.HOST.getValue(),
                        OSType.LINUX.getValue(),
                        90L,
                        78L,
                        ReservedInstanceSpecInfo.newBuilder().setType(riType2).build()));
        dsl.batchInsert(Arrays.asList(specRecordOne, specRecordTwo)).execute();
    }

}
