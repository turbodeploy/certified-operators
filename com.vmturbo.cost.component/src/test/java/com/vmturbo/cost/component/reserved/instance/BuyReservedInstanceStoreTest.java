package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.BUY_RESERVED_INSTANCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.util.CollectionUtils;

import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.BuyReservedInstanceRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceCostFilter;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceFilter;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisRecommendation;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class BuyReservedInstanceStoreTest {
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

    private BuyReservedInstanceStore buyRiStore = new BuyReservedInstanceStore(dsl, new DefaultIdentityProvider(0));

    private final ReservedInstanceBoughtInfo newRInfo = ReservedInstanceBoughtInfo.newBuilder()
            .setReservedInstanceSpec(99L)
            .setNumBought(18)
            .build();

    private final ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
            .setId(99L)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.getDefaultInstance())
            .build();

    @Before
    public void setup() throws Exception {
        insertDefaultReservedInstanceSpec();
    }

    /**
     * Gets the Buy RIs with no condition.
     */
    @Test
    public void testGetBuyRIs_noCondition() {
        insertOldBuyRIRecords();
        final BuyReservedInstanceFilter filter = BuyReservedInstanceFilter.newBuilder().build();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(filter);
        assertEquals(4, buyRIs.size());
    }

    /**
     * Get the Aggregated Amortized Cost with no conditions.
     */
    @Test
    public void testAggregatedAmortizedCosts_noCondition() {
        insertOldBuyRIRecords();
        final BuyReservedInstanceCostFilter costFilter = BuyReservedInstanceCostFilter.newBuilder().build();
        final Result<BuyReservedInstanceRecord> fetch =
                        dsl.selectFrom(BUY_RESERVED_INSTANCE).where(costFilter.getConditions())
                                        .fetch();
        final double expectedAmortizedCost = calculateExpectedAmortizedCost(fetch);
        final Cost.ReservedInstanceCostStat reservedInstanceAggregatedCost =
                        buyRiStore.getReservedInstanceAggregatedAmortizedCost(costFilter);
        assertEquals(expectedAmortizedCost, reservedInstanceAggregatedCost.getAmortizedCost(), 0D);
    }

    /**
     * Gets the Buy RIs in a region.
     */
    @Test
    public void testGetBuyRIs_filterByRegion() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                .newBuilder().setRegionFilter(RegionFilter.newBuilder().addRegionId(400L).build()).build();
        final BuyReservedInstanceFilter filter = BuyReservedInstanceFilter.newBuilder()
                        .setRegionFilter(request.getRegionFilter()).build();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(filter);
        assertEquals(1, buyRIs.size());
        assertEquals(1001L, buyRIs.iterator().next().getId());
    }

    /**
     * Get the Aggregated Amortized Cost scoped to a region.
     */
    @Test
    public void testAggregatedAmortizedCosts_filterByRegion() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                        .newBuilder().setRegionFilter(RegionFilter.newBuilder().addRegionId(400L).build()).build();
        final BuyReservedInstanceCostFilter costFilter = BuyReservedInstanceCostFilter.newBuilder()
                        .setRegionFilter(request.getRegionFilter()).build();
        final Result<BuyReservedInstanceRecord> fetch =
                        dsl.selectFrom(BUY_RESERVED_INSTANCE).where(costFilter.getConditions())
                                        .fetch();
        final double expectedAmortizedCost = calculateExpectedAmortizedCost(fetch);
        final Cost.ReservedInstanceCostStat reservedInstanceAggregatedCost =
                        buyRiStore.getReservedInstanceAggregatedAmortizedCost(costFilter);
        assertEquals(expectedAmortizedCost, reservedInstanceAggregatedCost.getAmortizedCost(), 0D);
    }

    /**
     * Gets the Buy RIs in a business account.
     */
    @Test
    public void testGetBuyRIs_filterByAccount() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                .newBuilder().setAccountFilter(AccountFilter.newBuilder().addAccountId(456L).build()).build();
        final BuyReservedInstanceFilter filter = BuyReservedInstanceFilter.newBuilder()
                        .setAccountFilter(request.getAccountFilter()).build();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(filter);
        assertEquals(1, buyRIs.size());
        assertEquals(1002L, buyRIs.iterator().next().getId());
    }

    /**
     * Get the Aggregated Amortized Cost scoped to accounts.
     */
    @Test
    public void testAggregatedAmortizedCosts_filterByAccount() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                        .newBuilder().setAccountFilter(AccountFilter.newBuilder().addAccountId(456L).build()).build();
        final BuyReservedInstanceCostFilter costFilter = BuyReservedInstanceCostFilter.newBuilder()
                        .setAccountFilter(request.getAccountFilter()).build();
        final Result<BuyReservedInstanceRecord> fetch =
                        dsl.selectFrom(BUY_RESERVED_INSTANCE).where(costFilter.getConditions())
                                        .fetch();
        final double expectedAmortizedCost = calculateExpectedAmortizedCost(fetch);
        final Cost.ReservedInstanceCostStat reservedInstanceAggregatedCost =
                        buyRiStore.getReservedInstanceAggregatedAmortizedCost(costFilter);
        assertEquals(expectedAmortizedCost, reservedInstanceAggregatedCost.getAmortizedCost(), 0D);
    }

    /**
     * Gets the Buy RIs in a topology context id.
     */
    @Test
    public void testGetBuyRIs_topologyContextId() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                .newBuilder().setTopologyContextId(1L).build();
        final BuyReservedInstanceFilter filter = BuyReservedInstanceFilter.newBuilder()
                        .addTopologyContextId(request.getTopologyContextId()).build();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(filter);
        assertEquals(3, buyRIs.size());
    }

    /**
     * Get the Aggregated Amortized Cost scoped to a Topology Context.
     */
    @Test
    public void testAggregatedAmortizedCosts_topologyContextId() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                        .newBuilder().setTopologyContextId(1L).build();
        final BuyReservedInstanceCostFilter costFilter = BuyReservedInstanceCostFilter.newBuilder()
                        .addTopologyContextId(request.getTopologyContextId()).build();
        final Result<BuyReservedInstanceRecord> fetch =
                        dsl.selectFrom(BUY_RESERVED_INSTANCE).where(costFilter.getConditions())
                                        .fetch();
        final double expectedAmortizedCost = calculateExpectedAmortizedCost(fetch);
        final Cost.ReservedInstanceCostStat reservedInstanceAggregatedCost =
                        buyRiStore.getReservedInstanceAggregatedAmortizedCost(costFilter);
        assertEquals(expectedAmortizedCost, reservedInstanceAggregatedCost.getAmortizedCost(), 0D);
    }

    /**
     * Gets the Buy RIs in a topology context id and region.
     */
    @Test
    public void testGetBuyRIs_topologyContextIdAndRegion() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                .newBuilder().setTopologyContextId(1L)
                .setRegionFilter(RegionFilter.newBuilder().addRegionId(300L).build())
                .build();
        final BuyReservedInstanceFilter filter = BuyReservedInstanceFilter.newBuilder().addTopologyContextId(request.getTopologyContextId())
                        .setRegionFilter(request.getRegionFilter()).build();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(filter);
        assertEquals(1, buyRIs.size());
        assertEquals(1000L, buyRIs.iterator().next().getId());
    }

    /**
     * Get the Aggregated Amortized Cost scoped to a list of Buy RI IDs.
     */
    @Test
    public void testGetBuyRIs_BuyRIId() {
        insertOldBuyRIRecords();
        List<Long> buyRIIdList = ImmutableList.of(1000L, 1001L);
        final BuyReservedInstanceCostFilter costFilter = BuyReservedInstanceCostFilter.newBuilder().addBuyRIIdList(buyRIIdList).build();
        final Result<BuyReservedInstanceRecord> fetch =
                        dsl.selectFrom(BUY_RESERVED_INSTANCE).where(costFilter.getConditions())
                                        .fetch();
        final double expectedAmortizedCost = calculateExpectedAmortizedCost(fetch);
        final Cost.ReservedInstanceCostStat reservedInstanceAggregatedCost =
                        buyRiStore.getReservedInstanceAggregatedAmortizedCost(costFilter);
        assertEquals(expectedAmortizedCost, reservedInstanceAggregatedCost.getAmortizedCost(), 0D);
    }

    /**
     * Inserts 3 Buy RI Record for topology context id 1 and 1 buy RI for topology context id 2.
     * Updates Buy RI Record for topology context id 1.
     */
    @Test
    public void testUpdateBuyRIs() {
        insertOldBuyRIRecords();

        final long recommendationId = 123L;
        final ReservedInstanceData riData = ReservedInstanceData.builder()
                .commitment(ReservedInstanceBought.newBuilder()
                        .setId(recommendationId)
                        .setReservedInstanceBoughtInfo(newRInfo)
                        .build())
                .spec(riSpec)
                .build();

        buyRiStore.updateBuyReservedInstances(Collections.singletonList(riData), 1L);

        final List<BuyReservedInstanceRecord> records = dsl.selectFrom(Tables.BUY_RESERVED_INSTANCE).fetch();
        Map<Long, List<BuyReservedInstanceRecord>> recordsByContextId = records.stream().collect(
                Collectors.groupingBy(BuyReservedInstanceRecord::getTopologyContextId));
        assertEquals(2, recordsByContextId.size());
        // Verify that Buy RI Record for topology context 2 was NOT updated
        assertEquals(1, recordsByContextId.get(2L).size());
        assertEquals(Integer.valueOf(9), recordsByContextId.get(2L).get(0).getCount());
        // Verify that Buy RI Record for topology context 1 was updated
        assertEquals(1, recordsByContextId.get(1L).size());
        assertEquals(Integer.valueOf(18), recordsByContextId.get(1L).get(0).getCount());
    }

    /**
     * Inserts 3 Buy RI Record for topology context id 1 and 1 buy RI for topology context id 2.
     * Deletes Buy RI Record for topology context id 1 and does not update as buy RI recommendations
     * are empty.
     */
    @Test
    public void testUpdateNoBuyRIs() {
        insertOldBuyRIRecords();
        buyRiStore.updateBuyReservedInstances(Collections.emptyList(), 1L);
        final List<BuyReservedInstanceRecord> records = dsl.selectFrom(Tables.BUY_RESERVED_INSTANCE).fetch();
        Map<Long, List<BuyReservedInstanceRecord>> recordsByContextId = records.stream().collect(
                        Collectors.groupingBy(BuyReservedInstanceRecord::getTopologyContextId));
        //Verify that data of Topology Context ID 2 is not cleared
        assertEquals(1, recordsByContextId.get(2L).size());
        //Verify that data of Topology Context ID 1 was deleted
        assertTrue(CollectionUtils.isEmpty(recordsByContextId.get(1L)));
    }

    /**
     * Inserts Buy RI Record for topology context id 1.
     */
    @Test
    public void testInsertBuyRIs() {

        final long recommendationId = 123L;
        final ReservedInstanceData riData = ReservedInstanceData.builder()
                .commitment(ReservedInstanceBought.newBuilder()
                        .setId(recommendationId)
                        .setReservedInstanceBoughtInfo(newRInfo)
                        .build())
                .spec(riSpec)
                .build();

        buyRiStore.updateBuyReservedInstances(Collections.singletonList(riData), 1L);

        final List<BuyReservedInstanceRecord> records = dsl.selectFrom(Tables.BUY_RESERVED_INSTANCE).fetch();
        assertEquals(1, records.size());
        assertEquals(Integer.valueOf(18), records.get(0).getCount());
    }

    /**
     * Foreign key error.
     */
    @Test(expected = DataIntegrityViolationException.class)
    public void testForeignKeyError() {
        final ReservedInstanceBoughtInfo newRInfo = ReservedInstanceBoughtInfo.newBuilder()
                .setReservedInstanceSpec(100L)
                .setNumBought(18)
                .build();

        final long recommendationId = 123L;
        final ReservedInstanceData riData = ReservedInstanceData.builder()
                .commitment(ReservedInstanceBought.newBuilder()
                        .setId(recommendationId)
                        .setReservedInstanceBoughtInfo(newRInfo)
                        .build())
                .spec(riSpec)
                .build();

        buyRiStore.updateBuyReservedInstances(Arrays.asList(riData), 1L);
    }

    private void insertDefaultReservedInstanceSpec() {
        final ReservedInstanceSpecRecord specRecordOne = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(99L,
                        OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getNumber(),
                        1,
                        Tenancy.DEDICATED.getValue(),
                        OSType.LINUX.getValue(),
                        88L,
                        77L,
                        ReservedInstanceSpecInfo.getDefaultInstance()));
        dsl.batchInsert(Arrays.asList(specRecordOne)).execute();
    }

    /**
     * Setup buy RI records for topology context ids 1 and 2.
     */
    private void insertOldBuyRIRecords() {
        final ReservedInstanceBoughtInfo oldRiInfoOne = ReservedInstanceBoughtInfo.newBuilder()
                .setReservedInstanceSpec(99L)
                .setNumBought(8)
                .build();

        final ReservedInstanceBoughtInfo oldRiInfoTwo = ReservedInstanceBoughtInfo.newBuilder()
                .setReservedInstanceSpec(99L)
                .setNumBought(9)
                .build();

        final BuyReservedInstanceRecord buyRIRecord1ForTopoContext1 = dsl.newRecord(Tables.BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(1000L, 1L, 123L, 300L, 99L, 8, oldRiInfoOne, 15D, 0.15D, 0.1517123288D));

        final BuyReservedInstanceRecord buyRIRecord2ForTopoContext1 = dsl.newRecord(Tables.BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(1001L, 1L, 123L, 400L, 99L, 8, oldRiInfoOne, 25D, 0.25D, 0.2528538813D));

        final BuyReservedInstanceRecord buyRIRecord3ForTopoContext1 = dsl.newRecord(Tables.BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(1002L, 1L, 456L, 500L, 99L, 8, oldRiInfoOne, 20D, 0.10D, 0.102283105D));

        final BuyReservedInstanceRecord buyRIRecord1ForTopoContext2 = dsl.newRecord(Tables.BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(1003L, 2L, 123L, 300L, 99L, 9, oldRiInfoTwo, 0D, 0.75D, 0.75D));

        dsl.batchInsert(Arrays.asList(buyRIRecord1ForTopoContext1, buyRIRecord2ForTopoContext1,
                buyRIRecord3ForTopoContext1, buyRIRecord1ForTopoContext2)).execute();
    }

    private static double calculateExpectedAmortizedCost(List<BuyReservedInstanceRecord> buyReservedInstanceRecords) {
        double aggregatedAmortizedCost = 0;
        for (BuyReservedInstanceRecord buyReservedInstanceRecord: buyReservedInstanceRecords) {
            aggregatedAmortizedCost += buyReservedInstanceRecord.getPerInstanceAmortizedCostHourly().doubleValue()
                            * buyReservedInstanceRecord.getCount();
        }
        return aggregatedAmortizedCost;
    }
}
