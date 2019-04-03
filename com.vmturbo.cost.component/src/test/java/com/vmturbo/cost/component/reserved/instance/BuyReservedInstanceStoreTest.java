package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore.BuyReservedInstanceInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.Tenancy;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class BuyReservedInstanceStoreTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private BuyReservedInstanceStore buyRiStore;

    private DSLContext dsl;

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
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        buyRiStore = new BuyReservedInstanceStore(dsl, new IdentityProvider(0));
        insertDefaultReservedInstanceSpec();
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    /**
     * Gets the Buy RIs with no condition
     */
    @Test
    public void testGetBuyRIs_noCondition() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest.getDefaultInstance();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(request);
        assertEquals(4, buyRIs.size());
    }

    /**
     * Gets the Buy RIs in a region
     */
    @Test
    public void testGetBuyRIs_filterByRegion() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                .newBuilder().setRegionFilter(RegionFilter.newBuilder().addRegionId(400L).build()).build();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(request);
        assertEquals(1, buyRIs.size());
        assertEquals(1001L, buyRIs.iterator().next().getId());
    }

    /**
     * Gets the Buy RIs in a business account
     */
    @Test
    public void testGetBuyRIs_filterByAccount() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                .newBuilder().setAccountFilter(AccountFilter.newBuilder().addAccountId(456L).build()).build();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(request);
        assertEquals(1, buyRIs.size());
        assertEquals(1002L, buyRIs.iterator().next().getId());
    }

    /**
     * Gets the Buy RIs in a topology context id
     */
    @Test
    public void testGetBuyRIs_topologyContextId() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                .newBuilder().setTopologyContextId(1L).build();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(request);
        assertEquals(3, buyRIs.size());
    }

    /**
     * Gets the Buy RIs in a topology context id and region
     */
    @Test
    public void testGetBuyRIs_topologyContextIdAndRegion() {
        insertOldBuyRIRecords();
        GetBuyReservedInstancesByFilterRequest request = GetBuyReservedInstancesByFilterRequest
                .newBuilder().setTopologyContextId(1L)
                .setRegionFilter(RegionFilter.newBuilder().addRegionId(300L).build())
                .build();
        Collection<ReservedInstanceBought> buyRIs = buyRiStore.getBuyReservedInstances(request);
        assertEquals(1, buyRIs.size());
        assertEquals(1000L, buyRIs.iterator().next().getId());
    }

    /**
     * Inserts 3 Buy RI Record for topology context id 1 and 1 buy RI for topology context id 2.
     * Updates Buy RI Record for topology context id 1.
     */
    @Test
    public void testUpdateBuyRIs() {
        insertOldBuyRIRecords();
        BuyReservedInstanceInfo newBuyRiInfo = ImmutableBuyReservedInstanceInfo.builder()
                .riBoughtInfo(newRInfo).riSpec(riSpec).topologyContextId(1L).build();

        buyRiStore.udpateBuyReservedInstances(Collections.singletonList(newBuyRiInfo));

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
     * Inserts Buy RI Record for topology context id 1.
     */
    @Test
    public void testInsertBuyRIs() {
        BuyReservedInstanceInfo newBuyRiInfo = ImmutableBuyReservedInstanceInfo.builder()
                .riBoughtInfo(newRInfo).riSpec(riSpec).topologyContextId(1L).build();

        buyRiStore.udpateBuyReservedInstances(Collections.singletonList(newBuyRiInfo));

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
        BuyReservedInstanceInfo newBuyRiInfo = ImmutableBuyReservedInstanceInfo.builder()
                .riBoughtInfo(newRInfo).riSpec(riSpec).topologyContextId(1L).build();

        buyRiStore.udpateBuyReservedInstances(Arrays.asList(newBuyRiInfo));
    }

    private void insertDefaultReservedInstanceSpec() {
        final ReservedInstanceSpecRecord specRecordOne = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(99L,
                        OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getValue(),
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
                new BuyReservedInstanceRecord(1000L, 1L, 123L, 300L, 99L, 8, oldRiInfoOne));

        final BuyReservedInstanceRecord buyRIRecord2ForTopoContext1 = dsl.newRecord(Tables.BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(1001L, 1L, 123L, 400L, 99L, 8, oldRiInfoOne));

        final BuyReservedInstanceRecord buyRIRecord3ForTopoContext1 = dsl.newRecord(Tables.BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(1002L, 1L, 456L, 500L, 99L, 8, oldRiInfoOne));

        final BuyReservedInstanceRecord buyRIRecord1ForTopoContext2 = dsl.newRecord(Tables.BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(1003L, 2L, 123L, 300L, 99L, 9, oldRiInfoTwo));

        dsl.batchInsert(Arrays.asList(buyRIRecord1ForTopoContext1, buyRIRecord2ForTopoContext1,
                buyRIRecord3ForTopoContext1, buyRIRecord1ForTopoContext2)).execute();
    }
}
