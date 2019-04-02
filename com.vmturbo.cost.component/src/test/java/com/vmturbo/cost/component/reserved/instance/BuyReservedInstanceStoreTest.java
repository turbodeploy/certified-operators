package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
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

    final ReservedInstanceBoughtInfo oldRiInfoOne = ReservedInstanceBoughtInfo.newBuilder()
            .setReservedInstanceSpec(99L)
            .setNumBought(8)
            .build();

    final ReservedInstanceBoughtInfo oldRiInfoTwo = ReservedInstanceBoughtInfo.newBuilder()
            .setReservedInstanceSpec(99L)
            .setNumBought(9)
            .build();

    final ReservedInstanceBoughtInfo newRInfo = ReservedInstanceBoughtInfo.newBuilder()
            .setReservedInstanceSpec(99L)
            .setNumBought(18)
            .build();

    final ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
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
     * Inserts 1 Buy RI Record for topology context id 1 and 2.
     * Updates Buy RI Record for topology context id 1.
     */
    @Test
    public void testUpdateBuyRIs() {
        insertOldBuyRIRecords();
        BuyReservedInstanceInfo newBuyRiInfo = ImmutableBuyReservedInstanceInfo.builder()
                .riBoughtInfo(newRInfo).riSpec(riSpec).topologyContextId(1L).build();

        buyRiStore.udpateBuyReservedInstances(Arrays.asList(newBuyRiInfo));

        final List<BuyReservedInstanceRecord> records = dsl.selectFrom(Tables.BUY_RESERVED_INSTANCE).fetch();
        Map<Long, List<BuyReservedInstanceRecord>> recordsByContextId = records.stream().collect(
                Collectors.groupingBy(record -> record.getTopologyContextId()));
        assertEquals(2, recordsByContextId.size());
        // Verify that Buy RI Record for topology context 2 was NOT updated
        assertEquals(1, recordsByContextId.get(2L).size());
        assertEquals(Integer.valueOf(9), recordsByContextId.get(2L).get(0).getCount());
        // Verify that Buy RI Record for topology context 1 was updated
        assertEquals(Integer.valueOf(18), recordsByContextId.get(1L).get(0).getCount());
    }

    /**
     * Inserts Buy RI Record for topology context id 1.
     */
    @Test
    public void testInsertBuyRIs() {
        BuyReservedInstanceInfo newBuyRiInfo = ImmutableBuyReservedInstanceInfo.builder()
                .riBoughtInfo(newRInfo).riSpec(riSpec).topologyContextId(1L).build();

        buyRiStore.udpateBuyReservedInstances(Arrays.asList(newBuyRiInfo));

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
     * Setup one buy RI record each for topology context ids 1 and 2.
     */
    private void insertOldBuyRIRecords() {
        final BuyReservedInstanceRecord buyRIRecord1 = dsl.newRecord(Tables.BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(
                        1000L,
                        1L,
                        123L,
                        300L,
                        99L,
                        8,
                        oldRiInfoOne));
        final BuyReservedInstanceRecord buyRIRecord2 = dsl.newRecord(Tables.BUY_RESERVED_INSTANCE,
                new BuyReservedInstanceRecord(
                        1001L,
                        2L,
                        123L,
                        300L,
                        99L,
                        9,
                        oldRiInfoTwo));
        dsl.batchInsert(Arrays.asList(buyRIRecord1, buyRIRecord2)).execute();
    }
}
