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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
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
public class ReservedInstanceBoughtStoreTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private DSLContext dsl;

    final ReservedInstanceBoughtInfo riInfoOne = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(123L)
            .setProbeReservedInstanceId("bar")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(100L)
            .setNumBought(10)
            .build();

    final ReservedInstanceBoughtInfo riInfoTwo = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(456)
            .setProbeReservedInstanceId("foo")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(100L)
            .setNumBought(20)
            .build();

    final ReservedInstanceBoughtInfo riInfoThree = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(789)
            .setProbeReservedInstanceId("test")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(50L)
            .setNumBought(30)
            .build();

    final ReservedInstanceBoughtInfo riInfoFour = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(789)
            .setProbeReservedInstanceId("qux")
            .setReservedInstanceSpec(100L)
            .setAvailabilityZoneId(50L)
            .setNumBought(40)
            .build();

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        reservedInstanceBoughtStore = new ReservedInstanceBoughtStore(dsl,
                new IdentityProvider(0));
        insertDefaultReservedInstanceSpec();
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void updateReservedInstanceBoughtOnlyAdd() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceBoughtInfos =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceBoughtInfos);

        final List<ReservedInstanceBoughtRecord> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT).fetch();
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
                .setReservedInstanceSpec(100L)
                .build();
        final ReservedInstanceBoughtInfo newRiInfoTwo = ReservedInstanceBoughtInfo.newBuilder(riInfoTwo)
                .setReservedInstanceSpec(100L)
                .build();
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl,
                Arrays.asList(newRiInfoOne, newRiInfoTwo));
        final List<ReservedInstanceBoughtRecord> records = dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT)
                .where(Tables.RESERVED_INSTANCE_BOUGHT.ID.in(recordIds))
                .fetch();
        assertEquals(2, records.size());
        assertEquals(Sets.newHashSet("bar", "foo"), records.stream()
                .map(ReservedInstanceBoughtRecord::getProbeReservedInstanceId)
                .collect(Collectors.toSet()));
        assertEquals(Sets.newHashSet(100L), records.stream()
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
        final ReservedInstanceBoughtFilter zeroAzFilter = ReservedInstanceBoughtFilter.newBuilder()
                .addAvailabilityZoneId(0L)
                .build();
        final List<ReservedInstanceBought> reservedInstancesByZeroAzFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(zeroAzFilter);
        assertEquals(0, reservedInstancesByZeroAzFilter.size());

        final ReservedInstanceBoughtFilter azFilter = ReservedInstanceBoughtFilter.newBuilder()
                .addAvailabilityZoneId(100L)
                .build();
        final List<ReservedInstanceBought> reservedInstancesByAzFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(azFilter);
        assertEquals(2, reservedInstancesByAzFilter.size());
    }


    @Test
    public void testGetReservedInstanceByRegionFilter() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos = Arrays.asList(riInfoOne, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        final ReservedInstanceBoughtFilter zeroRegionIdFilter = ReservedInstanceBoughtFilter.newBuilder()
                .addRegionId(0L)
                .setJoinWithSpecTable(true)
                .build();
        final List<ReservedInstanceBought> reservedInstancesByZeroRegionIdFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(zeroRegionIdFilter);
        assertEquals(0, reservedInstancesByZeroRegionIdFilter.size());

        final ReservedInstanceBoughtFilter regionIdFilter = ReservedInstanceBoughtFilter.newBuilder()
                .addRegionId(77L)
                .setJoinWithSpecTable(true)
                .build();
        final List<ReservedInstanceBought> reservedInstancesByRegionIdFilter =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(regionIdFilter);
        assertEquals(1, reservedInstancesByRegionIdFilter.size());
        assertEquals("bar", reservedInstancesByRegionIdFilter.get(0)
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
        assertEquals(60L, countForSpecOne);
        assertEquals(40L, countForSpecTwo);
    }

    @Test
    public void testGetReservedInstanceCountMapFilterByRegion() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder()
                .addRegionId(77L)
                .build();
        final Map<Long, Long> riCountMap = reservedInstanceBoughtStore.getReservedInstanceCountMap(filter);
        final long countForSpecOne = riCountMap.get(88L);

        assertEquals(1, riCountMap.size());
        assertEquals(60L, countForSpecOne);
    }

    @Test
    public void testGetReservedInstanceCountMapFilterByAZ() {
        final List<ReservedInstanceBoughtInfo> reservedInstanceInfos =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree, riInfoFour);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstanceInfos);
        final ReservedInstanceBoughtFilter filter = ReservedInstanceBoughtFilter.newBuilder()
                .addAvailabilityZoneId(100L)
                .build();
        final Map<Long, Long> riCountMap = reservedInstanceBoughtStore.getReservedInstanceCountMap(filter);
        final long countForSpecOne = riCountMap.get(88L);

        assertEquals(1, riCountMap.size());
        assertEquals(30L, countForSpecOne);
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
        final ReservedInstanceSpecRecord specRecordTwo = dsl.newRecord(Tables.RESERVED_INSTANCE_SPEC,
                new ReservedInstanceSpecRecord(100L,
                        OfferingClass.STANDARD.getValue(),
                        PaymentOption.ALL_UPFRONT.getValue(),
                        2,
                        Tenancy.HOST.getValue(),
                        OSType.LINUX.getValue(),
                        90L,
                        78L,
                        ReservedInstanceSpecInfo.getDefaultInstance()));
        dsl.batchInsert(Arrays.asList(specRecordOne, specRecordTwo)).execute();
    }
}
