package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceUtilizationLatestRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
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
public class ReservedInstanceUtilizationStoreTest {

    private final static double DELTA = 0.000001;

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private DSLContext dsl;

    final EntityReservedInstanceCoverage coverageOne = EntityReservedInstanceCoverage.newBuilder()
            .setEntityId(123L)
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testOne")
                    .setCoveredCoupons(10))
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testTwo")
                    .setCoveredCoupons(20))
            .build();

    final EntityReservedInstanceCoverage coverageTwo = EntityReservedInstanceCoverage.newBuilder()
            .setEntityId(124L)
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testOne")
                    .setCoveredCoupons(30))
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testThree")
                    .setCoveredCoupons(50))
            .build();

    final ReservedInstanceBoughtInfo riInfoOne = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(123L)
            .setProbeReservedInstanceId("testOne")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(100L)
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                    .setNumberOfCoupons(100))
            .setNumBought(10)
            .build();

    final ReservedInstanceBoughtInfo riInfoTwo = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(456)
            .setProbeReservedInstanceId("testTwo")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(100L)
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                    .setNumberOfCoupons(100))
            .setNumBought(20)
            .build();

    final ReservedInstanceBoughtInfo riInfoThree = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(789)
            .setProbeReservedInstanceId("testThree")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(50L)
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                    .setNumberOfCoupons(100))
            .setNumBought(30)
            .build();

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        reservedInstanceBoughtStore = new ReservedInstanceBoughtStore(dsl,
                new IdentityProvider(0));
        reservedInstanceSpecStore = new ReservedInstanceSpecStore(dsl,
                new IdentityProvider(0));
        entityReservedInstanceMappingStore = new EntityReservedInstanceMappingStore(dsl,
                reservedInstanceBoughtStore);
        reservedInstanceUtilizationStore = new ReservedInstanceUtilizationStore(dsl, reservedInstanceBoughtStore,
                reservedInstanceSpecStore, entityReservedInstanceMappingStore);
        insertDefaultReservedInstanceSpec();
    }

    @Test
    public void testUpdateReservedInstanceUtilization() {
        final List<ReservedInstanceBoughtInfo> reservedInstancesBoughtInfo =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree);
        final List<EntityReservedInstanceCoverage> entityCoverageLists =
                Arrays.asList(coverageOne, coverageTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstancesBoughtInfo);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl, entityCoverageLists);
        reservedInstanceUtilizationStore.updateReservedInstanceUtilization(dsl);
        List<ReservedInstanceUtilizationLatestRecord> records =
                dsl.selectFrom(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).fetch();
        List<ReservedInstanceBoughtRecord> riBought = dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT).fetch();
        Map<String, Long> riProbeIdMap = riBought.stream()
                .collect(Collectors.toMap(ReservedInstanceBoughtRecord::getProbeReservedInstanceId,
                        ReservedInstanceBoughtRecord::getId));
        assertEquals(3L, records.size());
        final Optional<ReservedInstanceUtilizationLatestRecord> firstRI =
                records.stream()
                        .filter(record -> record.getId().equals(riProbeIdMap.get("testOne")))
                        .findFirst();
        final Optional<ReservedInstanceUtilizationLatestRecord> secondRI =
                records.stream()
                        .filter(record -> record.getId().equals(riProbeIdMap.get("testTwo")))
                        .findFirst();
        final Optional<ReservedInstanceUtilizationLatestRecord> thirdRI =
                records.stream()
                        .filter(record -> record.getId().equals(riProbeIdMap.get("testThree")))
                        .findFirst();
        assertTrue(firstRI.isPresent());
        assertTrue(secondRI.isPresent());
        assertTrue(thirdRI.isPresent());
        assertEquals(100.0, firstRI.get().getTotalCoupons(), DELTA);
        assertEquals(40.0, firstRI.get().getUsedCoupons(), DELTA);
        assertEquals(100.0, secondRI.get().getTotalCoupons(), DELTA);
        assertEquals(20.0, secondRI.get().getUsedCoupons(), DELTA);
        assertEquals(100.0, thirdRI.get().getTotalCoupons(), DELTA);
        assertEquals(50.0, thirdRI.get().getUsedCoupons(), DELTA);
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
                        ReservedInstanceSpecInfo.newBuilder().setRegionId(77L).build()));
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
