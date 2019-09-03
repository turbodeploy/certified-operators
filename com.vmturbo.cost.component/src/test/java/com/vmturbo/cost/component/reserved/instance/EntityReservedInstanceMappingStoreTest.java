package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
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
public class EntityReservedInstanceMappingStoreTest {

    private final static double DELTA = 0.000001;

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private DSLContext dsl;

    final EntityRICoverageUpload coverageOne = EntityRICoverageUpload.newBuilder()
            .setEntityId(123L)
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testOne")
                    .setCoveredCoupons(10)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testTwo")
                    .setCoveredCoupons(20)
                    .setRiCoverageSource(Coverage.RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION))
            .build();

    final EntityRICoverageUpload coverageTwo = EntityRICoverageUpload.newBuilder()
            .setEntityId(124L)
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testOne")
                    .setCoveredCoupons(30)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testThree")
                    .setCoveredCoupons(40)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .build();

    final EntityRICoverageUpload coverageThree = EntityRICoverageUpload.newBuilder()
            .setEntityId(125L)
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testFour")
                    .setCoveredCoupons(10)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .addCoverage(Coverage.newBuilder()
                    .setProbeReservedInstanceId("testFour")
                    .setCoveredCoupons(30)
                    .setRiCoverageSource(Coverage.RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION))
            .build();

    final ReservedInstanceBoughtInfo riInfoOne = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(123L)
            .setProbeReservedInstanceId("testOne")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(100L)
            .setNumBought(10)
            .build();

    final ReservedInstanceBoughtInfo riInfoTwo = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(456)
            .setProbeReservedInstanceId("testTwo")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(100L)
            .setNumBought(20)
            .build();

    final ReservedInstanceBoughtInfo riInfoThree = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(789)
            .setProbeReservedInstanceId("testThree")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(50L)
            .setNumBought(30)
            .build();

    final ReservedInstanceBoughtInfo riInfoFour = ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(125)
            .setProbeReservedInstanceId("testFour")
            .setReservedInstanceSpec(99L)
            .setAvailabilityZoneId(50L)
            .setNumBought(50)
            .build();

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        reservedInstanceBoughtStore = new ReservedInstanceBoughtStore(dsl,
                new IdentityProvider(0));
        entityReservedInstanceMappingStore = new EntityReservedInstanceMappingStore(dsl,
                reservedInstanceBoughtStore);
        insertDefaultReservedInstanceSpec();
    }

    @Test
    public void testUpdateAndGetRIMapping() {
        final List<ReservedInstanceBoughtInfo> reservedInstancesBoughtInfo =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree, riInfoFour);
        final List<EntityRICoverageUpload> entityCoverageLists =
                Arrays.asList(coverageOne, coverageTwo, coverageThree);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstancesBoughtInfo);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl, entityCoverageLists);

        final Map<String, Long> riProbeIdMap = dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT).fetch()
                .stream()
                .collect(Collectors.toMap(ReservedInstanceBoughtRecord::getProbeReservedInstanceId,
                        ReservedInstanceBoughtRecord::getId));
        final long ri1Id = riProbeIdMap.get(riInfoOne.getProbeReservedInstanceId());
        final long ri2Id = riProbeIdMap.get(riInfoTwo.getProbeReservedInstanceId());
        final long ri3Id = riProbeIdMap.get(riInfoThree.getProbeReservedInstanceId());
        final long ri4Id = riProbeIdMap.get(riInfoFour.getProbeReservedInstanceId());

        final Map<Long, EntityReservedInstanceCoverage> coverageMap =
                entityReservedInstanceMappingStore.getEntityRiCoverage();
        final EntityReservedInstanceCoverage retCvg1 = coverageMap.get(coverageOne.getEntityId());
        assertThat(retCvg1.getEntityId(), is(coverageOne.getEntityId()));
        assertThat(retCvg1.getCouponsCoveredByRiMap(),
                is(ImmutableMap.of(ri1Id, 10.0, ri2Id, 20.0)));

        final EntityReservedInstanceCoverage retCvg2 = coverageMap.get(coverageTwo.getEntityId());
        assertThat(retCvg2.getCouponsCoveredByRiMap(),
                is(ImmutableMap.of(ri1Id, 30.0, ri3Id, 40.0)));

        final EntityReservedInstanceCoverage retCvg3 = coverageMap.get(coverageThree.getEntityId());
        assertThat(retCvg3.getCouponsCoveredByRiMap(),
                is(ImmutableMap.of(ri4Id, 40.0)));
    }

    @Test
    public void testGetRiMappingEmpty() {
        assertThat(entityReservedInstanceMappingStore.getEntityRiCoverage(),
                is(Collections.emptyMap()));
    }

    @Test
    public void testUpdateEntityReservedInstanceMapping() {
        final List<ReservedInstanceBoughtInfo> reservedInstancesBoughtInfo =
                Arrays.asList(riInfoOne, riInfoTwo, riInfoThree);
        final List<EntityRICoverageUpload> entityCoverageLists =
                Arrays.asList(coverageOne, coverageTwo);
        reservedInstanceBoughtStore.updateReservedInstanceBought(dsl, reservedInstancesBoughtInfo);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl, entityCoverageLists);
        List<EntityToReservedInstanceMappingRecord> records =
                dsl.selectFrom(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING).fetch();
        List<ReservedInstanceBoughtRecord> riBought = dsl.selectFrom(Tables.RESERVED_INSTANCE_BOUGHT).fetch();
        Map<String, Long> riProbeIdMap = riBought.stream()
                .collect(Collectors.toMap(ReservedInstanceBoughtRecord::getProbeReservedInstanceId,
                        ReservedInstanceBoughtRecord::getId));
        assertEquals(4, records.size());
        assertEquals(10.0, records.stream()
                .filter(record -> record.getEntityId().equals(123L)
                        && record.getReservedInstanceId().equals(riProbeIdMap.get("testOne")))
                .map(EntityToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
        assertEquals(20.0, records.stream()
                .filter(record -> record.getEntityId().equals(123L)
                        && record.getReservedInstanceId().equals(riProbeIdMap.get("testTwo")))
                .map(EntityToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
        assertEquals(30.0, records.stream()
                .filter(record -> record.getEntityId().equals(124L)
                        && record.getReservedInstanceId().equals(riProbeIdMap.get("testOne")))
                .map(EntityToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
        assertEquals(40.0, records.stream()
                .filter(record -> record.getEntityId().equals(124L)
                        && record.getReservedInstanceId().equals(riProbeIdMap.get("testThree")))
                .map(EntityToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);

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
