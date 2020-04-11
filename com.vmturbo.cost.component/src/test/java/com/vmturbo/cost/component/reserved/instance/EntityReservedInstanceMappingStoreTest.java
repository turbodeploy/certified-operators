package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityToReservedInstanceMappingRecord;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit tests for {@link EntityReservedInstanceMappingStore}.
 */
public class EntityReservedInstanceMappingStoreTest {

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static final double DELTA = 0.000001;

    private DSLContext dsl = dbConfig.getDslContext();

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore
        = new EntityReservedInstanceMappingStore(dsl);

    final EntityRICoverageUpload coverageOne = EntityRICoverageUpload.newBuilder()
            .setEntityId(123L)
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(456L)
                    .setCoveredCoupons(10)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(457L)
                    .setCoveredCoupons(20)
                    .setRiCoverageSource(Coverage.RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION))
            .build();

    final EntityRICoverageUpload coverageTwo = EntityRICoverageUpload.newBuilder()
            .setEntityId(124L)
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(457L)
                    .setCoveredCoupons(30)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(458L)
                    .setCoveredCoupons(40)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .build();

    final EntityRICoverageUpload coverageThree = EntityRICoverageUpload.newBuilder()
            .setEntityId(125L)
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(459L)
                    .setCoveredCoupons(10)
                    .setRiCoverageSource(Coverage.RICoverageSource.BILLING))
            .addCoverage(Coverage.newBuilder()
                    .setReservedInstanceId(459L)
                    .setCoveredCoupons(30)
                    .setRiCoverageSource(Coverage.RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION))
            .build();

    @Test
    public void testUpdateAndGetRIMapping() {
        final List<EntityRICoverageUpload> entityCoverageLists =
                Arrays.asList(coverageOne, coverageTwo, coverageThree);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl, entityCoverageLists);

        final Map<Long, EntityReservedInstanceCoverage> coverageMap =
                entityReservedInstanceMappingStore.getEntityRiCoverage();
        final EntityReservedInstanceCoverage retCvg1 = coverageMap.get(coverageOne.getEntityId());
        assertThat(retCvg1.getEntityId(), is(coverageOne.getEntityId()));
        assertThat(retCvg1.getCouponsCoveredByRiMap(),
                is(ImmutableMap.of(456L, 10.0, 457L, 20.0)));

        final EntityReservedInstanceCoverage retCvg2 = coverageMap.get(coverageTwo.getEntityId());
        assertThat(retCvg2.getCouponsCoveredByRiMap(),
                is(ImmutableMap.of(457L, 30.0, 458L, 40.0)));

        final EntityReservedInstanceCoverage retCvg3 = coverageMap.get(coverageThree.getEntityId());
        assertThat(retCvg3.getCouponsCoveredByRiMap(),
                is(ImmutableMap.of(459L, 40.0)));
    }

    @Test
    public void testGetRiMappingEmpty() {
        assertThat(entityReservedInstanceMappingStore.getEntityRiCoverage(),
                is(Collections.emptyMap()));
    }

    @Test
    public void testUpdateEntityReservedInstanceMapping() {
        final List<EntityRICoverageUpload> entityCoverageLists =
                Arrays.asList(coverageOne, coverageTwo);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl, entityCoverageLists);
        List<EntityToReservedInstanceMappingRecord> records =
                dsl.selectFrom(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING).fetch();

        assertEquals(4, records.size());
        assertEquals(10.0, records.stream()
                .filter(record -> record.getEntityId().equals(123L)
                        && record.getReservedInstanceId().equals(456L))
                .map(EntityToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
        assertEquals(20.0, records.stream()
                .filter(record -> record.getEntityId().equals(123L)
                        && record.getReservedInstanceId().equals(457L))
                .map(EntityToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
        assertEquals(30.0, records.stream()
                .filter(record -> record.getEntityId().equals(124L)
                        && record.getReservedInstanceId().equals(457L))
                .map(EntityToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);
        assertEquals(40.0, records.stream()
                .filter(record -> record.getEntityId().equals(124L)
                        && record.getReservedInstanceId().equals(458L))
                .map(EntityToReservedInstanceMappingRecord::getUsedCoupons)
                .findFirst()
                .orElse(0.0), DELTA);

    }

    @Test
    public void testGetRICoverageByEntity() {

        /*
        Setup store
         */
        final List<EntityRICoverageUpload> entityCoverageLists =
                Arrays.asList(coverageOne, coverageTwo, coverageThree);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl, entityCoverageLists);

        /*
        Invoke SUT
         */
        final Map<Long, Set<Coverage>> actualRICoverageByEntity =
                entityReservedInstanceMappingStore.getRICoverageByEntity();

        final Map<Long, Set<Coverage>> expectedRICoverageByEntity = ImmutableMap.of(
                coverageOne.getEntityId(), ImmutableSet.copyOf(coverageOne.getCoverageList()),
                coverageTwo.getEntityId(), ImmutableSet.copyOf(coverageTwo.getCoverageList()),
                coverageThree.getEntityId(), ImmutableSet.copyOf(coverageThree.getCoverageList()));

        assertThat(actualRICoverageByEntity, equalTo(expectedRICoverageByEntity));
    }

}
