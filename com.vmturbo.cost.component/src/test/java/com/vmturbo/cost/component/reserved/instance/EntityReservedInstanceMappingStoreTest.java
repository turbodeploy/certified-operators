package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.EntityToReservedInstanceMappingRecord;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit tests for {@link EntityReservedInstanceMappingStore}. //
 */
@RunWith(Parameterized.class)
public class EntityReservedInstanceMappingStoreTest extends MultiDbTestBase {
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
    public EntityReservedInstanceMappingStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost", TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    private static final double DELTA = 0.000001;

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    /**
     * Set up before each test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        entityReservedInstanceMappingStore
                = new EntityReservedInstanceMappingStore(dsl);
    }

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

    /**
     * Test for {@link EntityReservedInstanceMappingStore#getEntitiesCoveredByReservedInstances(Collection)}.
     */
    @Test
    public void testGetEntitiesCoveredByReservedInstances() {
        final List<EntityRICoverageUpload> entityCoverageLists = Arrays.asList(coverageOne,
                coverageTwo, coverageThree);
        entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(dsl,
                entityCoverageLists);
        ImmutableMap.of(Collections.singleton(457L),
                Collections.singletonMap(457L, ImmutableSet.of(123L, 124L)),
                Collections.<Long>emptyList(),
                ImmutableMap.of(456L, Collections.singleton(123L), 457L,
                        ImmutableSet.of(123L, 124L), 458L, Collections.singleton(124L), 459L,
                        Collections.singleton(125L))).forEach(
                (reservedInstances, reservedInstanceToCoveredEntities) -> Assert.assertEquals(
                        reservedInstanceToCoveredEntities,
                        entityReservedInstanceMappingStore.getEntitiesCoveredByReservedInstances(
                                reservedInstances)));
    }
}
