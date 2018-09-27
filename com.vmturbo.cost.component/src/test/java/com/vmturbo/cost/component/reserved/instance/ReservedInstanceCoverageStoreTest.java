package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class ReservedInstanceCoverageStoreTest {

    private final static double DELTA = 0.000001;

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private DSLContext dsl;

    private final ServiceEntityReservedInstanceCoverage firstEntity =
            ServiceEntityReservedInstanceCoverage.newBuilder()
                    .setId(123)
                    .setBusinessAccountId(12345L)
                    .setAvailabilityZoneId(100L)
                    .setRegionId(101L)
                    .setTotalCoupons(100)
                    .build();
    private final ServiceEntityReservedInstanceCoverage secondEntity =
            ServiceEntityReservedInstanceCoverage.newBuilder()
                    .setId(124)
                    .setBusinessAccountId(12345L)
                    .setAvailabilityZoneId(100L)
                    .setRegionId(101L)
                    .setTotalCoupons(200)
                    .build();
    private final ServiceEntityReservedInstanceCoverage thirdEntity =
            ServiceEntityReservedInstanceCoverage.newBuilder()
                    .setId(125)
                    .setBusinessAccountId(12345L)
                    .setAvailabilityZoneId(100L)
                    .setRegionId(101L)
                    .setTotalCoupons(50)
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
        reservedInstanceCoverageStore = new ReservedInstanceCoverageStore(dsl,
                entityReservedInstanceMappingStore);
        insertDefaultEntityReservedInstanceMapping();
    }

    @Test
    public void testUpdateReservedInstanceCoverageStore() {
        reservedInstanceCoverageStore.updateReservedInstanceCoverageStore(dsl,
                Arrays.asList(firstEntity, secondEntity, thirdEntity));
        final List<ReservedInstanceCoverageLatestRecord> records =
                dsl.selectFrom(Tables.RESERVED_INSTANCE_COVERAGE_LATEST).fetch();
        assertEquals(3, records.size());
        final Optional<ReservedInstanceCoverageLatestRecord> firstRecord = records.stream()
                .filter(ri -> ri.getEntityId().equals(123L))
                .findFirst();
        final Optional<ReservedInstanceCoverageLatestRecord> secondRecord = records.stream()
                .filter(ri -> ri.getEntityId().equals(124L))
                .findFirst();
        final Optional<ReservedInstanceCoverageLatestRecord> thirdRecord = records.stream()
                .filter(ri -> ri.getEntityId().equals(125L))
                .findFirst();
        assertTrue(firstRecord.isPresent());
        assertTrue(secondRecord.isPresent());
        assertTrue(thirdRecord.isPresent());
        assertEquals(100.0, firstRecord.get().getTotalCoupons(), DELTA);
        assertEquals(30.0, firstRecord.get().getUsedCoupons(), DELTA);
        assertEquals(200.0, secondRecord.get().getTotalCoupons(), DELTA);
        assertEquals(80.0, secondRecord.get().getUsedCoupons(), DELTA);
        assertEquals(50.0, thirdRecord.get().getTotalCoupons(), DELTA);
        assertEquals(0.0, thirdRecord.get().getUsedCoupons(), DELTA);
    }

    @Test
    public void testGetReservedInstanceCoverageStatsRecords() {
        reservedInstanceCoverageStore.updateReservedInstanceCoverageStore(dsl,
                Arrays.asList(firstEntity, secondEntity, thirdEntity));
        final ReservedInstanceCoverageFilter filer = ReservedInstanceCoverageFilter.newBuilder().build();
        final List<ReservedInstanceStatsRecord> riStatsRecords =
                reservedInstanceCoverageStore.getReservedInstanceCoverageStatsRecords(filer);
        assertEquals(1L, riStatsRecords.size());
        final ReservedInstanceStatsRecord riStatsRecord = riStatsRecords.get(0);
        assertEquals(200L, riStatsRecord.getCapacity().getMax(), DELTA);
        assertEquals(50L, riStatsRecord.getCapacity().getMin(), DELTA);
        assertEquals(350L, riStatsRecord.getCapacity().getTotal(), DELTA);
        assertEquals(80L, riStatsRecord.getValues().getMax(), DELTA);
        assertEquals(0L, riStatsRecord.getValues().getMin(), DELTA);
        assertEquals(110L, riStatsRecord.getValues().getTotal(), DELTA);
    }

    private void insertDefaultEntityReservedInstanceMapping() {
        final LocalDateTime curTime = LocalDateTime.now(ZoneOffset.UTC);
        final EntityToReservedInstanceMappingRecord firstRecord =
                dsl.newRecord(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING,
                        new EntityToReservedInstanceMappingRecord(curTime, 123L,
                                11L, 10.0));
        final EntityToReservedInstanceMappingRecord secondRecord =
                dsl.newRecord(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING,
                        new EntityToReservedInstanceMappingRecord(curTime, 123L,
                                12L, 20.0));
        final EntityToReservedInstanceMappingRecord thirdRecord =
                dsl.newRecord(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING,
                        new EntityToReservedInstanceMappingRecord(curTime, 124L,
                                11L, 30.0));
        final EntityToReservedInstanceMappingRecord fourthRecord =
                dsl.newRecord(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING,
                        new EntityToReservedInstanceMappingRecord(curTime, 124L,
                                13L, 50.0));
        dsl.batchInsert(Arrays.asList(firstRecord, secondRecord, thirdRecord, fourthRecord)).execute();
    }
}
