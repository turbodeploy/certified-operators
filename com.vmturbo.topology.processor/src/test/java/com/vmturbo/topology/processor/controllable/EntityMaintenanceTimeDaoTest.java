package com.vmturbo.topology.processor.controllable;

import static com.vmturbo.topology.processor.db.Tables.ENTITY_MAINTENANCE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.AutomationLevel;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;
import com.vmturbo.topology.processor.TestTopologyProcessorDbEndpointConfig;
import com.vmturbo.topology.processor.db.TopologyProcessor;
import com.vmturbo.topology.processor.db.tables.records.EntityMaintenanceRecord;

/**
 * Test {@link EntityMaintenanceTimeDao}.
 */
@RunWith(Parameterized.class)
public class EntityMaintenanceTimeDaoTest extends MultiDbTestBase {

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
     * Create an instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public EntityMaintenanceTimeDaoTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(TopologyProcessor.TOPOLOGY_PROCESSOR, configurableDbDialect, dialect,
                "topology-processor", TestTopologyProcessorDbEndpointConfig::tpEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage DB provisioining and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private Clock clock = mock(Clock.class);

    private EntityMaintenanceTimeDao entityMaintenanceTimeDao;

    /**
     * Set up before each test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        entityMaintenanceTimeDao = new EntityMaintenanceTimeDao(dsl, 30 * 60, clock, true);
    }

    /**
     * Test behavior of host exiting maintenance mode. See annotations for more detail.
     */
    @Test
    public void testEnterThenExitMaintenance() {
        final long oid1 = 1L;
        final long oid2 = 2L;

        // Two hosts enter maintenance mode
        // Records in db after state changes: (oid1, null), (oid2, null)
        EntitiesWithNewState.Builder builder = EntitiesWithNewState.newBuilder()
            .addTopologyEntity(TopologyEntityDTO.newBuilder()
                .setOid(oid1)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.MAINTENANCE)
                .build())
            .addTopologyEntity(TopologyEntityDTO.newBuilder()
                .setOid(oid2)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.MAINTENANCE)
                .build());
        entityMaintenanceTimeDao.onEntitiesWithNewState(builder.build());
        Result<EntityMaintenanceRecord> records = dsl.selectFrom(ENTITY_MAINTENANCE).fetch();
        assertThat(records.size(), is(2));
        assertThat(records.stream().map(EntityMaintenanceRecord::getEntityOid).collect(Collectors.toList()),
            containsInAnyOrder(oid1, oid2));
        assertThat(records.get(0).getExitTime(), is(nullValue()));
        assertThat(records.get(1).getExitTime(), is(nullValue()));

        // host with oid1 exits maintenance mode
        // Records in db after state changes: (oid1, timestamp), (oid2, null)
        final Instant timestamp = Instant.parse("2021-04-23T09:30:31Z");
        when(clock.instant()).thenReturn(timestamp);
        when(clock.getZone()).thenReturn(ZoneOffset.UTC);
        builder = EntitiesWithNewState.newBuilder()
            .addTopologyEntity(TopologyEntityDTO.newBuilder()
                .setOid(oid1)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
                    PhysicalMachineInfo.newBuilder().setAutomationLevel(AutomationLevel.FULLY_AUTOMATED)))
                .build());
        entityMaintenanceTimeDao.onEntitiesWithNewState(builder.build());
        records = dsl.selectFrom(ENTITY_MAINTENANCE).fetch();
        assertThat(records.size(), is(2));
        assertThat(records.stream().map(EntityMaintenanceRecord::getEntityOid).collect(Collectors.toList()),
            containsInAnyOrder(oid1, oid2));
        records.forEach(record -> {
            if (record.getEntityOid() == oid1) {
                assertThat(record.getExitTime(), is(LocalDateTime.now(clock)));
            } else {
                assertThat(record.getExitTime(), is(nullValue()));
            }
        });

        // After 20 mins, host with oid1 should be controllable false
        // Records in db after state changes: (oid1, timestamp), (oid2, null)
        final Instant afterTwentyMins = Instant.parse("2021-04-23T09:50:31Z");
        when(clock.instant()).thenReturn(afterTwentyMins);
        Set<Long> oids = entityMaintenanceTimeDao.getControllableFalseHost();
        assertThat(oids.size(), is(1));
        assertThat(oids.iterator().next(), is(oid1));
        records = dsl.selectFrom(ENTITY_MAINTENANCE).where(ENTITY_MAINTENANCE.ENTITY_OID.eq(oid1)).fetch();
        assertThat(records.size(), is(1));
        assertThat(records.get(0).getExitTime().toInstant(ZoneOffset.UTC), is(timestamp));

        // After 25 mins, host with oid1 enter FAILOVER state, it should still be controllable false
        // Records in db after state changes: (oid1, timestamp), (oid2, null)
        final Instant afterTwentyFiveMins = Instant.parse("2021-04-23T09:55:31Z");
        when(clock.instant()).thenReturn(afterTwentyFiveMins);
        builder = EntitiesWithNewState.newBuilder()
            .addTopologyEntity(TopologyEntityDTO.newBuilder()
                .setOid(oid1)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.FAILOVER)
                .build());
        entityMaintenanceTimeDao.onEntitiesWithNewState(builder.build());
        oids = entityMaintenanceTimeDao.getControllableFalseHost();
        assertThat(oids.size(), is(1));
        assertThat(oids.iterator().next(), is(oid1));
        records = dsl.selectFrom(ENTITY_MAINTENANCE).where(ENTITY_MAINTENANCE.ENTITY_OID.eq(oid1)).fetch();
        assertThat(records.size(), is(1));
        assertThat(records.get(0).getExitTime().toInstant(ZoneOffset.UTC), is(timestamp));

        // After 40 mins, record with oid1 should be deleted.
        // host with oid2 exits maintenance mode
        // Records in db after state changes: (oid2, afterFortyMins)
        final Instant afterFortyMins = Instant.parse("2021-04-23T10:10:31Z");
        when(clock.instant()).thenReturn(afterFortyMins);
        builder = EntitiesWithNewState.newBuilder()
            .addTopologyEntity(TopologyEntityDTO.newBuilder()
                .setOid(oid2)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
                    PhysicalMachineInfo.newBuilder().setAutomationLevel(AutomationLevel.FULLY_AUTOMATED)))
                .build());
        entityMaintenanceTimeDao.onEntitiesWithNewState(builder.build());
        oids = entityMaintenanceTimeDao.getControllableFalseHost();
        assertThat(oids.size(), is(1));
        assertThat(oids.iterator().next(), is(oid2));
        records = dsl.selectFrom(ENTITY_MAINTENANCE).fetch();
        assertThat(records.size(), is(1));
        assertThat(records.get(0).getEntityOid(), is(oid2));
        assertThat(records.get(0).getExitTime().toInstant(ZoneOffset.UTC), is(afterFortyMins));

        // After 50 mins, host with oid2 enters maintenance mode again
        // // Records in db after state changes: (oid2, null)
        final Instant afterFiftyMins = Instant.parse("2021-04-23T10:20:31Z");
        when(clock.instant()).thenReturn(afterFiftyMins);
        builder = EntitiesWithNewState.newBuilder()
            .addTopologyEntity(TopologyEntityDTO.newBuilder()
                .setOid(oid2)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.MAINTENANCE)
                .build());
        entityMaintenanceTimeDao.onEntitiesWithNewState(builder.build());
        oids = entityMaintenanceTimeDao.getControllableFalseHost();
        assertThat(oids.size(), is(0));
        records = dsl.selectFrom(ENTITY_MAINTENANCE).fetch();
        assertThat(records.size(), is(1));
        assertThat(records.get(0).getEntityOid(), is(oid2));
        assertThat(records.get(0).getExitTime(), is(nullValue()));
    }
}
