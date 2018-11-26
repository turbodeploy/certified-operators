package com.vmturbo.action.orchestrator.stats.groups;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Map;

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

import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=action"})
public class MgmtUnitSubgroupStoreTest {
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private DSLContext dsl;

    private MgmtUnitSubgroupStore mgmtUnitSubgroupStore;

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();

        mgmtUnitSubgroupStore = new MgmtUnitSubgroupStore(dsl);
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testUpsert() {
        final MgmtUnitSubgroupKey key = ImmutableMgmtUnitSubgroupKey.builder()
                .entityType(1)
                .environmentType(EnvironmentType.CLOUD)
                .mgmtUnitId(123)
                .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
                mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key));
        assertThat(subgroups.get(key).key(), is(key));
        assertThat(subgroups.get(key).id(), is(1));
    }

    @Test
    public void testUpsetrUnsetOptionals() {
        final MgmtUnitSubgroupKey key = ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(123)
                .build();
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
                mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key));
        assertThat(subgroups.get(key).key(), is(key));
        assertThat(subgroups.get(key).id(), is(1));
    }

    @Test
    public void testUpsertRetainExisting() {
        final MgmtUnitSubgroupKey key1 = ImmutableMgmtUnitSubgroupKey.builder()
                .entityType(1)
                .environmentType(EnvironmentType.CLOUD)
                .mgmtUnitId(123)
                .build();
        mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key1));

        final MgmtUnitSubgroupKey key2 = ImmutableMgmtUnitSubgroupKey.builder()
                .entityType(2)
                .environmentType(EnvironmentType.ON_PREM)
                .mgmtUnitId(432)
                .build();
        // Insert the same one again.
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
                mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key2));
        assertThat(subgroups.get(key2).key(), is(key2));
        // ID should be the original ID.
        assertThat(subgroups.get(key2).id(), is(2));
    }

    @Test
    public void testUpsertDuplicate() {
        final MgmtUnitSubgroupKey key = ImmutableMgmtUnitSubgroupKey.builder()
                .entityType(1)
                .environmentType(EnvironmentType.CLOUD)
                .mgmtUnitId(123)
                .build();
        mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key));
        // Insert the same one again.
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> subgroups =
                mgmtUnitSubgroupStore.ensureExist(Collections.singleton(key));
        assertThat(subgroups.get(key).key(), is(key));
        // ID should be the original ID.
        assertThat(subgroups.get(key).id(), is(1));
    }
}
