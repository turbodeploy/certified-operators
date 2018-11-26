package com.vmturbo.action.orchestrator.stats.groups;

import static com.vmturbo.action.orchestrator.db.Tables.ACTION_GROUP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

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

import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=action"})
public class ActionGroupStoreTest {
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private DSLContext dsl;

    private ActionGroupStore actionGroupStore;

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();

        actionGroupStore = new ActionGroupStore(dsl);
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testUpsert() {
        final ActionGroupKey groupKey = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.AUTOMATIC)
                .actionState(ActionState.CLEARED)
                .actionType(ActionType.ACTIVATE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .build();

        final Map<ActionGroupKey, ActionGroup> actionGroups =
                actionGroupStore.ensureExist(Collections.singleton(groupKey));
        assertThat(actionGroups.get(groupKey).key(), is(groupKey));
        assertThat(actionGroups.get(groupKey).id(), is(1));
    }

    @Test
    public void testUpsertRetainExisting() {
        final ActionGroupKey group1Key = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.AUTOMATIC)
                .actionState(ActionState.CLEARED)
                .actionType(ActionType.ACTIVATE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .build();

        actionGroupStore.ensureExist(Collections.singleton(group1Key));

        final ActionGroupKey group2Key = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.MANUAL)
                .actionState(ActionState.QUEUED)
                .actionType(ActionType.MOVE)
                .category(ActionCategory.PERFORMANCE_ASSURANCE)
                .build();

        final Map<ActionGroupKey, ActionGroup> actionGroups =
            actionGroupStore.ensureExist(Collections.singleton(group2Key));
        assertThat(actionGroups.get(group2Key).key(), is(group2Key));
        assertThat(actionGroups.get(group2Key).id(), is(2));

        final Set<Integer> ids =
                dsl.select(ACTION_GROUP.ID).from(ACTION_GROUP).fetchSet(ACTION_GROUP.ID);
        assertThat(ids, containsInAnyOrder(1, 2));
    }

    @Test
    public void testUpsertDuplicate() {
        final ActionGroupKey groupKey = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.AUTOMATIC)
                .actionState(ActionState.CLEARED)
                .actionType(ActionType.ACTIVATE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .build();

        actionGroupStore.ensureExist(Collections.singleton(groupKey));
        // Try to insert the same key again.
        final Map<ActionGroupKey, ActionGroup> actionGroups =
                actionGroupStore.ensureExist(Collections.singleton(groupKey));
        assertThat(actionGroups.get(groupKey).key(), is(groupKey));
        // Retain the initial ID.
        assertThat(actionGroups.get(groupKey).id(), is(1));
    }
}
