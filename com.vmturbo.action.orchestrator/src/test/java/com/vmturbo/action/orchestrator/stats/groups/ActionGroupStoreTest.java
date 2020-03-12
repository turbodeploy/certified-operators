package com.vmturbo.action.orchestrator.stats.groups;

import static com.vmturbo.action.orchestrator.db.Tables.ACTION_GROUP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore.MatchedActionGroups;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.ActionGroupFilter;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class ActionGroupStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Action.ACTION);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private DSLContext dsl = dbConfig.getDslContext();

    private ActionGroupStore actionGroupStore = new ActionGroupStore(dsl);

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

        final Map<ActionGroupKey, ActionGroup> initialGroups =
            actionGroupStore.ensureExist(Collections.singleton(groupKey));
        final int id = initialGroups.get(groupKey).id();

        // Try to insert the same key again.
        final Map<ActionGroupKey, ActionGroup> actionGroups =
                actionGroupStore.ensureExist(Collections.singleton(groupKey));
        assertThat(actionGroups.get(groupKey).key(), is(groupKey));
        // Retain the initial ID.
        assertThat(actionGroups.get(groupKey).id(), is(id));
    }

    @Test
    public void testQueryActionModeFilter() {
        final ActionGroupKey matching = ImmutableActionGroupKey.builder()
            .actionMode(ActionMode.AUTOMATIC)
            .actionState(ActionState.CLEARED)
            .actionType(ActionType.ACTIVATE)
            .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
            .build();
        final ActionGroupKey otherMatching = ImmutableActionGroupKey.copyOf(matching)
            // Mode is the same, state different.
            .withActionState(ActionState.READY);
        final ActionGroupKey nonMatching = ImmutableActionGroupKey.copyOf(matching)
            .withActionMode(ActionMode.MANUAL);
        final Map<ActionGroupKey, ActionGroup> groupByKey =
            actionGroupStore.ensureExist(Sets.newHashSet(matching, otherMatching, nonMatching));

        final MatchedActionGroups matchedActionGroups =
            // Should find a result.
            actionGroupStore.query(ActionGroupFilter.newBuilder()
                .addActionMode(ActionMode.AUTOMATIC)
                .build()).get();
        assertFalse(matchedActionGroups.allActionGroups());
        final ActionGroup matchingActionGroup = groupByKey.get(matching);
        final ActionGroup otherMatchingActionGroup = groupByKey.get(otherMatching);
        assertThat(matchedActionGroups.specificActionGroupsById().keySet(),
            containsInAnyOrder(matchingActionGroup.id(), otherMatchingActionGroup.id()));
        assertThat(matchedActionGroups.specificActionGroupsById().get(matchingActionGroup.id()),
            is(matchingActionGroup));
        assertThat(matchedActionGroups.specificActionGroupsById().get(otherMatchingActionGroup.id()),
            is(otherMatchingActionGroup));
    }

    @Test
    public void testQueryActionCategoryFilter() {
        final ActionGroupKey matching = ImmutableActionGroupKey.builder()
            .actionMode(ActionMode.AUTOMATIC)
            .actionState(ActionState.CLEARED)
            .actionType(ActionType.ACTIVATE)
            .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
            .build();
        final ActionGroupKey otherMatching = ImmutableActionGroupKey.copyOf(matching)
            // Category is the same, state different.
            .withActionState(ActionState.READY);
        final ActionGroupKey nonMatching = ImmutableActionGroupKey.copyOf(matching)
            .withCategory(ActionCategory.PERFORMANCE_ASSURANCE);
        final Map<ActionGroupKey, ActionGroup> groupByKey =
            actionGroupStore.ensureExist(Sets.newHashSet(matching, otherMatching, nonMatching));

        final MatchedActionGroups matchedActionGroups =
            // Should find a result.
            actionGroupStore.query(ActionGroupFilter.newBuilder()
                .addActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .build()).get();

        assertFalse(matchedActionGroups.allActionGroups());
        final ActionGroup matchingActionGroup = groupByKey.get(matching);
        final ActionGroup otherMatchingActionGroup = groupByKey.get(otherMatching);
        assertThat(matchedActionGroups.specificActionGroupsById().keySet(),
            containsInAnyOrder(matchingActionGroup.id(), otherMatchingActionGroup.id()));
        assertThat(matchedActionGroups.specificActionGroupsById().get(matchingActionGroup.id()),
            is(matchingActionGroup));
        assertThat(matchedActionGroups.specificActionGroupsById().get(otherMatchingActionGroup.id()),
            is(otherMatchingActionGroup));
    }

    @Test
    public void testQueryActionStateFilter() {
        final ActionGroupKey matching = ImmutableActionGroupKey.builder()
            .actionMode(ActionMode.AUTOMATIC)
            .actionState(ActionState.CLEARED)
            .actionType(ActionType.ACTIVATE)
            .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
            .build();
        final ActionGroupKey otherMatching = ImmutableActionGroupKey.copyOf(matching)
            // State is the same, category different.
            .withCategory(ActionCategory.COMPLIANCE);
        final ActionGroupKey nonMatching = ImmutableActionGroupKey.copyOf(matching)
            .withActionState(ActionState.QUEUED);
        final Map<ActionGroupKey, ActionGroup> groupByKey =
            actionGroupStore.ensureExist(Sets.newHashSet(matching, otherMatching, nonMatching));

        final MatchedActionGroups matchedActionGroups =
            // Should find a result.
            actionGroupStore.query(ActionGroupFilter.newBuilder()
                .addActionState(ActionState.CLEARED)
                .build()).get();

        assertFalse(matchedActionGroups.allActionGroups());
        final ActionGroup matchingActionGroup = groupByKey.get(matching);
        final ActionGroup otherMatchingActionGroup = groupByKey.get(otherMatching);
        assertThat(matchedActionGroups.specificActionGroupsById().keySet(),
            containsInAnyOrder(matchingActionGroup.id(), otherMatchingActionGroup.id()));
        assertThat(matchedActionGroups.specificActionGroupsById().get(matchingActionGroup.id()),
            is(matchingActionGroup));
        assertThat(matchedActionGroups.specificActionGroupsById().get(otherMatchingActionGroup.id()),
            is(otherMatchingActionGroup));
    }

    @Test
    public void testQueryActionTypeFilter() {
        final ActionGroupKey matching = ImmutableActionGroupKey.builder()
            .actionMode(ActionMode.AUTOMATIC)
            .actionState(ActionState.CLEARED)
            .actionType(ActionType.ACTIVATE)
            .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
            .build();
        final ActionGroupKey otherMatching = ImmutableActionGroupKey.copyOf(matching)
            // Type is the same, state different.
            .withActionState(ActionState.READY);
        final ActionGroupKey nonMatching = ImmutableActionGroupKey.copyOf(matching)
            .withActionType(ActionType.PROVISION);
        final Map<ActionGroupKey, ActionGroup> groupByKey =
            actionGroupStore.ensureExist(Sets.newHashSet(matching, otherMatching, nonMatching));

        final MatchedActionGroups matchedActionGroups =
            // Should find a result.
            actionGroupStore.query(ActionGroupFilter.newBuilder()
                .addActionType(ActionType.ACTIVATE)
                .build()).get();

        assertFalse(matchedActionGroups.allActionGroups());
        final ActionGroup matchingActionGroup = groupByKey.get(matching);
        final ActionGroup otherMatchingActionGroup = groupByKey.get(otherMatching);
        assertThat(matchedActionGroups.specificActionGroupsById().keySet(),
            containsInAnyOrder(matchingActionGroup.id(), otherMatchingActionGroup.id()));
        assertThat(matchedActionGroups.specificActionGroupsById().get(matchingActionGroup.id()),
            is(matchingActionGroup));
        assertThat(matchedActionGroups.specificActionGroupsById().get(otherMatchingActionGroup.id()),
            is(otherMatchingActionGroup));
    }

    @Test
    public void testQueryAllMatch() {
        final ActionGroupKey matching = ImmutableActionGroupKey.builder()
            .actionMode(ActionMode.AUTOMATIC)
            .actionState(ActionState.CLEARED)
            .actionType(ActionType.ACTIVATE)
            .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
            .build();
        // Every field is different.
        final ActionGroupKey otherMatching = ImmutableActionGroupKey.builder()
            .actionMode(ActionMode.MANUAL)
            .actionState(ActionState.READY)
            .actionType(ActionType.DEACTIVATE)
            .category(ActionCategory.PREVENTION)
            .build();
        final Map<ActionGroupKey, ActionGroup> groupByKey =
            actionGroupStore.ensureExist(Sets.newHashSet(matching, otherMatching));

        final MatchedActionGroups matchedActionGroups =
            // Should find a result.
            actionGroupStore.query(ActionGroupFilter.newBuilder()
                .build()).get();

        assertTrue(matchedActionGroups.allActionGroups());
        final ActionGroup matchingActionGroup = groupByKey.get(matching);
        final ActionGroup otherMatchingActionGroup = groupByKey.get(otherMatching);
        assertThat(matchedActionGroups.specificActionGroupsById().keySet(),
            containsInAnyOrder(matchingActionGroup.id(), otherMatchingActionGroup.id()));
        assertThat(matchedActionGroups.specificActionGroupsById().get(matchingActionGroup.id()),
            is(matchingActionGroup));
        assertThat(matchedActionGroups.specificActionGroupsById().get(otherMatchingActionGroup.id()),
            is(otherMatchingActionGroup));
    }

    @Test
    public void testQueryNoResults() {
        final Optional<MatchedActionGroups> matchedActionGroups =
            actionGroupStore.query(ActionGroupFilter.newBuilder()
                .build());
        assertFalse(matchedActionGroups.isPresent());
    }
}
