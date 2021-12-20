package com.vmturbo.action.orchestrator.stats.groups;

import static com.vmturbo.action.orchestrator.db.Tables.ACTION_GROUP;
import static com.vmturbo.action.orchestrator.db.Tables.RELATED_RISK_DESCRIPTION;
import static com.vmturbo.action.orchestrator.db.Tables.RELATED_RISK_FOR_ACTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jooq.DSLContext;
import org.jooq.Record4;
import org.jooq.Result;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionGroupRecord;
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

    private static final String MEM_CONGESTION = "Mem congestion";
    private static final String MEM_CONGESTION_CHK = DigestUtils.md5Hex(MEM_CONGESTION);
    private static final String CPU_CONGESTION = "CPU congestion";
    private static final String CPU_CONGESTION_CHK = DigestUtils.md5Hex(CPU_CONGESTION);
    private static final String COMBINED_CHK = DigestUtils.md5Hex(
            Stream.of(MEM_CONGESTION, CPU_CONGESTION).sorted()
                .collect(Collectors.joining(",")));

    @Test
    public void testUpsert() {
        final ActionGroupKey groupKey = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.AUTOMATIC)
                .actionState(ActionState.CLEARED)
                .actionType(ActionType.ACTIVATE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .addActionRelatedRisk(MEM_CONGESTION)
                .addActionRelatedRisk(CPU_CONGESTION)
                .build();

        final Map<ActionGroupKey, ActionGroup> actionGroups =
                actionGroupStore.ensureExist(Collections.singleton(groupKey));
        assertThat(actionGroups.get(groupKey).key(), is(groupKey));

        validateRiskSets(actionGroups.values());
    }

    @Test
    public void testUpsertRiskSet() {
        final ActionGroupKey group1Key = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.AUTOMATIC)
                .actionState(ActionState.CLEARED)
                .actionType(ActionType.ACTIVATE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .addActionRelatedRisk(MEM_CONGESTION)
                .build();

        final ActionGroupKey group2Key = ImmutableActionGroupKey.builder().from(group1Key)
                .addActionRelatedRisk(CPU_CONGESTION)
                .build();

        final Map<ActionGroupKey, ActionGroup> actionGroups =
                actionGroupStore.ensureExist(Sets.newHashSet(group1Key, group2Key));
        assertThat(actionGroups.keySet(), containsInAnyOrder(group1Key, group2Key));

        validateRiskSets(actionGroups.values());
    }

    @Test
    public void testUpsertRetainExisting() {
        final ActionGroupKey group1Key = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.AUTOMATIC)
                .actionState(ActionState.CLEARED)
                .actionType(ActionType.ACTIVATE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .addActionRelatedRisk(MEM_CONGESTION)
                .build();

        Map<ActionGroupKey, ActionGroup> g1 = actionGroupStore.ensureExist(Collections.singleton(group1Key));

        final ActionGroupKey group2Key = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.MANUAL)
                .actionState(ActionState.QUEUED)
                .actionType(ActionType.MOVE)
                .category(ActionCategory.PERFORMANCE_ASSURANCE)
                .addActionRelatedRisk(CPU_CONGESTION)
                .build();

        final Map<ActionGroupKey, ActionGroup> actionGroups =
            actionGroupStore.ensureExist(Collections.singleton(group2Key));
        assertThat(actionGroups.get(group2Key).key(), is(group2Key));
        assertThat(actionGroups.get(group2Key).id(), is(2));

        final Set<Integer> ids =
                dsl.select(ACTION_GROUP.ID).from(ACTION_GROUP).fetchSet(ACTION_GROUP.ID);
        assertThat(ids, containsInAnyOrder(1, 2));

        validateRiskSets(CollectionUtils.union(g1.values(), actionGroups.values()));
    }

    private void validateRiskSets(Collection<ActionGroup> actionGroups) {
        Map<Integer, Pair<Set<String>, String>> riskSetsByActionGroup = getRiskSetsByAGId();
        assertThat(riskSetsByActionGroup.size(), is(actionGroups.size()));
        actionGroups.forEach(ag -> {
            Pair<Set<String>, String> risksAndChecksum = riskSetsByActionGroup.get(ag.id());
            assertThat(risksAndChecksum.getKey(), is(ag.key().getActionRelatedRisk()));
            assertThat(risksAndChecksum.getValue(), is(DigestUtils.md5Hex(risksAndChecksum.getKey().stream()
                .sorted()
                .collect(Collectors.joining(",")))));
        });
    }

    @Test
    public void testUpsertDuplicate() {
        final ActionGroupKey groupKey = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.AUTOMATIC)
                .actionState(ActionState.CLEARED)
                .actionType(ActionType.ACTIVATE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .addActionRelatedRisk(MEM_CONGESTION)
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

        validateRiskSets(actionGroups.values());
    }


    @Test
    public void testQueryActionModeFilter() {
        final ActionGroupKey matching = ImmutableActionGroupKey.builder()
            .actionMode(ActionMode.AUTOMATIC)
            .actionState(ActionState.CLEARED)
            .actionType(ActionType.ACTIVATE)
            .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
            .addActionRelatedRisk(MEM_CONGESTION)
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
            .addActionRelatedRisk(MEM_CONGESTION)
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
            .addActionRelatedRisk(MEM_CONGESTION)
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
            .addActionRelatedRisk(MEM_CONGESTION)
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

    /**
     * Old records may not have the "risk" column set.
     * Ensure that we handle those rows properly.
     */
    @Test
    public void testRetrieveEmptyRisk() {
        ActionGroupRecord actionGroupRecord = dsl.newRecord(ACTION_GROUP);
        actionGroupRecord.setActionCategory((short)1);
        actionGroupRecord.setActionMode((short)1);
        actionGroupRecord.setActionState((short)1);
        actionGroupRecord.setActionType((short)1);
        // No risk id.
        actionGroupRecord.store();

        final MatchedActionGroups matchedGroups =
                actionGroupStore.query(ActionGroupFilter.getDefaultInstance()).get();
        ActionGroup actionGroup = matchedGroups.specificActionGroupsById().values().iterator().next();
        Set<String> risks = actionGroup.key().getActionRelatedRisk();
        assertThat(risks.size(), is(1));
        assertThat(risks.iterator().next(), containsString("N/A for actions"));
    }

    /**
     * Tests that queries for actionGroups filtered by action related risk return correct results.
     */
    @Test
    public void testQueryActionRelatedRiskFilter() {
        // GIVEN
        final ActionGroupKey matching1 = ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.AUTOMATIC)
                .actionState(ActionState.CLEARED)
                .actionType(ActionType.ACTIVATE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .addActionRelatedRisk(MEM_CONGESTION)
                .build();
        final ActionGroupKey partialRiskMatchKey = ImmutableActionGroupKey.copyOf(matching1)
                .withActionState(ActionState.READY)
                .withActionRelatedRisk(CPU_CONGESTION, MEM_CONGESTION);

        final ActionGroupKey matching2 = ImmutableActionGroupKey.copyOf(matching1)
                .withActionState(ActionState.READY);

        final ActionGroupKey nonMatching = ImmutableActionGroupKey.copyOf(matching1)
                .withActionRelatedRisk(CPU_CONGESTION);

        final Map<ActionGroupKey, ActionGroup> groupByKey =
                actionGroupStore.ensureExist(Sets.newHashSet(matching1, matching2, partialRiskMatchKey, nonMatching));

        // WHEN
        final MatchedActionGroups matchedActionGroups =
                // Should find a result.
                actionGroupStore.query(ActionGroupFilter.newBuilder()
                        .addActionRelatedRisk(MEM_CONGESTION)
                        .build()).get();

        // THEN
        assertFalse(matchedActionGroups.allActionGroups());
        final ActionGroup matchingActionGroup = groupByKey.get(matching1);
        final ActionGroup matchingActionGroup2 = groupByKey.get(matching2);
        final ActionGroup partialRiskMatchGroup = groupByKey.get(partialRiskMatchKey);
        assertThat(matchedActionGroups.specificActionGroupsById().keySet(),
                containsInAnyOrder(matchingActionGroup.id(), matchingActionGroup2.id(), partialRiskMatchGroup.id()));
        assertThat(matchedActionGroups.specificActionGroupsById().get(matchingActionGroup.id()),
                is(matchingActionGroup));
        assertThat(matchedActionGroups.specificActionGroupsById().get(matchingActionGroup2.id()),
                is(matchingActionGroup2));

        // The partial match group gets returned only with the matched risk.
        assertThat(matchedActionGroups.specificActionGroupsById().get(partialRiskMatchGroup.id()).key().getActionRelatedRisk(),
                // Does not include the CPU_CONGESTION.
                is(Collections.singleton(MEM_CONGESTION)));
    }

    @Test
    public void testQueryAllMatch() {
        final ActionGroupKey matching = ImmutableActionGroupKey.builder()
            .actionMode(ActionMode.AUTOMATIC)
            .actionState(ActionState.CLEARED)
            .actionType(ActionType.ACTIVATE)
            .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
            .addActionRelatedRisk(MEM_CONGESTION)
            .build();
        // Every field is different.
        final ActionGroupKey otherMatching = ImmutableActionGroupKey.builder()
            .actionMode(ActionMode.MANUAL)
            .actionState(ActionState.READY)
            .actionType(ActionType.DEACTIVATE)
            .category(ActionCategory.PREVENTION)
            .addActionRelatedRisk(CPU_CONGESTION)
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

    private Map<Integer, Pair<Set<String>, String>> getRiskSetsByAGId() {
        final Result<Record4<Integer, Integer, String, String>> relatedRisks = dsl.select(ACTION_GROUP.ID,
                RELATED_RISK_FOR_ACTION.ID, RELATED_RISK_FOR_ACTION.CHECKSUM,
                RELATED_RISK_DESCRIPTION.RISK_DESCRIPTION)
                .from(ACTION_GROUP.join(RELATED_RISK_FOR_ACTION)
                        .on(ACTION_GROUP.ACTION_RELATED_RISK.eq(RELATED_RISK_FOR_ACTION.ID))
                        .join(RELATED_RISK_DESCRIPTION)
                        .on(RELATED_RISK_FOR_ACTION.ID.eq(RELATED_RISK_DESCRIPTION.ID)))
                .fetch();
        final Map<Integer, Pair<Set<String>, String>> ret = new HashMap<>();
        final Map<Integer, Set<String>> riskSetsById = new HashMap<>();
        relatedRisks.forEach(record -> {
            final Integer riskId = record.value2();
            final Set<String> riskSet = riskSetsById.computeIfAbsent(riskId, k -> new HashSet<>());
            riskSet.add(record.value4());
            final Pair<Set<String>, String> p = ret.computeIfAbsent(record.value1(),
                    k -> Pair.of(riskSet, record.value3()));
            // Checksum should always be the same.
            assertThat(record.value3(), is(p.getRight()));
        });
        return ret;
    }
}
