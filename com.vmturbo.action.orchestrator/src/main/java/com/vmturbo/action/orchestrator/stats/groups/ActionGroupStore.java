package com.vmturbo.action.orchestrator.stats.groups;

import static com.vmturbo.action.orchestrator.db.tables.ActionGroup.ACTION_GROUP;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.google.common.collect.Maps;

import com.vmturbo.action.orchestrator.db.tables.records.ActionGroupRecord;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.ActionGroupFilter;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Responsible for storing and retrieving {@link ActionGroup}s to/from the underlying database.
 */
public class ActionGroupStore {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    public ActionGroupStore(@Nonnull final DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * Ensure that a set of {@link ActionGroupKey}s exist in the database, and return the
     * {@link ActionGroup} the keys represent. If an {@link ActionGroupKey} does not match an
     * existing {@link ActionGroup}, this method will create the {@link ActionGroup} record.
     *
     * @param keys A set of {@link ActionGroupKey}s.
     * @return key -> {@link ActionGroup}. Each key in the input should have an associated
     *         {@link ActionGroup}.
     */
    @Nonnull
    public Map<ActionGroupKey, ActionGroup> ensureExist(@Nonnull final Set<ActionGroupKey> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }

        if (logger.isTraceEnabled()) {
            logger.debug("Ensuring that these action group keys exist: {}", keys);
        } else {
            logger.debug("Ensuring that {} action group keys exist.", keys.size());
        }

        return dsl.transactionResult(transactionContext -> {
            final DSLContext transactionDsl = DSL.using(transactionContext);

            final int[] inserted = transactionDsl.batch(keys.stream()
                    .map(key -> transactionDsl.insertInto(ACTION_GROUP)
                        .set(keyToRecord(key))
                        .onDuplicateKeyIgnore())
                    .collect(Collectors.toList()))
                .execute();
            final int insertedSum = IntStream.of(inserted).sum();
            if (insertedSum > 0) {
                logger.info("Inserted {} action groups.", insertedSum);
            }

            final Map<ActionGroupKey, ActionGroup> allExistingActionGroups =
                transactionDsl.selectFrom(ACTION_GROUP)
                    .fetch()
                    .stream()
                    .map(this::recordToGroup)
                    .filter(Optional::isPresent).map(Optional::get)
                    .collect(Collectors.toMap(ActionGroup::key, Function.identity()));

            logger.debug("A total of {} action groups now exist.", allExistingActionGroups);

            return Maps.filterKeys(allExistingActionGroups, keys::contains);
        });
    }

    @Nonnull
    private Optional<ActionGroup> recordToGroup(@Nonnull final ActionGroupRecord record) {
        final ImmutableActionGroupKey.Builder keyBuilder = ImmutableActionGroupKey.builder();
        final ActionCategory actionCategory = ActionCategory.forNumber(record.getActionCategory());
        if (actionCategory != null) {
            keyBuilder.category(actionCategory);
        } else {
            logger.error("Invalid action category {} in database record.", record.getActionCategory());
        }

        final ActionMode actionMode = ActionMode.forNumber(record.getActionMode());
        if (actionMode != null) {
            keyBuilder.actionMode(actionMode);
        } else {
            logger.error("Invalid action mode {} in database record.", record.getActionMode());
        }

        final ActionType actionType = ActionType.forNumber(record.getActionType());
        if (actionType != null) {
            keyBuilder.actionType(actionType);
        } else {
            logger.error("Invalid action type {} in database record.", record.getActionType());
        }

        final ActionState actionState = ActionState.forNumber(record.getActionState());
        if (actionState != null) {
            keyBuilder.actionState(actionState);
        } else {
            logger.error("Invalid action state {} in database record.", record.getActionState());
        }

        try {
            return Optional.of(ImmutableActionGroup.builder()
                .id(record.getId())
                .key(keyBuilder.build())
                .build());
        } catch (IllegalStateException e) {
            logger.error("Failed to build group out of database record. Error: {}",
                    e.getLocalizedMessage());
            return Optional.empty();
        }
    }

    @Nonnull
    private ActionGroupRecord keyToRecord(@Nonnull final ActionGroupKey key) {
        ActionGroupRecord record = new ActionGroupRecord();
        // Leave record ID unset - database auto-increment takes care of ID assignment.

        record.setActionType((short)key.getActionType().getNumber());
        record.setActionMode((short)key.getActionMode().getNumber());
        record.setActionCategory((short)key.getCategory().getNumber());
        record.setActionState((short)key.getActionState().getNumber());
        return record;
    }

    /**
     * Query the {@link ActionGroupStore} to get the action groups that match a filter.
     *
     * @param actionGroupFilter An {@link ActionGroupFilter} indicating which
     *                          and which action groups to return.
     * @return A {@link Optional} containing a {@link MatchedActionGroups} if one or more action
     *         groups match the filter. An empty {@link Optional} otherwise.
     */
    @Nonnull
    public Optional<MatchedActionGroups> query(@Nonnull final ActionGroupFilter actionGroupFilter) {
        final List<Condition> conditions = new ArrayList<>(4);
        if (actionGroupFilter.getActionCategoryCount() > 0) {
            conditions.add(ACTION_GROUP.ACTION_CATEGORY.in(
                actionGroupFilter.getActionCategoryList().stream()
                    .map(category -> (short) category.getNumber())
                    .toArray(Short[]::new)));
        }

        if (actionGroupFilter.getActionModeCount() > 0) {
            conditions.add(ACTION_GROUP.ACTION_MODE.in(
                actionGroupFilter.getActionModeList().stream()
                    .map(mode -> (short) mode.getNumber())
                    .toArray(Short[]::new)));
        }

        if (actionGroupFilter.getActionStateCount() > 0) {
            conditions.add(ACTION_GROUP.ACTION_STATE.in(
                actionGroupFilter.getActionStateList().stream()
                    .map(state -> (short) state.getNumber())
                    .toArray(Short[]::new)));
        }

        if (actionGroupFilter.getActionTypeCount() > 0) {
            conditions.add(ACTION_GROUP.ACTION_TYPE.in(
                actionGroupFilter.getActionTypeList().stream()
                    .map(type -> (short) type.getNumber())
                    .toArray(Short[]::new)));
        }

        try (DataMetricTimer timer = Metrics.QUERY_HISTOGRAM.startTimer()) {
            final Map<Integer, ActionGroup> matchedActionGroups =
                dsl.selectFrom(ACTION_GROUP).where(conditions).fetch().stream()
                    .map(this::recordToGroup)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toMap(ActionGroup::id, Function.identity()));
            if (matchedActionGroups.isEmpty()) {
                // There are specific action groups requested, but no such groups are found.
                return Optional.empty();
            } else {
                return Optional.of(ImmutableMatchedActionGroups.builder()
                    .specificActionGroupsById(matchedActionGroups)
                    .allActionGroups(conditions.isEmpty())
                    .build());
            }
        }

    }

    /**
     * The result of querying the {@link ActionGroupStore}.
     */
    @Value.Immutable
    public interface MatchedActionGroups {

        /**
         * If true, indicate that all action groups matched the query.
         */
        boolean allActionGroups();

        /**
         * The action groups that matched the query, arranged by ID.
         * Should not be empty.
         */
        Map<Integer, ActionGroup> specificActionGroupsById();
    }

    static class Metrics {

        static final DataMetricHistogram QUERY_HISTOGRAM = DataMetricHistogram.builder()
            .withName("ao_action_group_store_query_duration_seconds")
            .withHelp("Duration of action group queries, in seconds.")
            .withBuckets(0.1, 0.5, 1, 5)
            .build()
            .register();
    }
}
