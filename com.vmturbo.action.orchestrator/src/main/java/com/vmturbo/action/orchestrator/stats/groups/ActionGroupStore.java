package com.vmturbo.action.orchestrator.stats.groups;

import static com.vmturbo.action.orchestrator.db.tables.ActionGroup.ACTION_GROUP;
import static com.vmturbo.action.orchestrator.db.tables.RelatedRiskForAction.RELATED_RISK_FOR_ACTION;

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

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Record6;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.tables.records.ActionGroupRecord;
import com.vmturbo.action.orchestrator.db.tables.records.RelatedRiskForActionRecord;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.ActionGroupFilter;
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
            // ensure all related risks exist and get the mapping from risk string to id
            final Map<String, Integer> riskToId = ensureRelatedRisksExist(keys.stream()
                    .map(ActionGroupKey::getActionRelatedRisk)
                    .collect(Collectors.toSet()), transactionDsl);

            final int[] inserted = transactionDsl.batch(keys.stream()
                    .map(key -> transactionDsl.insertInto(ACTION_GROUP)
                        .set(keyToRecord(key, riskToId))
                        .onDuplicateKeyIgnore())
                    .collect(Collectors.toList()))
                .execute();
            final int insertedSum = IntStream.of(inserted).sum();
            if (insertedSum > 0) {
                logger.info("Inserted {} action groups.", insertedSum);
            }

            // join with risk table and get string value of risk
            final Map<ActionGroupKey, ActionGroup> allExistingActionGroups = transactionDsl.select(
                    ACTION_GROUP.ID, ACTION_GROUP.ACTION_TYPE, ACTION_GROUP.ACTION_CATEGORY,
                    ACTION_GROUP.ACTION_MODE, ACTION_GROUP.ACTION_STATE,
                    RELATED_RISK_FOR_ACTION.RISK_DESCRIPTION)
                    .from(ACTION_GROUP)
                    .innerJoin(RELATED_RISK_FOR_ACTION)
                    .on(ACTION_GROUP.ACTION_RELATED_RISK.eq(RELATED_RISK_FOR_ACTION.ID))
                    .fetch()
                    .stream()
                    .map(this::recordToGroup)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toMap(ActionGroup::key, Function.identity()));

            logger.debug("A total of {} action groups now exist.", allExistingActionGroups);

            return Maps.filterKeys(allExistingActionGroups, keys::contains);
        });
    }

    /**
     * Ensures that the related risk description provided exists in the related_risk_for_action
     * table in the database, and returns the id of the corresponding record. If there is no such
     * string in the table, it creates a new record.
     *
     * <p>The records are in the form <id, String>. We store the risk descriptions in a different
     * table since they might be repeated many times in the action_group table, so we save space by
     * moving them to a separate table and storing just integers in action_group records.
     *
     * @param relatedRisks a set of strings representing the risk description
     * @param transactionDsl the transactional dsl to use for inserting
     * @return map from risk string to the id automatically assigned by db
     */
    private Map<String, Integer> ensureRelatedRisksExist(@Nonnull Set<String> relatedRisks,
            @Nonnull DSLContext transactionDsl) {
        int[] inserted = transactionDsl.batch(relatedRisks.stream()
                .map(risk -> {
                    RelatedRiskForActionRecord record = new RelatedRiskForActionRecord();
                    record.setRiskDescription(risk);
                    return record;
                })
                .map(riskRecord -> transactionDsl.insertInto(RELATED_RISK_FOR_ACTION)
                        .set(riskRecord)
                        .onDuplicateKeyIgnore())
                .collect(Collectors.toList()))
                .execute();
        if (inserted.length > 0) {
            logger.info("Inserted {} action risks", inserted.length);
        }
        return transactionDsl.select(RELATED_RISK_FOR_ACTION.ID, RELATED_RISK_FOR_ACTION.RISK_DESCRIPTION)
                .from(RELATED_RISK_FOR_ACTION)
                .where(RELATED_RISK_FOR_ACTION.RISK_DESCRIPTION.in(relatedRisks))
                .fetch()
                .stream()
                .collect(Collectors.toMap(Record2::value2, Record2::value1));
    }

    /**
     * Convert db record to {@link ActionGroup}.
     *
     * @param record db record containing following in order: id, action_type, action_category,
     *               action_mode, action_state, risk_description
     * @return optional {@link ActionGroup}
     */
    @Nonnull
    private Optional<ActionGroup> recordToGroup(
            @Nonnull final Record6<Integer, Short, Short, Short, Short, String> record) {
        final ImmutableActionGroupKey.Builder keyBuilder = ImmutableActionGroupKey.builder();
        final ActionCategory actionCategory = ActionCategory.forNumber(record.value3());
        if (actionCategory != null) {
            keyBuilder.category(actionCategory);
        } else {
            logger.error("Invalid action category {} in database record.", record.value3());
        }

        final ActionMode actionMode = ActionMode.forNumber(record.value4());
        if (actionMode != null) {
            keyBuilder.actionMode(actionMode);
        } else {
            logger.error("Invalid action mode {} in database record.", record.value4());
        }

        final ActionType actionType = ActionType.forNumber(record.value2());
        if (actionType != null) {
            keyBuilder.actionType(actionType);
        } else {
            logger.error("Invalid action type {} in database record.", record.value2());
        }

        final ActionState actionState = ActionState.forNumber(record.value5());
        if (actionState != null) {
            keyBuilder.actionState(actionState);
        } else {
            logger.error("Invalid action state {} in database record.", record.value5());
        }

        // if risk is null, it means it's an action group record that existed before the
        // action_risk column is added. It's only used for historical action stats before the
        // action_risk column is added; all new actions stats will use new ActionGroupKey.
        keyBuilder.actionRelatedRisk(record.value6() != null
                ? record.value6()
                // Use a default related risk string for action group records that existed before
                // the action_risk column was added to the database.
                : "N/A for actions executed with version 7.22.5 or earlier");

        try {
            return Optional.of(ImmutableActionGroup.builder()
                .id(record.value1())
                .key(keyBuilder.build())
                .build());
        } catch (IllegalStateException e) {
            logger.error("Failed to build group out of database record. Error: {}",
                    e.getLocalizedMessage());
            return Optional.empty();
        }
    }

    @Nonnull
    private ActionGroupRecord keyToRecord(@Nonnull final ActionGroupKey key,
                                          @Nonnull Map<String, Integer> riskToId) {
        ActionGroupRecord record = new ActionGroupRecord();
        // Leave record ID unset - database auto-increment takes care of ID assignment.

        record.setActionType((short)key.getActionType().getNumber());
        record.setActionMode((short)key.getActionMode().getNumber());
        record.setActionCategory((short)key.getCategory().getNumber());
        record.setActionState((short)key.getActionState().getNumber());
        record.setActionRelatedRisk(riskToId.get(key.getActionRelatedRisk()));
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

        if (actionGroupFilter.getActionRelatedRiskCount() > 0) {
            conditions.add(RELATED_RISK_FOR_ACTION.RISK_DESCRIPTION.in(
                    actionGroupFilter.getActionRelatedRiskList().toArray(new String[0]))
            );
        }

        try (DataMetricTimer timer = Metrics.QUERY_HISTOGRAM.startTimer()) {
            final Map<Integer, ActionGroup> matchedActionGroups = dsl.select(
                    ACTION_GROUP.ID, ACTION_GROUP.ACTION_TYPE, ACTION_GROUP.ACTION_CATEGORY,
                    ACTION_GROUP.ACTION_MODE, ACTION_GROUP.ACTION_STATE,
                    RELATED_RISK_FOR_ACTION.RISK_DESCRIPTION)
                    .from(ACTION_GROUP)
                    // left join since groups with null risk are also valid for those stats before
                    // adding risk column
                    .leftJoin(RELATED_RISK_FOR_ACTION)
                    .on(ACTION_GROUP.ACTION_RELATED_RISK.eq(RELATED_RISK_FOR_ACTION.ID))
                    .where(conditions).fetch().stream()
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
