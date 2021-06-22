package com.vmturbo.action.orchestrator.stats.groups;

import static com.vmturbo.action.orchestrator.db.tables.ActionGroup.ACTION_GROUP;
import static com.vmturbo.action.orchestrator.db.tables.RelatedRiskForAction.RELATED_RISK_FOR_ACTION;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.Record7;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionGroupRecord;
import com.vmturbo.action.orchestrator.db.tables.records.RelatedRiskDescriptionRecord;
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

    /**
     * Public constructor.
     *
     * @param dsl The {@link DSLContext}.
     */
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
            final Map<ActionGroupKey, Integer> riskToId = ensureRelatedRisksExist(keys, transactionDsl);

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

            final Map<Integer, ActionGroup> allActionGroups = getActionGroups(transactionDsl, Collections.emptyList());

            logger.debug("A total of {} action groups now exist.", allActionGroups);

            final Map<ActionGroupKey, ActionGroup> result = new HashMap<>(keys.size());
            allActionGroups.forEach((id, group) -> {
                if (keys.contains(group.key())) {
                    result.put(group.key(), group);
                }
            });

            return result;
        });
    }

    /**
     * Ensures that the related risk description provided exists in the related_risk_for_action
     * and related_risk_description tables.
     *
     * @param keys The {@link ActionGroupKey}s.
     * @param transactionDsl the transactional dsl to use for inserting
     * @return map from {@link ActionGroupKey} to the id automatically assigned by db.
     */
    private Map<ActionGroupKey, Integer> ensureRelatedRisksExist(@Nonnull Set<ActionGroupKey> keys,
            @Nonnull DSLContext transactionDsl) {
        final Map<String, Set<ActionGroupKey>> keysByCombinedRiskHash = new HashMap<>();
        final Map<String, Set<String>> risksByHash = new HashMap<>();
        keys.forEach(key -> {
            // Do not change the combination of risks into a single string without an associated
            // database migration!
            final String combinedRisks = key.getActionRelatedRisk().stream()
                // Important, because the subsequent hash is order-dependent.
                .sorted()
                .collect(Collectors.joining(","));
            final String combinedRiskHash = DigestUtils.md5Hex(combinedRisks);
            keysByCombinedRiskHash.computeIfAbsent(combinedRiskHash, r -> new HashSet<>())
                    .add(key);
            risksByHash.put(combinedRiskHash, key.getActionRelatedRisk());
        });

        // The list of hashes should be in the same order that the subsequent array of
        // modified row counts.
        final List<String> insertedHashes = new ArrayList<>(keysByCombinedRiskHash.size());
        int[] inserted = transactionDsl.batch(keysByCombinedRiskHash.keySet().stream()
            .peek(insertedHashes::add)
            .map(combinedRisksHash -> {
                return transactionDsl.insertInto(RELATED_RISK_FOR_ACTION)
                    .set(RELATED_RISK_FOR_ACTION.CHECKSUM, combinedRisksHash)
                    .onDuplicateKeyIgnore();
            }).collect(Collectors.toList())).execute();

        // To figure out which risks are newly added we look at which insertions caused a row
        // to be modified.
        final Set<String> newRisks = new HashSet<>();
        for (int i = 0; i < inserted.length && i < insertedHashes.size(); ++i) {
            if (inserted[i] > 0) {
                newRisks.add(insertedHashes.get(i));
            }
        }

        final Map<ActionGroupKey, Integer> riskIdsByActionGroup = new HashMap<>(keys.size());
        final List<Insert<RelatedRiskDescriptionRecord>> descriptionInserts = new ArrayList<>();
        transactionDsl.select(RELATED_RISK_FOR_ACTION.ID, RELATED_RISK_FOR_ACTION.CHECKSUM)
            .from(RELATED_RISK_FOR_ACTION)
            .where(RELATED_RISK_FOR_ACTION.CHECKSUM.in(keysByCombinedRiskHash.keySet()))
            .fetch()
            .forEach(r -> {
                Integer riskId = r.value1();
                String checksum = r.value2();
                keysByCombinedRiskHash.getOrDefault(checksum, Collections.emptySet())
                    .forEach(actionGroup -> {
                        riskIdsByActionGroup.put(actionGroup, riskId);
                    });

                // If a particular risk is new, we take its ID and make the appropriate inserts into
                // the related_risk_description table. We make the inserts in this roundabout way
                // because the "insert" into related_risk_for_action above doesn't return the ID.
                if (newRisks.contains(checksum)) {
                    risksByHash.getOrDefault(checksum, Collections.emptySet()).forEach(riskInSet -> {
                        descriptionInserts.add(transactionDsl.insertInto(Tables.RELATED_RISK_DESCRIPTION)
                                .set(Tables.RELATED_RISK_DESCRIPTION.ID, riskId)
                                .set(Tables.RELATED_RISK_DESCRIPTION.RISK_DESCRIPTION, riskInSet));
                    });
                }
            });

        if (!descriptionInserts.isEmpty()) {
            // Insert the descriptions for any new risk sets.
            transactionDsl.batch(descriptionInserts).execute();
        }

        if (keys.size() != riskIdsByActionGroup.size()) {
            logger.error("Some action group keys do not have assigned risk ids: {}", keys.stream()
                .filter(k -> !riskIdsByActionGroup.containsKey(k))
                .collect(Collectors.toList()));
        }
        return riskIdsByActionGroup;
    }

    /**
     * Convert db record to {@link ActionGroup}.
     *
     * @param record db record containing following in order: id, action_type, action_category,
     *               action_mode, action_state, risk_description
     * @return optional {@link ActionGroup}
     */
    @Nonnull
    private ImmutableActionGroupKey.Builder getKeyBuilder(
            @Nonnull final Record7<Integer, Short, Short, Short, Short, Integer, String> record) {
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

        return keyBuilder;
    }

    @Nonnull
    private ActionGroupRecord keyToRecord(@Nonnull final ActionGroupKey key,
                                          @Nonnull Map<ActionGroupKey, Integer> riskToId) {
        ActionGroupRecord record = new ActionGroupRecord();
        // Leave record ID unset - database auto-increment takes care of ID assignment.

        record.setActionType((short)key.getActionType().getNumber());
        record.setActionMode((short)key.getActionMode().getNumber());
        record.setActionCategory((short)key.getCategory().getNumber());
        record.setActionState((short)key.getActionState().getNumber());
        record.setActionRelatedRisk(riskToId.get(key));
        return record;
    }

    private Map<Integer, ActionGroup> getActionGroups(final DSLContext transactionDsl, final List<Condition> conditions) {
        // Risk descriptions can come in in multiple rows for actions with more than one risk.
        final Map<Integer, ImmutableActionGroupKey.Builder> keyBuildersById = new HashMap<>();
        // The immutable builder has no way to check if the list of risks is empty at the
        // end.
        final Map<Integer, Set<String>> risksByRiskId = new HashMap<>();
        final Map<Integer, Integer> riskIdsByActionGroupId = new HashMap<>();
        transactionDsl.select(ACTION_GROUP.ID, ACTION_GROUP.ACTION_TYPE, ACTION_GROUP.ACTION_CATEGORY,
                ACTION_GROUP.ACTION_MODE, ACTION_GROUP.ACTION_STATE,
                Tables.RELATED_RISK_DESCRIPTION.ID,
                Tables.RELATED_RISK_DESCRIPTION.RISK_DESCRIPTION)
                .from(ACTION_GROUP)
                // left join since groups with null risk are also valid for those stats before
                // adding risk column
                .leftJoin(Tables.RELATED_RISK_DESCRIPTION)
                .on(ACTION_GROUP.ACTION_RELATED_RISK.eq(Tables.RELATED_RISK_DESCRIPTION.ID))
                .where(conditions).fetch()
                .forEach(record -> {
                    Integer actionGroupId = record.value1();
                    Integer riskId = record.value6();
                    String riskDescription = record.value7();
                    riskIdsByActionGroupId.put(actionGroupId, riskId);
                    // Action groups recorded before the action_risk column was added will not
                    // have a risk description.
                    if (riskDescription != null) {
                        risksByRiskId.computeIfAbsent(riskId, k -> new HashSet<>()).add(
                                riskDescription);
                    }
                    keyBuildersById.computeIfAbsent(actionGroupId, k -> getKeyBuilder(record));
                });

        final Map<Integer, ActionGroup> matchedActionGroups = new HashMap<>(keyBuildersById.size());
        keyBuildersById.forEach((actionGroupId, keyBldr) -> {
            // -1 will never be a value returned from the database.
            final Integer riskId = riskIdsByActionGroupId.getOrDefault(actionGroupId, -1);
            Set<String> risks = risksByRiskId.getOrDefault(riskId, Collections.singleton(
                    // Use a default related risk string for action group records that existed before
                    // the action_risk column was added to the database.
                    "N/A for actions executed with version 7.22.5 or earlier"));
            keyBldr.addAllActionRelatedRisk(risks);
            try {
                matchedActionGroups.put(actionGroupId, ImmutableActionGroup.builder()
                        .id(actionGroupId)
                        .key(keyBldr.build())
                        .build());
            } catch (IllegalStateException e) {
                logger.error("Failed to build group out of database records", e);
            }
        });
        return matchedActionGroups;
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
                    .map(category -> (short)category.getNumber())
                    .toArray(Short[]::new)));
        }

        if (actionGroupFilter.getActionModeCount() > 0) {
            conditions.add(ACTION_GROUP.ACTION_MODE.in(
                actionGroupFilter.getActionModeList().stream()
                    .map(mode -> (short)mode.getNumber())
                    .toArray(Short[]::new)));
        }

        if (actionGroupFilter.getActionStateCount() > 0) {
            conditions.add(ACTION_GROUP.ACTION_STATE.in(
                actionGroupFilter.getActionStateList().stream()
                    .map(state -> (short)state.getNumber())
                    .toArray(Short[]::new)));
        }

        if (actionGroupFilter.getActionTypeCount() > 0) {
            conditions.add(ACTION_GROUP.ACTION_TYPE.in(
                actionGroupFilter.getActionTypeList().stream()
                    .map(type -> (short)type.getNumber())
                    .toArray(Short[]::new)));
        }

        if (actionGroupFilter.getActionRelatedRiskCount() > 0) {
            conditions.add(Tables.RELATED_RISK_DESCRIPTION.RISK_DESCRIPTION.in(
                    actionGroupFilter.getActionRelatedRiskList().toArray(new String[0]))
            );
        }

        try (DataMetricTimer timer = Metrics.QUERY_HISTOGRAM.startTimer()) {
            final Map<Integer, ActionGroup> matchedActionGroups = getActionGroups(dsl, conditions);
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
         *
         * @return Whether all action groups matched the query.
         */
        boolean allActionGroups();

        /**
         * The action groups that matched the query, arranged by ID.
         * Should not be empty.
         *
         * @return (id) -> ({@link ActionGroup})
         */
        Map<Integer, ActionGroup> specificActionGroupsById();
    }

    /**
     * Metrics for store.
     */
    static class Metrics {

        static final DataMetricHistogram QUERY_HISTOGRAM = DataMetricHistogram.builder()
            .withName("ao_action_group_store_query_duration_seconds")
            .withHelp("Duration of action group queries, in seconds.")
            .withBuckets(0.1, 0.5, 1, 5)
            .build()
            .register();
    }
}
