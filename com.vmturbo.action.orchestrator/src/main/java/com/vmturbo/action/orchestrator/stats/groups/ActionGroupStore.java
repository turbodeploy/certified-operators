package com.vmturbo.action.orchestrator.stats.groups;

import static com.vmturbo.action.orchestrator.db.tables.ActionGroup.ACTION_GROUP;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.google.common.collect.Maps;

import com.vmturbo.action.orchestrator.db.tables.records.ActionGroupRecord;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;

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
}
