package com.vmturbo.topology.processor.targets.status;

import static com.vmturbo.topology.processor.db.tables.TargetStatus.TARGET_STATUS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.topology.processor.targets.TargetStatusOuterClass.TargetStatus;

/**
 * The target status DAO class.
 */
public class SQLTargetStatusStore implements TargetStatusStore {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    /**
     * The class constructor.
     *
     * @param dsl The input context.
     */
    public SQLTargetStatusStore(@Nonnull DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * Delete the target status based on the input target ID.
     *
     * @param targetId The input target id.
     *
     * @throws TargetStatusStoreException If the target status cannot be deleted from the DB.
     */
    public void deleteTargetStatus(long targetId) throws TargetStatusStoreException {
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);

                transactionDsl
                    .delete(TARGET_STATUS)
                    .where(TARGET_STATUS.TARGET_ID.eq(targetId))
                    .execute();
            });
        } catch (DataAccessException e) {
            logger.warn("Cannot delete the target status info for target with ID: " + targetId);
            throw new TargetStatusStoreException(e.getMessage());
        }
    }

    @Override
    public void setTargetStatus(final TargetStatus targetStatus) throws TargetStatusStoreException {
        try {
            dsl.insertInto(TARGET_STATUS)
                    .set(TARGET_STATUS.TARGET_ID, targetStatus.getTargetId())
                    .set(TARGET_STATUS.STATUS, targetStatus)
                    .onDuplicateKeyUpdate()
                    .set(TARGET_STATUS.STATUS, targetStatus)
                    .execute();
        } catch (DataAccessException e) {
            logger.error("Failed to persist new target status for target {}.",
                    targetStatus.getTargetId(), e);
            throw new TargetStatusStoreException(e);
        }
    }

    @Nonnull
    @Override
    public Map<Long, TargetStatus> getTargetsStatuses(@Nullable final Collection<Long> targetIds) throws TargetStatusStoreException {
        if (targetIds != null && targetIds.isEmpty()) {
            return Long2ObjectMaps.emptyMap();
        }
        final Long2ObjectMap<TargetStatus> retMap = new Long2ObjectOpenHashMap<>();
        try {
            final List<Condition> conditions = new ArrayList<>();
            if (targetIds != null) {
                conditions.add(TARGET_STATUS.TARGET_ID.in(targetIds));
            }
            dsl.selectFrom(TARGET_STATUS)
                    .where(conditions)
                    .fetch()
                    .forEach(targetStatusRecord -> retMap.put(targetStatusRecord.getStatus().getTargetId(),
                            targetStatusRecord.getStatus()));
            return retMap;
        } catch (DataAccessException e) {
            logger.warn("Cannot retrieve the target status info for targets: " + targetIds);
            throw new TargetStatusStoreException(e);
        }
    }
}
