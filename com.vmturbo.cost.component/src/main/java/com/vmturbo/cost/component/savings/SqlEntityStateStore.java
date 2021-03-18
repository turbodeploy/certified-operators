package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_STATE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

import com.vmturbo.cost.component.db.tables.records.EntitySavingsStateRecord;

/**
 * Implementation of EntityStateStore that persists data in entity_savings_state table.
 */
public class SqlEntityStateStore implements EntityStateStore {
    /**
     * Used for batch operations.
     */
    private final int chunkSize;

    /**
     * JOOQ access.
     */
    private final DSLContext dsl;

    /**
     * Constructor.
     *
     * @param dsl Dsl context for jooq
     * @param chunkSize chunk size for batch operations
     */
    public SqlEntityStateStore(@Nonnull final DSLContext dsl, final int chunkSize) {
        this.dsl = dsl;
        this.chunkSize = chunkSize;
    }

    @Nonnull
    @Override
    public Map<Long, EntityState> getEntityStates(@Nonnull final Set<Long> entityIds) throws EntitySavingsException {
        final Result<Record2<Long, String>> records;
        try {
            records = dsl.select(ENTITY_SAVINGS_STATE.ENTITY_OID, ENTITY_SAVINGS_STATE.ENTITY_STATE)
                    .from(ENTITY_SAVINGS_STATE)
                    .where(ENTITY_SAVINGS_STATE.ENTITY_OID.in(entityIds))
                    .fetch();
            return records.stream().collect(Collectors.toMap(Record2::value1,
                    record -> EntityState.fromJson(record.value2())));
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when getting entity states from database.", e);
        }
    }

    @Override
    public Map<Long, EntityState> getUpdatedEntityStates() throws EntitySavingsException {
        final Result<Record2<Long, String>> records;
        try {
            records = dsl.select(ENTITY_SAVINGS_STATE.ENTITY_OID, ENTITY_SAVINGS_STATE.ENTITY_STATE)
                    .from(ENTITY_SAVINGS_STATE)
                    .where(ENTITY_SAVINGS_STATE.UPDATED.eq((byte)1))
                    .fetch();
            return records.stream().collect(Collectors.toMap(Record2::value1,
                    record -> EntityState.fromJson(record.value2())));
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when getting entity states from database.", e);
        }
    }

    @Override
    public void clearUpdatedFlags() throws EntitySavingsException {
        try {
            dsl.update(ENTITY_SAVINGS_STATE)
                    .set(ENTITY_SAVINGS_STATE.UPDATED, (byte)0)
                    .where(ENTITY_SAVINGS_STATE.UPDATED.eq((byte)1))
                    .execute();
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when getting entity states from database.", e);
        }
    }

    @Override
    public void deleteEntityStates(@Nonnull final Set<Long> entityIds) throws EntitySavingsException {
        try {
            dsl.deleteFrom(ENTITY_SAVINGS_STATE)
                    .where(ENTITY_SAVINGS_STATE.ENTITY_OID.in(entityIds))
                    .execute();
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when deleting entity states.", e);
        }
    }

    @Override
    public void updateEntityStates(@Nonnull final Map<Long, EntityState> entityStateMap) throws EntitySavingsException {
        List<EntitySavingsStateRecord> records = new ArrayList<>();
        entityStateMap.values().forEach(entityState -> {
            EntitySavingsStateRecord record = ENTITY_SAVINGS_STATE.newRecord();
            record.setEntityOid(entityState.getEntityId());
            record.setUpdated(entityState.isUpdated() ? (byte)1 : (byte)0);
            record.setEntityState(entityState.toJson());
            records.add(record);
        });

        try {
            dsl.loadInto(ENTITY_SAVINGS_STATE)
                    .batchAfter(chunkSize)
                    .onDuplicateKeyUpdate()
                    .loadRecords(records)
                    .fields(ENTITY_SAVINGS_STATE.ENTITY_OID,
                            ENTITY_SAVINGS_STATE.UPDATED,
                            ENTITY_SAVINGS_STATE.ENTITY_STATE)
                    .execute();
        } catch (IOException e) {
            throw new EntitySavingsException("Error occurred when updating entity states.", e);
        }
    }

    @Override
    public Stream<EntityState> getAllEntityStates() {
        // Use jooq lazy fetch to avoid load the whole table into memory.
        // https://www.jooq.org/doc/3.2/manual/sql-execution/fetching/lazy-fetching/
        // https://stackoverflow.com/questions/32209248/java-util-stream-with-resultset
        return dsl.selectFrom(ENTITY_SAVINGS_STATE)
                .fetchSize(chunkSize)
                .fetchStream()
                .map(record -> EntityState.fromJson(record.getEntityState()));
    }
}
