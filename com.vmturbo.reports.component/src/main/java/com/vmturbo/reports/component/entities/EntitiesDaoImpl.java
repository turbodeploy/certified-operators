package com.vmturbo.reports.component.entities;

import static com.vmturbo.history.schema.abstraction.tables.Entities.ENTITIES;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * Jooq-based entities DAO implementation.
 */
public class EntitiesDaoImpl implements EntitiesDao {

    private final Logger logger = LogManager.getLogger(getClass());

    private final DSLContext dsl;

    /**
     * Constructs the DAO based on the specified Jooq context.
     *
     * @param dsl Jooq context to use
     */
    public EntitiesDaoImpl(@Nonnull DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    @Nonnull
    @Override
    public Optional<String> getEntityName(long oid) throws DbException {
        logger.debug("Getting entity instance by id {}", oid);
        final List<EntitiesRecord> records;
        try {
            records = dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.selectFrom(ENTITIES)
                        .where(ENTITIES.ID.eq(oid))
                        .fetch()
                        .into(EntitiesRecord.class);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching entity with oid " + oid, e);
        }
        return records.isEmpty() ? Optional.empty() : Optional.of(records.get(0).getName());
    }
}
