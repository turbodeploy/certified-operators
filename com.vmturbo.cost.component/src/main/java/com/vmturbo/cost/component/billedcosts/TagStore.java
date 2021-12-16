package com.vmturbo.cost.component.billedcosts;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

import com.vmturbo.cost.component.db.tables.CostTag;
import com.vmturbo.cost.component.db.tables.records.CostTagRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * An object of this type contains data operations for the Cost Tag table.
 */
public class TagStore {

    private final DSLContext dslContext;
    private final BatchInserter batchInserter;

    /**
     * Creates an instance of TagStore.
     *
     * @param dslContext instance for executing queries.
     * @param batchInserter a utility object for bulk inserts.
     */
    public TagStore(@Nonnull final DSLContext dslContext, @Nonnull BatchInserter batchInserter) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.batchInserter = Objects.requireNonNull(batchInserter);
    }

    /**
     * Retrieves ALL CostTagRecords from the Cost Tag table.
     *
     * @return records for tags that are persisted in the Cost tag table.
     * @throws com.vmturbo.sql.utils.DbException on encountering error while executing select query.
     */
    public Result<CostTagRecord> retrieveAllCostTags() throws DbException {
        try {
            return dslContext.selectFrom(CostTag.COST_TAG).fetch();
        } catch (DataAccessException ex) {
            throw new DbException("Exception while retrieving tags.", ex);
        }
    }

    /**
     * Inserts CostTagRecords to the Cost Tag table.
     *
     * @param costTagRecords to be inserted into the Cost Tag table.
     * @throws com.vmturbo.sql.utils.DbException on encountering DataAccessException during query execution.
     */
    public void insertCostTagRecords(@Nonnull final Collection<CostTagRecord> costTagRecords) throws DbException {
        batchInserter.insert(new ArrayList<>(costTagRecords), CostTag.COST_TAG, dslContext, false);
    }
}