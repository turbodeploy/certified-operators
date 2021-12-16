package com.vmturbo.cost.component.billedcosts;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

import com.vmturbo.cost.component.db.tables.CostTagGrouping;
import com.vmturbo.cost.component.db.tables.records.CostTagGroupingRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * An object of this type contains data operations for the Cost tag grouping tables.
 */
public class TagGroupStore {

    private final DSLContext dslContext;
    private final BatchInserter batchInserter;

    /**
     * Creates an instance of TagGroupStore.
     *
     * @param dslContext instance for executing queries.
     * @param batchInserter a utility object for bulk inserts.
     */
    public TagGroupStore(@Nonnull final DSLContext dslContext, @Nonnull BatchInserter batchInserter) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.batchInserter = Objects.requireNonNull(batchInserter);
    }

    /**
     * Retrieves all tag group records from the cost tag grouping table.
     *
     * @return all tag group records.
     * @throws DbException on encountering error while executing select query.
     */
    public Result<CostTagGroupingRecord> retrieveAllTagGroups() throws DbException {
        try {
            return dslContext.selectFrom(CostTagGrouping.COST_TAG_GROUPING).fetch();
        } catch (DataAccessException ex) {
            throw new DbException("Exception while retrieving tag groups.", ex.getCause());
        }
    }

    /**
     * Insert provided tag groups into cost_tag_grouping table.
     *
     * @param costTagGroupingRecords tag groups to be inserted into the cost_tag_grouping table.
     * @throws com.vmturbo.sql.utils.DbException on encountering DataAccessException during query execution.
     */
    public void insertCostTagGroups(final Collection<CostTagGroupingRecord> costTagGroupingRecords) throws DbException {
        batchInserter.insert(new ArrayList<>(costTagGroupingRecords), CostTagGrouping.COST_TAG_GROUPING, dslContext,
            false);
    }
}