package com.vmturbo.cost.component.cleanup;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.DeleteConditionStep;
import org.jooq.exception.DataAccessException;

import com.vmturbo.cost.component.cleanup.CostTableCleanup.TableInfo;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.Trimmer;

/**
 * Class for trimming the cost tables.
 */
public class CostTableTrimmer implements Trimmer {

    private final Logger logger = LogManager.getLogger(getClass());

    private final DSLContext dslContext;

    private final TableInfo tableInfo;


    /**
     * Constructor for the cost table trimmer.
     *
     * @param dslContext the dsl context.
     * @param tableInfo The table info about the table to trim
     */
    public CostTableTrimmer(final DSLContext dslContext,
                            final TableInfo tableInfo) {
        this.dslContext = dslContext;
        this.tableInfo = tableInfo;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void trim(@Nonnull final LocalDateTime trimToTime) throws DataAccessException {
        logger.debug("Started trimming table {}", tableInfo.shortTableName());

        final Stopwatch aggregateStopwatch = Stopwatch.createStarted();
        int numRecordsDeleted = 0;

        boolean continueDelete = true;
        while (continueDelete) {

            final Stopwatch batchStopwatch = Stopwatch.createStarted();
            final DeleteConditionStep deleteStep = dslContext.deleteFrom(tableInfo.table())
                    .where(tableInfo.timeField().lessThan(trimToTime));

            final int numRowsDeletedBatch = tableInfo.numRowsToBatchDelete()
                    .map(deleteStep::limit)
                    .orElse(deleteStep)
                    .execute();

            logger.debug("Deleted batch of {} entries from the {} table in {}",
                    numRowsDeletedBatch, tableInfo.table(), batchStopwatch);

            continueDelete = numRowsDeletedBatch > 0 && tableInfo.numRowsToBatchDelete().isPresent();
            numRecordsDeleted += numRowsDeletedBatch;
        }

        logger.info("Deleted {} entries older than {} from the {} table in {}",
                numRecordsDeleted, trimToTime, tableInfo.table(), aggregateStopwatch);
    }
}
