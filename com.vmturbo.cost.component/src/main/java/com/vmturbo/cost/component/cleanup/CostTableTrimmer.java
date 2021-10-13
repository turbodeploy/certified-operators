package com.vmturbo.cost.component.cleanup;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.DeleteConditionStep;
import org.jooq.DeleteFinalStep;
import org.jooq.exception.DataAccessException;

import com.vmturbo.cloud.common.data.stats.DurationStatistics;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.TableCleanupInfo;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.TrimTimeResolver;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.Trimmer;

/**
 * Class for trimming the cost tables.
 */
public class CostTableTrimmer implements Trimmer {

    private final Logger logger = LogManager.getLogger(getClass());

    private final DSLContext dslContext;

    private final TableCleanupInfo tableInfo;

    /**
     * Constructor for the cost table trimmer.
     *
     * @param dslContext the dsl context.
     * @param tableInfo The table info about the table to trim
     */
    public CostTableTrimmer(final DSLContext dslContext,
                            final TableCleanupInfo tableInfo) {
        this.dslContext = dslContext;
        this.tableInfo = tableInfo;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void trim(@Nonnull final TrimTimeResolver trimTimeResolver) throws DataAccessException {


        logger.debug("Started trimming table {}", tableInfo.shortTableName());

        final DurationStatistics.Collector batchDurationCollector = DurationStatistics.collector();
        final Stopwatch aggregateStopwatch = Stopwatch.createStarted();
        int numRecordsDeleted = 0;

        LocalDateTime trimToTime = null;
        boolean continueDelete = true;
        while (continueDelete) {

            final Stopwatch batchStopwatch = Stopwatch.createStarted();

            trimToTime = trimTimeResolver.getTrimTime();
            final DeleteConditionStep<?> deleteStep = dslContext.deleteFrom(tableInfo.table())
                    .where(tableInfo.timeField().lessThan(trimToTime));

            final DeleteFinalStep<?> finalizedDelete;
            if (tableInfo.numRowsToBatchDelete() > 0) {
                finalizedDelete = deleteStep.limit(tableInfo.numRowsToBatchDelete());
            } else {
                finalizedDelete = deleteStep;
            }

            final int numRowsDeletedBatch = finalizedDelete.execute();

            batchDurationCollector.collect(batchStopwatch.elapsed());
            logger.debug("Deleted batch of {} entries from the {} table in {}",
                    numRowsDeletedBatch, tableInfo.table(), batchStopwatch);

            continueDelete = numRowsDeletedBatch > 0 && tableInfo.numRowsToBatchDelete() > 0;
            numRecordsDeleted += numRowsDeletedBatch;
        }

        logger.info("Deleted {} entries older than {} from the {} table in {} (Stats={})",
                numRecordsDeleted, trimToTime, tableInfo.table(), aggregateStopwatch,
                batchDurationCollector.toStatistics());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("Table Info", tableInfo)
                .build();
    }
}
