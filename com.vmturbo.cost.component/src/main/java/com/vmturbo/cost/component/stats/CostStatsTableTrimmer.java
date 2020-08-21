package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.vmturbo.cost.component.stats.CostStatTable.TableInfo;
import com.vmturbo.cost.component.stats.CostStatTable.Trimmer;

/**
 * Class for trimming the Cost Stats Tables.
 */
public class CostStatsTableTrimmer implements Trimmer {

    private final Logger logger = LogManager.getLogger(getClass());

    private final DSLContext dslContext;

    private final TableInfo tableInfo;

    protected final Clock clock;

    /**
     * Constructor for the Cost Stats Table trimmer.
     *
     * @param dslContext the dsl context.
     * @param clock The clock.
     * @param tableInfo The table info about the table to trim
     */
    public CostStatsTableTrimmer(final DSLContext dslContext,
                                 final Clock clock, final TableInfo tableInfo) {
        this.dslContext = dslContext;
        this.clock = clock;
        this.tableInfo = tableInfo;
    }


    @Override
    public void trim(@Nonnull final LocalDateTime trimToTime) throws DataAccessException {
        logger.debug("Started trimming table {}", tableInfo.shortTableName());
        Stopwatch stopwatch = Stopwatch.createStarted();
        final int numRowsDeleted = dslContext.deleteFrom(tableInfo.statTable())
                .where(tableInfo.statTableSnapshotTime().lessThan(trimToTime))
                .execute();
        logger.info("Deleted {} entries from the {} table in {}", numRowsDeleted, tableInfo.statTable(), stopwatch);
    }
}
