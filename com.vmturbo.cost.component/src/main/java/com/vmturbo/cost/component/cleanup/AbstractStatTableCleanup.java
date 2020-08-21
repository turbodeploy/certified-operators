package com.vmturbo.cost.component.cleanup;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher;

/**
 * An abstract implementation of {@link CostTableCleanup} for stats tables.
 */
public abstract class AbstractStatTableCleanup implements CostTableCleanup {

    protected final DSLContext dslContext;
    protected final Clock clock;
    protected final TableInfo tableInfo;
    protected final RetentionPeriodFetcher retentionPeriodFetcher;

    /**
     * Constructor for an abstract stat table cleanup.
     *
     * @param dslContext the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     * @param retentionPeriodFetcher A fetcher to resolve the stats retention periods.
     */
    public AbstractStatTableCleanup(@Nonnull final DSLContext dslContext,
                                    @Nonnull final Clock clock,
                                    @Nonnull final TableInfo tableInfo,
                                    @Nonnull RetentionPeriodFetcher retentionPeriodFetcher) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.clock = Objects.requireNonNull(clock);
        this.tableInfo = Objects.requireNonNull(tableInfo);
        this.retentionPeriodFetcher = Objects.requireNonNull(retentionPeriodFetcher);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Trimmer writer() {
        return new CostTableTrimmer(dslContext, tableInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public TableInfo tableInfo() {
        return tableInfo;
    }
}
