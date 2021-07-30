package com.vmturbo.action.orchestrator.stats.rollup.v2;

import java.time.Clock;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.action.orchestrator.stats.rollup.RolledUpStatCalculator;
import com.vmturbo.action.orchestrator.stats.rollup.export.RollupExporter;

/**
 * Factory class for {@link HourActionStatRollup}s.
 */
public class HourActionRollupFactory {
    private final RollupExporter rollupExporter;
    private final RolledUpStatCalculator rolledUpStatCalculator;
    private final Clock clock;

    HourActionRollupFactory(final RollupExporter rollupExporter,
            final RolledUpStatCalculator rolledUpStatCalculator,
            final Clock clock) {
        this.rollupExporter = rollupExporter;
        this.rolledUpStatCalculator = rolledUpStatCalculator;
        this.clock = clock;
    }

    @Nonnull
    HourActionStatRollup newRollup(DSLContext transactionContext, LocalDateTime hourTime, int numSnapshots) {
        return new HourActionStatRollup(hourTime, numSnapshots, rollupExporter, rolledUpStatCalculator, transactionContext, clock);
    }
}
