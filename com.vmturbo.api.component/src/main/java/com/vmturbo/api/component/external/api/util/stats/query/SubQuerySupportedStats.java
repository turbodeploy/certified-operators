package com.vmturbo.api.component.external.api.util.stats.query;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.statistic.StatApiInputDTO;

/**
 * Encapsulates the stats supported by a particular {@link StatsSubQuery}.
 */
public class SubQuerySupportedStats {

    /**
     * See {@link SubQuerySupportedStats#supportsLeftovers()}.
     */
    private final boolean supportsLeftovers;

    /**
     * The stats explicitly handled by this sub-query.
     * <p>
     * A sub-query can declare that it wants to handle some {@link StatApiInputDTO}s from a
     * stat request. If it does so, no other sub-query will be able to handle those
     * {@link StatApiInputDTO}s.
     */
    private final Set<StatApiInputDTO> supportedStats;

    private SubQuerySupportedStats(final boolean supportsLeftovers,
                                   @Nonnull final Set<StatApiInputDTO> supportedStats) {
        this.supportsLeftovers = supportsLeftovers;
        this.supportedStats = supportedStats;
    }

    public static SubQuerySupportedStats leftovers() {
        return new SubQuerySupportedStats(true, Collections.emptySet());
    }

    public static SubQuerySupportedStats none() {
        return new SubQuerySupportedStats(false, Collections.emptySet());
    }

    public static SubQuerySupportedStats some(@Nonnull final Set<StatApiInputDTO> handledStats) {
        return new SubQuerySupportedStats(false, handledStats);
    }

    /**
     * If true, this sub-query does not support a specific set of stats, but should support all
     * leftover stats not explicitly handled by any other sub-query.
     */
    public boolean supportsLeftovers() {
        return supportsLeftovers;
    }

    /**
     * Check if this sub-query explicitly supports a particular stat.
     */
    public boolean containsExplicitStat(@Nonnull final StatApiInputDTO stat) {
        return supportedStats.contains(stat);
    }
}
