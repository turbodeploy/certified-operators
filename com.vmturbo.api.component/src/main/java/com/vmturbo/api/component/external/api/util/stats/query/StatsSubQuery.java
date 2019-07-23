package com.vmturbo.api.component.external.api.util.stats.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;

/**
 * A sub-query of a high level stats query, responsible for getting specific stats from the
 * components that contain them.
 */
public interface StatsSubQuery {

    /**
     * Return whether or not the sub-query supports the scope in the {@link StatsQueryContext}.
     * Note - we pass in the full context so that auxiliary/shared information is available.
     */
    boolean applicableInContext(@Nonnull final StatsQueryContext context);

    /**
     * Return the stats this sub-query can support in the provided context.
     */
    SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context);

    /**
     * Get aggregated stats for this sub-query. "Aggregated" means that there will be a single
     * {@link StatApiDTO} representing all entities for each (stat, time) tuple.
     *
     * @param stats The requested stats. If empty, the request is for all stats supported by
     *              this sub-query.
     * @param context The context of the high level stats query.
     * @return (snapshot time) -> (stats). There should be one stat per (snapshot, name) tuple.
     * @throws OperationFailedException If anything goes wrong.
     */
    @Nonnull
    Map<Long, List<StatApiDTO>> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                  @Nonnull final StatsQueryContext context) throws OperationFailedException;

    default boolean containsStat(@Nonnull final String statName,
                                 @Nonnull final Set<StatApiInputDTO> requestedStats) {
        if (requestedStats.isEmpty()) {
            return true;
        } else {
            return requestedStats.stream()
                .anyMatch(stat -> stat.getName().equalsIgnoreCase(statName));
        }
    }
}
