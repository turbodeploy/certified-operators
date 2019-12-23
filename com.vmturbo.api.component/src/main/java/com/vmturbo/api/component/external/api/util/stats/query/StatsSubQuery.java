package com.vmturbo.api.component.external.api.util.stats.query;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;

/**
 * A sub-query of a high level stats query, responsible for getting specific stats from the
 * components that contain them.
 */
public interface StatsSubQuery {

    /**
     * Return whether or not the sub-query supports the scope in the {@link StatsQueryContext}.
     *
     * <p>Note - we pass in the full context so that auxiliary/shared information is available.</p>
     *
     * @param context the stats query context for this request
     * @return whether or not the sub-query supports the scope in the {@link StatsQueryContext}
     */
    boolean applicableInContext(@Nonnull StatsQueryContext context);

    /**
     * Return the stats this sub-query can support in the provided context.
     *
     * @param context the stats query context for this request
     * @return the stats that this sub-query can support in the provided context
     */
    SubQuerySupportedStats getHandledStats(@Nonnull StatsQueryContext context);

    /**
     * Get aggregated stats for this sub-query.
     *
     * <p>"Aggregated" means that there will be a single
     * {@link StatApiDTO} representing all entities for each (stat, time) tuple.</p>
     *
     * <p>The results are organized into a list of {@link StatSnapshotApiDTO}, each
     * representing a particular stats snapshot time. Within a particular stat snapshot, a
     * particular stat should appear at most once. Results are unordered</p>
     *
     * @param stats The requested stats. If empty, the request is for all stats supported by
     *              this sub-query.
     * @param context The context of the high level stats query.
     * @return a list of {@link StatSnapshotApiDTO}, each representing a particular stats snapshot
     *         time. There should be one stat per (snapshot, name) tuple.
     * @throws OperationFailedException If anything goes wrong.
     */
    @Nonnull
    List<StatSnapshotApiDTO> getAggregateStats(
        @Nonnull Set<StatApiInputDTO> stats,
        @Nonnull StatsQueryContext context) throws OperationFailedException;

    /**
     * Determine whether the set of requested stats contains a specific stat.
     *
     * @param statName the name of the stat to check for inclusion
     * @param requestedStats the set of stats requested
     * @return true if the provided stat is contained within the set of requested stats
     */
    default boolean containsStat(@Nonnull String statName,
                                 @Nonnull Set<StatApiInputDTO> requestedStats) {
        if (requestedStats.isEmpty()) {
            return true;
        } else {
            return requestedStats.stream()
                .anyMatch(stat -> stat.getName().equalsIgnoreCase(statName));
        }
    }
}
