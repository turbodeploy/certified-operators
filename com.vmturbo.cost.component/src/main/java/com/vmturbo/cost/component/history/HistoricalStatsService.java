package com.vmturbo.cost.component.history;

import java.util.List;

import com.vmturbo.common.protobuf.stats.Stats;

/**
 * Defines methods for interacting with the history stats service.
 */
public interface HistoricalStatsService {

    /**
     * Returns historical statistics for the specified OIDs, commodities, and number of days of requested history.
     *
     * @param oids         A list of OIDs for which to retrieve historical stats.
     * @param commodities  A list of commodities for which to retrieve historical stats.
     * @param numberOfDays The number of historical days for which to retrieve historical stats.
     * @return A list of the requested EntityStats.
     */
    List<Stats.EntityStats> getHistoricalStats(List<Long> oids, List<String> commodities, int numberOfDays);
}
