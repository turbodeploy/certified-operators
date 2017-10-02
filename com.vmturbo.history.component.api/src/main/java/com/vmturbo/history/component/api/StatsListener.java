package com.vmturbo.history.component.api;

import javax.annotation.Nonnull;

import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;

/**
 * A listener for updates about statistics availability in the history component.
 */
public interface StatsListener {
    /**
     * Indicates that the history component has new statistics available for a plan topology.
     *
     * Use the topology context id in the message to determine if the stats are for a plan
     * or live topology.
     *
     * @param statsAvailable A message describing the plan for which stats are available.
     */
    void onStatsAvailable(@Nonnull final StatsAvailable statsAvailable);
}
