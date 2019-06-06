/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats;

/**
 * {@link ICompleteTopologyStatsWriter} marker interface which emphasizes the fact that only
 * complete topology can be processed by those writers.
 */
public interface ICompleteTopologyStatsWriter extends IStatsWriter {
}
