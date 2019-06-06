/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.writers;

import java.util.Collection;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.IStatsWriter;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * {@link AbstractStatsWriter} provides common functionality for all statistic writers.
 */
public abstract class AbstractStatsWriter implements IStatsWriter {
    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public int processObjects(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull Collection<TopologyEntityDTO> objects) throws VmtDbException {
        try (DataMetricTimer dataMetricTimer = SharedMetrics.STATISTICS_AGGREGATION_SUMMARY
                        .labels(getClass().getSimpleName()).startTimer()) {
            logger.trace("Starting to process '{}' objects for '{}'", objects::size,
                            topologyInfo::toString);
            try {
                return process(topologyInfo, objects);
            } finally {
                logger.debug("Processed '{}' objects for '{}' in '{}'", objects::size,
                                topologyInfo::toString, dataMetricTimer::getTimeElapsedSecs);
            }
        }
    }

    /**
     * Aggregates and writes statistics from collection of the topology entities, without measuring
     * time taken by aggregation and writing.
     *
     * @param topologyInfo generic information about topology chunk which is going
     *                 to be aggregated and written into database.
     * @param objects collection of objects which needs to be aggregated and results
     *                 of aggregation would be written in DB.
     * @return number of processed objects
     * @throws VmtDbException in case data could not be properly written into
     *                 database.
     */
    protected abstract int process(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull Collection<TopologyEntityDTO> objects) throws VmtDbException;
}
