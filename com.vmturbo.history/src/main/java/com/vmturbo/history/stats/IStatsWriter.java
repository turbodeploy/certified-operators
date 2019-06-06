/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.VmtDbException;

/**
 * {@link IStatsWriter} aggregates and writes statistics from collection of topology entities.
 */
public interface IStatsWriter {
    /**
     * Aggregates and writes statistics from collection of the topology entities.
     *
     * @param topologyInfo generic information about topology chunk which is going
     *                 to be aggregated and written into database.
     * @param objects collection of objects which needs to be aggregated and results
     *                 of aggregation would be written in DB.
     * @return number of processed objects
     * @throws VmtDbException in case data could not be properly written into
     *                 database.
     */
    int processObjects(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull Collection<TopologyEntityDTO> objects) throws VmtDbException;

}
