/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.writers;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.VmtDbException;

/**
 * {@link HistUtilizationWriter} writes historical utilization into database.
 */
public class HistUtilizationWriter extends AbstractStatsWriter {

    @Override
    protected int process(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull Collection<TopologyEntityDTO> objects) throws VmtDbException {
        // TODO alexandr.vasin implement aggregation and writing into database.
        return 0;
    }
}
