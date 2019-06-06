/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.writers;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * {@link PercentileWriter} writes information about percentile into database.
 */
public class PercentileWriter extends AbstractStatsWriter {

    @Override
    protected int process(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull Collection<TopologyEntityDTO> objects) {
        // TODO alexandr.vasin implement percentile writer
        return 0;
    }
}
