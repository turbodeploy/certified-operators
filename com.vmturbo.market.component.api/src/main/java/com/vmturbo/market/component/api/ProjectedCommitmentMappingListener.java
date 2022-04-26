package com.vmturbo.market.component.api;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * Listener interface for the projected CC mappings.
 */
public interface ProjectedCommitmentMappingListener {

    /**
     * handler for the projectedCommitmentMapping.
     * @param topologyInfo topology info.
     * @param iterator the projCommitmentMappingListenerForTest.javaected mappings iterator
     */
    void onProjectedCommitmentMappingReceived(long topologyId, TopologyInfo topologyInfo,
            RemoteIterator<CloudCommitmentMapping> iterator);

}
