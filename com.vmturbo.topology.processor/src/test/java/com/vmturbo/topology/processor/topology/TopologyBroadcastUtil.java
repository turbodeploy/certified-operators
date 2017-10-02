package com.vmturbo.topology.processor.topology;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * Utilities for tests of topology broadcasts.
 */
public class TopologyBroadcastUtil {
    public static Set<TopologyEntityDTO> accumulate(RemoteIterator<TopologyEntityDTO> iterator)
            throws InterruptedException, TimeoutException, CommunicationException {
        final Set<TopologyEntityDTO> result = new HashSet<>();
        while (iterator.hasNext()) {
            result.addAll(iterator.nextChunk());
        }
        return result;
    }
}
