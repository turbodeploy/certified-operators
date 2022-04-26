package com.vmturbo.market.api;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.market.component.api.ProjectedCommitmentMappingListener;

/**
 * A test listener to test the commitment mappings notifications.
 */
public class CommitmentMappingListenerForTest implements ProjectedCommitmentMappingListener {


    private CountDownLatch latch;



    private List<CloudCommitmentMapping> mapingList = new ArrayList<>();
    private long topologyId;
    private TopologyInfo topologyInfo;


    @Override
    public void onProjectedCommitmentMappingReceived(long topologyId, TopologyInfo topologyInfo,
            RemoteIterator<CloudCommitmentMapping> iterator) {
        this.topologyId = topologyId;
        this.topologyInfo = TopologyInfo.newBuilder()
                            .setTopologyId(topologyInfo.getTopologyId())
                            .setTopologyContextId(topologyInfo.getTopologyContextId())
                            .setTopologyType(topologyInfo.getTopologyType()).build();
        try {
            this.mapingList.addAll(iterator.nextChunk());
        } catch (InterruptedException e) {
                // do nothing, the calling test should fail
        } catch (TimeoutException e) {
            // do nothing, the calling test should fail
        } catch (CommunicationException e) {
            // do nothing, the calling test should fail
        } finally {
            latch.countDown();
        }
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public void setLatch(final CountDownLatch latch) {
        this.latch = latch;
    }

    public List<CloudCommitmentMapping> getMapingList() {
        return mapingList;
    }

    public long getTopologyId() {
        return topologyId;
    }

    public TopologyInfo getTopologyInfo() {
        return topologyInfo;
    }
}
