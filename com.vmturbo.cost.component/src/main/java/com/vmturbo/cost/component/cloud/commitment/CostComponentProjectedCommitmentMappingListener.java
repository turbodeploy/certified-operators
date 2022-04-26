package com.vmturbo.cost.component.cloud.commitment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * The projected mappings listener.
 */
public class CostComponentProjectedCommitmentMappingListener implements
        com.vmturbo.market.component.api.ProjectedCommitmentMappingListener {

    private final Logger logger = LogManager.getLogger(CostComponentProjectedCommitmentMappingListener.class);
    private final ProjectedCommitmentMappingProcessor projectedCommitmentMappingProcessor;

    CostComponentProjectedCommitmentMappingListener(@Nonnull final ProjectedCommitmentMappingProcessor projectedCommitmentMappingProcessor) {
        this.projectedCommitmentMappingProcessor = Objects.requireNonNull(projectedCommitmentMappingProcessor);
    }

    @Override
    public void onProjectedCommitmentMappingReceived(@Nonnull final long topologyId,
                                                     @Nonnull final TopologyInfo topologyInfo,
                                                     @Nonnull final RemoteIterator<CloudCommitmentMapping> iterator) {
        logger.info("Received mappings: " + topologyId);
        final List<CloudCommitmentMapping> cloudCommitmentMappingList = new ArrayList<>();
        int chunkCount = 0;
        try {
            while (iterator.hasNext()) {
                try {
                    final Collection<CloudCommitmentMapping> nextChunk = iterator.nextChunk();
                    for (CloudCommitmentMapping cloudCommitment : nextChunk) {
                        cloudCommitmentMappingList.add(cloudCommitment);
                    }
                    chunkCount++;
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for processing projected cloud commitment mapping chunk."
                            + "Processed " + chunkCount + " chunks so far.", e);
                } catch (TimeoutException e) {
                    logger.error("Timed out waiting for next entity cloud commitment mapping  chunk."
                            + " Processed " + chunkCount + " chunks so far.", e);
                } catch (CommunicationException e) {
                    logger.error("Connection error when waiting for next entity cloud commitment mapping  chunk."
                            + " Processed " + chunkCount + " chunks so far.", e);
                }
            }
            projectedCommitmentMappingProcessor.mappingsAvailable(topologyId, topologyInfo, cloudCommitmentMappingList);
        } catch (Exception e) {
            logger.error("Error in processing the projected cloud commitment mapping. Processed "
                    + chunkCount + " chunks so far.", e);
        }
    }
}
