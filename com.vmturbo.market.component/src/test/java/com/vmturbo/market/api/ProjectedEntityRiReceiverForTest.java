package com.vmturbo.market.api;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.market.component.api.ProjectedReservedInstanceCoverageListener;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * @author nitya
 *
 */
public class ProjectedEntityRiReceiverForTest implements ProjectedReservedInstanceCoverageListener {

	// Logger
	private static final Logger logger = LogManager.getLogger();
	// RemoteIterator needs to be saved into a local variable so this
	// data is available for testing purposes
	private List<EntityReservedInstanceCoverage> receivedCoverageList;
	// projected topology id
	private long projectedTopoId;
	// original topology information for which data is being sent
	private TopologyInfo origTopoInfo;
	// Countdown latch for receiver to notify about processing complete
	private CountDownLatch latch;

	/**
	 *
	 * @return
	 */
	public CountDownLatch getLatch() {
		return latch;
	}

	/**
	 *
	 * @param latch
	 */
	public void setLatch(CountDownLatch latch) {
		this.latch = latch;
	}

	/**
	 *
	 * @return
	 */
	public long getProjectedTopoId() {
		return projectedTopoId;
	}

	/**
	 *
	 * @return
	 */
	public TopologyInfo getOrigTopoInfo() {
		return origTopoInfo;
	}

	/**
	 * Gets the data that was received from sender
	 * @return
	 */
	public List<EntityReservedInstanceCoverage> getReceivedCoverageList() {
		return receivedCoverageList;
	}


    @Override
    public void onProjectedEntityRiCoverageReceived(long projectedTopologyId, TopologyInfo originalTopologyInfo,
			RemoteIterator<EntityReservedInstanceCoverage> projectedEntityRiCoverage) {
		this.projectedTopoId = projectedTopologyId;
		this.origTopoInfo = originalTopologyInfo;
		while (projectedEntityRiCoverage.hasNext()) {
            try {
                final Collection<EntityReservedInstanceCoverage> nextChunk = projectedEntityRiCoverage.nextChunk();
                receivedCoverageList = Lists.newArrayList(nextChunk);

            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for processing projected entity ri coverage chunk.",e);
            } catch (TimeoutException e) {
                logger.error("Timed out waiting for next entity costs chunk.", e);
            } catch (CommunicationException e) {
                logger.error("Connection error when waiting for next entity costs chunk.", e);
            } finally {
                latch.countDown();
            }
		}
	}
}
