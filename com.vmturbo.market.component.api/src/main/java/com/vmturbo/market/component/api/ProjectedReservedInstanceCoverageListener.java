package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * A listener for projected per-entity reserved instance coverage broadcast by
 * the market as part of its results.
 */
public interface ProjectedReservedInstanceCoverageListener {

	/**
	 * This method will be called when new information about projected entity
	 * reserved instance coverage is broadcast by the market.
	 *
	 * @param projectedTopologyId
	 *            The ID of the projected topology. This is the topology the costs
	 *            are calculated from.
	 * @param originalTopologyInfo
	 *            Information about the original topology the analysis was ran on.
	 * @param projectedEntityRiCoverage
	 *            A {@link RemoteIterator} over the projected entity reserved
	 *            instance coverage for entities in the projected topology.
	 */
	void onProjectedEntityRiCoverageReceived(final long projectedTopologyId,
			@Nonnull final TopologyInfo originalTopologyInfo,
			@Nonnull final RemoteIterator<EntityReservedInstanceCoverage> projectedEntityRiCoverage);

}
