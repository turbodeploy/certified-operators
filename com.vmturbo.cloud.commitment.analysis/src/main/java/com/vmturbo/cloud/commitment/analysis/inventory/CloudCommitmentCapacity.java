package com.vmturbo.cloud.commitment.analysis.inventory;

import org.immutables.value.Value.Immutable;

/**
 * Interface representing the capacity of CCA available in the inventory. The capacity is broken down
 * into time segments.
 */
@Immutable
public interface CloudCommitmentCapacity {

    /**
     * The capacity offered by the cloud commitment in the current inventory of the CCA plan scope.
     *
     * @return capacity represented by a double.
     */
    double capacityAvailable();
}
