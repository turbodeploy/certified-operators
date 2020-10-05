package com.vmturbo.cloud.commitment.analysis.inventory;

import java.util.List;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;

/**
 * Used for querying and getting the cloud commitment defined in the scope of CCA analysis. For example,
 * will retrive all RI's from the reservedInstanceBoughtStore defined in the scope of the plan.
 *
 */
public interface CloudCommitmentBoughtResolver {

    /**
     * Given the ids of the cloud commitments in the CCA analysis request, retrieves the CCA objects.
     *
     * @param cloudCommitmentList The list of ids defined in the cca protobuf.
     *
     * @return A list representing the cloud commitments referenced by the ids in the cloudCommitmentList.
     */
    List<CloudCommitmentData> getCloudCommitment(CloudCommitmentInventory cloudCommitmentList);
}
