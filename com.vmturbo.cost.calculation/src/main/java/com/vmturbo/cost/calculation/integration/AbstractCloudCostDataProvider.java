package com.vmturbo.cost.calculation.integration;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.TopologyCommitmentData;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Abstract class responsible for getting cloud commitment data from the CloudTopology.
 */
public abstract class AbstractCloudCostDataProvider implements CloudCostDataProvider<TopologyDTO.TopologyEntityDTO> {
    protected static Map<Long, CloudCommitmentData<TopologyDTO.TopologyEntityDTO>> getCloudCommitments(CloudTopology<TopologyDTO.TopologyEntityDTO> cloudTopo) {
        return cloudTopo.getAllEntitiesOfType(CommonDTO.EntityDTO.EntityType.CLOUD_COMMITMENT_VALUE).stream()
                .map(entity -> TopologyCommitmentData.builder()
                        .commitment(entity)
                        .build())
                .collect(Collectors.toMap(data -> data.commitmentId(), Function.identity()));
    }
}
