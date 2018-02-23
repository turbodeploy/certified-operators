package com.vmturbo.topology.processor.group.policy;

import static com.vmturbo.topology.processor.group.filter.FilterUtils.topologyEntity;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.assertj.core.util.Lists;
import org.junit.Before;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.MergeType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.policy.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyGraph;


/**
 * The tests use the following topology:
 *
 *  VM4 VM6  VM5
 *   |       |
 *   |       |
 *  PM1      PM2  ST3
 */
public class MergePolicyComputationClusterTest extends MergePolicyTestBase {

    @Before
    public void setup() {
        super.mergePolicy = PolicyDTO.Policy.MergePolicy.newBuilder()
                .addAllMergeGroupIds(mergeGropuIds)
                .setMergeType(MergeType.CLUSTER)
                .build();
        super.group1 = PolicyGroupingHelper.policyGrouping(
                searchParametersCollection(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);

        super.group2 = PolicyGroupingHelper.policyGrouping(
                searchParametersCollection(), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);
        super.mergePolicyEntities= Lists.newArrayList(new PolicyEntities(group2),
                new PolicyEntities(group1));

        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 2));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE));

        super.topologyGraph = TopologyGraph.newGraph(topologyMap);
        super.policyMatcher = new PolicyMatcher(topologyGraph);
    }
}