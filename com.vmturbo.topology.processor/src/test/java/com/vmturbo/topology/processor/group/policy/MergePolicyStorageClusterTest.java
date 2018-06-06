package com.vmturbo.topology.processor.group.policy;

import static com.vmturbo.topology.processor.group.filter.FilterUtils.topologyEntity;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.policy.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * The tests use the following topology:
 *
 *  VM4 VM6 VM5
 *   |      |
 *   |      |
 *  ST1     ST2  PM3
 */
public class MergePolicyStorageClusterTest extends MergePolicyTestBase {

    @Before
    public void setup() {
        super.mergePolicy = PolicyInfo.MergePolicy.newBuilder()
                .addAllMergeGroupIds(mergeGropuIds)
                .setMergeType(PolicyInfo.MergePolicy.MergeType.STORAGE_CLUSTER)
                .build();
        super.group1 = PolicyGroupingHelper.policyGrouping(
                searchParametersCollection(), EntityType.STORAGE_VALUE, 1234L);
        super.group2 = PolicyGroupingHelper.policyGrouping(
                searchParametersCollection(), EntityType.STORAGE_VALUE, 5678L);
        super.mergePolicyEntities= Lists.newArrayList(new PolicyEntities(group2),
                new PolicyEntities(group1));
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.STORAGE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.STORAGE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 2));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE));

        super.topologyGraph = TopologyGraph.newGraph(topologyMap);
        super.policyMatcher = new PolicyMatcher(topologyGraph);
    }

}