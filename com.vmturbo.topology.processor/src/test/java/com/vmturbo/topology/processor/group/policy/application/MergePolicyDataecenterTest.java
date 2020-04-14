package com.vmturbo.topology.processor.group.policy.application;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;

import org.junit.Before;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper;
import com.vmturbo.topology.processor.group.policy.PolicyMatcher;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;


/**
 * The tests use the following topology:
 *
 *  VM4 VM6  VM5
 *   |       |
 *   |       |
 *  PM1      PM2  ST3
 */
public class MergePolicyDataecenterTest extends MergePolicyTestBase {

    @Before
    public void setup() {
        super.mergePolicy = PolicyDTO.PolicyInfo.MergePolicy.newBuilder()
                .addAllMergeGroupIds(mergeGropuIds)
                .setMergeType(PolicyInfo.MergePolicy.MergeType.DATACENTER)
                .build();
        super.group1 = PolicyGroupingHelper.policyGrouping(
                searchParametersCollection(), EntityType.PHYSICAL_MACHINE_VALUE, 1234L);

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

        super.topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        super.policyMatcher = new PolicyMatcher(topologyGraph);
    }
}