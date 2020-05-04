package com.vmturbo.topology.processor.group.policy.application;

import static com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper.resolvedGroup;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.GroupResolutionException;
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
    public void setup() throws GroupResolutionException {
        super.group1 = PolicyGroupingHelper.policyGrouping(
                searchParametersCollection(), EntityType.PHYSICAL_MACHINE_VALUE, 1234L);

        super.group2 = PolicyGroupingHelper.policyGrouping(
                searchParametersCollection(), EntityType.PHYSICAL_MACHINE_VALUE, 5678L);

        final Grouping group = PolicyGroupingHelper.policyGrouping(
            searchParametersCollection(), EntityType.DATACENTER_VALUE, 111L);

        super.mergePolicyEntities= Lists.newArrayList(new PolicyEntities(group));
        super.mergePolicy = PolicyDTO.PolicyInfo.MergePolicy.newBuilder()
            .addAllMergeGroupIds(Collections.singletonList(group.getId()))
            .setMergeType(PolicyInfo.MergePolicy.MergeType.DATACENTER)
            .build();

        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE, 7));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE, 8));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 2));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE));
        topologyMap.put(7L, topologyEntity(7L, EntityType.DATACENTER));
        topologyMap.put(8L, topologyEntity(8L, EntityType.DATACENTER));

        super.topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        super.policyMatcher = new PolicyMatcher(topologyGraph);

        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
            .thenReturn(resolvedGroup(group, 7L, 8L));
    }

    @Test
    public void testApplyEmpty() {}

    @Test
    public void testApplyEmptyGroup1() {}

    @Test
    public void testEmptyTopologyGraph() {}
}