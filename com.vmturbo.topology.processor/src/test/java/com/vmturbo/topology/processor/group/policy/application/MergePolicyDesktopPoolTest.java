package com.vmturbo.topology.processor.group.policy.application;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper;
import com.vmturbo.topology.processor.group.policy.PolicyMatcher;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

/**
 * The tests use the following topology.
 * BU4 BU6 BU5
 * |       |
 * |       |
 * DP1     DP2  VM3
 */
public class MergePolicyDesktopPoolTest extends MergePolicyTestBase {

    /**
     * Test initialization.
     */
    @Before
    public void setup() {
        mergePolicy = PolicyDTO.PolicyInfo.MergePolicy.newBuilder()
                .addAllMergeGroupIds(mergeGropuIds)
                .setMergeType(MergeType.DESKTOP_POOL)
                .build();
        group1 = PolicyGroupingHelper.policyGrouping(PolicyMatcher.searchParametersCollection(),
                EntityType.BUSINESS_USER_VALUE, CONSUMER_ID);
        group2 = PolicyGroupingHelper.policyGrouping(PolicyMatcher.searchParametersCollection(),
                EntityType.DESKTOP_POOL_VALUE, PROVIDER_ID);
        mergePolicyEntities = Arrays.asList(new PolicyEntities(group2), new PolicyEntities(group1));
        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(
                Stream.of(TopologyEntityUtils.topologyEntity(1L, EntityType.DESKTOP_POOL),
                        TopologyEntityUtils.topologyEntity(2L, EntityType.DESKTOP_POOL),
                        TopologyEntityUtils.topologyEntity(3L, EntityType.VIRTUAL_MACHINE),
                        TopologyEntityUtils.topologyEntity(4L, EntityType.BUSINESS_USER, 1),
                        TopologyEntityUtils.topologyEntity(5L, EntityType.BUSINESS_USER, 2),
                        TopologyEntityUtils.topologyEntity(6L, EntityType.BUSINESS_USER))
                        .collect(Collectors.toMap(Builder::getOid, e -> e)));
        policyMatcher = new PolicyMatcher(topologyGraph);
    }
}