package com.vmturbo.topology.processor.group.policy.application;

import static com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper.resolvedGroup;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper;
import com.vmturbo.topology.processor.group.policy.PolicyMatcher;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * The tests use the following topology:
 *
 *  VM4 VM6 VM5
 *   |      |
 *   |      |
 *  ST1     ST2  PM3
 */
public class MergePolicyStorageClusterTest extends MergePolicyTestBase {

    private static String ISO_STORAGE_CLUSTER_KEY = "ISO-Storage::abcd";
    private static String REAL_STORAGE_CLUSTER_KEY = "StorageCluster::qwerty";

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

        super.topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        super.policyMatcher = new PolicyMatcher(topologyGraph);
    }

    @Test
    public void testChangeOnlyRealStorageClusterCommodityKey() throws Exception {
        addStorageClusterCommSold(1L);
        addStorageClusterCommSold(2L);
        addStorageClusterCommBought(4L);
        addStorageClusterCommBought(5L);
        // assign merge policy to Policy
        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
            .setId(super.POLICY_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setMerge(mergePolicy))
            .build();

        // setup mocks
        when(groupResolver.resolve(eq(group1), eq(topologyGraph)))
            .thenReturn(resolvedGroup(group1, 4L, 5L));
        when(groupResolver.resolve(eq(group2), eq(topologyGraph)))
            .thenReturn(resolvedGroup(group2, 1L, 2L));

        // invoke Merge Policy
        final MergePolicy mergePolicy = new MergePolicy(policy, mergePolicyEntities);
        applyPolicy(mergePolicy);

        // assert that we change only the key of the real storage cluster commodity comm sold
        int mergeCommCount = 0;
        int isoCommCount = 0;
        List<CommoditySoldDTO> commoditySoldDTOList = topologyGraph.getEntity(1L).get()
            .getTopologyEntityDtoBuilder().getCommoditySoldListList();
        for (CommoditySoldDTO commSold : commoditySoldDTOList) {
            if (commSold.getCommodityType().getKey().equals(Long.toString(mergePolicy.getPolicyDefinition().getId()))) {
                mergeCommCount++;
            } else if (commSold.getCommodityType().getKey().equals(ISO_STORAGE_CLUSTER_KEY)) {
                isoCommCount ++;
            }
        }
        assertEquals(1, mergeCommCount);
        assertEquals(1, isoCommCount);

        // assert that we change only the key of the real storage cluster commodity comm bought
        mergeCommCount = isoCommCount = 0;
        List<CommodityBoughtDTO> commsBought = topologyGraph.getEntity(4L).get()
            .getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProviders(0).getCommodityBoughtList();
        for (CommodityBoughtDTO commBought : commsBought) {
            if (commBought.getCommodityType().getKey().equals(Long.toString(mergePolicy.getPolicyDefinition().getId()))) {
                mergeCommCount++;
            } else if (commBought.getCommodityType().getKey().equals(ISO_STORAGE_CLUSTER_KEY)) {
                isoCommCount ++;
            }
        }
        assertEquals(1, mergeCommCount);
        assertEquals(1, isoCommCount);
    }

    private void addStorageClusterCommSold(long oid) {
        TopologyEntity storage = super.topologyGraph.getEntity(oid).get();
        CommoditySoldDTO.Builder isoStClusterSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(storageClusterCommodityType(ISO_STORAGE_CLUSTER_KEY));
        CommoditySoldDTO.Builder realStClusterSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(storageClusterCommodityType(REAL_STORAGE_CLUSTER_KEY));
        storage.getTopologyEntityDtoBuilder()
            .addCommoditySoldList(isoStClusterSold)
            .addCommoditySoldList(realStClusterSold);
    }

    private void addStorageClusterCommBought(long oid) {
        TopologyEntity vm = super.topologyGraph.getEntity(oid).get();
        CommodityBoughtDTO.Builder isoStClusterBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(storageClusterCommodityType(ISO_STORAGE_CLUSTER_KEY));
        CommodityBoughtDTO.Builder realStClusterBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(storageClusterCommodityType(REAL_STORAGE_CLUSTER_KEY));
        CommoditiesBoughtFromProvider.Builder commBoughtFromProvider = vm.getTopologyEntityDtoBuilder()
            .getCommoditiesBoughtFromProvidersBuilder(0);
        commBoughtFromProvider.addCommodityBought(isoStClusterBought);
        commBoughtFromProvider.addCommodityBought(realStClusterBought);
    }

    private CommodityType.Builder storageClusterCommodityType(String key) {
        return CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)
            .setKey(key);
    }
}