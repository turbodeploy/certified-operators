package com.vmturbo.topology.processor.group.policy.application;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;
import static com.vmturbo.topology.processor.group.policy.PolicyMatcher.searchParametersCollection;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper;
import com.vmturbo.topology.processor.group.policy.PolicyMatcher;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * The tests use the following topology:
 *  VM5  VM8 VM6   VM7
 *   |     \  |  \/ |
 *   |      \ | / \ |
 *  PM1      PM2   ST3 ST4
 */
public class MustNotRunTogetherPolicyTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // vm group (consumers)
    private final Grouping group = PolicyGroupingHelper.policyGrouping(
        searchParametersCollection(), EntityType.VIRTUAL_MACHINE_VALUE, 1234L);
    private final long groupID = group.getId();

    // must not run together on host policy
    private final PolicyInfo.MustNotRunTogetherPolicy mustNotRunTogetherPolicy =
        PolicyInfo.MustNotRunTogetherPolicy.newBuilder()
            .setGroupId(groupID)
            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();

    private static final long POLICY_ID = 9999L;
    final PolicyDTO.Policy hostPolicyDefinition = PolicyDTO.Policy.newBuilder()
        .setId(POLICY_ID)
        .setPolicyInfo(PolicyInfo.newBuilder()
            .setMustNotRunTogether(mustNotRunTogetherPolicy))
        .build();

    // must not run together on storage policy
    private final PolicyInfo.MustNotRunTogetherPolicy mustNotRunTogetherOnStoragePolicy =
        PolicyInfo.MustNotRunTogetherPolicy.newBuilder()
            .setGroupId(groupID)
            .setProviderEntityType(EntityType.STORAGE_VALUE)
            .build();

    private static final long POLICY_ST_ID = 9998L;
    final PolicyDTO.Policy storagePolicyDefinition = PolicyDTO.Policy.newBuilder()
            .setId(POLICY_ST_ID)
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setMustNotRunTogether(mustNotRunTogetherOnStoragePolicy))
            .build();

    private final GroupResolver groupResolver = mock(GroupResolver.class);

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.STORAGE));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(8L, topologyEntity(8L, EntityType.VIRTUAL_MACHINE, 2));
        // replacement from template
        topologyMap.put(9L, topologyEntity(9L, EntityType.PHYSICAL_MACHINE));
        topologyMap.get(2L).getEntityBuilder().getEditBuilder().setReplaced(
                TopologyDTO.TopologyEntityDTO.Replaced.newBuilder().setPlanId(7777L).setReplacementId(9L).build());

        TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        PolicyMatcher policyMatcher = new PolicyMatcher(topologyGraph);

        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
            .thenReturn(Collections.emptySet());

        applyPolicy(new MustNotRunTogetherPolicy(hostPolicyDefinition,
            new PolicyEntities(group, Collections.emptySet())), topologyGraph);

        // entities will not buy segmentation, because the group is empty
        assertThat(topologyGraph.getEntity(1L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(2L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(9L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(3L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(5L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(7L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(8L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
    }

    @Test
    public void testApplyVmSeparateOnHost() throws GroupResolutionException, PolicyApplicationException {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.STORAGE));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(8L, topologyEntity(8L, EntityType.VIRTUAL_MACHINE, 2));
        // replacement from template
        topologyMap.put(9L, topologyEntity(9L, EntityType.PHYSICAL_MACHINE));
        topologyMap.get(2L).getEntityBuilder().getEditBuilder().setReplaced(
                TopologyDTO.TopologyEntityDTO.Replaced.newBuilder().setPlanId(7777L).setReplacementId(9L).build());

        TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        PolicyMatcher policyMatcher = new PolicyMatcher(topologyGraph);

        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
                .thenReturn(Sets.newHashSet(6L, 7L));

        applyPolicy(new MustNotRunTogetherPolicy(hostPolicyDefinition,
            new PolicyEntities(group, Collections.emptySet())), topologyGraph);

        // check consumers
        assertThat(topologyGraph.getEntity(5L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));
        assertThat(topologyGraph.getEntity(6L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(7L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));
        assertThat(topologyGraph.getEntity(8L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE)));

        // check providers
        // all the hosts need to sell a segmentation commodity with capacity 1
        assertThat(topologyGraph.getEntity(1L).get(),
                policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, 1.0f));
        assertThat(topologyGraph.getEntity(2L).get(),
                policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, 1.0f));
        // check if replaced host sells segment with capacity 1.0
        assertThat(topologyGraph.getEntity(9L).get(),
                policyMatcher.hasProviderSegmentWithCapacity(POLICY_ID, 1.0f));
        // storages should not sell it
        assertThat(topologyGraph.getEntity(3L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
        assertThat(topologyGraph.getEntity(4L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ID)));
    }

    @Test
    public void testApplyVmSeparateOnStorage() throws GroupResolutionException, PolicyApplicationException {
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE));
        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(4L, topologyEntity(4L, EntityType.STORAGE));
        topologyMap.put(5L, topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 1));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(7L, topologyEntity(7L, EntityType.VIRTUAL_MACHINE, 2, 3));
        topologyMap.put(8L, topologyEntity(8L, EntityType.VIRTUAL_MACHINE, 2));
        // replacement from template
        topologyMap.put(9L, topologyEntity(9L, EntityType.PHYSICAL_MACHINE));
        topologyMap.get(2L).getEntityBuilder().getEditBuilder().setReplaced(
                TopologyDTO.TopologyEntityDTO.Replaced.newBuilder().setPlanId(7777L).setReplacementId(9L).build());

        TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        PolicyMatcher policyMatcher = new PolicyMatcher(topologyGraph);

        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
                .thenReturn(Sets.newHashSet(6L, 7L));

        applyPolicy(new MustNotRunTogetherPolicy(storagePolicyDefinition,
            new PolicyEntities(group, Collections.emptySet())), topologyGraph);

        // check consumers
        assertThat(topologyGraph.getEntity(5L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ST_ID, EntityType.STORAGE)));
        assertThat(topologyGraph.getEntity(6L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ST_ID, EntityType.STORAGE));
        assertThat(topologyGraph.getEntity(7L).get(),
                policyMatcher.hasConsumerSegment(POLICY_ST_ID, EntityType.STORAGE));
        assertThat(topologyGraph.getEntity(8L).get(),
                not(policyMatcher.hasConsumerSegment(POLICY_ST_ID, EntityType.STORAGE)));

        // check providers
        // hosts should not sell it
        assertThat(topologyGraph.getEntity(1L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ST_ID)));
        assertThat(topologyGraph.getEntity(2L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ST_ID)));
        // assert if replaced host sells segment
        assertThat(topologyGraph.getEntity(9L).get(),
                not(policyMatcher.hasProviderSegment(POLICY_ST_ID)));
        // all the storages need to sell a segmentation commodity with capacity 1
        assertThat(topologyGraph.getEntity(3L).get(),
                policyMatcher.hasProviderSegmentWithCapacity(POLICY_ST_ID, 1.0f));
        assertThat(topologyGraph.getEntity(4L).get(),
                policyMatcher.hasProviderSegmentWithCapacity(POLICY_ST_ID, 1.0f));
    }

    @Test
    public void testPMProviderUseClusterKey() {
        testProviderUseClusterKey(EntityType.PHYSICAL_MACHINE);
    }

    @Test
    public void testSTProviderUseClusterKey() {
        testProviderUseClusterKey(EntityType.STORAGE);
    }

    @Test
    public void testPMProviderUseDCKeyIfNoClusterKey() {
        testProviderUseDCKeyIfNoClusterKey(EntityType.PHYSICAL_MACHINE);
    }

    @Test
    public void testSTProviderUseDCKeyIfNoClusterKey() {
        testProviderUseDCKeyIfNoClusterKey(EntityType.STORAGE);
    }


    @Test
    public void testPMProviderApplyToAllIfNoDCOrClusterKey() {
        final TopologyEntity.Builder host1 = topologyEntity(1L, EntityType.PHYSICAL_MACHINE);
        final TopologyEntity.Builder host2 = topologyEntity(3L, EntityType.PHYSICAL_MACHINE);
        final TopologyEntity.Builder vm = topologyEntity(5L, EntityType.VIRTUAL_MACHINE, 1);
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(host1.getOid(), host1);
        topologyMap.put(host2.getOid(), host2);
        topologyMap.put(vm.getOid(), vm);

        TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        PolicyMatcher policyMatcher = new PolicyMatcher(topologyGraph);

        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(vm.getOid()));

        applyPolicy(new MustNotRunTogetherPolicy(hostPolicyDefinition,
            new PolicyEntities(group, Collections.emptySet())), topologyGraph);

        // The VM should be buying the segmentation commodity.
        assertThat(topologyGraph.getEntity(vm.getOid()).get(),
            policyMatcher.hasConsumerSegment(POLICY_ID, EntityType.PHYSICAL_MACHINE));

        // All hosts should be selling the segmentation commodity.
        assertThat(topologyGraph.getEntity(host1.getOid()).get(),
            policyMatcher.hasProviderSegment(POLICY_ID));
        assertThat(topologyGraph.getEntity(host2.getOid()).get(),
            policyMatcher.hasProviderSegment(POLICY_ID));
    }

    private void applyPolicy(@Nonnull final MustNotRunTogetherPolicy policy,
                             @Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        MustNotRunTogetherPolicyApplication application =
            new MustNotRunTogetherPolicyApplication(groupResolver, topologyGraph);
        application.apply(Collections.singletonList(policy));
    }

    private void testProviderUseDCKeyIfNoClusterKey(@Nonnull final EntityType providerType) {
        Preconditions.checkArgument(providerType == EntityType.PHYSICAL_MACHINE ||
            providerType == EntityType.STORAGE);
        final CommodityType dcComm = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.DATACENTER_VALUE)
            .setKey("dc")
            .build();

        final TopologyEntity.Builder dcProvider1 = topologyEntity(1L, providerType);
        dcProvider1.getEntityBuilder()
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(dcComm));
        final TopologyEntity.Builder dcProvider2 = topologyEntity(3L, providerType);
        dcProvider2.getEntityBuilder()
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(dcComm));

        final TopologyEntity.Builder outofDCHost = topologyEntity(2L, providerType);

        final TopologyEntity.Builder vm = topologyEntity(5L, EntityType.VIRTUAL_MACHINE);
        vm.getEntityBuilder()
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(dcProvider1.getOid())
                .setProviderEntityType(providerType.getNumber())
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(dcComm)))
            .build();

        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(dcProvider1.getOid(), dcProvider1);
        topologyMap.put(dcProvider2.getOid(), dcProvider2);
        topologyMap.put(outofDCHost.getOid(), outofDCHost);
        topologyMap.put(vm.getOid(), vm);

        TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        PolicyMatcher policyMatcher = new PolicyMatcher(topologyGraph);

        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(vm.getOid()));

        final PolicyDTO.Policy policyDefinition = providerType == EntityType.PHYSICAL_MACHINE ?
                hostPolicyDefinition : storagePolicyDefinition;

        applyPolicy(new MustNotRunTogetherPolicy(policyDefinition,
            new PolicyEntities(group, Collections.emptySet())), topologyGraph);

        // The VM should be buying the segmentation commodity.
        assertThat(topologyGraph.getEntity(vm.getOid()).get(),
            policyMatcher.hasConsumerSegment(policyDefinition.getId(), providerType));

        // Both hosts selling the same kind of DC commodity should get the segmentation
        // commodity.
        assertThat(topologyGraph.getEntity(dcProvider1.getOid()).get(),
            policyMatcher.hasProviderSegment(policyDefinition.getId()));
        assertThat(topologyGraph.getEntity(dcProvider2.getOid()).get(),
            policyMatcher.hasProviderSegment(policyDefinition.getId()));

        // Hosts NOT selling the DC commodity should NOT get the segmentation commodity.
        assertThat(topologyGraph.getEntity(outofDCHost.getOid()).get(),
            not(policyMatcher.hasProviderSegment(policyDefinition.getId())));
    }

    private void testProviderUseClusterKey(@Nonnull final EntityType providerType) {
        Preconditions.checkArgument(providerType == EntityType.PHYSICAL_MACHINE ||
            providerType == EntityType.STORAGE);
        final boolean pmProvider = providerType == EntityType.PHYSICAL_MACHINE;
        final CommodityType clusterComm = CommodityType.newBuilder()
            .setType(pmProvider ?
                CommodityDTO.CommodityType.CLUSTER_VALUE :
                CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)
            .setKey("cluster")
            .build();

        final TopologyEntity.Builder clusterProvider1 = topologyEntity(1L, providerType);
        clusterProvider1.getEntityBuilder()
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(clusterComm));
        final TopologyEntity.Builder clusterProvider2 = topologyEntity(3L, providerType);
        clusterProvider2.getEntityBuilder()
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(clusterComm));

        final TopologyEntity.Builder outOfClusterProvider = topologyEntity(2L, providerType);

        final TopologyEntity.Builder vm = topologyEntity(5L, EntityType.VIRTUAL_MACHINE);
        vm.getEntityBuilder()
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(clusterProvider1.getOid())
                .setProviderEntityType(providerType.getNumber())
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(clusterComm)))
            .build();

        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(clusterProvider1.getOid(), clusterProvider1);
        topologyMap.put(clusterProvider2.getOid(), clusterProvider2);
        topologyMap.put(outOfClusterProvider.getOid(), outOfClusterProvider);
        topologyMap.put(vm.getOid(), vm);

        TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
        PolicyMatcher policyMatcher = new PolicyMatcher(topologyGraph);

        when(groupResolver.resolve(eq(group), eq(topologyGraph)))
            .thenReturn(Sets.newHashSet(vm.getOid()));


        final PolicyDTO.Policy policyDefinition = pmProvider ? hostPolicyDefinition : storagePolicyDefinition;

        applyPolicy(new MustNotRunTogetherPolicy(policyDefinition,
            new PolicyEntities(group, Collections.emptySet())), topologyGraph);

        // The VM should be buying the segmentation commodity.
        assertThat(topologyGraph.getEntity(vm.getOid()).get(),
            policyMatcher.hasConsumerSegment(policyDefinition.getId(), providerType));

        // Both hosts selling the same kind of cluster commodity should get the segmentation
        // commodity.
        assertThat(topologyGraph.getEntity(clusterProvider1.getOid()).get(),
            policyMatcher.hasProviderSegment(policyDefinition.getId()));
        assertThat(topologyGraph.getEntity(clusterProvider2.getOid()).get(),
            policyMatcher.hasProviderSegment(policyDefinition.getId()));

        // Hosts NOT selling the cluster commodity should NOT get the segmentation commodity.
        assertThat(topologyGraph.getEntity(outOfClusterProvider.getOid()).get(),
            not(policyMatcher.hasProviderSegment(policyDefinition.getId())));
    }
}
