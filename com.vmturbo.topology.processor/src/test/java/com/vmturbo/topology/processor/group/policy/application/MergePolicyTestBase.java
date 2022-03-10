package com.vmturbo.topology.processor.group.policy.application;

import static com.google.common.collect.Lists.newArrayList;
import static com.vmturbo.topology.processor.group.policy.PolicyGroupingHelper.resolvedGroup;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.map.HashedMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyMatcher;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;
import com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory;

/**
 * Base test class for merge policy.
 */
public class MergePolicyTestBase {
    protected static final long POLICY_ID = 9999L;
    private static final long NEW_POLICY_ID = 10000L;
    protected static final long CONSUMER_ID = 1234L;
    protected static final long PROVIDER_ID = 5678L;
    protected final List<Long> mergeGropuIds = newArrayList(CONSUMER_ID, PROVIDER_ID);
    protected final GroupResolver groupResolver = mock(GroupResolver.class);
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    protected Grouping group1;
    protected Grouping group2;
    protected List<PolicyEntities> mergePolicyEntities;
    protected TopologyGraph<TopologyEntity> topologyGraph;
    protected PolicyMatcher policyMatcher;
    protected PolicyDTO.PolicyInfo.MergePolicy mergePolicy;

    @Test
    public void testMergeClusterPolicy() throws GroupResolutionException, PolicyApplicationException {
        // assign merge policy to Policy
        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                .setId(POLICY_ID)
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

        // ensure PMs or Storage changed the key of the cluster commodity to the policy OID.
        assertThat(topologyGraph.getEntity(1L).get(),
                policyMatcher.hasCommoditySoldClusterType(mergePolicy, null));
        assertThat(topologyGraph.getEntity(2L).get(),
                policyMatcher.hasCommoditySoldClusterType(mergePolicy, null));

        // ensure storage is not affected when merge policy is computer cluster
        // or PM is not affected when merge policy is storage cluster
        assertThat(topologyGraph.getEntity(3L).get(),
                not(policyMatcher.hasCommoditySoldClusterType(mergePolicy, null)));

        // ensure VMs changed the key of the cluster commodity to the policy OID.
        assertThat(topologyGraph.getEntity(4L).get(),
                policyMatcher.hasCommodityBoughtClusterType(mergePolicy));
        assertThat(topologyGraph.getEntity(5L).get(),
                policyMatcher.hasCommodityBoughtClusterType(mergePolicy));

        // ensure only VMs attached to PMs will have merge policy ID set
        assertThat(topologyGraph.getEntity(6L).get(),
                not(policyMatcher.hasCommodityBoughtClusterType(mergePolicy)));

        assertThat(topologyGraph.getEntity(100L).get(),
                policyMatcher.hasCommoditySoldClusterType(mergePolicy, null));

        final double value = 99.0;
        updateTopologyGraph(value, mergePolicy); // add two new DTOs to commodity sold list
        applyPolicy(new MergePolicy(policy, mergePolicyEntities));

        // ensure PMs or Storage changed the key of the cluster commodity to the policy OID.
        assertThat(topologyGraph.getEntity(1L).get(),
                policyMatcher.hasCommoditySoldClusterType(mergePolicy, value));

        // ensure existing DTO, DATASTORE in the test, will still available after applying merge policy.
        assertThat(topologyGraph.getEntity(1L).get(),
                policyMatcher.hasCommoditySoldType(CommodityType.DATASTORE.getNumber()));

        // ensure VMs changed the key of the cluster commodity to the policy OID.
        assertThat(topologyGraph.getEntity(4L).get(),
                policyMatcher.hasCommodityBoughtClusterType(mergePolicy));

        // ensure we have only one bought provider
        Assert.assertEquals(1, topologyGraph
                .getEntity(4L)
                .get()
                .getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList()
                .size());

        // ensure we have only one bought commodity
        Assert.assertEquals(1, topologyGraph
                .getEntity(4L)
                .get()
                .getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList()
                .get(0) //we know there should have one provider
                .getCommodityBoughtList()
                .size());

        // ensure VMs changed the key of the cluster commodity to the policy OID.
        assertThat(topologyGraph.getEntity(5L).get(),
                policyMatcher.hasCommodityBoughtClusterType(mergePolicy));

        // ensure VMs don't changed the key of the cluster commodity to the policy OID if it's not attached to PM or Storage
        assertThat(topologyGraph.getEntity(5L).get(),
                policyMatcher.hasCommodityBoughtClusterType(NEW_POLICY_ID, CommodityType.DATASTORE.getNumber()));

        // ensure we have two bought provider, one is in the merge providers and one is not
        Assert.assertEquals(2, topologyGraph
                .getEntity(5L)
                .get()
                .getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList()
                .size());

        // ensure we have two bought commodity
        Assert.assertEquals(2, topologyGraph
                .getEntity(5L)
                .get()
                .getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList()
                .get(0) //we know there should have one provider
                .getCommodityBoughtList()
                .size());

        // make sure we don't merge the providers which are not in the merge cluster
        Assert.assertTrue(topologyGraph
                .getEntity(5L)
                .get()
                .getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersList()
                .get(1) //second one with provider NOT in the merge cluster
                .getCommodityBoughtList()
                .stream()
                .allMatch(commodityBoughtDTO ->
                        Long.parseLong(commodityBoughtDTO.getCommodityType().getKey()) != POLICY_ID));
    }

    private void updateTopologyGraph(double value, @Nonnull final MergePolicy policy) {
        updatePmOrStoragePropertiesInTopologyGraph(value, policy);
        updateVMPropertiesInTopologyGraph(policy);
    }

    private void updatePmOrStoragePropertiesInTopologyGraph(final double value, @Nonnull final MergePolicy policy) {
        // set the property of the Cluster DTO in entity's CommoditySoldList to ensure merge policy
        // keep it (instead of creating a new one)
        CommoditySoldView newCommoditySoldView = getCommoditySoldView(value, PolicyMatcher.getCommodityType(policy));
        // add DATASTORE DTO to commodity sold list
        CommoditySoldView newCommoditySoldView1 = getCommoditySoldView(value, CommodityType.DATASTORE.getNumber());

        topologyGraph.getEntity(1L)
                .get()
                .getTopologyEntityImpl()
                .clearCommoditySoldList()
                .addAllCommoditySoldList(newArrayList(newCommoditySoldView, newCommoditySoldView1));
    }

    /**
     * VM (4) replaced with one provider which has one boughtList which has one cluster DTO
     * VM (5) replaced with two providers, first provider has same boughtList as VM (4)
     * second provide, which is NOT in the merge host list, has boughtList with two DTO (cluster and datastore).
     */
    private void updateVMPropertiesInTopologyGraph(@Nonnull final MergePolicy policy) {
        List<CommoditiesBoughtFromProviderView> newCommodityBoughtFromProviderList = newArrayList();
        List<CommoditiesBoughtFromProviderView> newCommodityBoughtFromProviderList1 = newArrayList();

        CommodityBoughtView newClusterDTO = getCommodityBoughtDTO(PolicyMatcher.getCommodityType(policy));
        CommodityBoughtView newClusterDTO1 = getCommodityBoughtDTO(CommodityType.DATASTORE.getNumber());

        List<CommodityBoughtView> newCommodityBoughtList = newArrayList();
        List<CommodityBoughtView> newCommodityBoughtList1 = newArrayList();
        newCommodityBoughtList.add(newClusterDTO);
        newCommodityBoughtList1.add(newClusterDTO);
        newCommodityBoughtList1.add(newClusterDTO1);

        CommoditiesBoughtFromProviderView newCommodityBoughtFromProvider = new CommoditiesBoughtFromProviderImpl()
                .setProviderId(1L)
                .addAllCommodityBought(newCommodityBoughtList);
        newCommodityBoughtFromProviderList.add(newCommodityBoughtFromProvider);
        CommoditiesBoughtFromProviderView newCommodityBoughtFromProvider1 = new CommoditiesBoughtFromProviderImpl()
                .setProviderId(2L)
                .addAllCommodityBought(newCommodityBoughtList1);
        // adding a provider which is not in the merge host list
        CommoditiesBoughtFromProviderView newCommodityBoughtFromProvider2 = new CommoditiesBoughtFromProviderImpl()
                .setProviderId(10L)
                .addAllCommodityBought(newCommodityBoughtList1);
        newCommodityBoughtFromProviderList1.add(newCommodityBoughtFromProvider1);
        newCommodityBoughtFromProviderList1.add(newCommodityBoughtFromProvider2);

        topologyGraph.getEntity(4L)
                .get()
                .getTopologyEntityImpl()
                .clearCommoditiesBoughtFromProviders()
                .addAllCommoditiesBoughtFromProviders(newCommodityBoughtFromProviderList);

        topologyGraph.getEntity(5L)
                .get()
                .getTopologyEntityImpl()
                .clearCommoditiesBoughtFromProviders()
                .addAllCommoditiesBoughtFromProviders(newCommodityBoughtFromProviderList1);
    }

    private CommodityBoughtView getCommodityBoughtDTO(final int type) {
        CommodityTypeView newClusterType = new CommodityTypeImpl()
                .setKey(Long.toString(NEW_POLICY_ID)) // use new Policy ID
                .setType(type);

        // create cluster commodity.
        return new CommodityBoughtImpl()
                .setCommodityType(newClusterType);
    }

    private CommoditySoldView getCommoditySoldView(final double value, final int type) {
        CommodityTypeView soldCommodity = new CommodityTypeImpl()
                .setKey(Long.toString(NEW_POLICY_ID))
                .setType(type);

        return new CommoditySoldImpl()
                .setCommodityType(soldCommodity)
                .setCapacity(value); // add extra property
    }

    @Test
    public void testPolicyIsNotMergePolicy() throws GroupResolutionException, PolicyApplicationException {
        final PolicyDTO.PolicyInfo.BindToGroupPolicy bindToGroup =
                PolicyDTO.PolicyInfo.BindToGroupPolicy.newBuilder()
                    .setConsumerGroupId(CONSUMER_ID)
                    .setProviderGroupId(PROVIDER_ID)
                .build();

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setBindToGroup(bindToGroup)) // set BindToGroup policy (not merge policy).
                .build();

        // invoke Merge Policy should failed with IllegalArgumentException.
        expectedException
                .expect(IllegalArgumentException.class);
        applyPolicy(new MergePolicy(policy, mergePolicyEntities));
    }

    @Test
    public void testApplyEmpty() throws GroupResolutionException, PolicyApplicationException {
        // assign merge policy to Policy
        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setMerge(mergePolicy))
                .build();

        when(groupResolver.resolve(eq(group1), eq(topologyGraph)))
                .thenReturn(resolvedGroup(group1));
        when(groupResolver.resolve(eq(group2), eq(topologyGraph)))
                .thenReturn(resolvedGroup(group2));

        // invoke Merge Policy
        MergePolicy mergePolicy = new MergePolicy(policy, mergePolicyEntities);
        applyPolicy(mergePolicy);

        assertThat(topologyGraph.getEntity(1L).get(),
                not(policyMatcher.hasCommoditySoldClusterType(mergePolicy, null)));
        assertThat(topologyGraph.getEntity(2L).get(),
                not(policyMatcher.hasCommoditySoldClusterType(mergePolicy, null)));
        assertThat(topologyGraph.getEntity(4L).get(),
                not(policyMatcher.hasCommodityBoughtClusterType(mergePolicy)));
        assertThat(topologyGraph.getEntity(5L).get(),
                not(policyMatcher.hasCommodityBoughtClusterType(mergePolicy)));
    }


    @Test
    public void testApplyEmptyGroup1() throws GroupResolutionException, PolicyApplicationException {

        // assign merge policy to Policy
        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setMerge(mergePolicy))
                .build();
        when(groupResolver.resolve(eq(group1), eq(topologyGraph)))
                .thenReturn(resolvedGroup(group1));
        when(groupResolver.resolve(eq(group2), eq(topologyGraph)))
                .thenReturn(resolvedGroup(group2, 4L, 5L));

        MergePolicy mergePolicy = new MergePolicy(policy, mergePolicyEntities);
        applyPolicy(mergePolicy);

        assertThat(topologyGraph.getEntity(1L).get(),
                not(policyMatcher.hasCommoditySoldClusterType(mergePolicy, null)));
        assertThat(topologyGraph.getEntity(2L).get(),
                not(policyMatcher.hasCommoditySoldClusterType(mergePolicy, null)));

        // Since PM (or Storage) is not avaialbe, there will be no attached VMs.
        assertThat(topologyGraph.getEntity(4L).get(),
                not(policyMatcher.hasCommodityBoughtClusterType(mergePolicy)));
        assertThat(topologyGraph.getEntity(5L).get(),
                not(policyMatcher.hasCommodityBoughtClusterType(mergePolicy)));
    }

    @Test
    public void testApplyEmptyGroup2() throws GroupResolutionException, PolicyApplicationException {

        // assign merge policy to Policy
        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setMerge(mergePolicy))
                .build();
        when(groupResolver.resolve(eq(group1), eq(topologyGraph)))
                .thenReturn(resolvedGroup(group1, 1L, 2L));
        when(groupResolver.resolve(eq(group2), eq(topologyGraph)))
                .thenReturn(resolvedGroup(group2));

        MergePolicy mergePolicy = new MergePolicy(policy, mergePolicyEntities);
        applyPolicy(mergePolicy);


        assertThat(topologyGraph.getEntity(1L).get(),
                policyMatcher.hasCommoditySoldClusterType(mergePolicy, null));
        assertThat(topologyGraph.getEntity(2L).get(),
                policyMatcher.hasCommoditySoldClusterType(mergePolicy, null));

        // Since VM are searched from PMs (or Storage), VMs will still have the cluster key with policy OID.
        assertThat(topologyGraph.getEntity(4L).get(),
                policyMatcher.hasCommodityBoughtClusterType(mergePolicy));
        assertThat(topologyGraph.getEntity(5L).get(),
                policyMatcher.hasCommodityBoughtClusterType(mergePolicy));
    }

    // ensure empty topology graph doesn't throw exception.
    @Test
    public void testEmptyTopologyGraph() throws GroupResolutionException, PolicyApplicationException {
        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(new HashedMap<>());
        try {
            // assign merge policy to Policy
            final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                    .setId(POLICY_ID)
                    .setPolicyInfo(PolicyInfo.newBuilder()
                        .setMerge(mergePolicy))
                    .build();

            // setup mocks
            when(groupResolver.resolve(eq(group1), eq(topologyGraph)))
                    .thenReturn(resolvedGroup(group1, 4L, 5L));
            when(groupResolver.resolve(eq(group2), eq(topologyGraph)))
                    .thenReturn(resolvedGroup(group2, 1L, 2L));

            // invoke Merge Policy
            applyPolicy(new MergePolicy(policy, mergePolicyEntities));
        } catch (Exception e) {
            fail("Should not have thrown any exception");
        }

    }

    protected void applyPolicy(@Nonnull final MergePolicy mergePolicy) {
        final MergePolicyApplication application = new MergePolicyApplication(groupResolver, topologyGraph, new TopologyInvertedIndexFactory());
        application.apply(Collections.singletonList(mergePolicy));
    }
}
