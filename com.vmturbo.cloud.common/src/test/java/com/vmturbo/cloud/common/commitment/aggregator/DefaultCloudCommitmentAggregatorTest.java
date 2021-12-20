package com.vmturbo.cloud.common.commitment.aggregator;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.AggregationFailureException;
import com.vmturbo.cloud.common.commitment.aggregator.DefaultCloudCommitmentAggregator.DefaultCloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetriever;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;

public class DefaultCloudCommitmentAggregatorTest {

    private final IdentityProvider identityProvider = new DefaultIdentityProvider(0);

    private final ComputeTierFamilyResolver computeTierFamilyResolver =
            mock(ComputeTierFamilyResolver.class);

    private final BillingFamilyRetriever billingFamilyRetriever = mock(BillingFamilyRetriever.class);

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology = mock(CloudTopology.class);

    private DefaultCloudCommitmentAggregatorFactory aggregatorFactory;

    private static final GroupAndMembers BILLING_FAMILY = ImmutableGroupAndMembers.builder()
            .group(Grouping.newBuilder().setId(123).build())
            .entities(Collections.emptySet())
            .members(Collections.emptySet())
            .build();

    @Before
    public void setup() {

        final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory =
                mock(ComputeTierFamilyResolverFactory.class);
        when(computeTierFamilyResolverFactory.createResolver(any())).thenReturn(computeTierFamilyResolver);
        final BillingFamilyRetrieverFactory billingFamilyRetrieverFactory =
                mock(BillingFamilyRetrieverFactory.class);
        when(billingFamilyRetrieverFactory.newInstance()).thenReturn(billingFamilyRetriever);

        aggregatorFactory = new DefaultCloudCommitmentAggregatorFactory(
                identityProvider,
                computeTierFamilyResolverFactory,
                billingFamilyRetrieverFactory);

        when(computeTierFamilyResolver.getCoverageFamily(anyLong())).thenReturn(Optional.empty());
        when(billingFamilyRetriever.getBillingFamilyForAccount(anyLong())).thenReturn(Optional.empty());
    }

    /**
     * Verifies two identical RIs are correctly aggregated together when both are shared scope.
     */
    @Test
    public void testSharedBillingFamily() throws AggregationFailureException {

        // Setup the input data
        final ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                .setId(1)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                            .setSizeFlexible(false))
                .build();

        final long purchasingAccountA = 4;
        final ReservedInstanceBought riBoughtA = ReservedInstanceBought.newBuilder()
                .setId(2)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(riSpec.getId())
                        .setBusinessAccountId(purchasingAccountA)
                        .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                                .setShared(true)))
                .build();
        final ReservedInstanceData riDataA = ReservedInstanceData.builder()
                .spec(riSpec)
                .commitment(riBoughtA)
                .build();
        final ReservedInstanceBought riBoughtB = ReservedInstanceBought.newBuilder(riBoughtA)
                .setId(3)
                .build();
        final ReservedInstanceData riDataB = ReservedInstanceData.builder()
                .spec(riSpec)
                .commitment(riBoughtB)
                .build();

        // setup billing family retriever mock
        when(billingFamilyRetriever.getBillingFamilyForAccount(eq(purchasingAccountA)))
                .thenReturn(Optional.of(BILLING_FAMILY));

        // Setup the aggregator and pass in RI data
        final CloudCommitmentAggregator commitmentAggregator = aggregatorFactory.newAggregator(cloudTierTopology);

        commitmentAggregator.collectCommitment(riDataA);
        commitmentAggregator.collectCommitment(riDataB);

        final Set<CloudCommitmentAggregate> aggregateResult = commitmentAggregator.getAggregates();

        // ASSERTIONS
        assertThat(aggregateResult, hasSize(1));
        final CloudCommitmentAggregate commitmentAggregate = Iterables.getOnlyElement(aggregateResult);
        assertThat(commitmentAggregate.commitments(), hasSize(2));
        assertThat(commitmentAggregate.commitments(), containsInAnyOrder(riDataA, riDataB));
    }

    @Test
    public void testSingleScopedAggregation() throws AggregationFailureException {
        final ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                .setId(1)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                                .setSizeFlexible(false))
                .build();

        final long scopedAccount = 5;
        final long purchasingAccountA = 4;
        final ReservedInstanceBought riBoughtA = ReservedInstanceBought.newBuilder()
                .setId(2)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(riSpec.getId())
                        .setBusinessAccountId(purchasingAccountA)
                        .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                                .setShared(false)
                                .addApplicableBusinessAccountId(scopedAccount)))
                .build();

        final ReservedInstanceData riDataA = ReservedInstanceData.builder()
                .spec(riSpec)
                .commitment(riBoughtA)
                .build();

        final long purchasingAccountB = 6;
        final ReservedInstanceBought riBoughtB = ReservedInstanceBought.newBuilder()
                .setId(3)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(riSpec.getId())
                        .setBusinessAccountId(purchasingAccountB)
                        .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                                .setShared(false)
                                .addApplicableBusinessAccountId(scopedAccount)))
                .build();
        final ReservedInstanceData riDataB = ReservedInstanceData.builder()
                .spec(riSpec)
                .commitment(riBoughtB)
                .build();

        // setup billing family retriever mock
        when(billingFamilyRetriever.getBillingFamilyForAccount(eq(purchasingAccountA)))
                .thenReturn(Optional.of(BILLING_FAMILY));
        when(billingFamilyRetriever.getBillingFamilyForAccount(eq(purchasingAccountB)))
                .thenReturn(Optional.of(BILLING_FAMILY));

        // Setup the aggregator and pass in RI data
        final CloudCommitmentAggregator commitmentAggregator = aggregatorFactory.newAggregator(cloudTierTopology);

        commitmentAggregator.collectCommitment(riDataA);
        commitmentAggregator.collectCommitment(riDataB);

        final Set<CloudCommitmentAggregate> aggregateResult = commitmentAggregator.getAggregates();

        // ASSERTIONS
        assertThat(aggregateResult, hasSize(1));
        final CloudCommitmentAggregate commitmentAggregate = Iterables.getOnlyElement(aggregateResult);
        assertThat(commitmentAggregate.commitments(), hasSize(2));
        assertThat(commitmentAggregate.commitments(), containsInAnyOrder(riDataA, riDataB));
    }


    @Test
    public void testSizeFlexibility() throws AggregationFailureException {

        final ReservedInstanceSpec riSpecA = ReservedInstanceSpec.newBuilder()
                .setId(1)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setSizeFlexible(true)
                        .setTierId(123))
                .build();

        final ReservedInstanceBought riBoughtA = ReservedInstanceBought.newBuilder()
                .setId(2)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(riSpecA.getId())
                        .setBusinessAccountId(5))
                .build();

        final ReservedInstanceData riDataA = ReservedInstanceData.builder()
                .spec(riSpecA)
                .commitment(riBoughtA)
                .build();

        final ReservedInstanceSpec riSpecB = ReservedInstanceSpec.newBuilder()
                .setId(3)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setSizeFlexible(true)
                        .setTierId(456))
                .build();

        final ReservedInstanceBought riBoughtB = ReservedInstanceBought.newBuilder()
                .setId(4)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(riSpecB.getId())
                        .setBusinessAccountId(5))
                .build();
        final ReservedInstanceData riDataB = ReservedInstanceData.builder()
                .spec(riSpecB)
                .commitment(riBoughtB)
                .build();

        // setup compute tier family resolver for tiers 123 & 456
        final String family = "A";
        when(computeTierFamilyResolver.getCoverageFamily(eq(123L)))
                .thenReturn(Optional.of(family));
        when(computeTierFamilyResolver.getCoverageFamily(eq(456L)))
                .thenReturn(Optional.of(family));

        // Setup the aggregator and pass in RI data
        final CloudCommitmentAggregator commitmentAggregator = aggregatorFactory.newAggregator(cloudTierTopology);

        commitmentAggregator.collectCommitment(riDataA);
        commitmentAggregator.collectCommitment(riDataB);

        final Set<CloudCommitmentAggregate> aggregateResult = commitmentAggregator.getAggregates();

        // ASSERTIONS
        assertThat(aggregateResult, hasSize(1));
        final CloudCommitmentAggregate commitmentAggregate = Iterables.getOnlyElement(aggregateResult);
        assertThat(commitmentAggregate.commitments(), hasSize(2));
        assertThat(commitmentAggregate.commitments(), containsInAnyOrder(riDataA, riDataB));
    }
}
