package com.vmturbo.cloud.common.commitment.aggregator;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology;
import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology.CloudCommitmentTopologyFactory;
import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.commitment.TopologyCommitmentData;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.AggregationFailureException;
import com.vmturbo.cloud.common.commitment.aggregator.DefaultCloudCommitmentAggregator.DefaultCloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetriever;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocationType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentStatus;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class DefaultCloudCommitmentAggregatorTest {

    private final IdentityProvider identityProvider = new DefaultIdentityProvider(0);

    private final ComputeTierFamilyResolver computeTierFamilyResolver =
            mock(ComputeTierFamilyResolver.class);

    private final BillingFamilyRetriever billingFamilyRetriever = mock(BillingFamilyRetriever.class);

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology = mock(CloudTopology.class);

    private final CloudCommitmentTopologyFactory<TopologyEntityDTO> commitmentTopologyFactory = mock(CloudCommitmentTopologyFactory.class);

    private final CloudCommitmentTopology cloudCommitmentTopology = mock(CloudCommitmentTopology.class);

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
                billingFamilyRetrieverFactory,
                commitmentTopologyFactory);

        when(computeTierFamilyResolver.getCoverageFamily(anyLong())).thenReturn(Optional.empty());
        when(billingFamilyRetriever.getBillingFamilyForAccount(anyLong())).thenReturn(Optional.empty());

        when(cloudTierTopology.getServiceProvider(anyLong())).thenReturn(Optional.of(
                TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.SERVICE_PROVIDER_VALUE)
                        .setOid(123L)
                        .build()));

        when(commitmentTopologyFactory.createTopology(any())).thenReturn(cloudCommitmentTopology);
    }

    /**
     * Verifies two identical RIs are correctly aggregated together when both are shared scope.
     */
    @Test
    public void testSharedBillingFamilyRI() throws AggregationFailureException {

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
    public void testSingleScopedRIAggregation() throws AggregationFailureException {
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
    public void testSizeFlexibleRI() throws AggregationFailureException {

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

    /**
     * Test a topology cloud commitment with a regional connection, coupon capacity, and account scope.
     * @throws AggregationFailureException Unexpected exception during aggregation.
     */
    @Test
    public void testRegionalAccountCouponTopologyCommitment() throws AggregationFailureException {

        // set up region + accounts
        final TopologyEntityDTO region = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(1)
                .build();

        final TopologyEntityDTO purchasingAccount = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(2)
                .build();

        final TopologyEntityDTO serviceProvider = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.SERVICE_PROVIDER_VALUE)
                .setOid(6)
                .build();

        final long scopedAccountOid = 3;

        final long scopedCloudServiceOid = 4;

        // set up cloud commitment
        final String instanceFamily = "instanceFamilyTest";
        final TopologyEntityDTO commitment = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CLOUD_COMMITMENT_VALUE)
                .setOid(5)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setCloudCommitmentData(CloudCommitmentInfo.newBuilder()
                                .setCommitmentScope(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                                .setCommitmentStatus(CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_EXPIRED)
                                .setNumberCoupons(5)
                                .setFamilyRestricted(FamilyRestricted.newBuilder()
                                        .setInstanceFamily(instanceFamily)
                                        .build())
                                .build())
                        .build())
                .build();
        final CloudCommitmentData commitmentData = TopologyCommitmentData.builder()
                .commitment(commitment)
                .build();

        // set up cloud topology
        when(cloudTierTopology.getOwner(anyLong())).thenReturn(Optional.of(purchasingAccount));
        when(cloudTierTopology.getConnectedAvailabilityZone(anyLong())).thenReturn(Optional.empty());
        when(cloudTierTopology.getConnectedRegion(anyLong())).thenReturn(Optional.of(region));
        when(cloudTierTopology.getServiceProvider(anyLong())).thenReturn(Optional.of(serviceProvider));

        // set up coverage topology
        when(cloudCommitmentTopology.getCoveredAccounts(anyLong())).thenReturn(ImmutableSet.of(scopedAccountOid));
        when(cloudCommitmentTopology.getCoveredCloudServices(anyLong())).thenReturn(ImmutableSet.of(scopedCloudServiceOid));

        // invoke the aggregator
        final CloudCommitmentAggregator commitmentAggregator = aggregatorFactory.newAggregator(cloudTierTopology);
        commitmentAggregator.collectCommitment(commitmentData);

        final CloudCommitmentAggregate actualAggregate = Iterables.getOnlyElement(commitmentAggregator.getAggregates());
        final AggregationInfo aggregationInfo = actualAggregate.aggregationInfo();


        // ASSERTIONS

        assertThat(aggregationInfo.commitmentType(), equalTo(CloudCommitmentType.TOPOLOGY_COMMITMENT));
        assertThat(aggregationInfo.serviceProviderOid(), equalTo(serviceProvider.getOid()));
        assertThat(aggregationInfo.purchasingAccountOid(), equalTo(OptionalLong.of(purchasingAccount.getOid())));
        assertThat(aggregationInfo.coverageType(), equalTo(CloudCommitmentCoverageType.COUPONS));

        // Check entity scope
        assertThat(aggregationInfo.entityScope().getScopeType(), equalTo(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT));
        assertTrue(aggregationInfo.entityScope().hasEntityScope());
        assertThat(aggregationInfo.entityScope().getEntityScope().getEntityOidList(), containsInAnyOrder(scopedAccountOid));

        // Check location scope
        assertThat(aggregationInfo.location().getLocationType(), equalTo(CloudCommitmentLocationType.REGION));
        assertThat(aggregationInfo.location().getLocationOid(), equalTo(region.getOid()));

        assertThat(aggregationInfo.status(), equalTo(CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_EXPIRED));

    }
}
