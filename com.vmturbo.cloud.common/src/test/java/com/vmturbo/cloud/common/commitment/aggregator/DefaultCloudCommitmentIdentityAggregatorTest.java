package com.vmturbo.cloud.common.commitment.aggregator;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.AggregationFailureException;
import com.vmturbo.cloud.common.commitment.aggregator.DefaultCloudCommitmentAggregator.DefaultCloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetriever;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;

public class DefaultCloudCommitmentIdentityAggregatorTest {

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private final ComputeTierFamilyResolver computeTierFamilyResolver =
            mock(ComputeTierFamilyResolver.class);

    private final BillingFamilyRetriever billingFamilyRetriever = mock(BillingFamilyRetriever.class);

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology = mock(CloudTopology.class);

    private DefaultCloudCommitmentAggregatorFactory aggregatorFactory;

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
    }

    @Test
    public void testRIAggregation() throws AggregationFailureException {

        // Setup the input data
        final ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                .setId(1)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder())
                .build();

        final ReservedInstanceBought riBoughtA = ReservedInstanceBought.newBuilder()
                .setId(2)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(riSpec.getId()))
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

        // setup the mocks
        when(billingFamilyRetriever.getBillingFamilyForAccount(anyLong())).thenReturn(Optional.empty());
        when(computeTierFamilyResolver.getCoverageFamily(anyLong())).thenReturn(Optional.empty());

        final CloudCommitmentAggregator commitmentAggregator = aggregatorFactory.newIdentityAggregator(cloudTierTopology);

        commitmentAggregator.collectCommitment(riDataA);
        commitmentAggregator.collectCommitment(riDataB);

        final Set<CloudCommitmentAggregate> aggregateResult = commitmentAggregator.getAggregates();

        assertThat(aggregateResult, hasSize(2));
    }
}
